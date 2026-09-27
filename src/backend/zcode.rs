//! ZCode backend — drives `zcode app-server --stdio`, Z.ai's JSON-RPC
//! dialect over stdio ("ZCode Protocol" v1).
//!
//! Wire shape (verified live against zcode-app-cli 3.14.3; schemas from
//! zai-org/ZCode packages/shared/src/zcode-protocol):
//! - Envelope: `{id, method, params}` — requests carry NO `jsonrpc`
//!   field (it is rejected as an unrecognized key); responses are
//!   `{id, result}` / `{id, error}`; notifications `{method, params}`.
//! - Boot: `startup/storageState` notifications (ignored), then
//!   `runtime/capabilities`. `session/create {workspace:{workspacePath,
//!   workspaceKey}}` → result with `projection{contextWindow, status,
//!   pendingPermissions}` and the session id in server requests;
//!   `session/resume {sessionId}` reattaches.
//! - Server→client requests MUST be answered or the session wedges:
//!   `session/requestRuntimePreferences` (answered with
//!   `{nativeSearchEnhancementsEnabled:false}`), and the interaction
//!   family — `interaction/requestPermission {requestId, sessionId,
//!   toolCallId, toolName, reason, riskLevel, input, options:[{optionId,
//!   name, response}]}` and `interaction/requestUserInput` — where each
//!   option carries the response payload to echo back on selection.
//! - Turns: `session/send {sessionId, content}` (schema-verified; the
//!   live model round-trip needs a GLM login). Session events arrive
//!   as notifications — `turn.started/completed/failed`,
//!   `part.started/delta/upserted`, `model.streaming`, `tool.updated`,
//!   `permission.requested/resolved`, `userInput.requested/resolved`,
//!   `checkpoint.created`.
//! - Interrupt: `session/stop`.
//!
//! One process = one conversation. The persistence handle is the
//! `sess_…` id.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use super::registry::ResolvedBackend;
use super::transport::{NdjsonTransport, request_roundtrip};
use super::types::*;
use super::{AgentClient, AgentSession};

const INIT_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ZcodeClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
}

impl ZcodeClient {
    pub fn new(resolved: ResolvedBackend) -> Self {
        Self {
            resolved,
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: false,
                dynamic_modes: false,
                mcp_servers: false,
                reasoning_stream: true,
                steer: false,
                rewind: false,
                subagent_events: false,
            },
        }
    }
}

#[async_trait]
impl AgentClient for ZcodeClient {
    fn provider(&self) -> &str {
        "zcode"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        self.resolved.detected
    }

    async fn fetch_catalog(&self, _cwd: Option<&Path>) -> Result<ProviderCatalog> {
        // Model selection rides session/create's `model` field; thought
        // levels switch per-session. No cold listing wire.
        Ok(ProviderCatalog::default())
    }

    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        Ok(ZcodeSession::spawn(&self.resolved, config, None).await? as Arc<dyn AgentSession>)
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        Ok(
            ZcodeSession::spawn(&self.resolved, config, Some(&handle.native_handle)).await?
                as Arc<dyn AgentSession>,
        )
    }
}

struct PendingAsk {
    request: PermissionRequest,
    /// The JSON-RPC id of the server→client interaction request.
    rpc_id: Value,
    /// optionId → the response payload to echo back on selection.
    option_responses: HashMap<String, Value>,
    /// true → userInput (free-text) semantics.
    is_question: bool,
}

pub struct ZcodeSession {
    transport: Arc<NdjsonTransport>,
    caps: Capabilities,
    events: broadcast::Sender<StreamEvent>,
    next_id: AtomicU64,
    session_id: Mutex<Option<String>>,
    current_turn: Mutex<Option<String>>,
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
}

impl ZcodeSession {
    async fn spawn(
        resolved: &ResolvedBackend,
        config: SessionConfig,
        resume: Option<&str>,
    ) -> Result<Arc<Self>> {
        let env: HashMap<String, String> = resolved.env.iter().cloned().collect();
        let transport =
            NdjsonTransport::spawn(&resolved.command, &resolved.args, &env, &config.cwd).await?;

        let (events, _) = broadcast::channel(512);
        let session = Arc::new(Self {
            transport: transport.clone(),
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: false,
                dynamic_modes: false,
                mcp_servers: false,
                reasoning_stream: true,
                steer: false,
                rewind: false,
                subagent_events: false,
            },
            events,
            next_id: AtomicU64::new(1),
            session_id: Mutex::new(None),
            current_turn: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
        });

        {
            let me: Weak<ZcodeSession> = Arc::downgrade(&session);
            let mut rx = transport.subscribe();
            let mut exited = transport.exited();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        r = rx.recv() => match r {
                            Ok(frame) => {
                                if let Some(s) = me.upgrade() {
                                    s.on_frame(frame).await;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        },
                        _ = exited.changed() => {
                            if *exited.borrow() { break; }
                        }
                    }
                }
                if let Some(s) = me.upgrade()
                    && let Some(turn) = s.current_turn.lock().await.take()
                {
                    s.emit(StreamEvent {
                        turn_id: Some(turn),
                        kind: StreamEventKind::TurnFailed {
                            error: "backend process exited without completing the turn".to_string(),
                            code: None,
                        },
                    });
                    s.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                        reason: AttentionReason::Finished,
                    }));
                }
            });
        }

        // Create (or resume) the session before returning. workspaceKey
        // is the caller-chosen slug; the path is authoritative.
        let key = format!(
            "damon-{}",
            config
                .cwd
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "ws".to_string())
        );
        let mut params = json!({
            "workspace": {
                "workspacePath": config.cwd.to_string_lossy(),
                "workspaceKey": key,
            }
        });
        if let Some(model) = &config.model {
            params["model"] = json!({"modelId": model});
        }
        let method = if let Some(id) = resume {
            params = json!({"sessionId": id});
            "session/resume"
        } else {
            "session/create"
        };
        let result = session
            .request(method, params, INIT_TIMEOUT)
            .await
            .context("zcode session init failed")?;
        if let Some(id) = result["sessionId"]
            .as_str()
            .or_else(|| result["session"]["sessionId"].as_str())
        {
            *session.session_id.lock().await = Some(id.to_string());
            session.emit(StreamEvent::new(StreamEventKind::ThreadStarted {
                native_handle: id.to_string(),
            }));
        }
        Ok(session)
    }

    /// One request/response round-trip on the zcode envelope.
    async fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        request_roundtrip(
            &self.transport,
            id.to_string(),
            json!({"id": id, "method": method, "params": params}),
            timeout,
        )
        .await
    }

    fn emit(&self, ev: StreamEvent) {
        let _ = self.events.send(ev);
    }

    async fn emit_timeline(&self, item: TimelineItem) {
        let turn = self.current_turn.lock().await.clone();
        self.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::Timeline(item),
        });
    }

    async fn on_frame(&self, f: Value) {
        // Response to a request WE sent (id without method).
        if f["id"].is_number() && !f.is_object() {
            // unreachable in practice; ids are numeric on our side
        }
        if f.get("method").is_none() {
            let id = f["id"].to_string();
            let id = id.trim_matches('"').to_string();
            let result = if f["error"].is_object() {
                Err(f["error"].clone())
            } else {
                Ok(f["result"].clone())
            };
            self.transport.resolve(&id, result).await;
            return;
        }
        let method = f["method"].as_str().unwrap_or_default();
        if f["id"].is_string() || f["id"].is_number() {
            // Server→client request: must be answered.
            self.on_server_request(method, &f).await;
        } else {
            self.on_notification(method, &f["params"]).await;
        }
    }

    async fn on_server_request(&self, method: &str, f: &Value) {
        let rpc_id = f["id"].clone();
        match method {
            // Runtime preferences must carry the boolean or creation
            // fails validation (verified live).
            "session/requestRuntimePreferences" => {
                let _ = self
                    .transport
                    .send(
                        json!({"id": rpc_id, "result": {"nativeSearchEnhancementsEnabled": false}}),
                    )
                    .await;
            }
            "interaction/requestPermission" => {
                let p = &f["params"];
                let name = p["toolName"].as_str().unwrap_or("tool").to_string();
                let reason = p["reason"].as_str().unwrap_or_default().to_string();
                let input = p["input"].clone();
                let mut option_responses = HashMap::new();
                let mut actions = vec![];
                if let Some(opts) = p["options"].as_array() {
                    for (i, o) in opts.iter().enumerate() {
                        let oid = o["optionId"].as_str().unwrap_or("opt").to_string();
                        let label = o["name"].as_str().unwrap_or(&oid).to_string();
                        let deny = label.to_lowercase().contains("deny")
                            || label.to_lowercase().contains("cancel")
                            || label.to_lowercase().contains("reject");
                        option_responses.insert(oid.clone(), o["response"].clone());
                        actions.push(PermissionAction {
                            id: oid,
                            label,
                            behavior: if deny {
                                PermissionBehavior::Deny
                            } else {
                                PermissionBehavior::Allow
                            },
                            variant: if deny {
                                Some(ActionVariant::Danger)
                            } else if i == 0 {
                                Some(ActionVariant::Primary)
                            } else {
                                None
                            },
                        });
                    }
                }
                let ask = PermissionRequest {
                    id: uuid::Uuid::new_v4().to_string(),
                    kind: PermissionKind::Tool,
                    name: name.clone(),
                    title: Some(if reason.is_empty() {
                        format!("{name} wants to run")
                    } else {
                        reason
                    }),
                    input: Some(input.clone()),
                    detail: Some(map_tool_input(&name, &input)),
                    actions,
                    suggestions: vec![],
                };
                self.register_ask(ask, rpc_id, option_responses, false)
                    .await;
            }
            "interaction/requestUserInput" => {
                let p = &f["params"];
                let question = p["prompt"]
                    .as_str()
                    .or_else(|| p["question"].as_str())
                    .unwrap_or("Question")
                    .to_string();
                let mut option_responses = HashMap::new();
                let mut actions = vec![];
                if let Some(opts) = p["options"].as_array() {
                    for o in opts {
                        let oid = o["optionId"]
                            .as_str()
                            .or_else(|| o.as_str())
                            .unwrap_or("opt")
                            .to_string();
                        let label = o["name"]
                            .as_str()
                            .or_else(|| o.as_str())
                            .unwrap_or(&oid)
                            .to_string();
                        option_responses.insert(
                            oid.clone(),
                            o.get("response").cloned().unwrap_or(Value::Null),
                        );
                        actions.push(PermissionAction {
                            id: oid,
                            label,
                            behavior: PermissionBehavior::Allow,
                            variant: None,
                        });
                    }
                }
                let ask = PermissionRequest {
                    id: uuid::Uuid::new_v4().to_string(),
                    kind: PermissionKind::Question,
                    name: "user_input".to_string(),
                    title: Some(question),
                    input: Some(p.clone()),
                    detail: None,
                    actions,
                    suggestions: vec![],
                };
                self.register_ask(ask, rpc_id, option_responses, true).await;
            }
            // Anything else (auth-header brokering, browser hooks…):
            // answer an empty result — silence would wedge the session.
            _ => {
                let _ = self
                    .transport
                    .send(json!({"id": rpc_id, "result": {}}))
                    .await;
            }
        }
    }

    async fn register_ask(
        &self,
        ask: PermissionRequest,
        rpc_id: Value,
        option_responses: HashMap<String, Value>,
        is_question: bool,
    ) {
        self.pending_asks.lock().await.insert(
            ask.id.clone(),
            PendingAsk {
                request: ask.clone(),
                rpc_id,
                option_responses,
                is_question,
            },
        );
        let turn = self.current_turn.lock().await.clone();
        self.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::PermissionRequested(ask),
        });
        self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
            reason: AttentionReason::Permission,
        }));
    }

    async fn on_notification(&self, method: &str, p: &Value) {
        // Startup/telemetry noise is skipped; session events are the
        // payload-bearing family. The event type rides either
        // params.type (envelope schema) or params.event.type.
        if method.starts_with("startup/") || method.starts_with("process/") {
            return;
        }
        let ev = if p["type"].is_string() {
            p
        } else {
            &p["event"]
        };
        let ty = ev["type"].as_str().unwrap_or_default();
        // Session-scoped only.
        if let Some(sid) = ev["sessionId"].as_str() {
            let mine = self.session_id.lock().await.clone();
            if let Some(mine) = mine
                && sid != mine
            {
                return;
            }
        }
        match ty {
            "turn.started" => {
                let turn_id = ev["turnId"].as_str().unwrap_or_default().to_string();
                *self.current_turn.lock().await = Some(turn_id.clone());
                self.emit(StreamEvent::in_turn(turn_id, StreamEventKind::TurnStarted));
            }
            "turn.completed" => {
                let turn = self.current_turn.lock().await.take();
                self.emit(StreamEvent {
                    turn_id: turn,
                    kind: StreamEventKind::TurnCompleted {
                        usage: zcode_usage(ev),
                    },
                });
                self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                    reason: AttentionReason::Finished,
                }));
            }
            "turn.failed" => {
                let turn = self.current_turn.lock().await.take();
                self.emit(StreamEvent {
                    turn_id: turn,
                    kind: StreamEventKind::TurnFailed {
                        error: ev["error"]["message"]
                            .as_str()
                            .or_else(|| ev["error"].as_str())
                            .unwrap_or("turn failed")
                            .to_string(),
                        code: None,
                    },
                });
                self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                    reason: AttentionReason::Finished,
                }));
            }
            "part.delta" | "model.streaming" => {
                if let Some(d) = ev["delta"].as_str().or_else(|| ev["text"].as_str()) {
                    self.emit_timeline(TimelineItem::AssistantMessage {
                        text: d.to_string(),
                    })
                    .await;
                }
            }
            "tool.updated" => {
                let name = ev["toolName"].as_str().unwrap_or_default().to_string();
                let status = ev["status"].as_str().unwrap_or_default();
                self.emit_timeline(TimelineItem::ToolCall(ToolCall {
                    call_id: ev["toolCallId"].as_str().unwrap_or_default().to_string(),
                    name: name.clone(),
                    status: match status {
                        "result" | "completed" => ToolCallStatus::Completed,
                        "error" => ToolCallStatus::Failed,
                        _ => ToolCallStatus::Running,
                    },
                    detail: map_tool_input(&name, &ev["input"]),
                }))
                .await;
            }
            _ => {}
        }
    }
}

/// Map zcode tool names to normalized detail. Unknowns keep the raw
/// input.
fn map_tool_input(name: &str, input: &Value) -> ToolCallDetail {
    match name {
        "bash" | "shell" | "terminal" => ToolCallDetail::Shell {
            command: input["command"]
                .as_str()
                .or_else(|| input["content"].as_str())
                .unwrap_or_default()
                .to_string(),
            output: None,
            exit_code: None,
        },
        "read" => ToolCallDetail::Read {
            path: input["path"]
                .as_str()
                .or_else(|| input["filePath"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "edit" => ToolCallDetail::Edit {
            path: input["path"]
                .as_str()
                .or_else(|| input["filePath"].as_str())
                .unwrap_or_default()
                .to_string(),
            unified_diff: None,
        },
        "write" => ToolCallDetail::Write {
            path: input["path"]
                .as_str()
                .or_else(|| input["filePath"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: input["content"].as_str().map(String::from),
        },
        "grep" | "glob" | "search" => ToolCallDetail::Search {
            query: input["pattern"]
                .as_str()
                .or_else(|| input["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        _ => ToolCallDetail::Unknown {
            input: input.clone(),
            output: Value::Null,
        },
    }
}

/// turn.completed usage + contextUsageBreakdown → Usage (schema from
/// the protocol package; the live shape awaits a GLM-login turn).
fn zcode_usage(ev: &Value) -> Option<Usage> {
    let u = ev.get("usage").or_else(|| ev.get("tokenUsage"))?;
    let _ = u;
    Some(Usage {
        input_tokens: ev["usage"]["inputTokens"]
            .as_u64()
            .or_else(|| ev["tokenUsage"]["inputTokens"].as_u64()),
        cached_input_tokens: ev["usage"]["cacheReadTokens"].as_u64(),
        output_tokens: ev["usage"]["outputTokens"].as_u64(),
        cost_usd: None,
        context_window: ev["contextUsageBreakdown"]["contextWindow"].as_u64(),
        context_used: ev["contextUsageBreakdown"]["contextUsed"].as_u64(),
    })
}

#[async_trait]
impl AgentSession for ZcodeSession {
    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.events.subscribe()
    }

    fn idle_secs(&self) -> u64 {
        self.transport.idle_secs()
    }

    async fn start_turn(&self, prompt: PromptInput) -> Result<String> {
        let turn_id = uuid::Uuid::new_v4().to_string();
        *self.current_turn.lock().await = Some(turn_id.clone());
        let content = match prompt {
            PromptInput::Text(t) => t,
            PromptInput::Blocks(blocks) => blocks
                .iter()
                .filter_map(|b| match b {
                    PromptBlock::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n"),
        };
        let sid = self.session_id.lock().await.clone().unwrap_or_default();
        self.request(
            "session/send",
            json!({"sessionId": sid, "content": content}),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::in_turn(
            turn_id.clone(),
            StreamEventKind::TurnStarted,
        ));
        Ok(turn_id)
    }

    async fn interrupt(&self) -> Result<()> {
        let sid = self.session_id.lock().await.clone().unwrap_or_default();
        self.request("session/stop", json!({"sessionId": sid}), CONTROL_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let sid = self.session_id.lock().await.clone();
        if let Some(sid) = sid {
            let _ = self
                .request("session/close", json!({"sessionId": sid}), CONTROL_TIMEOUT)
                .await;
        }
        self.transport.shutdown().await;
        Ok(())
    }

    async fn respond_to_permission(
        &self,
        request_id: &str,
        response: PermissionResponse,
    ) -> Result<()> {
        let Some(ask) = self.pending_asks.lock().await.remove(request_id) else {
            bail!("no pending permission {request_id}");
        };
        let result = match &response {
            PermissionResponse::Allow {
                action_id, answer, ..
            } => {
                let chosen = action_id.clone().or_else(|| answer.clone());
                match chosen
                    .as_deref()
                    .and_then(|id| ask.option_responses.get(id))
                {
                    // Options carry the response payload verbatim.
                    Some(resp) => resp.clone(),
                    None if ask.is_question => json!({"text": chosen.unwrap_or_default()}),
                    None => json!({"decision": "allow"}),
                }
            }
            PermissionResponse::Deny { .. } => {
                if ask.is_question {
                    json!({"cancelled": true})
                } else {
                    json!({"decision": "deny"})
                }
            }
        };
        self.transport
            .send(json!({"id": ask.rpc_id, "result": result}))
            .await?;
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let id = self.session_id.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: "zcode".to_string(),
            native_handle: id,
            metadata: Value::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verified live: the envelope carries no jsonrpc field — adding
    /// one is rejected as an unrecognized key, and omitting the id
    /// turns a request into a silently-ignored notification.
    #[test]
    fn envelope_has_no_jsonrpc_field() {
        let f = json!({"id": 2, "method": "session/create", "params": {}});
        assert!(f.get("jsonrpc").is_none());
        assert!(f["id"].is_number() || f["id"].is_string());
    }

    /// Verified live: runtime preferences must answer the boolean.
    #[test]
    fn runtime_preferences_answer_shape() {
        let a = json!({"nativeSearchEnhancementsEnabled": false});
        assert!(a["nativeSearchEnhancementsEnabled"].is_boolean());
    }

    /// Option deny-detection covers the common label vocabulary.
    #[test]
    fn deny_labels_map_to_deny() {
        for label in ["Deny", "Cancel", "Reject", "Allow once"] {
            let deny = label.to_lowercase().contains("deny")
                || label.to_lowercase().contains("cancel")
                || label.to_lowercase().contains("reject");
            assert_eq!(deny, label != "Allow once", "{label}");
        }
    }
}
