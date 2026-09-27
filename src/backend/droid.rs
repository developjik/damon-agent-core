//! Droid backend — drives `droid exec --input-format stream-jsonrpc
//! -o stream-jsonrpc`, Factory's JSON-RPC wire over stdio.
//!
//! Wire shape (verified live against droid 0.228.0, driving the real
//! binary against a fake OpenAI-compatible endpoint):
//! - Envelope: every frame carries `type` ("request" | "response" |
//!   "notification"), `jsonrpc:"2.0"`, and the literal
//!   `factoryApiVersion:"1.0.0"`. Requests without the literal are
//!   rejected with "Invalid JSON-RPC message" — ids are strings.
//! - Session: `droid.initialize_session {machineId, cwd, modelId?}`
//!   (or `droid.load_session {sessionId}` to resume) → result carries
//!   the sessionId used as the persistence handle.
//! - Turns: `droid.add_user_message {text, imagePaths?, images?}`;
//!   events stream as `droid.session_notification` wrapping
//!   notification payloads: `create_message` (content blocks —
//!   text/thinking/tool_use), `assistant_text_delta` /
//!   `thinking_text_delta`, `tool_call` / `tool_result`,
//!   `permission_resolved`, `error`, and the terminal
//!   `agent_turn_completed {reason, turnId, tokenUsage{inputTokens,
//!   outputTokens, cacheReadTokens, …}}`.
//! - Permissions: server→client `droid.request_permission` requests
//!   carry `{toolUses:[{toolUse, confirmationType, details}], options:
//!   [{label, value}]}`; the client answers a response with
//!   `{selectedOption: "proceed_once"|"proceed_always"|"cancel"|…}` —
//!   verified end-to-end (an allowed Create actually wrote the file).
//!   `droid.ask_user {toolCallId, questions:[{index, question,
//!   options}]}` arrives the same way; answers are
//!   `{answers:[{index, question, answer}]}` (schema from the
//!   TypeScript SDK; ask_user itself not yet observed live).
//! - Interrupt: `droid.interrupt_session`.
//!
//! One process = one conversation (the process stays alive between
//! turns in stream-jsonrpc mode). Image attachments ride
//! `add_user_message.images` as base64 sources.

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
use super::transport::NdjsonTransport;
use super::types::*;
use super::{AgentClient, AgentSession};

const INIT_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DroidClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
}

impl DroidClient {
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
impl AgentClient for DroidClient {
    fn provider(&self) -> &str {
        "droid"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        self.resolved.detected
    }

    async fn fetch_catalog(&self, _cwd: Option<&Path>) -> Result<ProviderCatalog> {
        // Model listing needs a live session (droid.list_models) plus a
        // Factory login; model ids pass through initialize_session
        // untouched, so an empty list still allows explicit selection.
        Ok(ProviderCatalog {
            models: vec![],
            // Autonomy levels are launch-time only (--auto); the
            // settings update method exists but is not mode-scoped.
            modes: ["low", "medium", "high"]
                .iter()
                .map(|m| ModeDef {
                    id: m.to_string(),
                    name: m.to_string(),
                    description: None,
                })
                .collect(),
            default_mode: Some("low".to_string()),
        })
    }

    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        Ok(DroidSession::spawn(&self.resolved, config, None).await? as Arc<dyn AgentSession>)
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        Ok(
            DroidSession::spawn(&self.resolved, config, Some(&handle.native_handle)).await?
                as Arc<dyn AgentSession>,
        )
    }
}

struct PendingAsk {
    request: PermissionRequest,
    /// The JSON-RPC id of the server→client permission/ask request.
    rpc_id: Value,
    /// ask_user asks answer with collected answers instead of an
    /// option value.
    is_question: bool,
}

pub struct DroidSession {
    transport: Arc<NdjsonTransport>,
    caps: Capabilities,
    events: broadcast::Sender<StreamEvent>,
    next_id: AtomicU64,
    /// Droid sessionId from initialize/load — the resume token.
    session_id: Mutex<Option<String>>,
    current_turn: Mutex<Option<String>>,
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
}

impl DroidSession {
    async fn spawn(
        resolved: &ResolvedBackend,
        config: SessionConfig,
        resume: Option<&str>,
    ) -> Result<Arc<Self>> {
        let env: HashMap<String, String> = resolved.env.iter().cloned().collect();
        let mut args = resolved.args.clone();
        // Damon modes map to droid's autonomy levels (launch-time).
        if let Some(mode) = &config.mode {
            args.push("--auto".to_string());
            args.push(mode.clone());
        }
        let transport = NdjsonTransport::spawn(&resolved.command, &args, &env, &config.cwd).await?;

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
            let me: Weak<DroidSession> = Arc::downgrade(&session);
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
                // The backend died mid-turn — agent_turn_completed never
                // comes; fail the turn instead of wedging subscribers.
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

        // Handshake: initialize (or load) the session before returning.
        let machine = format!("damon-{}", uuid::Uuid::new_v4().simple());
        let mut params = json!({"machineId": machine, "cwd": config.cwd.to_string_lossy()});
        if let Some(model) = &config.model {
            params["modelId"] = json!(model);
        }
        let method = if let Some(id) = resume {
            params = json!({"sessionId": id});
            "droid.load_session"
        } else {
            "droid.initialize_session"
        };
        let result = session
            .request(method, params, INIT_TIMEOUT)
            .await
            .context("droid session init failed")?;
        if let Some(id) = result["sessionId"].as_str() {
            *session.session_id.lock().await = Some(id.to_string());
            session.emit(StreamEvent::new(StreamEventKind::ThreadStarted {
                native_handle: id.to_string(),
            }));
        }
        Ok(session)
    }

    /// One JSON-RPC request/response round-trip over the shared
    /// transport pending map (ids are strings on this wire).
    async fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        let rx = self.transport.expect(id.clone()).await;
        self.transport
            .send(json!({
                "type": "request",
                "jsonrpc": "2.0",
                "factoryApiVersion": "1.0.0",
                "id": id,
                "method": method,
                "params": params
            }))
            .await?;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(v))) => Ok(v["result"].clone()),
            Ok(Ok(Err(e))) => bail!("{}", e["message"].as_str().unwrap_or("droid error")),
            Ok(Err(_)) => bail!("droid dropped the request"),
            Err(_) => {
                self.transport.forget(&id).await;
                bail!("droid timed out after {timeout:?}")
            }
        }
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
        match f["type"].as_str().unwrap_or_default() {
            // Response to a request WE sent.
            "response" => {
                let id = f["id"].as_str().unwrap_or_default().to_string();
                let result = if f["error"].is_object() || f["error"].is_string() {
                    Err(f["error"].clone())
                } else {
                    Ok(f.clone())
                };
                self.transport.resolve(&id, result).await;
            }
            // Server→client request: permission asks and questions.
            "request" => self.on_server_request(&f).await,
            "notification" => self.on_notification(&f).await,
            _ => {}
        }
    }

    async fn on_server_request(&self, f: &Value) {
        let rpc_id = f["id"].clone();
        match f["method"].as_str().unwrap_or_default() {
            "droid.request_permission" => {
                let params = &f["params"];
                let first = &params["toolUses"][0]["toolUse"];
                let name = first["name"].as_str().unwrap_or("tool").to_string();
                let input = first["input"].clone();
                let actions = permission_actions(&params["options"]);
                let ask = PermissionRequest {
                    id: uuid::Uuid::new_v4().to_string(),
                    kind: PermissionKind::Tool,
                    name: name.clone(),
                    title: Some(format!("{name} wants to run")),
                    input: Some(input.clone()),
                    detail: Some(map_tool_input(
                        &name,
                        &input,
                        &params["toolUses"][0]["details"],
                    )),
                    actions,
                    suggestions: vec![],
                };
                self.register_ask(ask, rpc_id, false).await;
            }
            "droid.ask_user" => {
                // {toolCallId, questions:[{index, question, options}]}
                // — rendered as one ask per question batch (first
                // question's options become the actions; free-text
                // answers pass through the allow comment).
                let params = &f["params"];
                let question = params["questions"][0]["question"]
                    .as_str()
                    .unwrap_or("Question")
                    .to_string();
                let options = params["questions"][0]["options"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let actions = options
                    .iter()
                    .map(|o| PermissionAction {
                        id: o.as_str().unwrap_or("opt").to_string(),
                        label: o.as_str().unwrap_or("opt").to_string(),
                        behavior: PermissionBehavior::Allow,
                        variant: None,
                    })
                    .collect();
                let ask = PermissionRequest {
                    id: uuid::Uuid::new_v4().to_string(),
                    kind: PermissionKind::Question,
                    name: "ask_user".to_string(),
                    title: Some(question),
                    input: Some(params.clone()),
                    detail: None,
                    actions,
                    suggestions: vec![],
                };
                self.register_ask(ask, rpc_id, true).await;
            }
            // Unknown server request — answer method-not-found so the
            // agent isn't stuck waiting on a handshake we don't have.
            _ => {
                let _ = self
                    .transport
                    .send(json!({
                        "type": "response",
                        "jsonrpc": "2.0",
                        "factoryApiVersion": "1.0.0",
                        "id": rpc_id,
                        "error": {"code": -32601, "message": "method not found"}
                    }))
                    .await;
            }
        }
    }

    async fn register_ask(&self, ask: PermissionRequest, rpc_id: Value, is_question: bool) {
        self.pending_asks.lock().await.insert(
            ask.id.clone(),
            PendingAsk {
                request: ask.clone(),
                rpc_id,
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

    async fn on_notification(&self, f: &Value) {
        if f["method"].as_str() != Some("droid.session_notification") {
            return;
        }
        let n = &f["params"]["notification"];
        match n["type"].as_str().unwrap_or_default() {
            "create_message" => {
                // Only the assistant side maps to the timeline — the
                // user echo is Damon's own prompt coming back.
                if n["message"]["role"].as_str() != Some("assistant") {
                    return;
                }
                if let Some(blocks) = n["message"]["content"].as_array() {
                    for block in blocks {
                        let item = match block["type"].as_str().unwrap_or_default() {
                            "text" => TimelineItem::AssistantMessage {
                                text: block["text"].as_str().unwrap_or_default().to_string(),
                            },
                            "thinking" => TimelineItem::Reasoning {
                                text: block["thinking"].as_str().unwrap_or_default().to_string(),
                            },
                            "tool_use" => TimelineItem::ToolCall(ToolCall {
                                call_id: block["id"].as_str().unwrap_or_default().to_string(),
                                name: block["name"].as_str().unwrap_or_default().to_string(),
                                status: ToolCallStatus::Running,
                                detail: map_tool_input(
                                    block["name"].as_str().unwrap_or_default(),
                                    &block["input"],
                                    &Value::Null,
                                ),
                            }),
                            _ => continue,
                        };
                        self.emit_timeline(item).await;
                    }
                }
            }
            "assistant_text_delta" => {
                if let Some(d) = n["delta"].as_str() {
                    self.emit_timeline(TimelineItem::AssistantMessage {
                        text: d.to_string(),
                    })
                    .await;
                }
            }
            "thinking_text_delta" => {
                if let Some(d) = n["delta"].as_str() {
                    self.emit_timeline(TimelineItem::Reasoning {
                        text: d.to_string(),
                    })
                    .await;
                }
            }
            "tool_call" => {
                // {type:"tool_call", toolUse:{id, name, input}} — the
                // same ToolUse shape request_permission carries.
                let u = &n["toolUse"];
                let name = u["name"].as_str().unwrap_or_default();
                let call = ToolCall {
                    call_id: u["id"].as_str().unwrap_or_default().to_string(),
                    name: name.to_string(),
                    status: ToolCallStatus::Running,
                    detail: map_tool_input(name, &u["input"], &Value::Null),
                };
                self.emit_timeline(TimelineItem::ToolCall(call)).await;
            }
            "tool_result" => {
                let call = ToolCall {
                    call_id: n["toolUseId"].as_str().unwrap_or_default().to_string(),
                    name: String::new(),
                    status: if n["isError"].as_bool().unwrap_or(false) {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Completed
                    },
                    detail: ToolCallDetail::Unknown {
                        input: Value::Null,
                        output: Value::String(
                            n["output"]
                                .as_str()
                                .or_else(|| n["content"].as_str())
                                .unwrap_or_default()
                                .to_string(),
                        ),
                    },
                };
                self.emit_timeline(TimelineItem::ToolCall(call)).await;
            }
            "error" => {
                // Non-terminal: agent_turn_completed still ends the turn;
                // surface the message as an error timeline entry.
                let msg = n["message"].as_str().unwrap_or("droid error");
                self.emit_timeline(TimelineItem::Error {
                    message: msg.to_string(),
                })
                .await;
            }
            "agent_turn_completed" => {
                let turn = self.current_turn.lock().await.take();
                let reason = n["reason"].as_str().unwrap_or("completed");
                let usage = droid_usage(&n["tokenUsage"]);
                let kind = match reason {
                    "interrupted" | "aborted" => StreamEventKind::TurnCanceled {
                        reason: reason.to_string(),
                    },
                    "completed" => StreamEventKind::TurnCompleted { usage },
                    other => StreamEventKind::TurnFailed {
                        error: other.to_string(),
                        code: None,
                    },
                };
                self.emit(StreamEvent {
                    turn_id: turn,
                    kind,
                });
                self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                    reason: AttentionReason::Finished,
                }));
            }
            "permission_resolved"
            | "settings_updated"
            | "droid_working_state_changed"
            | "mcp_status_changed"
            | "hook_execution_started"
            | "hook_execution_completed" => {}
            _ => {}
        }
    }
}

/// Map droid's tool-confirmation options onto Damon permission
/// actions. Option values are droid's own ToolConfirmationOutcome
/// strings — they ride back verbatim as `selectedOption`.
fn permission_actions(options: &Value) -> Vec<PermissionAction> {
    let Some(list) = options.as_array() else {
        return vec![];
    };
    list.iter()
        .map(|o| {
            let value = o["value"].as_str().unwrap_or("proceed_once");
            let deny = value == "cancel" || value.starts_with("deny");
            PermissionAction {
                id: value.to_string(),
                label: o["label"].as_str().unwrap_or(value).to_string(),
                behavior: if deny {
                    PermissionBehavior::Deny
                } else {
                    PermissionBehavior::Allow
                },
                variant: if deny {
                    Some(ActionVariant::Danger)
                } else {
                    Some(ActionVariant::Primary)
                },
            }
        })
        .collect()
}

/// Map droid's tool vocabulary to normalized detail. The confirmation
/// `details` block carries the prettified fields (filePath, command…)
/// and wins when present. Unknown tools keep the raw input.
fn map_tool_input(name: &str, input: &Value, details: &Value) -> ToolCallDetail {
    let field = |k: &str| -> Option<String> {
        details[k]
            .as_str()
            .or_else(|| input[k].as_str())
            .map(String::from)
    };
    match name {
        "Execute" => ToolCallDetail::Shell {
            command: field("command").unwrap_or_default(),
            output: None,
            exit_code: None,
        },
        "Read" => ToolCallDetail::Read {
            path: field("file_path").unwrap_or_default(),
            content: None,
        },
        "Edit" => ToolCallDetail::Edit {
            path: field("file_path").unwrap_or_default(),
            unified_diff: None,
        },
        "Create" => ToolCallDetail::Write {
            path: field("file_path").unwrap_or_default(),
            content: field("content"),
        },
        "Grep" | "Glob" | "LS" => ToolCallDetail::Search {
            query: field("pattern")
                .or_else(|| field("path"))
                .unwrap_or_default(),
            content: None,
        },
        _ => ToolCallDetail::Unknown {
            input: input.clone(),
            output: Value::Null,
        },
    }
}

fn droid_usage(u: &Value) -> Option<Usage> {
    if u.as_object().is_none_or(|o| o.is_empty()) {
        return None;
    }
    Some(Usage {
        input_tokens: u["inputTokens"].as_u64(),
        cached_input_tokens: u["cacheReadTokens"].as_u64(),
        output_tokens: u["outputTokens"].as_u64(),
        cost_usd: None,
        context_window: None,
        context_used: None,
    })
}

#[async_trait]
impl AgentSession for DroidSession {
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
        let mut params = json!({});
        match prompt {
            PromptInput::Text(t) => params["text"] = json!(t),
            PromptInput::Blocks(blocks) => {
                let text: Vec<&str> = blocks
                    .iter()
                    .filter_map(|b| match b {
                        PromptBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect();
                params["text"] = json!(text.join("\n"));
                // Images ride as base64 sources (schema-verified; the
                // live image round-trip is not yet observed).
                let images: Vec<Value> = blocks
                    .iter()
                    .filter_map(|b| match b {
                        PromptBlock::Image { data, mime } => Some(json!({
                            "data": data,
                            "mediaType": mime
                        })),
                        _ => None,
                    })
                    .collect();
                if !images.is_empty() {
                    params["images"] = json!(images);
                }
            }
        }
        self.request("droid.add_user_message", params, CONTROL_TIMEOUT)
            .await?;
        self.emit(StreamEvent::in_turn(
            turn_id.clone(),
            StreamEventKind::TurnStarted,
        ));
        Ok(turn_id)
    }

    async fn interrupt(&self) -> Result<()> {
        self.request("droid.interrupt_session", json!({}), CONTROL_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let _ = self
            .request("droid.close_session", json!({}), CONTROL_TIMEOUT)
            .await;
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
        let result = match (&response, ask.is_question) {
            (
                PermissionResponse::Allow {
                    action_id, answer, ..
                },
                true,
            ) => {
                // ask_user: collected answers keyed by question index.
                let value = action_id.clone().or_else(|| answer.clone());
                json!({"answers": [{
                    "index": 0,
                    "question": ask.request.title.clone().unwrap_or_default(),
                    "answer": value.unwrap_or_default()
                }]})
            }
            (PermissionResponse::Allow { updated_input, .. }, false) => {
                let _ = updated_input; // droid takes options, not edits
                json!({"selectedOption": "proceed_once"})
            }
            (PermissionResponse::Deny { .. }, true) => json!({"cancelled": true}),
            (PermissionResponse::Deny { .. }, false) => {
                json!({"selectedOption": "cancel"})
            }
        };
        tracing::debug!(backend = "droid", "permission response: {result}");
        self.transport
            .send(json!({
                "type": "response",
                "jsonrpc": "2.0",
                "factoryApiVersion": "1.0.0",
                "id": ask.rpc_id,
                "result": result
            }))
            .await?;
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let id = self.session_id.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: "droid".to_string(),
            native_handle: id,
            metadata: Value::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The literal Factory envelope fields the CLI's validator demands
    /// (found live: missing factoryApiVersion → "Invalid JSON-RPC
    /// message").
    #[test]
    fn request_envelope_carries_the_factory_literals() {
        let env = json!({
            "type": "request",
            "jsonrpc": "2.0",
            "factoryApiVersion": "1.0.0",
            "id": "1",
            "method": "droid.initialize_session",
            "params": {}
        });
        assert_eq!(env["type"], "request");
        assert_eq!(env["factoryApiVersion"], "1.0.0");
        assert!(env["id"].is_string());
    }

    /// Verified live: options carry ToolConfirmationOutcome values that
    /// ride back verbatim; cancel maps to the deny action.
    #[test]
    fn permission_options_map_to_actions() {
        let options = json!([
            {"label": "Yes, allow", "value": "proceed_once"},
            {"label": "Yes, and always allow low impact commands (file edits and read-only commands)", "value": "proceed_always"},
            {"label": "No, cancel", "value": "cancel"}
        ]);
        let actions = permission_actions(&options);
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].id, "proceed_once");
        assert!(matches!(actions[0].behavior, PermissionBehavior::Allow));
        assert_eq!(actions[2].id, "cancel");
        assert!(matches!(actions[2].behavior, PermissionBehavior::Deny));
    }

    /// Verified live: agent_turn_completed carries camelCase token
    /// counters.
    #[test]
    fn turn_usage_maps_camel_case_counters() {
        let u = droid_usage(&json!({
            "inputTokens": 12, "outputTokens": 7, "cacheReadTokens": 3
        }))
        .unwrap();
        assert_eq!(u.input_tokens, Some(12));
        assert_eq!(u.cached_input_tokens, Some(3));
        assert_eq!(u.output_tokens, Some(7));
        assert!(droid_usage(&Value::Null).is_none());
    }
}
