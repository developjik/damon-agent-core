//! Codex backend — drives `codex app-server`, the JSON-RPC 2.0 interface
//! that powers the Codex VS Code extension.
//!
//! Wire shape (per developers.openai.com/codex/app-server):
//! - stdio JSONL, JSON-RPC 2.0 WITHOUT the `"jsonrpc"` header on the wire.
//! - Handshake: `initialize` request → response, then `initialized`
//!   notification. Requests before it are rejected.
//! - `thread/start` → `{thread: {id}}`; `thread/resume` reattaches.
//! - `turn/start` {threadId, input:[{type:"text",text}]} → streams
//!   `turn/started`, `item/started`, `item/completed`,
//!   `item/agentMessage/delta`, `turn/completed` notifications.
//! - `turn/steer` appends input to the in-flight turn; `turn/interrupt`
//!   cancels it.
//! - Approvals arrive as server→client requests:
//!   `item/commandExecution/requestApproval`,
//!   `item/fileChange/requestApproval`, `item/permissions/requestApproval`
//!   — answered with a decision payload; `serverRequest/resolved`
//!   confirms.
//!
//! One app-server process can host many threads; we still spawn one per
//! session for crash isolation (sharing is a later optimization).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60 * 30);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct CodexClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
}

impl CodexClient {
    pub fn new(resolved: ResolvedBackend) -> Self {
        Self {
            resolved,
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: true,
                dynamic_modes: true,
                mcp_servers: false,
                reasoning_stream: true,
                steer: true,
                rewind: false,
                subagent_events: false,
            },
        }
    }
}

#[async_trait]
impl AgentClient for CodexClient {
    fn provider(&self) -> &str {
        "codex"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        self.resolved.detected
    }

    async fn fetch_catalog(&self, cwd: Option<&Path>) -> Result<ProviderCatalog> {
        // model/list needs a live connection; spawn a probe process.
        let probe = CodexSession::spawn(
            &self.resolved,
            SessionConfig {
                cwd: cwd.unwrap_or_else(|| Path::new("/")).to_path_buf(),
                ..Default::default()
            },
        )
        .await?;
        let models = probe.catalog_models().await.unwrap_or_default();
        let _ = probe.close().await;
        Ok(ProviderCatalog {
            models,
            modes: ["default", "plan", "full-access"]
                .iter()
                .map(|m| ModeDef {
                    id: m.to_string(),
                    name: m.to_string(),
                    description: None,
                })
                .collect(),
            default_mode: Some("default".to_string()),
        })
    }

    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        Ok(CodexSession::spawn(&self.resolved, config).await? as Arc<dyn AgentSession>)
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        let session = CodexSession::spawn(&self.resolved, config).await?;
        session.attach_thread(&handle.native_handle).await?;
        Ok(session as Arc<dyn AgentSession>)
    }

    /// `thread/list` enumerates stored threads — needs a live process.
    async fn list_importable_sessions(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let probe = CodexSession::spawn(
            &self.resolved,
            SessionConfig {
                cwd: cwd.to_path_buf(),
                ..Default::default()
            },
        )
        .await?;
        let result = probe
            .request("thread/list", json!({}), CONTROL_TIMEOUT)
            .await;
        let _ = probe.close().await;
        let Ok(v) = result else { return Ok(vec![]) };
        let threads = v["data"].as_array().or_else(|| v["threads"].as_array());
        let Some(threads) = threads else {
            return Ok(vec![]);
        };
        Ok(threads
            .iter()
            .filter_map(|t| {
                let id = t["id"].as_str()?;
                Some(ImportableSession {
                    handle: PersistenceHandle {
                        provider: "codex".to_string(),
                        native_handle: id.to_string(),
                        metadata: t.clone(),
                    },
                    title: t["name"]
                        .as_str()
                        .or_else(|| t["title"].as_str())
                        .map(String::from),
                    cwd: t["cwd"].as_str().map(PathBuf::from),
                    modified_at: t["updatedAt"].as_u64().or_else(|| t["updated_at"].as_u64()),
                })
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

struct PendingAsk {
    request: PermissionRequest,
    /// The JSON-RPC id of the server→client approval request.
    rpc_id: Value,
}

pub struct CodexSession {
    transport: Arc<NdjsonTransport>,
    caps: Capabilities,
    events: broadcast::Sender<StreamEvent>,
    next_id: AtomicU64,
    thread_id: Mutex<Option<String>>,
    current_turn: Mutex<Option<String>>,
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
    /// itemId → running ToolCall so item/completed can finalize it.
    open_items: Mutex<HashMap<String, ToolCall>>,
}

impl CodexSession {
    async fn spawn(resolved: &ResolvedBackend, config: SessionConfig) -> Result<Arc<Self>> {
        let env: HashMap<String, String> = resolved.env.iter().cloned().collect();
        let transport =
            NdjsonTransport::spawn(&resolved.command, &resolved.args, &env, &config.cwd).await?;

        let (events, _) = broadcast::channel(512);
        let session = Arc::new(Self {
            transport: transport.clone(),
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: true,
                dynamic_modes: true,
                mcp_servers: false,
                reasoning_stream: true,
                steer: true,
                rewind: false,
                subagent_events: false,
            },
            events,
            next_id: AtomicU64::new(1),
            thread_id: Mutex::new(None),
            current_turn: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            open_items: Mutex::new(HashMap::new()),
        });

        // Frame dispatch task: routes responses/notifications, and fails
        // the in-flight turn when the process dies without completing
        // it (turn/start's own waiter unblocks via the pending drain).
        {
            let me: Weak<CodexSession> = Arc::downgrade(&session);
            let mut rx = transport.subscribe();
            let mut exited = transport.exited();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        r = rx.recv() => match r {
                            Ok(frame) => {
                                let Some(s) = me.upgrade() else { break };
                                s.on_frame(frame).await;
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        },
                        _ = exited.changed() => {
                            if *exited.borrow() { break; }
                        }
                    }
                }
                // The backend died mid-turn — its turn/completed never
                // comes; tell subscribers instead of spinning forever.
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

        // initialize handshake — required before any other request.
        session
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "damon",
                        "title": "Damon",
                        "version": env!("CARGO_PKG_VERSION"),
                    }
                }),
                INIT_TIMEOUT,
            )
            .await
            .context("codex initialize failed")?;
        transport
            .send(json!({"method": "initialized", "params": {}}))
            .await?;

        // Start the thread unless we're resuming (attach_thread does it).
        if session.thread_id.lock().await.is_none() {
            let mut params = json!({});
            if let Some(model) = &config.model {
                params["model"] = json!(model);
            }
            params["cwd"] = json!(config.cwd.to_string_lossy());
            let result = session
                .request("thread/start", params, INIT_TIMEOUT)
                .await
                .context("codex thread/start failed")?;
            let tid = result["thread"]["id"]
                .as_str()
                .context("thread/start returned no thread id")?
                .to_string();
            *session.thread_id.lock().await = Some(tid.clone());
            session.emit(StreamEvent::new(StreamEventKind::ThreadStarted {
                native_handle: tid,
            }));
        }
        Ok(session)
    }

    /// Resume: attach to an existing thread.
    async fn attach_thread(&self, thread_id: &str) -> Result<()> {
        self.request(
            "thread/resume",
            json!({"threadId": thread_id}),
            INIT_TIMEOUT,
        )
        .await?;
        *self.thread_id.lock().await = Some(thread_id.to_string());
        self.emit(StreamEvent::new(StreamEventKind::ThreadStarted {
            native_handle: thread_id.to_string(),
        }));
        Ok(())
    }

    async fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        request_roundtrip(
            &self.transport,
            id.to_string(),
            json!({"method": method, "id": id, "params": params}),
            timeout,
        )
        .await
    }

    async fn catalog_models(&self) -> Result<Vec<ModelDef>> {
        let v = self
            .request("model/list", json!({}), CONTROL_TIMEOUT)
            .await?;
        let models = v["data"].as_array().or_else(|| v["models"].as_array());
        Ok(models
            .map(|ms| {
                ms.iter()
                    .filter_map(|m| {
                        let id = m["id"].as_str().or_else(|| m["model"].as_str())?;
                        Some(ModelDef {
                            id: id.to_string(),
                            name: m["name"]
                                .as_str()
                                .or_else(|| m["displayName"].as_str())
                                .unwrap_or(id)
                                .to_string(),
                            selectable: true,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn on_frame(&self, f: Value) {
        // Response to one of our requests?
        if let Some(id) = f.get("id") {
            if f.get("method").is_none() {
                let key = match id {
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.clone(),
                    _ => return,
                };
                let result = if let Some(err) = f.get("error") {
                    Err(err.clone())
                } else {
                    Ok(f["result"].clone())
                };
                self.transport.resolve(&key, result).await;
                return;
            }
            // Server→client request (approvals, elicitation).
            self.on_server_request(&f).await;
            return;
        }
        // Notification.
        let method = f["method"].as_str().unwrap_or_default();
        let params = &f["params"];
        match method {
            "turn/started" => {
                let turn_id = params["turn"]["id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                *self.current_turn.lock().await = Some(turn_id.clone());
                self.emit(StreamEvent::in_turn(turn_id, StreamEventKind::TurnStarted));
            }
            "turn/completed" => {
                let turn = &params["turn"];
                let turn_id = self.current_turn.lock().await.take();
                let status = turn["status"].as_str().unwrap_or_default();
                let kind = match status {
                    "interrupted" => StreamEventKind::TurnCanceled {
                        reason: "interrupted".to_string(),
                    },
                    "failed" => StreamEventKind::TurnFailed {
                        error: turn["error"]["message"]
                            .as_str()
                            .unwrap_or("turn failed")
                            .to_string(),
                        code: None,
                    },
                    _ => StreamEventKind::TurnCompleted {
                        usage: codex_usage(params),
                    },
                };
                self.emit(StreamEvent { turn_id, kind });
                self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                    reason: AttentionReason::Finished,
                }));
            }
            "item/started" => self.on_item_started(params).await,
            "item/completed" => self.on_item_completed(params).await,
            "item/agentMessage/delta" => {
                // Streaming text delta — emit as a lightweight timeline
                // update; the completed item carries the full text.
                if let Some(delta) = params["delta"].as_str() {
                    let turn = self.current_turn.lock().await.clone();
                    self.emit(StreamEvent {
                        turn_id: turn,
                        kind: StreamEventKind::Timeline(TimelineItem::AssistantMessage {
                            text: delta.to_string(),
                        }),
                    });
                }
            }
            "item/reasoning/summaryTextDelta" => {
                if let Some(delta) = params["delta"].as_str() {
                    let turn = self.current_turn.lock().await.clone();
                    self.emit(StreamEvent {
                        turn_id: turn,
                        kind: StreamEventKind::Timeline(TimelineItem::Reasoning {
                            text: delta.to_string(),
                        }),
                    });
                }
            }
            "turn/plan/updated" => {
                if let Some(plan) = params["plan"].as_array() {
                    let items: Vec<TaskItem> = plan
                        .iter()
                        .map(|p| TaskItem {
                            content: p["step"].as_str().unwrap_or_default().to_string(),
                            status: match p["status"].as_str().unwrap_or_default() {
                                "inProgress" => TaskStatus::InProgress,
                                "completed" => TaskStatus::Completed,
                                _ => TaskStatus::Pending,
                            },
                        })
                        .collect();
                    let turn = self.current_turn.lock().await.clone();
                    self.emit(StreamEvent {
                        turn_id: turn,
                        kind: StreamEventKind::Timeline(TimelineItem::Todo { items }),
                    });
                }
            }
            "serverRequest/resolved" => {
                if let Some(rid) = params["requestId"].as_str() {
                    let mut asks = self.pending_asks.lock().await;
                    let found = asks
                        .iter()
                        .find(|(_, a)| {
                            a.request.id == rid
                                || a.rpc_id.as_str() == Some(rid)
                                || a.rpc_id.as_u64().is_some_and(|n| n.to_string() == rid)
                        })
                        .map(|(k, _)| k.clone());
                    if let Some(k) = found {
                        asks.remove(&k);
                    }
                }
            }
            _ => {}
        }
    }

    async fn on_item_started(&self, params: &Value) {
        let item = &params["item"];
        let ty = item["type"].as_str().unwrap_or_default();
        let id = item["id"].as_str().unwrap_or_default().to_string();
        let call = match ty {
            "commandExecution" => ToolCall {
                call_id: id.clone(),
                name: "shell".to_string(),
                status: ToolCallStatus::Running,
                detail: ToolCallDetail::Shell {
                    command: item["command"].as_str().unwrap_or_default().to_string(),
                    output: None,
                    exit_code: None,
                },
            },
            "fileChange" => ToolCall {
                call_id: id.clone(),
                name: "edit".to_string(),
                status: ToolCallStatus::Running,
                detail: ToolCallDetail::Edit {
                    path: item["changes"][0]["path"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    unified_diff: None,
                },
            },
            "mcpToolCall" => ToolCall {
                call_id: id.clone(),
                name: item["tool"].as_str().unwrap_or("mcp").to_string(),
                status: ToolCallStatus::Running,
                detail: ToolCallDetail::Unknown {
                    input: item["arguments"].clone(),
                    output: Value::Null,
                },
            },
            "plan" => ToolCall {
                call_id: id.clone(),
                name: "plan".to_string(),
                status: ToolCallStatus::Running,
                detail: ToolCallDetail::Plan {
                    text: item["text"].as_str().unwrap_or_default().to_string(),
                },
            },
            _ => return, // userMessage/agentMessage handled via deltas/completed
        };
        self.open_items.lock().await.insert(id, call.clone());
        self.emit_timeline(TimelineItem::ToolCall(call)).await;
    }

    async fn on_item_completed(&self, params: &Value) {
        let item = &params["item"];
        let ty = item["type"].as_str().unwrap_or_default();
        let id = item["id"].as_str().unwrap_or_default().to_string();
        match ty {
            "agentMessage" => {
                if let Some(text) = item["text"].as_str() {
                    self.emit_timeline(TimelineItem::AssistantMessage {
                        text: text.to_string(),
                    })
                    .await;
                }
            }
            "reasoning" => {
                if let Some(text) = item["text"].as_str() {
                    self.emit_timeline(TimelineItem::Reasoning {
                        text: text.to_string(),
                    })
                    .await;
                }
            }
            "userMessage" => {
                if let Some(text) = item["text"].as_str() {
                    self.emit_timeline(TimelineItem::UserMessage {
                        text: text.to_string(),
                    })
                    .await;
                }
            }
            "contextCompaction" => {
                self.emit_timeline(TimelineItem::Compaction {
                    summary: item["summary"].as_str().unwrap_or_default().to_string(),
                })
                .await;
            }
            _ => {
                // Finalize a tracked tool call.
                if let Some(mut call) = self.open_items.lock().await.remove(&id) {
                    call.status = match item["status"].as_str().unwrap_or_default() {
                        "failed" | "declined" => ToolCallStatus::Failed,
                        _ => ToolCallStatus::Completed,
                    };
                    if let ToolCallDetail::Shell {
                        ref mut output,
                        ref mut exit_code,
                        ..
                    } = call.detail
                    {
                        *output = item["aggregatedOutput"]
                            .as_str()
                            .or_else(|| item["output"].as_str())
                            .map(String::from);
                        *exit_code = item["exitCode"].as_i64().map(|c| c as i32);
                    }
                    self.emit_timeline(TimelineItem::ToolCall(call)).await;
                }
            }
        }
    }

    /// Server→client requests: approvals and elicitation.
    async fn on_server_request(&self, f: &Value) {
        let method = f["method"].as_str().unwrap_or_default();
        let rpc_id = f["id"].clone();
        let params = &f["params"];
        let (kind, name, title, input, detail, actions) = match method {
            "item/commandExecution/requestApproval" => {
                let cmd = params["command"].as_str().unwrap_or_default().to_string();
                (
                    PermissionKind::Tool,
                    "commandExecution".to_string(),
                    format!("Run: {cmd}"),
                    params.clone(),
                    ToolCallDetail::Shell {
                        command: cmd,
                        output: None,
                        exit_code: None,
                    },
                    codex_decisions(&["accept", "acceptForSession", "decline", "cancel"]),
                )
            }
            "item/fileChange/requestApproval" => (
                PermissionKind::Tool,
                "fileChange".to_string(),
                "Apply file changes".to_string(),
                params.clone(),
                ToolCallDetail::Edit {
                    path: params["grantRoot"].as_str().unwrap_or_default().to_string(),
                    unified_diff: None,
                },
                codex_decisions(&["accept", "acceptForSession", "decline", "cancel"]),
            ),
            "item/permissions/requestApproval" => (
                PermissionKind::Tool,
                "permissions".to_string(),
                params["reason"]
                    .as_str()
                    .unwrap_or("Permission request")
                    .to_string(),
                params.clone(),
                ToolCallDetail::Unknown {
                    input: params.clone(),
                    output: Value::Null,
                },
                codex_decisions(&["accept", "decline"]),
            ),
            "item/tool/requestUserInput" | "mcpServer/elicitation/request" => (
                PermissionKind::Question,
                "input".to_string(),
                params["message"]
                    .as_str()
                    .unwrap_or("Input requested")
                    .to_string(),
                params.clone(),
                ToolCallDetail::Unknown {
                    input: params.clone(),
                    output: Value::Null,
                },
                codex_decisions(&["accept", "decline", "cancel"]),
            ),
            _ => {
                // Unknown server request — answer method-not-found so the
                // agent isn't stuck.
                let _ = self
                    .transport
                    .send(json!({
                        "id": rpc_id,
                        "error": {"code": -32601, "message": "method not found"}
                    }))
                    .await;
                return;
            }
        };

        let ask_id = uuid::Uuid::new_v4().to_string();
        let ask = PermissionRequest {
            id: ask_id.clone(),
            kind,
            name,
            title: Some(title),
            input: Some(input),
            detail: Some(detail),
            actions,
            suggestions: vec![],
        };
        self.pending_asks.lock().await.insert(
            ask_id.clone(),
            PendingAsk {
                request: ask.clone(),
                rpc_id,
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
}
/// Map a `turn/completed` usage block into the normalized [`Usage`].
///
/// Codex reports cumulative token accounting under
/// `params.usage.totalTokenUsage` (per-turn under `lastTokenUsage`), with
/// snake_case token fields. We prefer the cumulative block since it's what
/// a context-window consumer cares about; a flat `usage` object is accepted
/// as a fallback for protocol revisions that inline it. Codex reports no
/// cost or context-window figures here — those stay `None`.
fn codex_usage(params: &Value) -> Option<Usage> {
    let usage = params.get("usage")?;
    let tokens = usage
        .get("totalTokenUsage")
        .filter(|t| {
            t.get("input_tokens")
                .or_else(|| t.get("output_tokens"))
                .is_some()
        })
        .or_else(|| usage.get("lastTokenUsage"))
        .unwrap_or(usage);
    if tokens.get("input_tokens").is_none() && tokens.get("output_tokens").is_none() {
        return None;
    }
    Some(Usage {
        input_tokens: tokens["input_tokens"].as_u64(),
        cached_input_tokens: tokens["cached_input_tokens"].as_u64(),
        output_tokens: tokens["output_tokens"].as_u64(),
        cost_usd: None,
        context_window: None,
        context_used: None,
    })
}

/// Build the app-server result payload answering an approval/input request.
///
/// Approvals take `{decision}`; free-text questions additionally carry the
/// user's answer alongside the decision (`answer` field). Wire shape is
/// UNVERIFIED against a live codex app-server — mirrors the documented
/// decision payload with the freeform answer appended.
fn codex_approval_result(kind: PermissionKind, response: &PermissionResponse) -> Value {
    let (decision, answer) = match response {
        PermissionResponse::Allow {
            action_id, answer, ..
        } => (
            action_id.clone().unwrap_or_else(|| "accept".to_string()),
            answer.clone(),
        ),
        PermissionResponse::Deny { action_id, .. } => (
            action_id.clone().unwrap_or_else(|| "decline".to_string()),
            None,
        ),
    };
    let mut result = json!({"decision": decision});
    if kind == PermissionKind::Question
        && let Some(text) = answer
    {
        result["answer"] = json!(text);
    }
    result
}

/// Codex approval decisions → normalized actions.
fn codex_decisions(decisions: &[&str]) -> Vec<PermissionAction> {
    decisions
        .iter()
        .map(|d| PermissionAction {
            id: d.to_string(),
            label: match *d {
                "accept" => "Allow".to_string(),
                "acceptForSession" => "Always allow (session)".to_string(),
                "decline" => "Deny".to_string(),
                "cancel" => "Cancel".to_string(),
                other => other.to_string(),
            },
            behavior: match *d {
                "accept" | "acceptForSession" => PermissionBehavior::Allow,
                _ => PermissionBehavior::Deny,
            },
            variant: match *d {
                "accept" => Some(ActionVariant::Primary),
                "decline" | "cancel" => Some(ActionVariant::Danger),
                _ => Some(ActionVariant::Secondary),
            },
        })
        .collect()
}

#[async_trait]
impl AgentSession for CodexSession {
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
        let thread_id = self
            .thread_id
            .lock()
            .await
            .clone()
            .context("no active thread")?;
        let input = match prompt {
            PromptInput::Text(t) => vec![json!({"type": "text", "text": t})],
            PromptInput::Blocks(blocks) => blocks
                .iter()
                .map(|b| match b {
                    PromptBlock::Text { text } => json!({"type": "text", "text": text}),
                    PromptBlock::Image { data, mime } => json!({
                        "type": "image", "data": data, "mimeType": mime
                    }),
                })
                .collect(),
        };
        let result = self
            .request(
                "turn/start",
                json!({"threadId": thread_id, "input": input}),
                REQUEST_TIMEOUT,
            )
            .await?;
        let turn_id = result["turn"]["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        *self.current_turn.lock().await = Some(turn_id.clone());
        Ok(turn_id)
    }

    async fn steer(&self, prompt: PromptInput, _expected_turn: &str) -> Result<SteerResult> {
        let thread_id = self
            .thread_id
            .lock()
            .await
            .clone()
            .context("no active thread")?;
        let PromptInput::Text(t) = prompt else {
            bail!("steer supports text only");
        };
        self.request(
            "turn/steer",
            json!({"threadId": thread_id, "input": [{"type": "text", "text": t}]}),
            CONTROL_TIMEOUT,
        )
        .await?;
        Ok(SteerResult::Accepted)
    }

    async fn interrupt(&self) -> Result<()> {
        let thread_id = self.thread_id.lock().await.clone();
        let turn_id = self.current_turn.lock().await.clone();
        let (Some(tid), Some(turn)) = (thread_id, turn_id) else {
            return Ok(()); // nothing in flight
        };
        self.request(
            "turn/interrupt",
            json!({"threadId": tid, "turnId": turn}),
            CONTROL_TIMEOUT,
        )
        .await?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
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
        let result = codex_approval_result(ask.request.kind, &response);
        self.transport
            .send(json!({"id": ask.rpc_id, "result": result}))
            .await?;
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let id = self.thread_id.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: "codex".to_string(),
            native_handle: id,
            metadata: Value::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn turn_completed_usage_maps_total_token_usage() {
        // UNVERIFIED fixture: shape mirrors the documented app-server
        // turn/completed notification.
        let params = json!({
            "threadId": "th1",
            "turn": {"id": "t1", "status": "completed"},
            "usage": {
                "totalTokenUsage": {
                    "input_tokens": 120,
                    "cached_input_tokens": 80,
                    "output_tokens": 45,
                    "total_tokens": 165
                },
                "lastTokenUsage": {
                    "input_tokens": 10,
                    "output_tokens": 5
                }
            }
        });
        let usage = codex_usage(&params).expect("usage expected");
        assert_eq!(usage.input_tokens, Some(120));
        assert_eq!(usage.cached_input_tokens, Some(80));
        assert_eq!(usage.output_tokens, Some(45));
        assert_eq!(usage.cost_usd, None);
        assert_eq!(usage.context_window, None);
        assert_eq!(usage.context_used, None);
    }

    #[test]
    fn turn_completed_usage_falls_back_to_flat_block() {
        let params = json!({
            "turn": {"id": "t1", "status": "completed"},
            "usage": {"input_tokens": 7, "output_tokens": 3}
        });
        let usage = codex_usage(&params).expect("usage expected");
        assert_eq!(usage.input_tokens, Some(7));
        assert_eq!(usage.cached_input_tokens, None);
        assert_eq!(usage.output_tokens, Some(3));
    }

    #[test]
    fn turn_completed_without_usage_yields_none() {
        let params = json!({"turn": {"id": "t1", "status": "completed"}});
        assert!(codex_usage(&params).is_none());
    }

    #[test]
    fn question_allow_with_answer_carries_answer_on_wire() {
        let response = PermissionResponse::Allow {
            action_id: None,
            updated_input: None,
            answer: Some("use the staging cluster".to_string()),
        };
        let result = codex_approval_result(PermissionKind::Question, &response);
        assert_eq!(result["decision"], json!("accept"));
        assert_eq!(result["answer"], json!("use the staging cluster"));
    }

    #[test]
    fn tool_approval_result_stays_decision_only() {
        // Even when an answer tag-along is present, tool approvals must not
        // leak free text into the decision payload.
        let response = PermissionResponse::Allow {
            action_id: Some("acceptForSession".to_string()),
            updated_input: None,
            answer: Some("stray text".to_string()),
        };
        let result = codex_approval_result(PermissionKind::Tool, &response);
        assert_eq!(result, json!({"decision": "acceptForSession"}));
    }

    #[test]
    fn deny_result_defaults_to_decline_without_answer() {
        let response = PermissionResponse::Deny {
            action_id: None,
            message: Some("not that one".to_string()),
            interrupt: false,
        };
        let result = codex_approval_result(PermissionKind::Question, &response);
        assert_eq!(result, json!({"decision": "decline"}));
    }

    #[test]
    fn allow_response_without_answer_field_deserializes() {
        // Back-compat: existing daemon clients send allow without `answer`.
        let v: PermissionResponse = serde_json::from_value(json!({"behavior": "allow"})).unwrap();
        match v {
            PermissionResponse::Allow { answer, .. } => assert_eq!(answer, None),
            other => panic!("expected allow, got {other:?}"),
        }
    }
}
