//! Oh My Pi backend — drives `omp --mode rpc`, its newline-delimited
//! JSON protocol over stdio.
//!
//! Wire shape (observed against omp 18.2.6 + rpc.md):
//! - stdout opens with `{"type":"ready","protocolVersion":1,
//!   "supportedProtocolVersions":[1,2],"maxFrameBytes":…}` — we answer
//!   `negotiate_protocol` v2 so oversized frames arrive as `rpc_chunk`
//!   sequences we reassemble.
//! - Commands: `{id, type:"prompt"|"abort"|"new_session"|"set_model"|…}`;
//!   responses are `{id, type:"response", command, success, data|error}`.
//! - Events: `agent_start`, `turn_start`, `message_start`,
//!   `message_update` (assistantMessageEvent deltas), `message_end`,
//!   `turn_end`, `agent_end{messages, isTerminal}`, `tool_execution_*`,
//!   `extension_ui_request` (confirm/select/input → permission asks).
//! - `prompt` is acked immediately; a turn completes on `agent_end`
//!   with `isTerminal !== false`.
//!
//! One process = one session. Resume uses `switch_session` with the
//! session file path recorded in the persistence handle.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use base64::Engine;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use super::registry::ResolvedBackend;
use super::transport::{NdjsonTransport, request_roundtrip};
use super::types::*;
use super::{AgentClient, AgentSession};

const INIT_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct OmpClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
}

impl OmpClient {
    pub fn new(resolved: ResolvedBackend) -> Self {
        Self {
            resolved,
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: false,
                dynamic_modes: false,
                mcp_servers: true,
                reasoning_stream: true,
                steer: true,
                rewind: false,
                subagent_events: true,
            },
        }
    }
}

#[async_trait]
impl AgentClient for OmpClient {
    fn provider(&self) -> &str {
        "omp"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        self.resolved.detected
    }

    async fn fetch_catalog(&self, cwd: Option<&Path>) -> Result<ProviderCatalog> {
        // get_available_models needs a live session.
        let probe = OmpSession::spawn(
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
            // omp's RPC protocol has no set_approval_mode command, so modes
            // are launch-time only (see mode_args below) — hence
            // dynamic_modes: false.
            modes: ["default", "acceptEdits", "bypassPermissions"]
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
        Ok(OmpSession::spawn(&self.resolved, config).await? as Arc<dyn AgentSession>)
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        let session = OmpSession::spawn(&self.resolved, config).await?;
        session
            .command(
                "switch_session",
                json!({"type": "switch_session", "sessionPath": handle.native_handle}),
                CONTROL_TIMEOUT,
            )
            .await?;
        Ok(session as Arc<dyn AgentSession>)
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

struct PendingAsk {
    request: PermissionRequest,
    /// The extension_ui_request id to answer.
    ui_id: String,
}

/// v2 chunk reassembly state: one in-flight sequence at a time.
struct ChunkBuf {
    count: usize,
    byte_length: usize,
    parts: Vec<Option<Vec<u8>>>,
}

/// Damon permission modes → omp's native `--approval-mode` (omp 18.2.6:
/// always-ask | write | yolo). The RPC protocol cannot change it at
/// runtime, so the mode is fixed at spawn; unknown modes defer to omp's
/// own `tools.approvalMode` setting.
fn mode_args(mode: Option<&str>) -> Vec<String> {
    match mode {
        Some("bypassPermissions") => ["--approval-mode", "yolo"],
        Some("acceptEdits") => ["--approval-mode", "write"],
        _ => return vec![],
    }
    .iter()
    .map(|s| s.to_string())
    .collect()
}

pub struct OmpSession {
    transport: Arc<NdjsonTransport>,
    caps: Capabilities,
    events: broadcast::Sender<StreamEvent>,
    next_id: AtomicU64,
    /// OMP session file path, from get_state — the resume token.
    session_file: Mutex<Option<String>>,
    current_turn: Mutex<Option<String>>,
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
    /// toolCallId → running ToolCall for tool_execution_* correlation.
    open_tools: Mutex<HashMap<String, ToolCall>>,
    /// In-flight rpc_chunk reassembly (protocol v2).
    chunks: Mutex<HashMap<String, ChunkBuf>>,
}

impl OmpSession {
    async fn spawn(resolved: &ResolvedBackend, config: SessionConfig) -> Result<Arc<Self>> {
        let env: HashMap<String, String> = resolved.env.iter().cloned().collect();
        let mut args = resolved.args.clone();
        args.extend(mode_args(config.mode.as_deref()));
        let transport = NdjsonTransport::spawn(&resolved.command, &args, &env, &config.cwd).await?;

        let (events, _) = broadcast::channel(512);
        let session = Arc::new(Self {
            transport: transport.clone(),
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: false,
                dynamic_modes: false,
                mcp_servers: true,
                reasoning_stream: true,
                steer: true,
                rewind: false,
                subagent_events: true,
            },
            events,
            next_id: AtomicU64::new(1),
            session_file: Mutex::new(None),
            current_turn: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            open_tools: Mutex::new(HashMap::new()),
            chunks: Mutex::new(HashMap::new()),
        });

        // Frame dispatch task.
        {
            let me = session.clone();
            let mut rx = transport.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(frame) => me.on_frame(frame).await,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                    if !me.transport.is_alive() {
                        break;
                    }
                }
            });
        }

        // Handshake: wait for ready, then negotiate v2.
        let ready = async {
            let mut rx = transport.subscribe();
            loop {
                match rx.recv().await {
                    Ok(f) if f["type"] == "ready" => return Ok(f),
                    Ok(_) => continue,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => bail!("omp exited before ready"),
                }
            }
        };
        let ready = tokio::time::timeout(INIT_TIMEOUT, ready)
            .await
            .context("omp ready timed out")??;
        if ready["supportedProtocolVersions"]
            .as_array()
            .map(|v| v.iter().any(|x| x.as_u64() == Some(2)))
            .unwrap_or(false)
        {
            session
                .command(
                    "negotiate_protocol",
                    json!({"type": "negotiate_protocol", "protocolVersion": 2}),
                    CONTROL_TIMEOUT,
                )
                .await
                .context("omp protocol negotiation failed")?;
        }

        // Record the session file for resume.
        if let Ok(state) = session
            .command("get_state", json!({"type": "get_state"}), CONTROL_TIMEOUT)
            .await
            && let Some(f) = state["sessionFile"].as_str()
        {
            *session.session_file.lock().await = Some(f.to_string());
            session.emit(StreamEvent::new(StreamEventKind::ThreadStarted {
                native_handle: f.to_string(),
            }));
        }
        Ok(session)
    }

    /// Send a typed command and await its response.
    async fn command(&self, name: &str, mut frame: Value, timeout: Duration) -> Result<Value> {
        let id = format!("d{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        frame["id"] = json!(id);
        let v = request_roundtrip(&self.transport, id, frame, timeout).await?;
        if v["success"].as_bool().unwrap_or(false) {
            Ok(v["data"].clone())
        } else {
            bail!(
                "{name}: {}",
                v["error"].as_str().unwrap_or("command failed")
            )
        }
    }

    async fn catalog_models(&self) -> Result<Vec<ModelDef>> {
        let v = self
            .command(
                "get_available_models",
                json!({"type": "get_available_models"}),
                CONTROL_TIMEOUT,
            )
            .await?;
        let models = v["models"].as_array();
        Ok(models
            .map(|ms| {
                ms.iter()
                    .filter_map(|m| {
                        let id = m["id"].as_str().or_else(|| m["modelId"].as_str())?;
                        let provider = m["provider"].as_str().unwrap_or_default();
                        Some(ModelDef {
                            id: format!("{provider}/{id}"),
                            name: m["name"].as_str().unwrap_or(id).to_string(),
                            selectable: true,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn on_frame(&self, f: Value) {
        let ty = f["type"].as_str().unwrap_or_default();

        // v2 chunk reassembly.
        if ty == "rpc_chunk" {
            if let Some(frame) = self.reassemble(&f).await {
                Box::pin(self.on_frame(frame)).await;
            }
            return;
        }

        // Command response?
        if ty == "response" {
            if let Some(id) = f["id"].as_str() {
                self.transport.resolve(id, Ok(f.clone())).await;
            }
            return;
        }

        match ty {
            "agent_start" => {
                let turn_id = uuid::Uuid::new_v4().to_string();
                *self.current_turn.lock().await = Some(turn_id.clone());
                self.emit(StreamEvent::in_turn(turn_id, StreamEventKind::TurnStarted));
            }
            "agent_end" => {
                let turn = self.current_turn.lock().await.take();
                let terminal = f["isTerminal"].as_bool().unwrap_or(true);
                if terminal {
                    self.emit(StreamEvent {
                        turn_id: turn,
                        kind: StreamEventKind::TurnCompleted { usage: None },
                    });
                    self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                        reason: AttentionReason::Finished,
                    }));
                }
            }
            "message_update" => {
                let ev = &f["assistantMessageEvent"];
                let item = match ev["type"].as_str().unwrap_or_default() {
                    "text_delta" => Some(TimelineItem::AssistantMessage {
                        text: ev["delta"].as_str().unwrap_or_default().to_string(),
                    }),
                    "thinking_delta" => Some(TimelineItem::Reasoning {
                        text: ev["delta"].as_str().unwrap_or_default().to_string(),
                    }),
                    _ => None,
                };
                if let Some(item) = item {
                    let turn = self.current_turn.lock().await.clone();
                    self.emit(StreamEvent {
                        turn_id: turn,
                        kind: StreamEventKind::Timeline(item),
                    });
                }
            }
            "tool_execution_start" => {
                let call = ToolCall {
                    call_id: f["toolCallId"].as_str().unwrap_or_default().to_string(),
                    name: f["toolName"].as_str().unwrap_or_default().to_string(),
                    status: ToolCallStatus::Running,
                    detail: map_tool(&f),
                };
                self.open_tools
                    .lock()
                    .await
                    .insert(call.call_id.clone(), call.clone());
                self.emit_timeline(TimelineItem::ToolCall(call)).await;
            }
            "tool_execution_end" => {
                let id = f["toolCallId"].as_str().unwrap_or_default();
                if let Some(mut call) = self.open_tools.lock().await.remove(id) {
                    call.status = if f["isError"].as_bool().unwrap_or(false) {
                        ToolCallStatus::Failed
                    } else {
                        ToolCallStatus::Completed
                    };
                    self.emit_timeline(TimelineItem::ToolCall(call)).await;
                }
            }
            "extension_ui_request" => self.on_ui_request(&f).await,
            "extension_ui_response" => {}
            "subagent_lifecycle" | "subagent_progress" | "subagent_event" => {
                let turn = self.current_turn.lock().await.clone();
                self.emit(StreamEvent {
                    turn_id: turn,
                    kind: StreamEventKind::Subagent { event: f.clone() },
                });
            }
            "auto_compaction_start" | "auto_compaction_end" => {}
            _ => {}
        }
    }

    /// extension_ui_request → PermissionRequest. confirm/select become
    /// permission asks; input becomes a question; notify/setWidget etc.
    /// are auto-acknowledged (no user decision needed).
    async fn on_ui_request(&self, f: &Value) {
        let ui_id = f["id"].as_str().unwrap_or_default().to_string();
        let method = f["method"].as_str().unwrap_or_default();
        let (kind, title, actions) = match method {
            "confirm" => (
                PermissionKind::Tool,
                f["message"].as_str().unwrap_or("Confirm").to_string(),
                vec![
                    PermissionAction {
                        id: "confirm".to_string(),
                        label: "Confirm".to_string(),
                        behavior: PermissionBehavior::Allow,
                        variant: Some(ActionVariant::Primary),
                    },
                    PermissionAction {
                        id: "cancel".to_string(),
                        label: "Cancel".to_string(),
                        behavior: PermissionBehavior::Deny,
                        variant: Some(ActionVariant::Danger),
                    },
                ],
            ),
            "select" => {
                let options = f["options"].as_array().cloned().unwrap_or_default();
                (
                    PermissionKind::Question,
                    f["title"]
                        .as_str()
                        .or_else(|| f["message"].as_str())
                        .unwrap_or("Select")
                        .to_string(),
                    options
                        .iter()
                        .enumerate()
                        .map(|(i, o)| PermissionAction {
                            id: format!("opt{i}"),
                            label: o.as_str().unwrap_or_default().to_string(),
                            behavior: PermissionBehavior::Allow,
                            variant: None,
                        })
                        .collect(),
                )
            }
            "input" => (
                PermissionKind::Question,
                f["title"]
                    .as_str()
                    .or_else(|| f["message"].as_str())
                    .unwrap_or("Input")
                    .to_string(),
                vec![],
            ),
            _ => {
                // notify/setWidget/setStatus/setTitle — no decision needed.
                let _ = self
                    .transport
                    .send(json!({
                        "type": "extension_ui_response",
                        "id": ui_id,
                        "confirmed": true
                    }))
                    .await;
                return;
            }
        };

        let ask_id = uuid::Uuid::new_v4().to_string();
        let ask = PermissionRequest {
            id: ask_id.clone(),
            kind,
            name: method.to_string(),
            title: Some(title),
            input: Some(f.clone()),
            detail: None,
            actions,
            suggestions: vec![],
        };
        self.pending_asks.lock().await.insert(
            ask_id.clone(),
            PendingAsk {
                request: ask.clone(),
                ui_id,
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

    /// Reassemble one rpc_chunk sequence; returns the complete frame
    /// when the last chunk arrives.
    async fn reassemble(&self, f: &Value) -> Option<Value> {
        let chunk_id = f["chunkId"].as_str()?.to_string();
        let index = f["index"].as_u64()? as usize;
        let count = f["count"].as_u64()? as usize;
        let byte_length = f["byteLength"].as_u64()? as usize;
        let data = base64::engine::general_purpose::STANDARD
            .decode(f["data"].as_str()?)
            .ok()?;

        let mut chunks = self.chunks.lock().await;
        let buf = chunks.entry(chunk_id.clone()).or_insert_with(|| ChunkBuf {
            count,
            byte_length,
            parts: vec![None; count],
        });
        if index >= buf.count {
            chunks.remove(&chunk_id);
            return None;
        }
        buf.parts[index] = Some(data);
        if buf.parts.iter().any(|p| p.is_none()) {
            return None;
        }
        let done = chunks.remove(&chunk_id)?;
        let bytes: Vec<u8> = done.parts.into_iter().flatten().flatten().collect();
        if bytes.len() != done.byte_length {
            return None;
        }
        serde_json::from_slice(&bytes).ok()
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

/// Map a tool_execution_start frame to normalized detail.
fn map_tool(f: &Value) -> ToolCallDetail {
    let name = f["toolName"].as_str().unwrap_or_default();
    let args = &f["args"];
    match name {
        "bash" => ToolCallDetail::Shell {
            command: args["command"].as_str().unwrap_or_default().to_string(),
            output: None,
            exit_code: None,
        },
        "read" => ToolCallDetail::Read {
            path: args["path"].as_str().unwrap_or_default().to_string(),
            content: None,
        },
        "edit" => ToolCallDetail::Edit {
            path: args["path"].as_str().unwrap_or_default().to_string(),
            unified_diff: None,
        },
        "write" => ToolCallDetail::Write {
            path: args["path"].as_str().unwrap_or_default().to_string(),
            content: args["content"].as_str().map(String::from),
        },
        "grep" | "glob" | "web_search" => ToolCallDetail::Search {
            query: args["pattern"]
                .as_str()
                .or_else(|| args["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        _ => ToolCallDetail::Unknown {
            input: args.clone(),
            output: Value::Null,
        },
    }
}

#[async_trait]
impl AgentSession for OmpSession {
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
        let text = match prompt {
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
        self.command(
            "prompt",
            json!({"type": "prompt", "message": text}),
            CONTROL_TIMEOUT,
        )
        .await?;
        // The turn id is assigned when agent_start arrives; return a
        // placeholder the caller correlates via events.
        Ok(self.current_turn.lock().await.clone().unwrap_or_default())
    }

    async fn steer(&self, prompt: PromptInput, _expected_turn: &str) -> Result<SteerResult> {
        let PromptInput::Text(t) = prompt else {
            bail!("steer supports text only");
        };
        self.command(
            "steer",
            json!({"type": "steer", "message": t}),
            CONTROL_TIMEOUT,
        )
        .await?;
        Ok(SteerResult::Accepted)
    }

    async fn interrupt(&self) -> Result<()> {
        self.command("abort", json!({"type": "abort"}), CONTROL_TIMEOUT)
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
        let resp = match &response {
            PermissionResponse::Allow { action_id, .. } => {
                // select asks map action ids back to option labels.
                if let Some(idx) = action_id
                    .as_deref()
                    .and_then(|a| a.strip_prefix("opt"))
                    .and_then(|i| i.parse::<usize>().ok())
                {
                    let label = ask
                        .request
                        .actions
                        .get(idx)
                        .map(|a| a.label.clone())
                        .unwrap_or_default();
                    json!({"type": "extension_ui_response", "id": ask.ui_id, "value": label})
                } else {
                    json!({"type": "extension_ui_response", "id": ask.ui_id, "confirmed": true})
                }
            }
            PermissionResponse::Deny { .. } => {
                json!({"type": "extension_ui_response", "id": ask.ui_id, "cancelled": true})
            }
        };
        self.transport.send(resp).await?;
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let f = self.session_file.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: "omp".to_string(),
            native_handle: f,
            metadata: Value::Null,
        })
    }

    async fn set_model(&self, model: &str) -> Result<()> {
        let (provider, model_id) = model
            .split_once('/')
            .map(|(p, m)| (p.to_string(), m.to_string()))
            .unwrap_or_else(|| (String::new(), model.to_string()));
        self.command(
            "set_model",
            json!({"type": "set_model", "provider": provider, "modelId": model_id}),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_args_map_to_approval_mode() {
        assert_eq!(
            mode_args(Some("bypassPermissions")),
            vec!["--approval-mode".to_string(), "yolo".to_string()]
        );
        assert_eq!(
            mode_args(Some("acceptEdits")),
            vec!["--approval-mode".to_string(), "write".to_string()]
        );
        // Unknown/default modes defer to omp's own tools.approvalMode.
        assert!(mode_args(None).is_empty());
        assert!(mode_args(Some("plan")).is_empty());
    }
}
