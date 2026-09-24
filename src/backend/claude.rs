//! Claude Code backend — drives `claude -p` in bidirectional stream-json
//! mode (`--output-format stream-json --input-format stream-json`).
//!
//! Wire shape (observed against claude 2.1.270):
//! - stdout: `system.init` (session_id, tools, model, permissionMode),
//!   `assistant` messages (content blocks incl. tool_use), `user`
//!   messages carrying tool_result blocks, `system.*` housekeeping
//!   (hooks, thinking_tokens), and a final `result` frame per turn.
//! - stdin: `{"type":"user","message":{...}}` prompts, plus
//!   `control_request`/`control_response` frames for the SDK control
//!   protocol (can_use_tool permission asks, interrupt, set_model,
//!   set_permission_mode).
//!
//! One process = one conversation. Resume spawns a fresh process with
//! `--resume <session_id>`; Claude's own transcript file is the durable
//! record.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, broadcast};

use super::registry::ResolvedBackend;
use super::transport::{NdjsonTransport, request_roundtrip};
use super::types::*;
use super::{AgentClient, AgentSession};

/// Control requests we send (interrupt, set_model) get a bounded wait.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ClaudeClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
}

impl ClaudeClient {
    pub fn new(resolved: ResolvedBackend) -> Self {
        Self {
            resolved,
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: true,
                dynamic_modes: true,
                mcp_servers: true,
                reasoning_stream: true,
                steer: true,
                rewind: true,
                subagent_events: true,
            },
        }
    }
}

#[async_trait]
impl AgentClient for ClaudeClient {
    fn provider(&self) -> &str {
        "claude"
    }

    fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    async fn is_available(&self) -> bool {
        self.resolved.detected
    }

    async fn fetch_catalog(&self, _cwd: Option<&Path>) -> Result<ProviderCatalog> {
        // Claude has no model-listing wire call; the init frame reports
        // the active model. Modes are the CLI's permission modes.
        Ok(ProviderCatalog {
            models: vec![],
            modes: ["default", "acceptEdits", "plan", "bypassPermissions"]
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
        ClaudeSession::spawn(&self.resolved, config, None).await
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
        _purpose: ResumePurpose,
    ) -> Result<Arc<dyn AgentSession>> {
        ClaudeSession::spawn(&self.resolved, config, Some(&handle.native_handle)).await
    }

    /// Claude persists every session as `~/.claude/projects/<cwd-slug>/*.jsonl`.
    async fn list_importable_sessions(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let home = directories::BaseDirs::new()
            .map(|b| b.home_dir().to_path_buf())
            .unwrap_or_default();
        let slug = cwd.to_string_lossy().replace('/', "-");
        let dir = home.join(".claude").join("projects").join(slug);
        let mut out = vec![];
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return Ok(out),
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            let modified = entry
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs());
            out.push(ImportableSession {
                handle: PersistenceHandle {
                    provider: "claude".to_string(),
                    native_handle: stem.to_string(),
                    metadata: json!({"transcript": path.to_string_lossy()}),
                },
                title: None,
                cwd: Some(cwd.to_path_buf()),
                modified_at: modified,
            });
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

/// One pending can_use_tool ask: the normalized request plus the control
/// request_id needed to answer it.
struct PendingAsk {
    request: PermissionRequest,
    control_id: String,
}

pub struct ClaudeSession {
    transport: Arc<NdjsonTransport>,
    caps: Capabilities,
    events: broadcast::Sender<StreamEvent>,
    /// Native session id from the init frame.
    session_id: Mutex<Option<String>>,
    /// Pending permission asks keyed by our normalized request id.
    pending_asks: Mutex<HashMap<String, PendingAsk>>,
    /// Accumulated transcript for history().
    timeline: Mutex<Vec<TimelineItem>>,
    /// Current turn id (uuid per prompt — claude has no turn concept).
    current_turn: Mutex<Option<String>>,
    /// Latest usage from the result frame.
    last_usage: Mutex<Option<Usage>>,
}

impl ClaudeSession {
    async fn spawn(
        resolved: &ResolvedBackend,
        config: SessionConfig,
        resume: Option<&str>,
    ) -> Result<Arc<dyn AgentSession>> {
        let mut args = resolved.args.clone();
        if let Some(mode) = &config.mode {
            args.push("--permission-mode".to_string());
            args.push(mode.clone());
        }
        if let Some(model) = &config.model {
            args.push("--model".to_string());
            args.push(model.clone());
        }
        if let Some(id) = resume {
            args.push("--resume".to_string());
            args.push(id.to_string());
        }
        if !config.mcp_servers.is_empty() {
            // Claude takes MCP servers as a JSON config flag.
            let servers: Value = config
                .mcp_servers
                .iter()
                .map(|(name, s)| {
                    (
                        name.clone(),
                        json!({"command": s.command, "args": s.args, "env": s.env}),
                    )
                })
                .collect::<serde_json::Map<String, Value>>()
                .into();
            args.push("--mcp-config".to_string());
            args.push(json!({"mcpServers": servers}).to_string());
        }

        let transport =
            NdjsonTransport::spawn(&resolved.command, &args, &config.env, &config.cwd).await?;

        let (events, _) = broadcast::channel(512);
        let session = Arc::new(Self {
            transport: transport.clone(),
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: true,
                dynamic_modes: true,
                mcp_servers: true,
                reasoning_stream: true,
                steer: true,
                rewind: true,
                subagent_events: true,
            },
            events,
            session_id: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            timeline: Mutex::new(Vec::new()),
            current_turn: Mutex::new(None),
            last_usage: Mutex::new(None),
        });

        // Frame dispatch task: translate stream-json → StreamEvent.
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

        // system.init arrives only when the first prompt is processed —
        // spawn returns immediately and the dispatch loop captures the
        // session_id (the resume token) when init lands.
        Ok(session as Arc<dyn AgentSession>)
    }

    /// Translate one inbound stream-json frame into normalized events.
    async fn on_frame(&self, f: Value) {
        let ty = f["type"].as_str().unwrap_or_default();
        match ty {
            "system" => self.on_system(&f).await,
            "assistant" => self.on_assistant(&f).await,
            "user" => self.on_user(&f).await,
            "result" => self.on_result(&f).await,
            "control_request" => self.on_control_request(&f).await,
            "control_response" => {
                // Answer to a control request WE sent (interrupt, set_model).
                let id = f["response"]["request_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                let resp = &f["response"]["response"];
                let result = if resp["subtype"].as_str() == Some("error")
                    || f["response"]["subtype"].as_str() == Some("error")
                {
                    Err(resp.clone())
                } else {
                    Ok(resp.clone())
                };
                self.transport.resolve(&id, result).await;
            }
            _ => {}
        }
    }

    async fn on_system(&self, f: &Value) {
        if f["subtype"].as_str() == Some("init") {
            let id = f["session_id"].as_str().unwrap_or_default().to_string();
            let mut slot = self.session_id.lock().await;
            if slot.is_none() {
                *slot = Some(id.clone());
                drop(slot);
                self.emit(StreamEvent::new(StreamEventKind::ThreadStarted {
                    native_handle: id,
                }));
            }
        }
        // Other system subtypes (hooks, thinking_tokens) are housekeeping.
    }

    async fn on_assistant(&self, f: &Value) {
        let Some(blocks) = f["message"]["content"].as_array() else {
            return;
        };
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
                    ),
                }),
                _ => TimelineItem::Unknown { raw: block.clone() },
            };
            self.emit_timeline(item).await;
        }
    }

    async fn on_user(&self, f: &Value) {
        // tool_result blocks arrive as user messages.
        let Some(blocks) = f["message"]["content"].as_array() else {
            return;
        };
        for block in blocks {
            if block["type"].as_str() != Some("tool_result") {
                continue;
            }
            let call_id = block["tool_use_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let is_error = block["is_error"].as_bool().unwrap_or(false);
            let output = match &block["content"] {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let item = TimelineItem::ToolCall(ToolCall {
                call_id,
                name: String::new(),
                status: if is_error {
                    ToolCallStatus::Failed
                } else {
                    ToolCallStatus::Completed
                },
                detail: ToolCallDetail::Unknown {
                    input: Value::Null,
                    output: Value::String(output),
                },
            });
            self.emit_timeline(item).await;
        }
    }

    async fn on_result(&self, f: &Value) {
        let usage = Usage {
            input_tokens: f["usage"]["input_tokens"].as_u64(),
            cached_input_tokens: f["usage"]["cache_read_input_tokens"].as_u64(),
            output_tokens: f["usage"]["output_tokens"].as_u64(),
            cost_usd: f["total_cost_usd"].as_f64(),
            context_window: None,
            context_used: None,
        };
        *self.last_usage.lock().await = Some(usage.clone());
        let turn = self.current_turn.lock().await.take();
        let kind = if f["is_error"].as_bool().unwrap_or(false) {
            StreamEventKind::TurnFailed {
                error: f["result"].as_str().unwrap_or("unknown error").to_string(),
                code: f["subtype"].as_str().map(|s| s.to_string()),
            }
        } else if f["subtype"].as_str() == Some("interrupted") {
            StreamEventKind::TurnCanceled {
                reason: "interrupted".to_string(),
            }
        } else {
            StreamEventKind::TurnCompleted { usage: Some(usage) }
        };
        self.emit(StreamEvent {
            turn_id: turn,
            kind,
        });
        self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
            reason: AttentionReason::Finished,
        }));
    }

    /// Agent → daemon control requests. `can_use_tool` becomes a
    /// PermissionRequest; anything else gets a minimal success response
    /// so the agent isn't blocked on a handshake we don't implement.
    async fn on_control_request(&self, f: &Value) {
        let request_id = f["request_id"].as_str().unwrap_or_default().to_string();
        let req = &f["request"];
        match req["subtype"].as_str().unwrap_or_default() {
            "can_use_tool" => {
                let tool = req["tool_name"].as_str().unwrap_or_default().to_string();
                let input = req["input"].clone();
                let ask_id = uuid::Uuid::new_v4().to_string();
                let ask = PermissionRequest {
                    id: ask_id.clone(),
                    kind: PermissionKind::Tool,
                    name: tool.clone(),
                    title: Some(format!("{tool} wants to run")),
                    input: Some(input.clone()),
                    detail: Some(map_tool_input(&tool, &input)),
                    actions: vec![
                        PermissionAction {
                            id: "allow".to_string(),
                            label: "Allow".to_string(),
                            behavior: PermissionBehavior::Allow,
                            variant: Some(ActionVariant::Primary),
                        },
                        PermissionAction {
                            id: "deny".to_string(),
                            label: "Deny".to_string(),
                            behavior: PermissionBehavior::Deny,
                            variant: Some(ActionVariant::Danger),
                        },
                    ],
                    suggestions: req["permission_suggestions"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default(),
                };
                self.pending_asks.lock().await.insert(
                    ask_id.clone(),
                    PendingAsk {
                        request: ask.clone(),
                        control_id: request_id,
                    },
                );
                self.emit(StreamEvent {
                    turn_id: self.current_turn.lock().await.clone(),
                    kind: StreamEventKind::PermissionRequested(ask),
                });
                self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                    reason: AttentionReason::Permission,
                }));
            }
            _ => {
                // Unknown control request — answer success so the agent
                // isn't stuck waiting on a handshake we don't implement.
                let _ = self
                    .transport
                    .send(json!({
                        "type": "control_response",
                        "response": {"request_id": request_id, "response": {}}
                    }))
                    .await;
            }
        }
    }

    fn emit(&self, ev: StreamEvent) {
        let _ = self.events.send(ev);
    }

    async fn emit_timeline(&self, item: TimelineItem) {
        self.timeline.lock().await.push(item.clone());
        let turn = self.current_turn.lock().await.clone();
        self.emit(StreamEvent {
            turn_id: turn,
            kind: StreamEventKind::Timeline(item),
        });
    }
}

#[async_trait]
impl AgentSession for ClaudeSession {
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
            PromptInput::Text(t) => json!(t),
            PromptInput::Blocks(blocks) => json!(
                blocks
                    .iter()
                    .map(|b| match b {
                        PromptBlock::Text { text } => json!({"type": "text", "text": text}),
                        PromptBlock::Image { data, mime } => json!({
                            "type": "image",
                            "source": {"type": "base64", "media_type": mime, "data": data}
                        }),
                    })
                    .collect::<Vec<_>>()
            ),
        };
        self.transport
            .send(json!({
                "type": "user",
                "message": {"role": "user", "content": content}
            }))
            .await?;
        self.emit(StreamEvent::in_turn(
            turn_id.clone(),
            StreamEventKind::TurnStarted,
        ));
        Ok(turn_id)
    }

    async fn steer(&self, prompt: PromptInput, _expected_turn: &str) -> Result<SteerResult> {
        // stream-json accepts a new user message mid-turn; claude queues
        // it as steering input.
        let content = match prompt {
            PromptInput::Text(t) => json!(t),
            PromptInput::Blocks(_) => bail!("steer supports text only"),
        };
        self.transport
            .send(json!({
                "type": "user",
                "message": {"role": "user", "content": content}
            }))
            .await?;
        Ok(SteerResult::Accepted)
    }

    async fn interrupt(&self) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        request_roundtrip(
            &self.transport,
            id.clone(),
            json!({
                "type": "control_request",
                "request_id": id,
                "request": {"subtype": "interrupt"}
            }),
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
    ) -> Result<PermissionResult> {
        let Some(ask) = self.pending_asks.lock().await.remove(request_id) else {
            bail!("no pending permission {request_id}");
        };
        let resp = match response {
            PermissionResponse::Allow { updated_input, .. } => json!({
                "behavior": "allow",
                "updatedInput": updated_input.unwrap_or_else(|| ask.request.input.clone().unwrap_or(Value::Null)),
            }),
            PermissionResponse::Deny { message, .. } => json!({
                "behavior": "deny",
                "message": message.unwrap_or_else(|| "denied by user".to_string()),
            }),
        };
        self.transport
            .send(json!({
                "type": "control_response",
                "response": {"request_id": ask.control_id, "response": resp}
            }))
            .await?;
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(PermissionResult::default())
    }

    fn pending_permissions(&self) -> Vec<PermissionRequest> {
        // try_lock: this is a sync accessor on an async mutex; an empty
        // list under contention is acceptable for a status query.
        match self.pending_asks.try_lock() {
            Ok(asks) => asks.values().map(|a| a.request.clone()).collect(),
            Err(_) => vec![],
        }
    }

    async fn history(&self) -> Result<Vec<TimelineItem>> {
        Ok(self.timeline.lock().await.clone())
    }

    fn persistence_handle(&self) -> Option<PersistenceHandle> {
        let id = self.session_id.try_lock().ok()?.clone()?;
        Some(PersistenceHandle {
            provider: "claude".to_string(),
            native_handle: id,
            metadata: Value::Null,
        })
    }

    async fn set_mode(&self, mode: &str) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        request_roundtrip(
            &self.transport,
            id.clone(),
            json!({
                "type": "control_request",
                "request_id": id,
                "request": {"subtype": "set_permission_mode", "mode": mode}
            }),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModeChanged {
            mode: Some(mode.to_string()),
        }));
        Ok(())
    }

    async fn set_model(&self, model: &str) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        request_roundtrip(
            &self.transport,
            id.clone(),
            json!({
                "type": "control_request",
                "request_id": id,
                "request": {"subtype": "set_model", "model": model}
            }),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
        }));
        Ok(())
    }
}

/// Map a tool_use input to normalized detail. Unknown tools keep the raw
/// input so nothing is silently dropped.
fn map_tool_input(name: &str, input: &Value) -> ToolCallDetail {
    match name {
        "Bash" => ToolCallDetail::Shell {
            command: input["command"].as_str().unwrap_or_default().to_string(),
            output: None,
            exit_code: None,
        },
        "Read" => ToolCallDetail::Read {
            path: input["file_path"].as_str().unwrap_or_default().to_string(),
            content: None,
        },
        "Edit" | "MultiEdit" => ToolCallDetail::Edit {
            path: input["file_path"].as_str().unwrap_or_default().to_string(),
            unified_diff: None,
        },
        "Write" => ToolCallDetail::Write {
            path: input["file_path"].as_str().unwrap_or_default().to_string(),
            content: input["content"].as_str().map(|s| s.to_string()),
        },
        "Grep" | "Glob" | "WebSearch" => ToolCallDetail::Search {
            query: input["pattern"]
                .as_str()
                .or_else(|| input["query"].as_str())
                .unwrap_or_default()
                .to_string(),
            content: None,
        },
        "WebFetch" => ToolCallDetail::Fetch {
            url: input["url"].as_str().unwrap_or_default().to_string(),
            result: None,
        },
        "Task" => ToolCallDetail::SubAgent {
            description: input["description"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            log: String::new(),
        },
        "TodoWrite" => ToolCallDetail::Plan {
            text: input["todos"].to_string(),
        },
        _ => ToolCallDetail::Unknown {
            input: input.clone(),
            output: Value::Null,
        },
    }
}
