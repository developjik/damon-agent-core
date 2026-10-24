//! Codex backend — drives `codex app-server`, the JSON-RPC 2.0 interface
//! that powers the Codex VS Code extension.
//!
//! Wire shape (per developers.openai.com/codex/app-server):
//! - stdio JSONL, JSON-RPC 2.0 WITHOUT the `"jsonrpc"` header on the wire.
//! - Handshake: `initialize` request → response, then `initialized`
//!   notification. Requests before it are rejected.
//! - `thread/start` {cwd, model?, approvalPolicy?, sandbox?, config?} →
//!   `{thread: {id}}`; `thread/resume` reattaches. `sandbox` is a
//!   kebab-case `SandboxMode` string ("read-only" | "workspace-write" |
//!   "danger-full-access"); `config` is a free-form config.toml override
//!   object — MCP servers ride in under `mcp_servers.<name>`.
//! - `turn/start` {threadId, input:[{type:"text",text}]} → streams
//!   `turn/started`, `item/started`, `item/completed`,
//!   `item/agentMessage/delta`, `turn/completed` notifications. It also
//!   accepts overrides that hold "for this turn and subsequent turns":
//!   `model`, `approvalPolicy`, and `sandboxPolicy` — the struct-shaped
//!   `{"type": "readOnly" | "workspaceWrite" | "dangerFullAccess"}`.
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
            caps: codex_capabilities(),
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
            // Damon-side modes: each id maps to an approvalPolicy +
            // sandbox/sandboxPolicy pair applied on thread/start and
            // every turn/start (codex has no native mode entity).
            modes: codex_catalog_modes(),
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
                    size: None,
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
    /// Last-seen `thread/tokenUsage/updated` payload — newer app-servers
    /// report usage only there, not on `turn/completed`.
    last_usage: parking_lot::Mutex<Option<Usage>>,
    /// Active mode id (SessionConfig.mode / `set_mode`) — applied as
    /// approvalPolicy + sandbox on `thread/start` and as approvalPolicy +
    /// sandboxPolicy on every subsequent `turn/start`.
    mode: Mutex<Option<String>>,
    /// Model override from `set_model` — sent as `model` on every
    /// subsequent `turn/start`.
    pending_model: Mutex<Option<String>>,
}

impl CodexSession {
    async fn spawn(resolved: &ResolvedBackend, config: SessionConfig) -> Result<Arc<Self>> {
        let env: HashMap<String, String> = resolved.env.iter().cloned().collect();
        let transport =
            NdjsonTransport::spawn(&resolved.command, &resolved.args, &env, &config.cwd).await?;

        let (events, _) = broadcast::channel(512);
        let session = Arc::new(Self {
            transport: transport.clone(),
            caps: codex_capabilities(),
            events,
            next_id: AtomicU64::new(1),
            thread_id: Mutex::new(None),
            current_turn: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            open_items: Mutex::new(HashMap::new()),
            last_usage: parking_lot::Mutex::new(None),
            mode: Mutex::new(None),
            pending_model: Mutex::new(None),
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
            // Seed the mode state so every turn/start re-applies it;
            // unknown ids defer to codex's own defaults.
            if let Some(mode) = config.mode.as_deref().filter(|m| codex_mode(m).is_some()) {
                *session.mode.lock().await = Some(mode.to_string());
            }
            let result = session
                .request(
                    "thread/start",
                    codex_thread_start_params(&config),
                    INIT_TIMEOUT,
                )
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
                        // Newer app-servers carry no usage on turn/completed
                        // — it arrives via thread/tokenUsage/updated and is
                        // stashed in last_usage. Prefer whichever is present.
                        usage: codex_usage(params).or_else(|| self.last_usage.lock().clone()),
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
            "thread/tokenUsage/updated" => {
                // Live usage reporting (the only usage source on newer
                // app-servers): stash the latest snapshot; the
                // completing turn picks it up.
                let payload = params
                    .get("usage")
                    .filter(|u| u.is_object())
                    .unwrap_or(params);
                if let Some(u) = token_usage_event(payload) {
                    *self.last_usage.lock() = Some(u);
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
            questions: Vec::new(),
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

/// Capability flags advertised to Damon. Every `true` is backed by
/// implemented wire support covered by a unit test; rewind stays off —
/// Damon has no rewind RPC consumer.
fn codex_capabilities() -> Capabilities {
    Capabilities {
        streaming: true,
        session_persistence: true,
        session_listing: true,
        // Damon-side modes translated to approvalPolicy +
        // sandbox/sandboxPolicy on thread/start and turn/start.
        dynamic_modes: true,
        // thread/start `config` carries config.toml overrides; servers
        // land under mcp_servers.<name> exactly as `codex mcp add`
        // would write them.
        mcp_servers: true,
        reasoning_stream: true,
        steer: true,
        rewind: false,
        subagent_events: false,
        tool_allowlist: false,
    }
}

/// Translate normalized MCP servers into the free-form `config` override
/// carried by `thread/start` — same keys as config.toml, so servers land
/// under `mcp_servers.<name> = {command, args, env}` exactly as
/// `codex mcp add <name> -- <cmd…>` writes them. Empty args/env are
/// dropped (codex defaults them); `None` means send no `config` key.
fn codex_mcp_config(mcp_servers: &HashMap<String, McpServerConfig>) -> Option<Value> {
    if mcp_servers.is_empty() {
        return None;
    }
    let servers: serde_json::Map<String, Value> = mcp_servers
        .iter()
        .map(|(name, cfg)| {
            let mut entry = serde_json::Map::new();
            entry.insert("command".into(), json!(cfg.command));
            if !cfg.args.is_empty() {
                entry.insert("args".into(), json!(cfg.args));
            }
            if !cfg.env.is_empty() {
                entry.insert("env".into(), json!(cfg.env));
            }
            (name.clone(), Value::Object(entry))
        })
        .collect();
    Some(json!({"mcp_servers": Value::Object(servers)}))
}

/// Resolve a Damon mode id to its codex wire forms: the `approvalPolicy`
/// plus the sandbox in both spellings — `thread/start` takes the
/// kebab-case `SandboxMode` string, `turn/start` the struct-shaped
/// `SandboxPolicy` with a camelCase type tag. Unknown ids → `None`
/// (callers leave the params untouched).
fn codex_mode(mode: &str) -> Option<(&'static str, &'static str, Value)> {
    Some(match mode {
        // Default: ask when needed, writes confined to the workspace.
        "default" => (
            "on-request",
            "workspace-write",
            json!({"type": "workspaceWrite"}),
        ),
        // Read-only: still ask, but the sandbox denies writes outright.
        "read-only" => ("on-request", "read-only", json!({"type": "readOnly"})),
        // Full access: no approvals, no sandbox.
        "full-access" => (
            "never",
            "danger-full-access",
            json!({"type": "dangerFullAccess"}),
        ),
        _ => return None,
    })
}

/// The Damon-facing mode catalog. Codex has no native mode list — ids
/// resolve through [`codex_mode`] to the approval/sandbox pair applied
/// on `thread/start` and every `turn/start`.
fn codex_catalog_modes() -> Vec<ModeDef> {
    [
        (
            "default",
            "Default",
            "Ask for approvals; writes confined to the workspace",
        ),
        (
            "read-only",
            "Read-only",
            "Ask for approvals; the sandbox denies file writes",
        ),
        ("full-access", "Full access", "No approvals, no sandbox"),
    ]
    .into_iter()
    .map(|(id, name, description)| ModeDef {
        id: id.to_string(),
        name: name.to_string(),
        description: Some(description.to_string()),
    })
    .collect()
}

/// Build `thread/start` params: cwd plus any SessionConfig overrides —
/// model, MCP servers (via the `config` object) and, when a mode is set
/// before the first turn, its approvalPolicy + sandbox.
fn codex_thread_start_params(config: &SessionConfig) -> Value {
    let mut params = json!({});
    if let Some(model) = &config.model {
        params["model"] = json!(model);
    }
    params["cwd"] = json!(config.cwd.to_string_lossy());
    if let Some(overrides) = codex_mcp_config(&config.mcp_servers) {
        params["config"] = overrides;
    }
    if let Some(mode) = config.mode.as_deref()
        && let Some((approval, sandbox, _)) = codex_mode(mode)
    {
        params["approvalPolicy"] = json!(approval);
        params["sandbox"] = json!(sandbox);
    }
    params
}

/// Build `turn/start` params, re-applying the session's model and mode
/// state. Codex persists each override "for this turn and subsequent
/// turns" thread-side, but re-sending keeps Damon's state authoritative
/// across switches and thread resumes.
fn codex_turn_params(
    thread_id: &str,
    input: Vec<Value>,
    model: Option<&str>,
    mode: Option<&str>,
) -> Value {
    let mut params = json!({"threadId": thread_id, "input": input});
    if let Some(model) = model {
        params["model"] = json!(model);
    }
    if let Some(mode) = mode
        && let Some((approval, _, policy)) = codex_mode(mode)
    {
        params["approvalPolicy"] = json!(approval);
        params["sandboxPolicy"] = policy;
    }
    params
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

/// Parse a `thread/tokenUsage/updated` payload into [`Usage`]. Key
/// spelling varies across app-server revisions (snake_case vs
/// camelCase token counters, optional `modelContextWindow`) — the
/// parser accepts both rather than pinning one revision.
fn token_usage_event(v: &Value) -> Option<Usage> {
    let get = |names: &[&str]| -> Option<u64> {
        names.iter().find_map(|n| v.get(*n).and_then(Value::as_u64))
    };
    let input = get(&["input_tokens", "inputTokens", "input"]);
    let output = get(&["output_tokens", "outputTokens", "output"]);
    if input.is_none() && output.is_none() {
        return None;
    }
    Some(Usage {
        input_tokens: input,
        cached_input_tokens: get(&["cached_input_tokens", "cachedTokens", "cached"]),
        output_tokens: output,
        cost_usd: None,
        context_window: get(&[
            "modelContextWindow",
            "model_context_window",
            "contextWindow",
        ]),
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
        let model = self.pending_model.lock().await.clone();
        let mode = self.mode.lock().await.clone();
        let result = self
            .request(
                "turn/start",
                codex_turn_params(&thread_id, input, model.as_deref(), mode.as_deref()),
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

    /// Modes are client-side state: validated against the catalog,
    /// stored, then applied on every subsequent `turn/start` — and on
    /// `thread/start` when carried in via SessionConfig before the
    /// first turn.
    async fn set_mode(&self, mode: &str) -> Result<()> {
        if codex_mode(mode).is_none() {
            bail!("unknown codex mode: {mode}");
        }
        *self.mode.lock().await = Some(mode.to_string());
        self.emit(StreamEvent::new(StreamEventKind::ModeChanged {
            mode: Some(mode.to_string()),
        }));
        Ok(())
    }

    async fn set_model(&self, model: &str) -> Result<()> {
        *self.pending_model.lock().await = Some(model.to_string());
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
        }));
        Ok(())
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
            answers: None,
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
            answers: None,
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

    #[test]
    fn mcp_config_translates_servers_to_toml_shape() {
        let mut env = HashMap::new();
        env.insert("API_KEY".to_string(), "sk-1".to_string());
        let mut servers = HashMap::new();
        servers.insert(
            "context7".to_string(),
            McpServerConfig {
                command: "npx".to_string(),
                args: vec!["-y".to_string(), "@upstash/context7-mcp".to_string()],
                env,
            },
        );
        let config = codex_mcp_config(&servers).expect("config expected");
        let server = &config["mcp_servers"]["context7"];
        assert_eq!(server["command"], json!("npx"));
        assert_eq!(server["args"], json!(["-y", "@upstash/context7-mcp"]));
        assert_eq!(server["env"], json!({"API_KEY": "sk-1"}));
    }

    #[test]
    fn mcp_config_drops_empty_args_and_env() {
        let mut servers = HashMap::new();
        servers.insert(
            "bare".to_string(),
            McpServerConfig {
                command: "uvx".to_string(),
                ..Default::default()
            },
        );
        let config = codex_mcp_config(&servers).expect("config expected");
        assert_eq!(config["mcp_servers"]["bare"], json!({"command": "uvx"}));
    }

    #[test]
    fn mcp_config_without_servers_sends_no_config_key() {
        assert!(codex_mcp_config(&HashMap::new()).is_none());
    }

    #[test]
    fn modes_map_to_approval_policy_and_sandbox_forms() {
        // thread/start takes the kebab-case SandboxMode string; turn/start
        // takes the struct-shaped SandboxPolicy.
        assert_eq!(
            codex_mode("default"),
            Some((
                "on-request",
                "workspace-write",
                json!({"type": "workspaceWrite"})
            ))
        );
        assert_eq!(
            codex_mode("read-only"),
            Some(("on-request", "read-only", json!({"type": "readOnly"})))
        );
        assert_eq!(
            codex_mode("full-access"),
            Some((
                "never",
                "danger-full-access",
                json!({"type": "dangerFullAccess"})
            ))
        );
        // Not in the catalog → no override, codex defaults apply.
        assert!(codex_mode("plan").is_none());
    }

    #[test]
    fn mode_switch_mid_session_changes_turn_overrides() {
        let before = codex_turn_params("th1", vec![], None, Some("default"));
        assert_eq!(before["approvalPolicy"], json!("on-request"));
        assert_eq!(before["sandboxPolicy"], json!({"type": "workspaceWrite"}));

        // set_mode("full-access") then the next turn/start:
        let after = codex_turn_params("th1", vec![], None, Some("full-access"));
        assert_eq!(after["approvalPolicy"], json!("never"));
        assert_eq!(after["sandboxPolicy"], json!({"type": "dangerFullAccess"}));
    }

    #[test]
    fn pending_model_override_applies_at_turn_start() {
        let params = codex_turn_params(
            "th1",
            vec![json!({"type": "text", "text": "hi"})],
            Some("gpt-5.2"),
            None,
        );
        assert_eq!(params["threadId"], json!("th1"));
        assert_eq!(params["model"], json!("gpt-5.2"));
        assert_eq!(params["input"], json!([{"type": "text", "text": "hi"}]));

        // Nothing pending → none of the override keys appear.
        let plain = codex_turn_params("th1", vec![], None, None);
        assert!(plain.get("model").is_none());
        assert!(plain.get("approvalPolicy").is_none());
        assert!(plain.get("sandboxPolicy").is_none());
    }

    #[test]
    fn thread_start_params_carry_model_mcp_and_mode() {
        let mut servers = HashMap::new();
        servers.insert(
            "fs".to_string(),
            McpServerConfig {
                command: "npx".to_string(),
                args: vec![
                    "-y".to_string(),
                    "@modelcontextprotocol/server-fs".to_string(),
                ],
                env: HashMap::new(),
            },
        );
        let config = SessionConfig {
            cwd: PathBuf::from("/tmp/proj"),
            model: Some("gpt-5.2".to_string()),
            mode: Some("read-only".to_string()),
            mcp_servers: servers,
            ..Default::default()
        };
        let params = codex_thread_start_params(&config);
        assert_eq!(params["cwd"], json!("/tmp/proj"));
        assert_eq!(params["model"], json!("gpt-5.2"));
        assert_eq!(params["approvalPolicy"], json!("on-request"));
        assert_eq!(params["sandbox"], json!("read-only"));
        assert_eq!(
            params["config"]["mcp_servers"]["fs"],
            json!({"command": "npx", "args": ["-y", "@modelcontextprotocol/server-fs"]})
        );
    }

    #[test]
    fn catalog_lists_three_modes_all_resolving_to_wire_forms() {
        let modes = codex_catalog_modes();
        let ids: Vec<&str> = modes.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["default", "read-only", "full-access"]);
        assert!(modes.iter().all(|m| codex_mode(&m.id).is_some()));
    }

    #[test]
    fn capabilities_advertise_only_tested_wire_support() {
        let caps = codex_capabilities();
        assert!(caps.dynamic_modes);
        assert!(caps.mcp_servers);
        // Damon has no rewind RPC consumer — the flag stays off.
        assert!(!caps.rewind);
    }
}
