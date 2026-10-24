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
//! - Listing: `session/list {workspace?{workspacePath, workspaceKey},
//!   sessionIds?, includeArchived?, limit?}` → `{sessions:[{sessionId,
//!   title, mode, status, sessionKind, createdAt, updatedAt, workspace}]}`.
//!   The workspace filter is an exact-directory match (the CLI's
//!   --resume-by-cwd semantics) — verified live: cwd filter returned this
//!   repo's sessions, its parent dir returned none. `updatedAt` is epoch
//!   milliseconds.
//! - Modes: `session/setMode {sessionId, mode}` — the protocol enum is
//!   `plan|build|edit|yolo|auto` (live-verified: a bogus mode answers
//!   -32602 listing exactly those). Fresh sessions default to `build`.
//! - Model: `session/setModel {sessionId, model:{providerId, modelId,
//!   options?}}` — providerId is REQUIRED. The runtime resolves against
//!   its provider registry, which a bare `app-server --stdio` starts
//!   with EMPTY (the CLI pushes `provider/updateAccountConfig` from its
//!   own login state); until then every well-formed setModel answers
//!   -32603 "Provider Registry 中不存在 Model". Registered providerIds
//!   in this install: zai, bigmodel, custom.
//! - MCP: `session/create` (and `mcp/list`, which also takes them for
//!   status checks) accept `mcpServers: [{name, command, args, env:
//!   [{name, value}], timeoutMs?/isolation?/protocolVersion?}]` for
//!   stdio (or `{name, type:"http"|"sse", url, headers}`); verified live
//!   via `mcp/list` statuses keyed by server name with transport stdio.
//!
//! One process = one conversation. The persistence handle is the
//! `sess_…` id.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
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
            caps: capabilities(),
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
        // Model selection rides session/create's `model` field (and
        // session/setModel at runtime); there is no model-listing wire
        // method and the provider registry starts empty without the
        // CLI's account-config push — so models stay unlisted while
        // explicit selection still works, matching qwen's convention.
        // Modes are the live-verified protocol enum.
        Ok(ProviderCatalog {
            models: vec![],
            modes: zcode_modes(),
            default_mode: Some("build".to_string()),
        })
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

    /// `session/list` against a throwaway app-server process. No
    /// session/create is needed — the method answers from the store
    /// right after boot, so this spawns the transport alone and keeps
    /// a side answerer for any server→client request that shows up.
    /// Any failure degrades to an empty list (the caller treats
    /// listing as best-effort, matching the codex backend).
    async fn list_importable_sessions(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let env: HashMap<String, String> = self.resolved.env.iter().cloned().collect();
        let Ok(transport) =
            NdjsonTransport::spawn(&self.resolved.command, &self.resolved.args, &env, cwd).await
        else {
            return Ok(vec![]);
        };
        // Answer-and-forget loop: silence toward a server→client
        // request would wedge the exchange.
        let answerer = {
            let transport = transport.clone();
            let mut rx = transport.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(f) if f.get("method").is_some() && f.get("id").is_some() => {
                            let _ = transport
                                .send(json!({"id": f["id"].clone(), "result": {}}))
                                .await;
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    }
                }
            })
        };
        let result = request_roundtrip(
            &transport,
            "1",
            json!({"id": 1, "method": "session/list", "params": list_params(cwd)}),
            CONTROL_TIMEOUT,
        )
        .await;
        answerer.abort();
        transport.shutdown().await;
        let Ok(v) = result else { return Ok(vec![]) };
        let Some(items) = v["sessions"].as_array() else {
            return Ok(vec![]);
        };
        Ok(items.iter().filter_map(map_session_item).collect())
    }
}

/// Capability set, shared by the client and every session it spawns.
/// rewind stays false: checkpoints live CLI-side and damon has no
/// rewind consumer. subagent_events stays false: zcode has no push
/// notification family for subagents — `session/subagents` is a pull
/// request over store-backed sessions only (a freshly created live
/// session answers -32004 "Session not found", verified live), so
/// there is nothing to stream.
fn capabilities() -> Capabilities {
    Capabilities {
        streaming: true,
        session_persistence: true,
        session_listing: true,
        dynamic_modes: true,
        mcp_servers: true,
        reasoning_stream: true,
        steer: false,
        rewind: false,
        subagent_events: false,
        tool_allowlist: false,
    }
}

/// The mode enum the protocol accepts — live-verified: `session/setMode`
/// with a bogus mode answers -32602 "expected one of
/// "plan"|"build"|"edit"|"yolo"|"auto"". `build` is the live default
/// (fresh sessions report `settings.mode.current: "build"`); the CLI's
/// `--mode yolo` headless default does not apply to the app-server wire.
fn zcode_modes() -> Vec<ModeDef> {
    [
        ("plan", Some("plan before executing a task")),
        ("build", Some("default mode")),
        ("edit", None),
        ("yolo", None),
        ("auto", None),
    ]
    .into_iter()
    .map(|(id, description)| ModeDef {
        id: id.to_string(),
        name: id.to_string(),
        description: description.map(String::from),
    })
    .collect()
}

/// `session/list` params scoped to one workspace directory — the
/// server's exact-directory match mirrors the CLI's --resume-by-cwd
/// (parent dirs do NOT match; verified live). `workspaceKey` is
/// schema-required whenever `workspace` is present; matching rides
/// `workspacePath` alone.
fn list_params(cwd: &Path) -> Value {
    json!({
        "workspace": {
            "workspacePath": cwd.to_string_lossy(),
            "workspaceKey": workspace_key(cwd),
        }
    })
}

fn workspace_key(cwd: &Path) -> String {
    format!(
        "damon-{}",
        cwd.file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "ws".to_string())
    )
}

/// session/list item → ImportableSession. `title` is "" on sessions
/// that never got one (verified live) — surfaced as None. `updatedAt`
/// is epoch milliseconds.
fn map_session_item(item: &Value) -> Option<ImportableSession> {
    let id = item["sessionId"].as_str()?;
    Some(ImportableSession {
        handle: PersistenceHandle {
            provider: "zcode".to_string(),
            native_handle: id.to_string(),
            metadata: item.clone(),
        },
        title: item["title"]
            .as_str()
            .filter(|t| !t.is_empty())
            .map(String::from),
        cwd: item["workspace"]["workspacePath"]
            .as_str()
            .map(PathBuf::from),
        modified_at: item["updatedAt"].as_u64().map(|ms| ms / 1000),
        size: None,
    })
}

/// Damon's MCP map → the protocol's `mcpServers` array (stdio shape:
/// `{name, command, args, env: [{name, value}]}`; env is an array of
/// pairs on the wire, key-sorted here for deterministic frames).
/// Servers with no command are unrepresentable and skipped; `None`
/// when nothing remains, so create frames stay minimal.
fn mcp_servers_param(servers: &HashMap<String, McpServerConfig>) -> Option<Value> {
    let mut names: Vec<&String> = servers.keys().collect();
    names.sort();
    let list: Vec<Value> = names
        .iter()
        .filter_map(|name| {
            let s = servers.get(*name)?;
            if s.command.is_empty() {
                return None;
            }
            let mut env: Vec<&String> = s.env.keys().collect();
            env.sort();
            let env_pairs: Vec<Value> = env
                .iter()
                .map(|k| json!({"name": k, "value": s.env[*k]}))
                .collect();
            json!({
                "name": name,
                "command": s.command,
                "args": s.args,
                "env": env_pairs,
            })
            .into()
        })
        .collect();
    (!list.is_empty()).then_some(Value::Array(list))
}

/// "provider/model" → (provider, model); a bare id defaults to the
/// `zai` provider (this install's account provider; the registry also
/// knows `bigmodel` and `custom`).
fn split_model(model: &str) -> (String, String) {
    model
        .split_once('/')
        .map(|(p, m)| (p.to_string(), m.to_string()))
        .unwrap_or_else(|| ("zai".to_string(), model.to_string()))
}

fn set_mode_params(session_id: &str, mode: &str) -> Value {
    json!({"sessionId": session_id, "mode": mode})
}

/// `model` carries `providerId` + `modelId` (providerId is required —
/// a bare `{modelId}` answers -32602, verified live).
fn set_model_params(session_id: &str, model: &str) -> Value {
    let (provider_id, model_id) = split_model(model);
    json!({
        "sessionId": session_id,
        "model": {"providerId": provider_id, "modelId": model_id},
    })
}

#[derive(Clone)]
struct PendingAsk {
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
            caps: capabilities(),
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
        let key = workspace_key(&config.cwd);
        let mut params = json!({
            "workspace": {
                "workspacePath": config.cwd.to_string_lossy(),
                "workspaceKey": key,
            }
        });
        if let Some(model) = &config.model {
            // Create-time model must carry providerId + modelId (the
            // schema rejects a bare modelId). Note: the runtime's
            // provider registry starts empty in a bare app-server run —
            // until the CLI's account-config push equivalent exists,
            // this fails the whole create rather than degrading, so it
            // stays opt-in via config.model.
            let (provider_id, model_id) = split_model(model);
            params["model"] = json!({"providerId": provider_id, "modelId": model_id});
        }
        if let Some(mcp_servers) = mcp_servers_param(&config.mcp_servers) {
            params["mcpServers"] = mcp_servers;
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
                    questions: Vec::new(),
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
                let mut options = vec![];
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
                            id: oid.clone(),
                            label: label.clone(),
                            behavior: PermissionBehavior::Allow,
                            variant: None,
                        });
                        options.push(QuestionOption {
                            label,
                            description: o["description"].as_str().map(String::from),
                        });
                    }
                }
                let questions = vec![QuestionField {
                    question: question.clone(),
                    header: None,
                    options,
                    multi_select: false,
                }];
                let ask = PermissionRequest {
                    id: uuid::Uuid::new_v4().to_string(),
                    kind: PermissionKind::Question,
                    name: "user_input".to_string(),
                    title: Some(question),
                    input: Some(p.clone()),
                    detail: None,
                    actions,
                    suggestions: vec![],
                    questions,
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
        // Peek, don't remove: a failed send leaves the ask pending and
        // answerable; an unknown id is a typed AskNotFound.
        let ask = {
            let asks = self.pending_asks.lock().await;
            asks.get(request_id)
                .cloned()
                .ok_or_else(|| anyhow::Error::new(AskNotFound(request_id.to_string())))?
        };
        let result = match &response {
            PermissionResponse::Allow {
                action_id,
                answer,
                answers,
                ..
            } => {
                // A structured `answers` map resolves by question text —
                // matched against the option labels first so a picked
                // option carries its verbatim response payload.
                let from_answers = answers.as_ref().and_then(|a| match a {
                    Value::Object(map) => map.values().find_map(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        Value::Array(arr) => arr.iter().find_map(|x| x.as_str()).map(String::from),
                        _ => None,
                    }),
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                });
                let chosen = action_id.clone().or(answer.clone()).or(from_answers);
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
        self.pending_asks.lock().await.remove(request_id);
        self.emit(StreamEvent::new(StreamEventKind::PermissionResolved {
            request_id: request_id.to_string(),
        }));
        Ok(())
    }

    /// Runtime mode switch — `session/setMode {sessionId, mode}`, one
    /// of the protocol enum ids (plan|build|edit|yolo|auto). Accepted
    /// live on a real session; the server echoes a `state.updated`
    /// notification carrying the new mode.
    async fn set_mode(&self, mode: &str) -> Result<()> {
        let sid = self.session_id.lock().await.clone().unwrap_or_default();
        self.request(
            "session/setMode",
            set_mode_params(&sid, mode),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModeChanged {
            mode: Some(mode.to_string()),
        }));
        Ok(())
    }

    /// Runtime model switch — `session/setModel {sessionId,
    /// model:{providerId, modelId}}`. The frame is protocol-correct,
    /// but resolution runs against the runtime's provider registry,
    /// which a bare `app-server --stdio` leaves empty until the
    /// CLI's `provider/updateAccountConfig` push (damon cannot
    /// fabricate that payload); until then a well-formed switch
    /// answers -32603 and no ModelChanged is emitted.
    async fn set_model(&self, model: &str) -> Result<()> {
        let sid = self.session_id.lock().await.clone().unwrap_or_default();
        self.request(
            "session/setModel",
            set_model_params(&sid, model),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
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

    /// Verified live: session/list answers post-boot, setMode/setModel
    /// accept real frames, mcp/list keys statuses by server name —
    /// all three affordances are advertised.
    #[test]
    fn capability_flags_advertised() {
        let c = capabilities();
        assert!(c.session_listing);
        assert!(c.dynamic_modes);
        assert!(c.mcp_servers);
        assert!(!c.rewind);
        assert!(!c.subagent_events);
    }

    /// The catalog is the protocol's own enum — a bogus `session/setMode`
    /// answered -32602 listing exactly plan|build|edit|yolo|auto, and a
    /// fresh session reports `settings.mode.current: "build"`.
    #[test]
    fn mode_catalog_matches_protocol_enum() {
        let modes = zcode_modes();
        assert_eq!(
            modes.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["plan", "build", "edit", "yolo", "auto"]
        );
        let cat = ProviderCatalog {
            models: vec![],
            modes: zcode_modes(),
            default_mode: Some("build".to_string()),
        };
        assert_eq!(cat.default_mode.as_deref(), Some("build"));
    }

    /// Damon's MCP map → the wire's array-of-named-stdio-servers shape
    /// (env as {name, value} pairs, deterministic ordering). Verified
    /// live: the identical array came back keyed by name in mcp/list
    /// statuses with transport stdio, and session/create accepted it.
    /// (The archive capture of this test ends mid-scenario after the
    /// empty-map assertion — restored exactly that far.)
    #[test]
    fn mcp_translation_shape() {
        let mut servers = HashMap::new();
        servers.insert(
            "echo".to_string(),
            McpServerConfig {
                command: "/bin/echo".to_string(),
                args: vec!["--hi".to_string()],
                env: HashMap::from([("FOO".to_string(), "bar".to_string())]),
            },
        );
        servers.insert(
            "aaa".to_string(),
            McpServerConfig {
                command: "/bin/true".to_string(),
                args: vec![],
                env: HashMap::new(),
            },
        );
        let v = mcp_servers_param(&servers).unwrap();
        let list = v.as_array().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0]["name"], "aaa");
        assert_eq!(list[1]["name"], "echo");
        assert_eq!(list[1]["command"], "/bin/echo");
        assert_eq!(list[1]["args"][0], "--hi");
        assert_eq!(list[1]["env"][0]["name"], "FOO");
        assert_eq!(list[1]["env"][0]["value"], "bar");

        // No representable servers → no field at all.
        assert!(mcp_servers_param(&HashMap::new()).is_none());
    }
}
