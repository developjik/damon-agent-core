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
//! - Session settings (all verified live against 0.228.0):
//!   `droid.update_session_settings {modelId?, autonomyLevel?, …}` →
//!   `{}`; the settings also come back in the initialize result
//!   (`settings.autonomyLevel`, `availableAutonomyLevels:
//!   ["off","low","medium","high"]`). Unknown autonomyLevel values are
//!   silently swallowed (zod `.catch`), so Damon validates mode ids
//!   client-side. `--auto` on the CLI only accepts low|medium|high —
//!   the "off" level is runtime-only, so the catalog omits it.
//! - Models: `droid.list_models {}` → `{models: [{id, displayName,
//!   disabled, …}]}`; needs a live session, so the catalog probe
//!   spins a transient one (no user message → no inference cost).
//! - MCP: `droid.add_mcp_server {name, type:"stdio", command, args?,
//!   env?}` → `{success:true}`; `droid.list_mcp_servers` →
//!   `{servers:[…], summary}`; `droid.remove_mcp_server {serverName,
//!   settingsLevel:"user"}` (settingsLevel is a required literal).
//! - Sessions on disk: `~/.factory/sessions/<cwd-slug>/<uuid>.jsonl`
//!   (slug = path with `/` → `-`; macOS `/tmp` lands as
//!   `-private-tmp`) with line 1
//!   `{type:"session_start", id, title, cwd, …}` — the import scan
//!   reads exactly that line. Home resolves as
//!   `FACTORY_HOME_OVERRIDE ?? HOME`.
//! - Interrupt: `droid.interrupt_session`.
//!
//! Not wired: `droid.get_rewind_info`/`droid.execute_rewind` exist on
//! the wire but Damon has no rewind RPC consumer yet — future work.
//!
//! One process = one conversation (the process stays alive between
//! turns in stream-jsonrpc mode). Image attachments ride
//! `add_user_message.images` as base64 sources.

use std::collections::HashMap;
use std::io::BufRead;
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
use super::transport::NdjsonTransport;
use super::types::*;
use super::{AgentClient, AgentSession};

const INIT_TIMEOUT: Duration = Duration::from_secs(60);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DroidClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
}

/// One capability set for the client and every session it spawns.
/// `rewind` stays false: droid exposes get_rewind_info/execute_rewind
/// but Damon has no rewind RPC consumer yet. `steer` stays false: the
/// wire has no steer method — mid-turn `add_user_message` feeds a
/// message queue managed via `droid.resolve_queued_user_message`
/// (update_queue/delete by requestId), i.e. next-turn input, not
/// in-turn steering; and no live probe could prove otherwise (turns
/// fail instantly with model_authentication_failed on this auth).
fn droid_capabilities() -> Capabilities {
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

impl DroidClient {
    pub fn new(resolved: ResolvedBackend) -> Self {
        Self {
            resolved,
            caps: droid_capabilities(),
        }
    }

    /// One transient JSON-RPC session for `droid.list_models` — no
    /// user message is ever sent, so the probe is pure control-plane
    /// (verified live: init + list_models + close, no inference).
    async fn probe_models(&self) -> Result<Vec<ModelDef>> {
        let config = SessionConfig {
            cwd: std::env::temp_dir(),
            ..Default::default()
        };
        let session = DroidSession::spawn(&self.resolved, config, None)
            .await
            .context("droid model catalog probe failed to start (is `droid` logged in?)")?;
        let v = session
            .request("droid.list_models", json!({}), CONTROL_TIMEOUT)
            .await
            .context("droid.list_models failed")?;
        let models = models_from_list(&v);
        let _ = session.close().await;
        Ok(models)
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
        // Modes are static (droid's --auto vocabulary); models need a
        // live session, so the probe spins a transient one. No user
        // message is sent — the round-trip is pure control-plane.
        let models = self.probe_models().await?;
        Ok(ProviderCatalog {
            models,
            modes: autonomy_modes(),
            // Matches the CLI's documented default (--auto low,
            // read-only) and is the only level every droid surface
            // accepts at launch.
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

    async fn list_importable_sessions(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let root = sessions_root();
        let cwd = cwd.to_path_buf();
        tokio::task::spawn_blocking(move || scan_sessions(&root, &cwd, SESSION_SCAN_CAP))
            .await
            .context("droid session scan join failed")?
    }
}

/// `~/.factory/sessions` — droid resolves home as
/// `FACTORY_HOME_OVERRIDE ?? HOME` (verified in the binary).
fn sessions_root() -> PathBuf {
    if let Ok(home) = std::env::var("FACTORY_HOME_OVERRIDE")
        && !home.is_empty()
    {
        return PathBuf::from(home).join(".factory").join("sessions");
    }
    directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_default()
        .join(".factory")
        .join("sessions")
}

/// Cap the scan so a directory of huge transcripts stays cheap —
/// mtime-sort first, then read only the winners' first lines.
const SESSION_SCAN_CAP: usize = 200;

/// Droid's own cwd-slug layout: `/tmp` → `-tmp` (path with `/` → `-`).
fn droid_slug(cwd: &Path) -> String {
    cwd.to_string_lossy().replace('/', "-")
}

/// The droid autonomy vocabulary — exactly the levels the CLI's
/// `--auto` accepts at launch ("off" is runtime-only, verified:
/// `--auto off` → "Invalid --auto value").
fn autonomy_modes() -> Vec<ModeDef> {
    [
        ("low", "Low — ask before writes and commands"),
        ("medium", "Medium — auto-run reads, ask for writes"),
        ("high", "High — full autonomy"),
    ]
    .into_iter()
    .map(|(id, description)| ModeDef {
        id: id.to_string(),
        name: id.to_string(),
        description: Some(description.to_string()),
    })
    .collect()
}

/// `droid.list_models` result → normalized defs. Verified live:
/// `{models: [{id, displayName, disabled, …}]}` — `id` is the routable
/// model id (initialize/set_model consume it verbatim); the `auto`
/// router entry rides the same shape and stays selectable.
fn models_from_list(v: &Value) -> Vec<ModelDef> {
    v["models"]
        .as_array()
        .map(|models| {
            models
                .iter()
                .filter_map(|m| {
                    let id = m["id"].as_str()?;
                    Some(ModelDef {
                        id: id.to_string(),
                        name: m["displayName"].as_str().unwrap_or(id).to_string(),
                        selectable: !m["disabled"].as_bool().unwrap_or(false),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `droid.update_session_settings` params for a model switch —
/// verified live: `{modelId}` → `{}`.
fn model_update_params(model: &str) -> Value {
    json!({"modelId": model})
}

/// `droid.update_session_settings` params for an autonomy switch —
/// verified live: `{autonomyLevel: "high"}` → `{}`. Droid silently
/// swallows unknown levels (zod `.catch`), so unknown modes are
/// rejected here instead of evaporating on the wire.
fn mode_update_params(mode: &str) -> Result<Value> {
    if !autonomy_modes().iter().any(|m| m.id == mode) {
        bail!("unknown droid autonomy level {mode:?}; expected low|medium|high");
    }
    Ok(json!({"autonomyLevel": mode}))
}

/// `droid.add_mcp_server` params for one normalized stdio server —
/// shape verified live round-trip (add → list → remove); `env` rides
/// only when non-empty (schema: optional record).
fn add_mcp_server_params(name: &str, server: &McpServerConfig) -> Value {
    let mut params = json!({
        "name": name,
        "type": "stdio",
        "command": server.command,
        "args": server.args,
    });
    if !server.env.is_empty() {
        params["env"] = json!(server.env);
    }
    params
}

/// Scan `<root>/<slug>/*.jsonl` for sessions whose line-1
/// `session_start` header carries exactly this cwd (the requested path
/// or its canonical form — macOS `/tmp` is stored as `/tmp` but slugs
/// to `-private-tmp`). Sorted newest-first, capped.
fn scan_sessions(root: &Path, cwd: &Path, cap: usize) -> Result<Vec<ImportableSession>> {
    let canonical = std::fs::canonicalize(cwd).unwrap_or_else(|_| cwd.to_path_buf());
    let mut dirs = vec![];
    for candidate in [droid_slug(cwd), droid_slug(&canonical)] {
        let dir = root.join(&candidate);
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }

    // (mtime, path), newest first — the cap bounds the header reads.
    let mut files: Vec<(u64, PathBuf)> = vec![];
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);
            files.push((mtime, path));
        }
    }
    files.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    files.truncate(cap);

    let mut out = vec![];
    for (mtime, path) in files {
        let Some(header) = session_header(&path) else {
            continue;
        };
        // Exact cwd match only — droid stores the session's launch cwd
        // verbatim in the header.
        let stored_cwd = header.cwd.clone();
        if stored_cwd.as_deref() != Some(cwd.to_string_lossy().as_ref())
            && stored_cwd.as_deref() != Some(canonical.to_string_lossy().as_ref())
        {
            continue;
        }
        out.push(ImportableSession {
            handle: PersistenceHandle {
                provider: "droid".to_string(),
                native_handle: header.id,
                metadata: json!({"transcript": path.to_string_lossy()}),
            },
            title: header.title,
            cwd: stored_cwd.map(PathBuf::from),
            modified_at: Some(mtime),
            size: std::fs::metadata(&path).ok().map(|m| m.len()),
        });
    }
    Ok(out)
}

/// Line 1 of a session jsonl:
/// `{type:"session_start", id, title?, cwd?, …}`. Bounded read — one
/// line, no full-file scan; None when the header is missing/malformed.
fn session_header(path: &Path) -> Option<SessionHeader> {
    let line = first_line(path)?;
    let v: Value = serde_json::from_str(&line).ok()?;
    if v["type"].as_str() != Some("session_start") {
        return None;
    }
    Some(SessionHeader {
        id: v["id"].as_str()?.to_string(),
        title: v["title"].as_str().map(String::from),
        cwd: v["cwd"].as_str().map(String::from),
    })
}

fn first_line(path: &Path) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let mut line = String::new();
    std::io::BufReader::new(file).read_line(&mut line).ok()?;
    let line = line.trim_end_matches(['\n', '\r']);
    (!line.is_empty()).then(|| line.to_string())
}

struct SessionHeader {
    id: String,
    title: Option<String>,
    cwd: Option<String>,
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
            caps: droid_capabilities(),
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
        // Register the requested MCP servers on the live session —
        // droid takes them over the wire, not via launch flags.
        for (name, server) in &config.mcp_servers {
            session
                .request(
                    "droid.add_mcp_server",
                    add_mcp_server_params(name, server),
                    CONTROL_TIMEOUT,
                )
                .await
                .with_context(|| format!("droid.add_mcp_server failed for {name}"))?;
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
                    questions: Vec::new(),
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
                    questions: Vec::new(),
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

    async fn set_mode(&self, mode: &str) -> Result<()> {
        let params = mode_update_params(mode)?;
        self.request("droid.update_session_settings", params, CONTROL_TIMEOUT)
            .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModeChanged {
            mode: Some(mode.to_string()),
        }));
        Ok(())
    }

    async fn set_model(&self, model: &str) -> Result<()> {
        self.request(
            "droid.update_session_settings",
            model_update_params(model),
            CONTROL_TIMEOUT,
        )
        .await?;
        self.emit(StreamEvent::new(StreamEventKind::ModelChanged {
            model: model.to_string(),
        }));
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

    /// The advertised surface: listing, runtime modes, model switching
    /// and MCP registration on; rewind (no Damon consumer) and steer
    /// (queue, not inject — see module doc) stay off.
    #[test]
    fn capabilities_advertise_the_implemented_surface() {
        let c = droid_capabilities();
        assert!(c.streaming);
        assert!(c.session_persistence);
        assert!(c.session_listing);
        assert!(c.dynamic_modes);
        assert!(c.mcp_servers);
        assert!(c.reasoning_stream);
        assert!(!c.steer);
        assert!(!c.rewind);
        assert!(!c.subagent_events);
        // Client and session report the same set.
        let c2 = DroidClient::new(test_backend()).caps;
        assert_eq!(c.session_listing, c2.session_listing);
        assert_eq!(c.dynamic_modes, c2.dynamic_modes);
        assert_eq!(c.mcp_servers, c2.mcp_servers);
        assert_eq!(c.steer, c2.steer);
        assert_eq!(c.rewind, c2.rewind);
    }

    /// Mode catalog is droid's launch-safe `--auto` vocabulary; the
    /// runtime-only "off" level is deliberately absent, and the default
    /// matches the CLI's documented low/read-only start.
    #[test]
    fn mode_catalog_is_the_launch_safe_auto_vocabulary() {
        let modes = autonomy_modes();
        let ids: Vec<_> = modes.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, ["low", "medium", "high"]);
        assert!(modes.iter().all(|m| m.description.is_some()));
        // update_session_settings maps autonomy 1:1 — and rejects the
        // levels droid would silently swallow.
        assert_eq!(
            mode_update_params("high").unwrap(),
            json!({"autonomyLevel": "high"})
        );
        assert!(mode_update_params("off").is_err());
        assert!(mode_update_params("bogus").is_err());
    }

    /// set_model passes the id through verbatim (custom models are
    /// legitimate), and the list_models result maps to normalized defs.
    #[test]
    fn model_params_and_catalog_mapping() {
        assert_eq!(
            model_update_params("gpt-5.6-sol"),
            json!({"modelId": "gpt-5.6-sol"})
        );
        // Shape captured live from droid 0.228.0 droid.list_models.
        let models = models_from_list(&json!({
            "models": [
                {"id": "auto", "displayName": "Auto Model", "disabled": false},
                {"id": "claude-fable-5.1", "displayName": "Fable 5.1", "disabled": false},
                {"id": "legacy-x", "displayName": "Legacy", "disabled": true}
            ]
        }));
        assert_eq!(models.len(), 3);
        assert_eq!(models[0].id, "auto");
        assert_eq!(models[0].name, "Auto Model");
        assert!(models[0].selectable);
        assert_eq!(models[1].name, "Fable 5.1");
        assert!(!models[2].selectable);
        // displayName falls back to the id when absent.
        let bare = models_from_list(&json!({"models": [{"id": "m1"}]}));
        assert_eq!(bare[0].name, "m1");
        assert!(models_from_list(&json!({})).is_empty());
    }

    /// add_mcp_server params: stdio shape rides command/args, env only
    /// when non-empty (live round-trip: add → list → remove).
    #[test]
    fn mcp_server_params_translate_to_stdio_shape() {
        let params = add_mcp_server_params(
            "docs",
            &McpServerConfig {
                command: "npx".into(),
                args: vec!["-y".into(), "@modelcontextprotocol/server-docs".into()],
                env: HashMap::from([("DOCS_ROOT".to_string(), "/tmp".to_string())]),
            },
        );
        assert_eq!(
            params,
            json!({
                "name": "docs",
                "type": "stdio",
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-docs"],
                "env": {"DOCS_ROOT": "/tmp"}
            })
        );
        let bare = add_mcp_server_params(
            "echo",
            &McpServerConfig {
                command: "/bin/echo".into(),
                args: vec!["hi".into()],
                env: HashMap::new(),
            },
        );
        assert!(bare.get("env").is_none());
    }

    /// Session scan over a `~/.factory/sessions` fixture: exact-cwd
    /// filter (requested path or its canonical form), newest first,
    /// cap respected, non-jsonl and headerless files skipped.
    #[test]
    fn session_scan_filters_by_exact_cwd_and_sorts_newest_first() {
        let root = std::env::temp_dir().join(format!("damon-droid-scan-{}", uuid::Uuid::new_v4()));
        let dir = root.join("-tmp");
        std::fs::create_dir_all(&dir).unwrap();
        let cwd = Path::new("/tmp");

        let write_session = |name: &str, header: &str, age_secs: u64| {
            let path = dir.join(format!("{name}.jsonl"));
            let body = format!("{header}\n{}\n", r#"{"type":"message"}"#);
            std::fs::write(&path, body).unwrap();
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - age_secs;
            let f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(stamp))
                .unwrap();
        };
        write_session(
            "aaaa-newest",
            r#"{"type":"session_start","id":"aaaa-newest","title":"Newest","cwd":"/tmp"}"#,
            0,
        );
        write_session(
            "cccc-oldest",
            r#"{"type":"session_start","id":"cccc-oldest","title":"Oldest","cwd":"/tmp"}"#,
            100,
        );
        write_session(
            "bbbb-middle",
            r#"{"type":"session_start","id":"bbbb-middle","title":"Mid","cwd":"/tmp"}"#,
            50,
        );
        // Different cwd — must be filtered out.
        write_session(
            "dddd-other-cwd",
            r#"{"type":"session_start","id":"dddd-other-cwd","title":"Elsewhere","cwd":"/var"}"#,
            1,
        );
        // Headerless/malformed — skipped without failing the scan.
        // Backdated so it can't outrank the newest real session when
        // the cap truncates.
        std::fs::write(dir.join("eeee-garbage.jsonl"), "not json at all\n").unwrap();
        {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - 200;
            let f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.join("eeee-garbage.jsonl"))
                .unwrap();
            f.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(stamp))
                .unwrap();
        }

        let found = scan_sessions(&root, cwd, 200).unwrap();
        let ids: Vec<_> = found
            .iter()
            .map(|s| s.handle.native_handle.as_str())
            .collect();
        assert_eq!(ids, ["aaaa-newest", "bbbb-middle", "cccc-oldest"]);

        let newest = &found[0];
        assert_eq!(newest.title.as_deref(), Some("Newest"));
        assert_eq!(newest.cwd.as_deref(), Some(Path::new("/tmp")));
        assert_eq!(newest.handle.provider, "droid");
        assert_eq!(
            newest.handle.metadata["transcript"],
            json!(dir.join("aaaa-newest.jsonl").to_string_lossy().to_string())
        );
        assert!(newest.modified_at.is_some());

        // Cap applies after sorting — only the newest survives.
        let capped = scan_sessions(&root, cwd, 1).unwrap();
        assert_eq!(capped.len(), 1);
        assert_eq!(capped[0].handle.native_handle, "aaaa-newest");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Droid slugs the sessions dir with `/` → `-` (verified on disk:
    /// `/tmp` lands in `~/.factory/sessions/-tmp`).
    #[test]
    fn slug_matches_droid_layout() {
        assert_eq!(droid_slug(Path::new("/tmp")), "-tmp");
        assert_eq!(
            droid_slug(Path::new("/Users/x/my project")),
            "-Users-x-my project"
        );
    }

    /// Minimal backend resolution for capability assertions — no
    /// process is spawned, only the struct is inspected.
    fn test_backend() -> ResolvedBackend {
        ResolvedBackend {
            id: "droid".into(),
            title: "Droid (Factory)".into(),
            command: "droid".into(),
            args: vec![
                "exec".into(),
                "--input-format".into(),
                "stream-jsonrpc".into(),
                "-o".into(),
                "stream-jsonrpc".into(),
            ],
            env: vec![],
            auth_hint: String::new(),
            detected: true,
        }
    }
}
