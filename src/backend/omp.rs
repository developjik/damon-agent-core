//! pi-family backend — drives `omp --mode rpc` / `pi --mode rpc`, the
//! newline-delimited JSON protocol over stdio the two CLIs share.
//!
//! Wire shape (observed against omp 18.2.6 + rpc.md):
//! - stdout opens with `{"type":"ready","protocolVersion":1,
//!   "supportedProtocolVersions":[1,2],"maxFrameBytes":…}` — when v2 is
//!   advertised we answer `negotiate_protocol` so oversized frames
//!   arrive as `rpc_chunk` sequences we reassemble (a fork addition;
//!   plain pi advertises v1 only and the negotiation is skipped).
//! - Commands: `{id, type:"prompt"|"abort"|"new_session"|"set_model"|…}`;
//!   responses are `{id, type:"response", command, success, data|error}`.
//! - Events: `agent_start`, `turn_start`, `message_start`,
//!   `message_update` (assistantMessageEvent deltas), `message_end`,
//!   `turn_end`, `agent_end{messages, isTerminal}`, `tool_execution_*`,
//!   `extension_ui_request` (confirm/select/input → permission asks).
//! - `prompt` is acked immediately; omp settles a turn on `agent_end`
//!   with `isTerminal !== false`, while pi additionally emits
//!   `agent_settled` as its full settlement and its bare `agent_end`
//!   frames (no flag, before retries) stay provisional — the
//!   [`PiFamily::strict_terminal`] knob.
//!
//! One process = one session. Resume uses `switch_session` with the
//! session file path recorded in the persistence handle.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Weak;
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

/// Per-CLI profile of the shared rpc wire — where pi and its omp fork
/// diverge, expressed as data so one session implementation drives both.
pub struct PiFamily {
    /// Damon-facing provider id (routing + persistence handles).
    pub id: &'static str,
    /// omp opens with a `ready` frame and answers negotiate_protocol
    /// (the v2 chunking is a fork addition). pi 0.7x emits no ready
    /// frame and silently ignores negotiate_protocol — commands work
    /// immediately, so sending it would hang the spawn.
    pub handshakes: bool,
    /// pi emits `agent_end` without `isTerminal` before model retries;
    /// with this set, only an explicit `isTerminal: true` (or pi's
    /// `agent_settled`) settles the turn. omp always terminalizes its
    /// `agent_end`, so it keeps the lenient default.
    pub strict_terminal: bool,
    /// omp takes `--approval-mode yolo|write` at launch; pi (0.8x) has
    /// no approval switches — its mode list is honest about that.
    pub has_mode_args: bool,
    /// omp's rpc raises subagent lifecycle frames; plain pi's tool set
    /// does not surface them the same way, so the capability stays off
    /// until verified.
    pub subagent_events: bool,
    /// Damon permission modes the CLI accepts (launch-time only — the
    /// rpc protocol cannot change approval at runtime).
    pub modes: &'static [&'static str],
    /// On-disk agent dir holding `sessions/<cwd-slug>/<ts>_<ulid>.jsonl`
    /// transcripts — the listing scan root. Env overrides win over the
    /// `~/` default (skills_hub::targets resolution, shared with the hub).
    pub sessions_home: fn() -> Option<PathBuf>,
    pub default_mode: Option<&'static str>,
}

/// Oh My Pi (verified against omp 18.2.6).
pub const OMP: PiFamily = PiFamily {
    id: "omp",
    handshakes: true,
    strict_terminal: false,
    has_mode_args: true,
    subagent_events: true,
    modes: &["default", "acceptEdits", "bypassPermissions"],
    sessions_home: crate::skills_hub::targets::omp_home,
    default_mode: Some("default"),
};

/// pi (pi.dev, verified against 0.73.1: no ready frame, unknown
/// commands silently ignored, get_state answers with sessionFile).
/// Permission asks ride extension dialogs only — pi's own tools run
/// under its trust model, so Damon surfaces no mode switches for it.
pub const PI: PiFamily = PiFamily {
    id: "pi",
    handshakes: false,
    strict_terminal: true,
    has_mode_args: false,
    subagent_events: false,
    modes: &["default"],
    sessions_home: crate::skills_hub::targets::pi_home,
    default_mode: Some("default"),
};

pub struct OmpClient {
    resolved: ResolvedBackend,
    caps: Capabilities,
    profile: &'static PiFamily,
}

impl OmpClient {
    pub fn new(resolved: ResolvedBackend) -> Self {
        Self::with_profile(resolved, &OMP)
    }

    pub fn with_profile(resolved: ResolvedBackend, profile: &'static PiFamily) -> Self {
        Self {
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: true,
                dynamic_modes: false,
                mcp_servers: false,
                reasoning_stream: true,
                steer: true,
                rewind: false,
                subagent_events: profile.subagent_events,
                tool_allowlist: false,
            },
            resolved,
            profile,
        }
    }
}

#[async_trait]
impl AgentClient for OmpClient {
    fn provider(&self) -> &str {
        self.profile.id
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
            self.profile,
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
            // The RPC protocol has no set_approval_mode command, so modes
            // are launch-time only (see mode_args below) — hence
            // dynamic_modes: false.
            modes: self
                .profile
                .modes
                .iter()
                .map(|m| ModeDef {
                    id: m.to_string(),
                    name: m.to_string(),
                    description: None,
                })
                .collect(),
            default_mode: self.profile.default_mode.map(|m| m.to_string()),
        })
    }

    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>> {
        Ok(OmpSession::spawn(&self.resolved, self.profile, config).await? as Arc<dyn AgentSession>)
    }

    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>> {
        let session = OmpSession::spawn(&self.resolved, self.profile, config).await?;
        session
            .command(
                "switch_session",
                json!({"type": "switch_session", "sessionPath": handle.native_handle}),
                CONTROL_TIMEOUT,
            )
            .await?;
        Ok(session as Arc<dyn AgentSession>)
    }

    /// Native sessions for this cwd, scanned off disk. Storage shape
    /// (verified against omp 18.6.0 / pi 0.73.1):
    /// `<agent-dir>/sessions/<cwd-slug>/<timestamp>_<ulid>.jsonl`, where
    /// the slug is the absolute cwd with `/` → `-` and the file's own
    /// path is the resume token `switch_session` consumes. The scan runs
    /// on the blocking pool; a missing agent/slug dir simply yields none.
    async fn list_importable_sessions(&self, cwd: &Path) -> Result<Vec<ImportableSession>> {
        let Some(agent_dir) = (self.profile.sessions_home)() else {
            return Ok(vec![]);
        };
        let provider = self.profile.id;
        let cwd = cwd.to_path_buf();
        tokio::task::spawn_blocking(move || scan_importable_sessions(&agent_dir, &cwd, provider))
            .await
            .context("session scan join failed")
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
/// own `tools.approvalMode` setting. pi has no approval switches at all.
fn mode_args(profile: &PiFamily, mode: Option<&str>) -> Vec<String> {
    if !profile.has_mode_args {
        return vec![];
    }
    match mode {
        Some("bypassPermissions") => ["--approval-mode", "yolo"],
        Some("acceptEdits") => ["--approval-mode", "write"],
        _ => return vec![],
    }
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Result cap per scan — bounds work on a large session library.
const IMPORT_SCAN_LIMIT: usize = 200;
/// Transcript head inspected for the `{"type":"session"}` header line.
const SESSION_HEAD_LINES: usize = 8;

/// Sessions on disk for `cwd` (sync — runs inside `spawn_blocking`).
/// Only top-level `<timestamp>_<ulid>.jsonl` files of the cwd's slug dir
/// count: nested subdirectories hold subagent task transcripts
/// (`<Task>.jsonl`), and omp keeps same-stem directories beside the
/// session files. The parsed `cwd` is matched exactly against the
/// argument — slugs collide (`/work/project` and `/work-project` both
/// slug to `-work-project`), so the header check is what makes the
/// listing honest. Ordered newest-first.
fn scan_importable_sessions(
    agent_dir: &Path,
    cwd: &Path,
    provider: &str,
) -> Vec<ImportableSession> {
    let dir = agent_dir
        .join("sessions")
        .join(cwd.to_string_lossy().replace('/', "-"));
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return vec![];
    };
    let mut out: Vec<ImportableSession> = Vec::new();
    for entry in entries.flatten() {
        if out.len() >= IMPORT_SCAN_LIMIT {
            break;
        }
        let path = entry.path();
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !is_session_file_name(name) {
            continue;
        }
        let Some((session_cwd, title)) = session_head(&path) else {
            continue;
        };
        if session_cwd.as_path() != cwd {
            continue;
        }
        let meta = entry.metadata().ok();
        let modified_at = meta
            .as_ref()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        out.push(ImportableSession {
            handle: PersistenceHandle {
                provider: provider.to_string(),
                native_handle: path.to_string_lossy().into_owned(),
                metadata: Value::Null,
            },
            title,
            cwd: Some(cwd.to_path_buf()),
            modified_at,
            size: meta.as_ref().map(|m| m.len()),
        });
    }
    out.sort_by_key(|s| std::cmp::Reverse(s.modified_at));
    out
}

/// `2026-09-20T15-20-30-799Z_01a09ba1-….jsonl` — real sessions open with
/// a date-time prefix before the `_`; task-named transcripts don't.
fn is_session_file_name(name: &str) -> bool {
    let Some((ts, _)) = name.split_once('_') else {
        return false;
    };
    let b = ts.as_bytes();
    b.len() >= 11
        && b[..4].iter().all(u8::is_ascii_digit)
        && b[4] == b'-'
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[7] == b'-'
        && b[8..10].iter().all(u8::is_ascii_digit)
        && b[10] == b'T'
}

/// The `{"type":"session"}` header of a transcript: its cwd and title.
/// The header also carries an id and timestamp, but the normalized shape
/// needs neither — the resume token is the file path and ordering uses
/// fs mtimes. Returns None for transcripts without a cwd-bearing header
/// in the bounded head (not sessions, or truncated/foreign files).
fn session_head(path: &Path) -> Option<(PathBuf, Option<String>)> {
    use std::io::{BufRead, BufReader};
    let f = std::fs::File::open(path).ok()?;
    let mut cwd = None;
    let mut title = None;
    for line in BufReader::new(f).lines().take(SESSION_HEAD_LINES) {
        let Ok(line) = line else { break };
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if v["type"] == "session" {
            cwd = v["cwd"].as_str().map(PathBuf::from);
            title = v["title"].as_str().map(String::from);
            break;
        }
    }
    cwd.map(|c| (c, title))
}

pub struct OmpSession {
    transport: Arc<NdjsonTransport>,
    caps: Capabilities,
    profile: &'static PiFamily,
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
    async fn spawn(
        resolved: &ResolvedBackend,
        profile: &'static PiFamily,
        config: SessionConfig,
    ) -> Result<Arc<Self>> {
        let env: HashMap<String, String> = resolved.env.iter().cloned().collect();
        let mut args = resolved.args.clone();
        args.extend(mode_args(profile, config.mode.as_deref()));
        let transport = NdjsonTransport::spawn(&resolved.command, &args, &env, &config.cwd).await?;

        let (events, _) = broadcast::channel(512);
        let session = Arc::new(Self {
            transport: transport.clone(),
            caps: Capabilities {
                streaming: true,
                session_persistence: true,
                session_listing: true,
                dynamic_modes: false,
                mcp_servers: false,
                reasoning_stream: true,
                steer: true,
                rewind: false,
                subagent_events: profile.subagent_events,
                tool_allowlist: false,
            },
            profile,
            events,
            next_id: AtomicU64::new(1),
            session_file: Mutex::new(None),
            current_turn: Mutex::new(None),
            pending_asks: Mutex::new(HashMap::new()),
            open_tools: Mutex::new(HashMap::new()),
            chunks: Mutex::new(HashMap::new()),
        });

        // Frame dispatch task: routes command responses/events, and
        // fails the in-flight turn when the process dies (prompt is
        // acked immediately, so agent_end would never arrive).
        {
            let me: Weak<OmpSession> = Arc::downgrade(&session);
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
                // The backend died mid-turn — agent_end never comes.
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

        // Handshake: wait for ready, then negotiate v2. pi skips both —
        // it emits no ready frame and ignores negotiate_protocol.
        if profile.handshakes {
            let ready = async {
                let mut rx = transport.subscribe();
                let mut exited = transport.exited();
                loop {
                    tokio::select! {
                        r = rx.recv() => match r {
                            Ok(f) if f["type"] == "ready" => return Ok(f),
                            Ok(_) => continue,
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => bail!("omp exited before ready"),
                        },
                        _ = exited.changed() => {
                            if *exited.borrow() {
                                bail!("omp exited before ready");
                            }
                        }
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
                // pi emits agent_end without isTerminal before model
                // retries — those stay provisional (the outcome arrives
                // with agent_settled or EOF). omp always terminalizes.
                let terminal = match (self.profile.strict_terminal, f["isTerminal"].as_bool()) {
                    (true, None | Some(false)) => false,
                    (_, t) => t.unwrap_or(true),
                };
                if terminal {
                    self.emit(StreamEvent {
                        turn_id: turn,
                        kind: StreamEventKind::TurnCompleted { usage: None },
                    });
                    self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                        reason: AttentionReason::Finished,
                    }));
                } else {
                    *self.current_turn.lock().await = turn;
                }
            }
            // pi-only: the definitive end of an agent run, emitted after
            // every retry settled.
            "agent_settled" => {
                let turn = self.current_turn.lock().await.take();
                self.emit(StreamEvent {
                    turn_id: turn,
                    kind: StreamEventKind::TurnCompleted { usage: None },
                });
                self.emit(StreamEvent::new(StreamEventKind::AttentionRequired {
                    reason: AttentionReason::Finished,
                }));
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
            // Context auto-compaction boundaries — the same normalized
            // item codex emits, so every surface renders one shape.
            // The frames carry no summary payload worth trusting (the
            // shape is UNVERIFIED against a real omp), so the boundary
            // itself is the message.
            "auto_compaction_start" | "auto_compaction_end" => {
                if let Some(item) = compaction_item(ty) {
                    self.emit_timeline(item).await;
                }
            }
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
            questions: Vec::new(),
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

/// auto_compaction frame → the Compaction timeline item it maps to
/// (None for anything else). Pure so the mapping itself stays pinned
/// by a unit test — `on_frame` just routes it.
fn compaction_item(ty: &str) -> Option<TimelineItem> {
    match ty {
        "auto_compaction_start" => Some(TimelineItem::Compaction {
            summary: "auto compaction started".to_string(),
        }),
        "auto_compaction_end" => Some(TimelineItem::Compaction {
            summary: "auto compaction completed".to_string(),
        }),
        _ => None,
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
            provider: self.profile.id.to_string(),
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
            mode_args(&OMP, Some("bypassPermissions")),
            vec!["--approval-mode".to_string(), "yolo".to_string()]
        );
        assert_eq!(
            mode_args(&OMP, Some("acceptEdits")),
            vec!["--approval-mode".to_string(), "write".to_string()]
        );
        // Unknown/default modes defer to omp's own tools.approvalMode.
        assert!(mode_args(&OMP, None).is_empty());
        assert!(mode_args(&OMP, Some("plan")).is_empty());
        // pi has no approval switches — every mode defers to the CLI.
        assert!(mode_args(&PI, Some("bypassPermissions")).is_empty());
    }

    /// The terminality contract the two profiles encode: omp settles on
    /// any agent_end, pi only on an explicit isTerminal (its bare
    /// agent_end frames precede model retries).
    #[test]
    fn profiles_encode_terminal_semantics() {
        let decide = |strict: bool, is_terminal: Option<bool>| match (strict, is_terminal) {
            (true, None | Some(false)) => false,
            (_, t) => t.unwrap_or(true),
        };
        // omp: lenient — missing flag means done.
        assert!(decide(OMP.strict_terminal, None));
        assert!(decide(OMP.strict_terminal, Some(true)));
        assert!(!decide(OMP.strict_terminal, Some(false)));
        // pi: strict — missing flag stays provisional.
        assert!(!decide(PI.strict_terminal, None));
        assert!(decide(PI.strict_terminal, Some(true)));
        assert!(!decide(PI.strict_terminal, Some(false)));
    }

    #[test]
    fn compaction_frames_map_to_timeline_items() {
        match compaction_item("auto_compaction_start") {
            Some(TimelineItem::Compaction { summary }) => {
                assert_eq!(summary, "auto compaction started")
            }
            _ => panic!("start must map to a Compaction item"),
        }
        match compaction_item("auto_compaction_end") {
            Some(TimelineItem::Compaction { summary }) => {
                assert_eq!(summary, "auto compaction completed")
            }
            _ => panic!("end must map to a Compaction item"),
        }
        assert!(compaction_item("message_update").is_none());
        assert!(compaction_item("").is_none());
    }

    // -- session listing -------------------------------------------------

    /// Timestamped session-file name as omp/pi write it on disk.
    const TS: &str = "2026-09-20T15-20-30-799Z";

    /// A transcript head: an auto title line, then the session header.
    fn head_lines(cwd: &str, title: Option<&str>) -> String {
        let title_line = json!({"type":"title","v":1,"title":"auto","source":"auto"}).to_string();
        let mut session = json!({
            "type": "session",
            "version": 3,
            "id": "01a09ba1-4f6a-7000-bfa4-cf2e63cbfa2d",
            "timestamp": "2026-09-20T15:20:30.799Z",
            "cwd": cwd,
        });
        if let Some(t) = title {
            session["title"] = json!(t);
        }
        format!("{title_line}\n{session}\n")
    }

    /// A throwaway agent-dir tree, removed on drop (no tempfile dep).
    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let dir =
                std::env::temp_dir().join(format!("damon-omp-sessions-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(dir.join("sessions")).unwrap();
            Self(dir)
        }

        fn write(&self, rel: &str, body: &str) -> PathBuf {
            let path = self.0.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, body).unwrap();
            path
        }

        fn set_mtime(&self, rel: &str, secs: u64) {
            let f = std::fs::OpenOptions::new()
                .append(true)
                .open(self.0.join(rel))
                .unwrap();
            f.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
                .unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Slug dirs collide (`/work/project` and `/work-project` both slug
    /// to `-work-project`), so the scan must verify the header cwd — and
    /// skip nested subagent transcripts, task-named files, same-stem
    /// directories, and order survivors by mtime.
    #[test]
    fn scan_matches_filters_and_orders_fixture() {
        let fx = Fixture::new();
        let slug = "-work-project";
        let keep_old = format!("{TS}_01a09ba1-4f6a-7000-bfa4-cf2e63cbfa2d.jsonl");
        let keep_new = format!("{TS}_01a0bf67-914f-7000-a2df-5439f834b5f1.jsonl");
        fx.write(
            &format!("sessions/{slug}/{keep_old}"),
            &head_lines("/work/project", Some("older task")),
        );
        // Title-less header — the title stays None.
        fx.write(
            &format!("sessions/{slug}/{keep_new}"),
            &head_lines("/work/project", None),
        );
        // Same slug dir, but the header cwd is the colliding path.
        fx.write(
            &format!("sessions/{slug}/{TS}_01a0bf68-fb68-7000-9761-affcc27c0801.jsonl"),
            &head_lines("/work-project", Some("slug collision")),
        );
        // Subagent transcript nested in a subdirectory of the slug dir.
        fx.write(
            &format!("sessions/{slug}/subagents/{TS}_01a0d156-6363-7000-8b9b-8867c19f544e.jsonl"),
            &head_lines("/work/project", Some("nested subagent")),
        );
        // Task-named top-level transcript: no timestamp prefix.
        fx.write(
            &format!("sessions/{slug}/Refactor auth.jsonl"),
            &head_lines("/work/project", Some("task transcript")),
        );
        // Same-stem directory kept beside the session file (observed on
        // this machine's ~/.omp/agent/sessions).
        std::fs::create_dir_all(fx.0.join(format!(
            "sessions/{slug}/{TS}_01a0bf67-914f-7000-a2df-5439f834b5f1"
        )))
        .unwrap();

        fx.set_mtime(&format!("sessions/{slug}/{keep_old}"), 1_700_000_100);
        fx.set_mtime(&format!("sessions/{slug}/{keep_new}"), 1_700_000_500);

        let out = scan_importable_sessions(&fx.0, Path::new("/work/project"), "omp");
        assert_eq!(out.len(), 2, "only real sessions of the cwd surface");
        assert_eq!(out[0].modified_at, Some(1_700_000_500), "newest first");
        assert_eq!(out[0].title, None);
        assert_eq!(out[0].cwd.as_deref(), Some(Path::new("/work/project")));
        assert_eq!(out[1].title.as_deref(), Some("older task"));
        assert_eq!(out[1].modified_at, Some(1_700_000_100));
        assert_eq!(out[1].handle.provider, "omp");
        assert!(
            out[1].handle.native_handle.ends_with(&keep_old),
            "resume token is the session file path: {}",
            out[1].handle.native_handle
        );
        // pi shares the storage layout — only the provider id differs.
        let pi = scan_importable_sessions(&fx.0, Path::new("/work/project"), "pi");
        assert_eq!(pi.len(), 2);
        assert_eq!(pi[0].handle.provider, "pi");
    }

    #[test]
    fn client_advertises_session_listing() {
        let resolved = ResolvedBackend {
            id: "omp".to_string(),
            title: "omp".to_string(),
            command: "omp".to_string(),
            args: vec![],
            env: vec![],
            auth_hint: String::new(),
            detected: true,
        };
        for profile in [&OMP, &PI] {
            let client = OmpClient::with_profile(resolved.clone(), profile);
            assert!(client.capabilities().session_listing);
        }
    }

    #[test]
    fn session_head_reads_bounded_prefix() {
        let fx = Fixture::new();
        // Header past the bounded head → not a listing candidate.
        let mut body = "{}\n".repeat(SESSION_HEAD_LINES);
        body.push_str(r#"{"type":"session","cwd":"/w/p","title":"too late"}"#);
        let late = fx.write("sessions/late.jsonl", &body);
        assert_eq!(session_head(&late), None);

        let early = fx.write("sessions/early.jsonl", &head_lines("/w/p", Some("found")));
        assert_eq!(
            session_head(&early),
            Some((PathBuf::from("/w/p"), Some("found".to_string())))
        );

        // No header at all — foreign or truncated transcript.
        let none = fx.write("sessions/none.jsonl", "{}\n");
        assert_eq!(session_head(&none), None);
    }

    #[test]
    fn session_file_names_require_timestamp_prefix() {
        assert!(is_session_file_name(
            "2026-09-20T15-20-30-799Z_01a0bf67-914f-7000-a2df-5439f834b5f1.jsonl"
        ));
        assert!(!is_session_file_name("Refactor auth.jsonl"));
        assert!(!is_session_file_name("2026-09-20X15-20-30-799Z_ulid.jsonl"));
        assert!(!is_session_file_name("no_underscores_here.jsonl"));
    }
}
