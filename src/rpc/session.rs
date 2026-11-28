//! `session.*` — the session control family, and the first slice of the
//! `dispatch` params refactor (see [`super::params`]).
//!
//! Every method here used to read its request out of the raw
//! `serde_json::Value` inside `dispatch`'s giant match: `params["cwd"]`,
//! `req_str(&params, "sessionId")`, `params["limit"].as_u64()` — 22
//! methods, ~50 untyped reads, and a schema catalog maintained by hand
//! next to them. Each method now has one request struct that is the
//! single description of its params: the struct is what the handler
//! takes, the struct's [`Field`] table is what the daemon checks, and
//! `tests` in this module hold that table to the catalog `hello` /
//! `damond --print-rpc-schema` prints. The next families (`turn.*`,
//! `mission.*`, …) copy this shape: struct + `FIELDS` + a one-line
//! `pub(super)` handler, and one arm per method in `dispatch`.
//!
//! Wire behavior is unchanged on purpose — codes, messages, defaults and
//! the silently-ignored optional params included.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::api::AppState;
use crate::backend::types::{McpServerConfig, PersistenceHandle, SessionConfig};
use crate::store::{SearchFilter, SessionFilter};

use super::params::{Field, Params, Present};
use super::{
    Conn, RpcError, check_cwd_allowed, effective_grants, error_code, parse_time_bound, reattach,
};

// ─────────────────────────────────────────────────────────────────────
// Requests
// ─────────────────────────────────────────────────────────────────────

/// `session.create` — a new session. Every param is optional; the
/// project (when named) fills whatever the request left out.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Create {
    backend: Option<String>,
    cwd: Option<String>,
    model: Option<String>,
    mode: Option<String>,
    mcp_servers: Option<Map<String, Value>>,
    project_id: Option<String>,
    persona: Option<String>,
    /// Tool allowlist for backends that can enforce one at launch
    /// (claude: `--allowedTools`). Loose elements are dropped, not
    /// refused — the pre-refactor read filtered non-strings out.
    allowed_tools: Option<Vec<Value>>,
    /// Per-session sandbox override (`auto|bwrap|seatbelt|docker|none`).
    /// Unset follows the backend's config. A value the backend or host
    /// cannot honor fails the create with the reason — see
    /// `sandbox::decide`.
    sandbox: Option<String>,
}

impl Params for Create {
    const METHOD: &'static str = "session.create";
    const FIELDS: &'static [Field] = &[
        Field::str_opt("backend"),
        Field::str_opt("cwd"),
        Field::str_opt("model"),
        Field::str_opt("mode"),
        Field::object_opt("mcpServers"),
        Field::str_opt("projectId"),
        Field::str_opt("persona"),
        Field::array_opt("allowedTools"),
        Field::str_opt("sandbox"),
    ];
}

impl Create {
    /// The launch config this request asks for, before project defaults
    /// and grants are layered on. `cwd` falls back to the daemon's own
    /// working directory exactly as before.
    fn config(&self) -> Result<SessionConfig> {
        Ok(SessionConfig {
            cwd: self
                .cwd
                .as_deref()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into())),
            model: self.model.clone(),
            mode: self.mode.clone(),
            mcp_servers: mcp_servers(self.mcp_servers.as_ref()),
            granted_roots: Vec::new(),
            allowed_tools: self
                .allowed_tools
                .as_deref()
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            // A misspelled mode is an error, not a silent "whatever the
            // config says": the caller asked this session to be confined.
            sandbox: match self.sandbox.as_deref() {
                None => None,
                Some(raw) => Some(
                    crate::sandbox::SandboxRequest::parse(raw)
                        .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, e.to_string()))?,
                ),
            },
        })
    }
}

/// `session.resume` — one of two shapes: `sessionId` (a Damon row) or
/// `handle` (import a native session that has no row yet). Exactly one
/// is required; the check lives in the handler because it is a *choice*
/// between fields, which a field table cannot express.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Resume {
    session_id: Option<String>,
    handle: Present<Value>,
    cwd: Option<String>,
    title: Option<String>,
}

impl Params for Resume {
    const METHOD: &'static str = "session.resume";
    const FIELDS: &'static [Field] = &[
        Field::str_opt("sessionId"),
        Field::json_opt("handle"),
        Field::str_opt("cwd"),
        Field::str_opt("title"),
    ];
}

impl Resume {
    /// The either/or rule a field table cannot express: without a
    /// `handle` the call must name a session, and the shape that names
    /// neither keeps the historical message.
    fn required_session_id(&self) -> Result<&str> {
        self.session_id
            .as_deref()
            .ok_or_else(|| RpcError::error(error_code::INVALID_PARAMS, "sessionId required"))
    }
}

/// `session.restart` — kill the live backend (if any), reattach through
/// the persisted handle.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Restart {
    session_id: String,
}

impl Params for Restart {
    const METHOD: &'static str = "session.restart";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `session.list` — paged rows with AND-ed filters.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct List {
    limit: Option<u64>,
    offset: Option<u64>,
    backend: Option<String>,
    cwd: Option<String>,
    tag: Option<String>,
    project_id: Option<String>,
    include_archived: Option<bool>,
}

impl Params for List {
    const METHOD: &'static str = "session.list";
    const FIELDS: &'static [Field] = &[
        Field::uint_opt("limit"),
        Field::uint_opt("offset"),
        Field::str_opt("backend"),
        Field::str_opt("cwd"),
        Field::str_opt("tag"),
        Field::str_opt("projectId"),
        Field::bool_opt("includeArchived"),
    ];
}

/// `session.status` — takes nothing; the live picture is daemon-wide.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Status {}

impl Params for Status {
    const METHOD: &'static str = "session.status";
    const FIELDS: &'static [Field] = &[];
}

/// `session.watch` — subscribe this connection to a live session's
/// events without touching the session.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Watch {
    session_id: String,
}

impl Params for Watch {
    const METHOD: &'static str = "session.watch";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `session.unwatch` — drop this connection's subscription.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Unwatch {
    session_id: String,
}

impl Params for Unwatch {
    const METHOD: &'static str = "session.unwatch";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `session.messages` — persisted transcript, paged.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Messages {
    session_id: String,
    limit: Option<u64>,
    offset: Option<u64>,
}

impl Params for Messages {
    const METHOD: &'static str = "session.messages";
    const FIELDS: &'static [Field] = &[
        Field::str("sessionId"),
        Field::uint_opt("limit"),
        Field::uint_opt("offset"),
    ];
}

/// `session.export` — header, whole transcript and usage in one frame.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Export {
    session_id: String,
}

impl Params for Export {
    const METHOD: &'static str = "session.export";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `session.import` — what a backend can import from a directory.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Import {
    backend: String,
    cwd: Option<String>,
}

impl Params for Import {
    const METHOD: &'static str = "session.import";
    const FIELDS: &'static [Field] = &[Field::str("backend"), Field::str_opt("cwd")];
}

/// `session.delete` — row and history gone.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Delete {
    session_id: String,
}

impl Params for Delete {
    const METHOD: &'static str = "session.delete";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `session.close` — kill the live backend, keep row and history.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Close {
    session_id: String,
}

impl Params for Close {
    const METHOD: &'static str = "session.close";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// `session.rescan` — the history sweep; single-flight, no params.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Rescan {}

impl Params for Rescan {
    const METHOD: &'static str = "session.rescan";
    const FIELDS: &'static [Field] = &[];
}

/// `session.set_tags` — replaces the whole list.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct SetTags {
    session_id: String,
    tags: Vec<String>,
}

impl Params for SetTags {
    const METHOD: &'static str = "session.set_tags";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::str_array("tags")];
}

/// `session.set_pinned`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct SetPinned {
    session_id: String,
    pinned: bool,
}

impl Params for SetPinned {
    const METHOD: &'static str = "session.set_pinned";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::bool("pinned")];
}

/// `session.set_archived`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct SetArchived {
    session_id: String,
    archived: bool,
}

impl Params for SetArchived {
    const METHOD: &'static str = "session.set_archived";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::bool("archived")];
}

/// `session.rename`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Rename {
    session_id: String,
    title: String,
}

impl Params for Rename {
    const METHOD: &'static str = "session.rename";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::str("title")];
}

/// `session.fork` — copy the session, optionally only up to a message.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Fork {
    session_id: String,
    upto: Option<i64>,
}

impl Params for Fork {
    const METHOD: &'static str = "session.fork";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::int_opt("upto")];
}

/// `session.usage` — per-session, per-model (global) or daily (global).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Usage {
    session_id: Option<String>,
    daily: Option<bool>,
    days: Option<u64>,
    project_id: Option<String>,
}

impl Params for Usage {
    const METHOD: &'static str = "session.usage";
    const FIELDS: &'static [Field] = &[
        Field::str_opt("sessionId"),
        Field::bool_opt("daily"),
        Field::uint_opt("days"),
        Field::str_opt("projectId"),
    ];
}

/// `session.search` — full-text over the transcript with AND-ed filters.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Search {
    query: String,
    limit: Option<u64>,
    session_id: Option<String>,
    backend: Option<String>,
    cwd: Option<String>,
    project_id: Option<String>,
    since: Option<String>,
    until: Option<String>,
}

impl Params for Search {
    const METHOD: &'static str = "session.search";
    const FIELDS: &'static [Field] = &[
        Field::str("query"),
        Field::uint_opt("limit"),
        Field::str_opt("sessionId"),
        Field::str_opt("backend"),
        Field::str_opt("cwd"),
        Field::str_opt("projectId"),
        Field::str_opt("since"),
        Field::str_opt("until"),
    ];
}

/// `session.set_model`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct SetModel {
    session_id: String,
    model: String,
}

impl Params for SetModel {
    const METHOD: &'static str = "session.set_model";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::str("model")];
}

/// `session.set_mode`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct SetMode {
    session_id: String,
    mode: String,
}

impl Params for SetMode {
    const METHOD: &'static str = "session.set_mode";
    const FIELDS: &'static [Field] = &[Field::str("sessionId"), Field::str("mode")];
}

/// Wire `mcpServers` → launch config. An entry without a `command` is
/// dropped, exactly as the pre-refactor parser did.
fn mcp_servers(raw: Option<&Map<String, Value>>) -> HashMap<String, McpServerConfig> {
    raw.map(|m| {
        m.iter()
            .filter_map(|(name, v)| {
                Some((
                    name.clone(),
                    McpServerConfig {
                        command: v["command"].as_str()?.to_string(),
                        args: serde_json::from_value(v["args"].clone()).unwrap_or_default(),
                        env: serde_json::from_value(v["env"].clone()).unwrap_or_default(),
                    },
                ))
            })
            .collect()
    })
    .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────

pub(super) async fn create(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    params: &Value,
) -> Result<Value> {
    let req = Create::parse(params)?;
    // A `sandbox` that is present but not a string must fail here. The field
    // table drops ill-typed *optional* fields — the wire-compatible rule for
    // every other param — but dropping this one would answer a client that
    // asked to be confined with a session it never asked for.
    // (`null` is the wire's "not set" and stays equivalent to omitting it.)
    if let Some(raw) = params.get("sandbox")
        && !raw.is_null()
        && !raw.is_string()
    {
        return Err(RpcError::error(
            error_code::INVALID_PARAMS,
            "sandbox must be a string — auto|bwrap|seatbelt|docker|none",
        ));
    }
    let mut cfg = req.config()?;
    let mut backend = req.backend.clone();
    // A project scopes the session: its root becomes the cwd default and
    // its defaults fill whatever the request left unset (explicit params
    // always win).
    let project = match req.project_id.as_deref() {
        Some(pid) => {
            let p = state.store.get_project(pid).await?.ok_or_else(|| {
                RpcError::error(error_code::INVALID_PARAMS, format!("unknown project {pid}"))
            })?;
            // Stored defaults carry the create shape ({backend?, model?,
            // mode?, mcpServers?}) — parse them through the same struct
            // so one description covers both.
            let defaults: Value = serde_json::from_str(&p.defaults).unwrap_or_default();
            let defaults = Create::parse(&defaults)?;
            if req.cwd.is_none() {
                cfg.cwd = std::path::PathBuf::from(&p.root);
            }
            if cfg.model.is_none() {
                cfg.model = defaults.model;
            }
            if cfg.mode.is_none() {
                cfg.mode = defaults.mode;
            }
            if cfg.mcp_servers.is_empty() {
                cfg.mcp_servers = mcp_servers(defaults.mcp_servers.as_ref());
            }
            if backend.is_none() {
                backend = defaults.backend;
            }
            Some(pid.to_string())
        }
        None => None,
    };
    check_cwd_allowed(state, &cfg.cwd)?;
    // Persona (optional): resolve the bot's files NOW — a typo'd or
    // since-deleted id must fail the create, not the first turn. The
    // composed block is fixed for the session's life; edits to the bot's
    // files reach the NEXT session.
    let persona = match req.persona.as_deref() {
        Some(id) => {
            let root = crate::bots::bots_root(state.config.read().data_dir.as_deref());
            Some(crate::bots::resolve(&root, id).ok_or_else(|| {
                RpcError::error(
                    error_code::INVALID_PARAMS,
                    format!("unknown persona `{id}` — bot.save it first"),
                )
            })?)
        }
        None => None,
    };
    // Effective granted roots (global + the project's) ride the launch
    // line for backends with a native widening flag (claude --add-dir)
    // and pre-warm the file jail.
    cfg.granted_roots = effective_grants(state, project.clone()).await?;
    let ms = state
        .sessions
        .create(backend.as_deref(), cfg.clone(), persona.clone())
        .await?;
    state
        .store
        .create_session(&ms.id, &cfg.cwd.to_string_lossy(), Some(&ms.provider))
        .await?;
    // An explicit per-session sandbox rides the row: resume/restart must
    // re-apply the same confinement instead of falling back to the
    // backend's default (which may be looser).
    if let Some(mode) = cfg.sandbox {
        let _ = state
            .store
            .set_session_sandbox(&ms.id, Some(&mode.to_string()))
            .await;
    }
    if let Some(pid) = &project {
        let _ = state.store.set_session_project(&ms.id, Some(pid)).await;
    }
    // The persona binding rides the session row so resume/restart
    // re-resolve it after a daemon restart.
    if let Some(p) = &persona {
        let _ = state.store.set_session_persona(&ms.id, Some(&p.bot)).await;
    }
    // Persist the resolved default model (explicit or project default) —
    // the row is the search/list surface for it.
    if let Some(m) = &cfg.model {
        let _ = state.store.set_session_model(&ms.id, m).await;
    }
    let replayed = conn.subscribe(state, &ms).await;
    let mut out = json!({"sessionId": ms.id, "backend": ms.provider, "replayed": replayed, "cursor": ms.cursor()});
    if let Some(p) = &ms.persona {
        out["persona"] = json!(p.bot);
    }
    Ok(out)
}

pub(super) async fn resume(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    params: &Value,
) -> Result<Value> {
    let req = Resume::parse(params)?;
    // Import path: a PersistenceHandle points at a native session that
    // has no Damon row yet — mint one, bind it, resume.
    if let Some(handle) = req.handle.0 {
        let handle: PersistenceHandle = serde_json::from_value(handle).map_err(|_| {
            RpcError::error(
                error_code::INVALID_PARAMS,
                "handle must be {provider, native_handle, metadata?}",
            )
        })?;
        if handle.provider.is_empty() || handle.native_handle.is_empty() {
            return Err(RpcError::error(
                error_code::INVALID_PARAMS,
                "handle requires provider and native_handle",
            ));
        }
        // Re-importing the same native session returns the existing row
        // instead of duplicating it.
        let id = match state
            .store
            .session_by_agent_session(&handle.provider, &handle.native_handle)
            .await?
        {
            Some(existing) => existing,
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                let cwd = req
                    .cwd
                    .clone()
                    .or_else(|| handle.metadata["cwd"].as_str().map(String::from))
                    .unwrap_or_else(|| ".".to_string());
                state
                    .store
                    .create_session(&id, &cwd, Some(&handle.provider))
                    .await?;
                state
                    .store
                    .set_agent_session(&id, &handle.provider, &handle.native_handle)
                    .await?;
                if let Some(title) = req.title.as_deref() {
                    let _ = state.store.rename_session(&id, title).await;
                }
                id
            }
        };
        let cwd = state
            .store
            .session_cwd(&id)
            .await?
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("."));
        check_cwd_allowed(state, &cwd)?;
        // An import is a NEW session: there is no stored per-session sandbox
        // to re-apply, so the backend's configured policy governs it (and a
        // serve-mode backend still refuses an explicit request there).
        let mut cfg = SessionConfig {
            cwd,
            ..Default::default()
        };
        cfg.granted_roots =
            effective_grants(state, state.store.session_project(&id).await?).await?;
        let ms = state.sessions.resume(&id, &handle, cfg, None).await?;
        let replayed = conn.subscribe(state, &ms).await;
        return Ok(
            json!({"sessionId": id, "backend": ms.provider, "replayed": replayed, "cursor": ms.cursor()}),
        );
    }
    // Neither shape given: the historical message names the plain one.
    let id = req.required_session_id()?;
    // Already live? Just subscribe and return.
    if let Some(ms) = state.sessions.get(id).await {
        let replayed = conn.subscribe(state, &ms).await;
        return Ok(
            json!({"sessionId": id, "backend": ms.provider, "replayed": replayed, "cursor": ms.cursor()}),
        );
    }
    reattach(state, conn, id).await
}

pub(super) async fn restart(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    params: &Value,
) -> Result<Value> {
    let req = Restart::parse(params)?;
    let id = req.session_id.as_str();
    // A live session is killed first (a crashed/wedged process included —
    // close is unconditional), then reattached through the persisted
    // handle; a not-live session just reattaches. Busy sessions refuse:
    // restarting mid-turn would kill the turn's process underneath the
    // collector.
    if state.sessions.get(id).await.is_some() {
        if state.sessions.record(id).is_some_and(|r| r.is_active()) {
            return Err(RpcError::error(
                error_code::TURN_IN_PROGRESS,
                "a turn is already running on this session",
            ));
        }
        state.sessions.close(id).await?;
    }
    reattach(state, conn, id).await
}

pub(super) async fn list(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = List::parse(params)?;
    let limit = req.limit.unwrap_or(50) as u32;
    let offset = req.offset.unwrap_or(0) as u32;
    let filter = SessionFilter {
        backend: req.backend,
        cwd: req.cwd,
        tag: req.tag,
        project_id: req.project_id,
        include_archived: req.include_archived.unwrap_or(false),
    };
    let rows = state
        .store
        .list_sessions_paged(limit, offset, filter)
        .await?;
    let sessions: Vec<Value> = rows
        .into_iter()
        .map(|s| {
            json!({"sessionId": s.id, "createdAt": s.created_at,
                   "backend": s.backend, "title": s.title,
                   "cwd": s.cwd, "tags": s.tags,
                   "pinned": s.pinned, "archived": s.archived})
        })
        .collect();
    Ok(json!({"sessions": sessions}))
}

pub(super) async fn status(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    // Validate the (empty) contract: a caller sending this method's
    // params through the typed path gets the same answer as one that
    // sends none.
    Status::parse(params)?;
    // Live-session introspection: which backend processes are resident
    // right now, what each one is doing (idle, mid-turn, or parked on an
    // answer), how long idle. `recovered` names what a dead daemon left
    // unfinished — a client reopening the app learns that a turn was cut
    // off instead of finding a session that silently looks idle.
    // Complements the /metrics gauges with per-session detail.
    let rows = state.sessions.live_status().await;
    let sessions: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let mut v = json!({
                "sessionId": row.id,
                "backend": row.provider,
                "busy": row.phase.is_active(),
                "phase": row.phase.as_str(),
                "idleSecs": row.idle_secs,
            });
            if let Some(turn_id) = row.turn_id {
                v["turnId"] = json!(turn_id);
            }
            if let Some(ask) = row.pending_ask {
                v["pendingAsk"] = json!({
                    "requestId": ask.request_id,
                    "kind": ask.kind.as_str(),
                    "label": ask.label,
                    "askedAtMs": ask.asked_at_ms,
                });
            }
            v
        })
        .collect();
    let recovered: Vec<Value> = state
        .sessions
        .recovered_sessions()
        .into_iter()
        .filter_map(|r| {
            let mark = r.recovered?;
            Some(json!({
                "sessionId": r.id,
                "backend": r.provider,
                "phase": mark.from_phase.as_str(),
                "sinceMs": mark.at_ms,
                "turnId": mark.turn_id,
            }))
        })
        .collect();
    Ok(json!({"sessions": sessions, "recovered": recovered}))
}

pub(super) async fn watch(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    params: &Value,
) -> Result<Value> {
    // Subscribe THIS connection to a session's events without
    // creating/resuming/turning on it — the cross-surface notification
    // path. A chat bridge watches a session the web UI started and pings
    // the chat when it finishes or asks for permission. Requires a live
    // session: a detached one has no backend process broadcasting
    // anything. The subscribe replays missed history (replay-tagged),
    // which notification consumers skip.
    let req = Watch::parse(params)?;
    let id = req.session_id.as_str();
    let ms = state.sessions.get(id).await.ok_or_else(|| {
        RpcError::error(
            error_code::SESSION_NOT_LIVE,
            "session not live — resume it before watching",
        )
    })?;
    let replayed = conn.subscribe(state, &ms).await;
    Ok(json!({"sessionId": id, "subscribed": true, "replayed": replayed, "cursor": ms.cursor()}))
}

pub(super) async fn unwatch(conn: &Arc<Conn>, params: &Value) -> Result<Value> {
    let req = Unwatch::parse(params)?;
    let id = req.session_id.as_str();
    let removed = conn.unsubscribe(id).await;
    Ok(json!({"sessionId": id, "subscribed": !removed}))
}

pub(super) async fn messages(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Messages::parse(params)?;
    let limit = req.limit.unwrap_or(200) as u32;
    let offset = req.offset.unwrap_or(0) as u32;
    let msgs = state
        .store
        .messages_paged(&req.session_id, limit, offset)
        .await?;
    Ok(json!({"messages": msgs}))
}

pub(super) async fn export(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Export::parse(params)?;
    let id = req.session_id.as_str();
    // Header + transcript + usage in one shot — the wire-side superset of
    // the CLI's client-rendered export.
    let overview = state
        .store
        .session_overview(id)
        .await?
        .with_context(|| format!("session not found: {id}"))?;
    let msgs = state.store.messages_paged(id, u32::MAX, 0).await?;
    let (used, size, cost, turns) = state.store.session_usage(id).await?;
    Ok(json!({
        "session": {
            "sessionId": overview.id, "createdAt": overview.created_at,
            "backend": overview.backend, "title": overview.title,
            "tags": overview.tags, "cwd": overview.cwd,
        },
        // Same per-row fields as session.messages minus the redundant
        // session_id — it is in the header.
        "messages": msgs.iter().map(|m| json!({
            "id": m.id, "role": m.role, "ts": m.ts, "data": m.data,
        })).collect::<Vec<_>>(),
        "usage": {
            "contextUsed": used, "contextSize": size,
            "costUsd": cost, "turns": turns,
        },
    }))
}

pub(super) async fn import(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Import::parse(params)?;
    let clients = state.sessions.clients();
    let client = clients
        .iter()
        .find(|(id, _)| id == &req.backend)
        .map(|(_, c)| c.clone())
        .ok_or_else(|| RpcError::error(error_code::INVALID_PARAMS, "unknown backend"))?;
    let cwd = req
        .cwd
        .as_deref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));
    check_cwd_allowed(state, &cwd)?;
    let found = client.list_importable_sessions(&cwd).await?;
    Ok(json!({"sessions": found}))
}

pub(super) async fn delete(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Delete::parse(params)?;
    state.sessions.close(&req.session_id).await.ok();
    state.store.delete_session(&req.session_id).await?;
    Ok(json!({"deleted": true}))
}

pub(super) async fn close(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Close::parse(params)?;
    // Frees a max_sessions slot deterministically: kills the live backend
    // process, keeps the row and history (the next session.resume
    // reattaches). Idempotent — closing a not-live id succeeds without
    // touching anything.
    state.sessions.close(&req.session_id).await?;
    Ok(json!({"closed": true}))
}

pub(super) async fn rescan(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    params: &Value,
) -> Result<Value> {
    Rescan::parse(params)?;
    // History scan: single-flight globally — a second call while one runs
    // gets the in-flight scanId (and no progress events; the running
    // caller owns the pipeline view). Progress frames ride the requesting
    // connection (worktree precedent).
    let scan_id = {
        let mut slot = state.history_scan.lock();
        match slot.as_ref() {
            Some(in_flight) => {
                return Ok(json!({
                    "scanId": in_flight,
                    "alreadyRunning": true,
                }));
            }
            None => {
                let id = uuid::Uuid::new_v4().to_string();
                *slot = Some(id.clone());
                id
            }
        }
    };
    let pconn = conn.clone();
    let sid = scan_id.clone();
    let progress = move |payload: serde_json::Value| {
        pconn.send(json!({
            "event": "history.scan_progress",
            "scanId": sid,
            "data": payload,
        }));
    };
    let report = crate::history_scan::scan_history(state, &scan_id, &progress).await;
    *state.history_scan.lock() = None;
    let report = report?;
    Ok(json!({
        "scanId": scan_id,
        "alreadyRunning": false,
        "report": report.to_json(),
    }))
}

pub(super) async fn set_tags(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = SetTags::parse(params)?;
    let ok = state.store.set_tags(&req.session_id, &req.tags).await?;
    Ok(json!({"updated": ok}))
}

pub(super) async fn set_pinned(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = SetPinned::parse(params)?;
    let ok = state.store.set_pinned(&req.session_id, req.pinned).await?;
    Ok(json!({"updated": ok}))
}

pub(super) async fn set_archived(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = SetArchived::parse(params)?;
    let ok = state
        .store
        .set_archived(&req.session_id, req.archived)
        .await?;
    Ok(json!({"updated": ok}))
}

pub(super) async fn rename(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Rename::parse(params)?;
    let ok = state
        .store
        .rename_session(&req.session_id, &req.title)
        .await?;
    Ok(json!({"renamed": ok}))
}

pub(super) async fn fork(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Fork::parse(params)?;
    let new_id = state.store.fork_session(&req.session_id, req.upto).await?;
    Ok(json!({"sessionId": new_id}))
}

pub(super) async fn usage(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Usage::parse(params)?;
    // `daily` is a global-view mode: with a sessionId it is ignored — the
    // per-session shape carries no day grouping.
    if req.session_id.is_none() && req.daily.unwrap_or(false) {
        let days = req.days.unwrap_or(7).max(1) as u32;
        let rows = state.store.daily_usage(days).await?;
        let daily: Vec<Value> = rows
            .into_iter()
            .map(|(date, turns, cost, used)| {
                json!({"date": date, "turns": turns,
                       "costUsd": cost, "contextUsed": used})
            })
            .collect();
        return Ok(json!({"daily": daily}));
    }
    match req.session_id.as_deref() {
        Some(id) => {
            let (used, size, cost, turns) = state.store.session_usage(id).await?;
            Ok(json!({
                "sessionId": id,
                "contextUsed": used, "contextSize": size,
                "costUsd": cost, "turns": turns,
            }))
        }
        None => {
            let rows = state.store.usage_summary(req.project_id).await?;
            let sessions: Vec<Value> = rows
                .into_iter()
                .map(|(model, used, size, cost, turns)| {
                    json!({"model": model, "contextUsed": used, "contextSize": size,
                           "costUsd": cost, "turns": turns})
                })
                .collect();
            Ok(json!({"sessions": sessions}))
        }
    }
}

pub(super) async fn search(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Search::parse(params)?;
    let limit = req.limit.unwrap_or(20) as usize;
    // Wire timestamps (RFC3339 or date-only) become unix-ms bounds here;
    // the store stays a pure-ms API.
    let bound = |v: Option<&str>| {
        v.map(parse_time_bound)
            .transpose()
            .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{e:#}")))
    };
    let filter = SearchFilter {
        session_id: req.session_id,
        backend: req.backend,
        cwd: req.cwd,
        project_id: req.project_id,
        since_ms: bound(req.since.as_deref())?,
        until_ms: bound(req.until.as_deref())?,
    };
    let hits = state
        .store
        .search_filtered(&req.query, limit, filter)
        .await?;
    let results: Vec<Value> = hits
        .into_iter()
        .map(|(session_id, message_id, snippet)| {
            json!({"sessionId": session_id, "messageId": message_id, "snippet": snippet})
        })
        .collect();
    Ok(json!({"results": results}))
}

pub(super) async fn set_model(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = SetModel::parse(params)?;
    let ms = state
        .sessions
        .get(&req.session_id)
        .await
        .ok_or_else(|| RpcError::error(error_code::SESSION_NOT_LIVE, "session not live"))?;
    ms.session.set_model(&req.model).await?;
    Ok(json!({}))
}

pub(super) async fn set_mode(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = SetMode::parse(params)?;
    let ms = state
        .sessions
        .get(&req.session_id)
        .await
        .ok_or_else(|| RpcError::error(error_code::SESSION_NOT_LIVE, "session not live"))?;
    ms.session.set_mode(&req.mode).await?;
    Ok(json!({}))
}

// ─────────────────────────────────────────────────────────────────────
// The family contract, for the drift tests
// ─────────────────────────────────────────────────────────────────────

/// Every method in this module with the fields its struct reads. The
/// drift tests walk this table against `rpc_methods()`; a new session
/// method adds its row here (and the tests fail until it does).
#[cfg(test)]
pub(super) const FAMILY: &[(&str, &[Field])] = &[
    (Create::METHOD, Create::FIELDS),
    (Resume::METHOD, Resume::FIELDS),
    (Restart::METHOD, Restart::FIELDS),
    (List::METHOD, List::FIELDS),
    (Status::METHOD, Status::FIELDS),
    (Watch::METHOD, Watch::FIELDS),
    (Unwatch::METHOD, Unwatch::FIELDS),
    (Messages::METHOD, Messages::FIELDS),
    (Export::METHOD, Export::FIELDS),
    (Import::METHOD, Import::FIELDS),
    (Delete::METHOD, Delete::FIELDS),
    (Close::METHOD, Close::FIELDS),
    (Rescan::METHOD, Rescan::FIELDS),
    (SetTags::METHOD, SetTags::FIELDS),
    (SetPinned::METHOD, SetPinned::FIELDS),
    (SetArchived::METHOD, SetArchived::FIELDS),
    (Rename::METHOD, Rename::FIELDS),
    (Fork::METHOD, Fork::FIELDS),
    (Usage::METHOD, Usage::FIELDS),
    (Search::METHOD, Search::FIELDS),
    (SetModel::METHOD, SetModel::FIELDS),
    (SetMode::METHOD, SetMode::FIELDS),
];

/// Parse one family method's params through its typed struct — the seam
/// the drift tests drive. `dispatch` calls the handlers directly; this
/// table keeps the *checks* reachable without a live daemon, so a field
/// cannot be declared and silently unread.
#[cfg(test)]
pub(super) fn parse_family(method: &str, params: &Value) -> Result<()> {
    match method {
        Create::METHOD => Create::parse(params).map(drop),
        Resume::METHOD => Resume::parse(params).map(drop),
        Restart::METHOD => Restart::parse(params).map(drop),
        List::METHOD => List::parse(params).map(drop),
        Status::METHOD => Status::parse(params).map(drop),
        Watch::METHOD => Watch::parse(params).map(drop),
        Unwatch::METHOD => Unwatch::parse(params).map(drop),
        Messages::METHOD => Messages::parse(params).map(drop),
        Export::METHOD => Export::parse(params).map(drop),
        Import::METHOD => Import::parse(params).map(drop),
        Delete::METHOD => Delete::parse(params).map(drop),
        Close::METHOD => Close::parse(params).map(drop),
        Rescan::METHOD => Rescan::parse(params).map(drop),
        SetTags::METHOD => SetTags::parse(params).map(drop),
        SetPinned::METHOD => SetPinned::parse(params).map(drop),
        SetArchived::METHOD => SetArchived::parse(params).map(drop),
        Rename::METHOD => Rename::parse(params).map(drop),
        Fork::METHOD => Fork::parse(params).map(drop),
        Usage::METHOD => Usage::parse(params).map(drop),
        Search::METHOD => Search::parse(params).map(drop),
        SetModel::METHOD => SetModel::parse(params).map(drop),
        SetMode::METHOD => SetMode::parse(params).map(drop),
        other => panic!("{other} is in FAMILY but has no typed parse"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::params::drift;
    use super::{FAMILY, parse_family};

    /// The catalog's `session.*` entries and the structs in this module
    /// are one contract, checked both ways (see `params::drift`).
    #[test]
    fn catalog_declares_exactly_the_fields_the_structs_read() {
        drift::catalog_matches(FAMILY, &["session."]);
    }

    /// The table is what parse enforces: every required field removed or
    /// mistyped fails with its declared message, every optional field
    /// tolerates junk.
    #[test]
    fn the_declared_fields_are_the_fields_that_are_checked() {
        drift::fields_are_checked(FAMILY, parse_family);
    }

    /// The documented shapes parse: one representative request per method,
    /// spelled exactly as `docs/protocol-v2.md` spells it.
    #[test]
    fn documented_requests_parse() {
        for (method, params) in [
            (
                "session.create",
                json!({"backend": "claude", "cwd": "/tmp", "persona": "p"}),
            ),
            ("session.resume", json!({"sessionId": "s"})),
            (
                "session.resume",
                json!({"handle": {"provider": "claude", "native_handle": "n"}}),
            ),
            ("session.restart", json!({"sessionId": "s"})),
            (
                "session.list",
                json!({"limit": 10, "includeArchived": true}),
            ),
            ("session.status", json!({})),
            ("session.watch", json!({"sessionId": "s"})),
            ("session.unwatch", json!({"sessionId": "s"})),
            ("session.messages", json!({"sessionId": "s", "limit": 5})),
            ("session.export", json!({"sessionId": "s"})),
            (
                "session.import",
                json!({"backend": "claude", "cwd": "/tmp"}),
            ),
            ("session.delete", json!({"sessionId": "s"})),
            ("session.close", json!({"sessionId": "s"})),
            ("session.rescan", json!({})),
            (
                "session.set_tags",
                json!({"sessionId": "s", "tags": ["a", "b"]}),
            ),
            (
                "session.set_pinned",
                json!({"sessionId": "s", "pinned": true}),
            ),
            (
                "session.set_archived",
                json!({"sessionId": "s", "archived": false}),
            ),
            ("session.rename", json!({"sessionId": "s", "title": "t"})),
            ("session.fork", json!({"sessionId": "s", "upto": 3})),
            ("session.usage", json!({"daily": true, "days": 7})),
            ("session.usage", json!({"sessionId": "s"})),
            ("session.usage", json!({"projectId": "p"})),
            (
                "session.search",
                json!({"query": "q", "since": "2026-09-01"}),
            ),
            ("session.set_model", json!({"sessionId": "s", "model": "m"})),
            (
                "session.set_mode",
                json!({"sessionId": "s", "mode": "plan"}),
            ),
        ] {
            parse_family(method, &params).unwrap_or_else(|e| panic!("{method} {params}: {e}"));
        }
    }

    /// The conditional rule on `session.resume`: a `handle` key — even a
    /// literal null — wins over `sessionId` and is refused by the import
    /// path, and neither shape answers the historical `sessionId`
    /// message. Pinned here because the field table deliberately does
    /// not encode the either/or.
    #[test]
    fn resume_needs_one_of_its_two_shapes() {
        use super::{Params, Resume};
        let err = Resume::parse(&json!({}))
            .unwrap()
            .required_session_id()
            .unwrap_err();
        assert_eq!("sessionId required", format!("{err}"));
        assert_eq!(
            "s",
            Resume::parse(&json!({"sessionId": "s"}))
                .unwrap()
                .required_session_id()
                .unwrap()
        );
        // A null handle is *present*: it must not fall through to the
        // sessionId path, exactly as `params.get("handle")` behaved.
        let req = Resume::parse(&json!({"handle": null})).unwrap();
        assert!(req.handle.0.is_some(), "null handle read as absent");
        let req = Resume::parse(&json!({"sessionId": "s"})).unwrap();
        assert!(req.handle.0.is_none());
    }
}
