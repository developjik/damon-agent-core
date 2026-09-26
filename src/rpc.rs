//!
//! Wire shape:
//!   request:   {"id": N, "method": "session.create", "params": {...}}
//!   response:  {"id": N, "result": {...}} | {"id": N, "error": {"code","message"}}
//!   event:     {"event": "session.event", "sessionId": "...", "data": <StreamEvent>}
//!   hello:     {"hello": {"protocol": 2, "daemon": "damond", "version": "..."}}
//!
//! One connection multiplexes every session. Each request is dispatched
//! as its own task so a long `turn.start` never serializes the socket —
//! `turn.steer`, `turn.cancel`, and `permission.respond` stay live while
//! a turn streams. Events are pushed to every connection that has
//! touched the session (create/resume/load/turn), not just the turn
//! owner: the daemon is single-user, and a relay or second client must
//! see the same stream.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::backend::types::*;
use crate::session::ManagedSession;
use crate::store::{SearchFilter, SessionFilter};

#[derive(Deserialize)]
pub struct WsQuery {
    /// Single-use auth ticket minted by POST /v1/ws-ticket. Raw tokens
    /// in the query string are refused — they leak into logs.
    ticket: Option<String>,
}

/// GET /ws — JSON-RPC over WebSocket. Auth: bearer header or ?ticket=.
pub async fn ws_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<WsQuery>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
) -> Result<Response, StatusCode> {
    if !state
        .check_auth(&headers, q.ticket.as_deref(), peer.ip())
        .await
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(ws.on_upgrade(move |sock| run_connection(state, sock)))
}

/// Global bound on concurrent prompt turns — every other resource in
/// this path is capped, and an unbounded spawn per request would let one
/// client exhaust memory and agent processes.
static PROMPT_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(64);

/// Largest single outbound frame — a runaway response must not grow
/// the relay's encrypt buffer without bound.
pub const MAX_RESPONSE_BYTES: usize = 1 << 20;

/// JSON-RPC error codes this daemon emits. `-32700..-32600` is the
/// range the spec reserves for frame/request errors; Damon's own codes
/// live in `-32000..-32099` and are wire contract — clients branch on
/// the typed ones instead of parsing messages (e.g. auto-resume on
/// `SESSION_NOT_LIVE`, queue the text as the next prompt on
/// `NOT_SUPPORTED` steering).
pub mod error_code {
    /// The frame was not valid JSON.
    pub const PARSE_ERROR: i64 = -32700;
    /// Valid JSON, but not a request (an id to answer, no method).
    pub const INVALID_REQUEST: i64 = -32600;
    /// Unknown method name.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// A required parameter is missing or malformed.
    pub const INVALID_PARAMS: i64 = -32602;
    /// Untyped server failure — the default for any anyhow error.
    pub const INTERNAL_ERROR: i64 = -32000;
    /// No live backend session for the id — resume it first.
    pub const SESSION_NOT_LIVE: i64 = -32001;
    /// The backend lacks the capability (model/mode switch, steering).
    pub const NOT_SUPPORTED: i64 = -32002;
    /// The named backend is not detected/registered.
    pub const BACKEND_UNAVAILABLE: i64 = -32003;
    /// The `max_sessions` cap is reached.
    pub const SESSION_LIMIT: i64 = -32004;
    /// The response exceeds the frame budget — page it down.
    pub const RESPONSE_TOO_LARGE: i64 = -32005;
    /// A turn is already running on the session — one at a time.
    pub const TURN_IN_PROGRESS: i64 = -32006;
}

/// An error carrying a typed JSON-RPC code to the wire. Produced via
/// [`RpcError::error`] at the failure's source; anything else an
/// anyhow chain holds serializes as `INTERNAL_ERROR`.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct RpcError {
    /// One of [`error_code`] (or a future server code).
    pub code: i64,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Wrap as `anyhow::Error` so `?` plumbing stays unchanged.
    pub fn error(code: i64, message: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(Self::new(code, message))
    }
}

/// (code, message) for the wire: a chained `RpcError` keeps its code,
/// everything else renders as `INTERNAL_ERROR`. The message stays the
/// full top-level rendering, exactly what pre-code clients matched on.
fn error_payload(e: &anyhow::Error) -> (i64, String) {
    let code = e
        .chain()
        .find_map(|c| c.downcast_ref::<RpcError>())
        .map_or(error_code::INTERNAL_ERROR, |r| r.code);
    (code, e.to_string())
}

/// A required string parameter — the most common param shape in
/// `dispatch`; missing → `INVALID_PARAMS` with the historical message.
fn req_str<'a>(params: &'a Value, key: &str) -> Result<&'a str> {
    params[key]
        .as_str()
        .ok_or_else(|| RpcError::error(error_code::INVALID_PARAMS, format!("{key} required")))
}

/// Cap on persisted rows replayed on subscribe. Beyond this the client
/// pages `session.messages` explicitly.
const REPLAY_HISTORY: u32 = 200;

/// Map one persisted store row onto the StreamEvent it replays as, so
/// replayed frames carry exactly the live wire shape (typed event
/// model, not ad-hoc JSON).
fn replay_event(row: &crate::store::StoredMessage) -> Option<Value> {
    let text = || row.data["content"].as_str().map(String::from);
    let item = match row.role.as_str() {
        "user" => TimelineItem::UserMessage { text: text()? },
        "assistant" => TimelineItem::AssistantMessage { text: text()? },
        "reasoning" => TimelineItem::Reasoning { text: text()? },
        "tool" => TimelineItem::ToolCall(serde_json::from_value(row.data.clone()).ok()?),
        "compaction" => TimelineItem::Compaction { summary: text()? },
        // A failed turn's durable marker — the live wire carries the
        // TurnFailed event; replay re-raises it for late subscribers.
        "error" => TimelineItem::Error {
            message: text().unwrap_or_default(),
        },
        // Genuinely unknown roles have no event shape — skip rather
        // than invent one.
        _ => return None,
    };
    serde_json::to_value(StreamEvent {
        turn_id: None,
        kind: StreamEventKind::Timeline(item),
    })
    .ok()
}

struct Conn {
    tx: mpsc::Sender<Message>,
    /// session id → forwarder task. A session's events reach this
    /// connection once it has touched the session.
    subscriptions: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Turns this connection owns — disconnect cancels them.
    owned_turns: Mutex<HashMap<String, CancellationToken>>,
    /// Set once the connection's read loop has ended. A turn that
    /// registers ownership after the disconnect sweep already ran (it
    /// found an empty map) sees this flag and cancels itself — the
    /// claim-busy→register pair is not atomic with the sweep.
    closed: std::sync::atomic::AtomicBool,
    /// Live log-follower cancel token (`logs.follow`) — unfollow and
    /// disconnect both stop the forwarder.
    log_follow: Mutex<Option<CancellationToken>>,
}

impl Conn {
    /// Push one JSON value as a text frame. Bounded channel: a stalled
    /// client drops events rather than stalling the daemon.
    fn send(&self, v: Value) {
        let _ = self.tx.try_send(Message::Text(v.to_string().into()));
    }

    /// Start forwarding a session's event stream to this connection.
    /// Idempotent — one forwarder per (connection, session). Before the
    /// live forwarder starts, everything the connection missed is
    /// replayed: persisted history from the store, then the in-flight
    /// turn's journal, each frame tagged `"replay": true` so clients
    /// can render catch-up differently from live streaming. Returns the
    /// number of replayed frames.
    ///
    /// The subscriptions lock is held across the store read on purpose:
    /// it serializes concurrent subscribes so replay frames are always
    /// queued before this forwarder's live frames.
    async fn subscribe(&self, state: &Arc<AppState>, session: &Arc<ManagedSession>) -> usize {
        let mut subs = self.subscriptions.lock().await;
        if subs.contains_key(&session.id) {
            return 0;
        }
        let mut replayed = 0;
        if let Ok(rows) = state
            .store
            .messages_paged(&session.id, REPLAY_HISTORY, 0)
            .await
        {
            for row in &rows {
                if let Some(ev) = replay_event(row) {
                    replayed += 1;
                    self.send(json!({
                        "event": "session.event",
                        "sessionId": session.id,
                        "replay": true,
                        "data": ev,
                    }));
                }
            }
        }
        for ev in session.journal.snapshot_current_turn() {
            replayed += 1;
            self.send(json!({
                "event": "session.event",
                "sessionId": session.id,
                "replay": true,
                "data": ev,
            }));
        }
        let mut rx = session.session.subscribe();
        let tx = self.tx.clone();
        let sid = session.id.clone();
        subs.insert(
            sid.clone(),
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(ev) => {
                            let frame = json!({
                                "event": "session.event",
                                "sessionId": sid,
                                "data": ev,
                            });
                            if tx
                                .try_send(Message::Text(frame.to_string().into()))
                                .is_err()
                            {
                                // Client lagging or gone — drop the frame,
                                // keep the forwarder (channel may recover).
                                continue;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }),
        );
        replayed
    }

    /// Drop one session subscription (`session.unwatch`): stop the
    /// forwarder task. The session itself — backend process and
    /// history — is untouched; other connections keep their
    /// subscriptions. Returns whether this connection was subscribed.
    async fn unsubscribe(&self, session_id: &str) -> bool {
        let mut subs = self.subscriptions.lock().await;
        match subs.remove(session_id) {
            Some(handle) => {
                handle.abort();
                true
            }
            None => false,
        }
    }
}

async fn run_connection(state: Arc<AppState>, socket: WebSocket) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    // Bounded outbound queue — a slow client sheds events, never blocks.
    let (tx, mut rx) = mpsc::channel::<Message>(256);
    let conn = Arc::new(Conn {
        tx,
        subscriptions: Mutex::new(HashMap::new()),
        owned_turns: Mutex::new(HashMap::new()),
        closed: std::sync::atomic::AtomicBool::new(false),
        log_follow: Mutex::new(None),
    });

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    conn.send(json!({
        "hello": {
            "protocol": 2,
            "daemon": "damond",
            "version": env!("CARGO_PKG_VERSION"),
            "permissionTimeoutSecs": state.config.read().permission_timeout_secs,
        }
    }));

    while let Some(Ok(msg)) = ws_rx.next().await {
        let Message::Text(text) = msg else { continue };
        handle_frame(&state, &conn, &text);
    }

    // Disconnect: cancel every turn this connection started so a dead
    // client can't leave an agent burning tokens. The closed flag goes
    // up first — a turn registering ownership after this sweep sees it
    // and cancels itself.
    conn.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    for (_, token) in conn.owned_turns.lock().await.drain() {
        token.cancel();
    }
    if let Some(t) = conn.log_follow.lock().await.take() {
        t.cancel();
    }
    writer.abort();
}

/// Relay-tunnel entry point: same dispatch as the WS path, over raw
/// string channels instead of a WebSocket. The relay pipes decrypted
/// frames in and takes ciphertext out — the daemon sees plaintext JSON.
pub async fn handle_socket(
    mut in_rx: mpsc::Receiver<String>,
    out_tx: mpsc::Sender<String>,
    state: Arc<AppState>,
) {
    let (tx, mut rx) = mpsc::channel::<Message>(256);
    let conn = Arc::new(Conn {
        tx,
        subscriptions: Mutex::new(HashMap::new()),
        owned_turns: Mutex::new(HashMap::new()),
        closed: std::sync::atomic::AtomicBool::new(false),
        log_follow: Mutex::new(None),
    });

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Message::Text(t) = msg
                && out_tx.send(t.to_string()).await.is_err()
            {
                break;
            }
        }
    });

    conn.send(json!({
        "hello": {
            "protocol": 2,
            "daemon": "damond",
            "version": env!("CARGO_PKG_VERSION"),
            "permissionTimeoutSecs": state.config.read().permission_timeout_secs,
        }
    }));

    while let Some(text) = in_rx.recv().await {
        handle_frame(&state, &conn, &text);
    }

    // Same closed-flag ordering as the WebSocket path — see run_connection.
    conn.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    for (_, token) in conn.owned_turns.lock().await.drain() {
        token.cancel();
    }
    if let Some(t) = conn.log_follow.lock().await.take() {
        t.cancel();
    }
    writer.abort();
}

/// Parse one inbound JSON frame and spawn its dispatch. Requests run
/// concurrently — a long turn never serializes the socket. Frame-level
/// failures answer with their reserved JSON-RPC code instead of being
/// dropped: a client that sent a malformed or method-less request has
/// an id outstanding and would otherwise wait forever.
fn handle_frame(state: &Arc<AppState>, conn: &Arc<Conn>, text: &str) {
    let req = match serde_json::from_str::<Value>(text) {
        Ok(v) => v,
        Err(e) => {
            state.metrics.record_rpc_error(error_code::PARSE_ERROR);
            conn.send(json!({"id": null, "error": {
                "code": error_code::PARSE_ERROR,
                "message": format!("parse error: {e}"),
            }}));
            return;
        }
    };
    let Some(method) = req["method"].as_str() else {
        // Only answer frames carrying an id — notifications and
        // unknown shapes stay dropped, as before.
        if !req.get("id").is_some_and(|i| i.is_null()) {
            state.metrics.record_rpc_error(error_code::INVALID_REQUEST);
            conn.send(json!({"id": req["id"], "error": {
                "code": error_code::INVALID_REQUEST,
                "message": "invalid request: missing method",
            }}));
        }
        return;
    };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let params = req.get("params").cloned().unwrap_or(json!({}));
    let state = state.clone();
    let conn = conn.clone();
    let method = method.to_string();
    tokio::spawn(async move {
        state.metrics.record_rpc_request();
        let result = dispatch(&state, &conn, &method, params).await;
        let frame = match result {
            Ok(v) => json!({"id": id, "result": v}),
            Err(e) => {
                let (code, message) = error_payload(&e);
                state.metrics.record_rpc_error(code);
                json!({"id": id, "error": {"code": code, "message": message}})
            }
        };
        // A response over the frame budget cannot cross the relay's
        // encrypt cap — it would vanish there. Fail loud with a paging
        // hint instead.
        let frame = if frame.to_string().len() > MAX_RESPONSE_BYTES {
            state
                .metrics
                .record_rpc_error(error_code::RESPONSE_TOO_LARGE);
            json!({"id": id, "error": {
                "code": error_code::RESPONSE_TOO_LARGE,
                "message": format!(
                    "response exceeds the {}-byte frame budget — page it with limit/offset",
                    MAX_RESPONSE_BYTES
                ),
            }})
        } else {
            frame
        };
        conn.send(frame);
    });
}

async fn dispatch(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    method: &str,
    params: Value,
) -> Result<Value> {
    match method {
        "hello" => Ok(json!({
            "protocol": 2,
            "daemon": "damond",
            "version": env!("CARGO_PKG_VERSION"),
            "backends": state.sessions.available_backends(),
            "methods": rpc_methods(),
        })),

        "backend.list" => {
            let mut out = Vec::new();
            for (id, client) in state.sessions.clients() {
                out.push(json!({
                    "id": id,
                    "available": client.is_available().await,
                    "capabilities": client.capabilities(),
                }));
            }
            Ok(json!({"backends": out}))
        }

        "project.create" => {
            let name = params["name"].as_str().unwrap_or_default();
            let root = req_str(&params, "root")?;
            if !std::path::Path::new(root).is_absolute() {
                return Err(RpcError::error(
                    error_code::INVALID_PARAMS,
                    "root must be an absolute path",
                ));
            }
            // Defaults shape check up front — a typo'd key fails here,
            // not at the first session.create that consumes it.
            let defaults = params.get("defaults").cloned().unwrap_or(json!({}));
            if !defaults.is_object() {
                return Err(RpcError::error(
                    error_code::INVALID_PARAMS,
                    "defaults must be an object",
                ));
            }
            for key in defaults.as_object().unwrap().keys() {
                if !matches!(key.as_str(), "backend" | "model" | "mode" | "mcpServers") {
                    return Err(RpcError::error(
                        error_code::INVALID_PARAMS,
                        format!("unknown default {key:?} — backend/model/mode/mcpServers"),
                    ));
                }
            }
            let id = uuid::Uuid::new_v4().to_string();
            state
                .store
                .create_project(&id, name, root, &defaults.to_string())
                .await?;
            Ok(json!({"projectId": id, "name": name, "root": root, "defaults": defaults}))
        }

        "project.list" => {
            let rows = state.store.list_projects().await?;
            let projects: Vec<Value> = rows
                .into_iter()
                .map(|p| {
                    json!({
                        "projectId": p.id,
                        "name": p.name,
                        "root": p.root,
                        "defaults": serde_json::from_str::<Value>(&p.defaults)
                            .unwrap_or_default(),
                    })
                })
                .collect();
            Ok(json!({"projects": projects}))
        }

        "project.get" => {
            let id = req_str(&params, "projectId")?;
            let p = state.store.get_project(id).await?.ok_or_else(|| {
                RpcError::error(error_code::INVALID_PARAMS, format!("unknown project {id}"))
            })?;
            Ok(json!({
                "projectId": p.id, "name": p.name, "root": p.root,
                "defaults": serde_json::from_str::<Value>(&p.defaults).unwrap_or_default(),
            }))
        }

        "project.set_defaults" => {
            let id = req_str(&params, "projectId")?;
            let defaults = params.get("defaults").cloned().unwrap_or(Value::Null);
            if !defaults.is_object() {
                return Err(RpcError::error(
                    error_code::INVALID_PARAMS,
                    "defaults must be an object",
                ));
            }
            let updated = state
                .store
                .set_project_defaults(id, &defaults.to_string())
                .await?;
            Ok(json!({"updated": updated}))
        }

        "project.delete" => {
            let id = req_str(&params, "projectId")?;
            state
                .store
                .delete_project(id)
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, e.to_string()))?;
            Ok(json!({"deleted": true}))
        }

        "session.create" => {
            let mut cfg = session_config(&params)?;
            let mut backend = params["backend"].as_str().map(String::from);
            // A project scopes the session: its root becomes the cwd
            // default and its defaults fill whatever the request left
            // unset (explicit params always win).
            let project = match params["projectId"].as_str() {
                Some(pid) => {
                    let p = state.store.get_project(pid).await?.ok_or_else(|| {
                        RpcError::error(
                            error_code::INVALID_PARAMS,
                            format!("unknown project {pid}"),
                        )
                    })?;
                    let defaults: Value = serde_json::from_str(&p.defaults).unwrap_or_default();
                    if !params["cwd"].is_string() {
                        cfg.cwd = std::path::PathBuf::from(&p.root);
                    }
                    if cfg.model.is_none()
                        && let Some(m) = defaults["model"].as_str()
                    {
                        cfg.model = Some(m.to_string());
                    }
                    if cfg.mode.is_none()
                        && let Some(m) = defaults["mode"].as_str()
                    {
                        cfg.mode = Some(m.to_string());
                    }
                    if cfg.mcp_servers.is_empty()
                        && let Some(mcp) = defaults["mcpServers"].as_object()
                    {
                        cfg.mcp_servers = session_config(&json!({"mcpServers": mcp}))?.mcp_servers;
                    }
                    if backend.is_none()
                        && let Some(b) = defaults["backend"].as_str()
                    {
                        backend = Some(b.to_string());
                    }
                    Some(pid.to_string())
                }
                None => None,
            };
            check_cwd_allowed(state, &cfg.cwd)?;
            let ms = state
                .sessions
                .create(backend.as_deref(), cfg.clone())
                .await?;
            state
                .store
                .create_session(&ms.id, &cfg.cwd.to_string_lossy(), Some(&ms.provider))
                .await?;
            if let Some(pid) = &project {
                let _ = state.store.set_session_project(&ms.id, Some(pid)).await;
            }
            // Persist the resolved default model (explicit or project
            // default) — the row is the search/list surface for it.
            if let Some(m) = &cfg.model {
                let _ = state.store.set_session_model(&ms.id, m).await;
            }
            let replayed = conn.subscribe(state, &ms).await;
            Ok(json!({"sessionId": ms.id, "backend": ms.provider, "replayed": replayed}))
        }

        "session.resume" => {
            // Import path: a PersistenceHandle points at a native session
            // that has no Damon row yet — mint one, bind it, resume.
            if let Some(hv) = params.get("handle") {
                let handle: PersistenceHandle =
                    serde_json::from_value(hv.clone()).map_err(|_| {
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
                // Re-importing the same native session returns the
                // existing row instead of duplicating it.
                let id = match state
                    .store
                    .session_by_agent_session(&handle.provider, &handle.native_handle)
                    .await?
                {
                    Some(existing) => existing,
                    None => {
                        let id = uuid::Uuid::new_v4().to_string();
                        let cwd = params["cwd"]
                            .as_str()
                            .map(String::from)
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
                        if let Some(title) = params["title"].as_str() {
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
                let ms = state
                    .sessions
                    .resume(
                        &id,
                        &handle,
                        SessionConfig {
                            cwd,
                            ..Default::default()
                        },
                    )
                    .await?;
                let replayed = conn.subscribe(state, &ms).await;
                return Ok(json!({"sessionId": id, "backend": ms.provider, "replayed": replayed}));
            }
            let id = req_str(&params, "sessionId")?;
            // Already live? Just subscribe and return.
            if let Some(ms) = state.sessions.get(id).await {
                let replayed = conn.subscribe(state, &ms).await;
                return Ok(json!({"sessionId": id, "backend": ms.provider, "replayed": replayed}));
            }
            reattach(state, conn, id).await
        }

        "session.restart" => {
            let id = req_str(&params, "sessionId")?;
            // A live session is killed first (a crashed/wedged process
            // included — close is unconditional), then reattached
            // through the persisted handle; a not-live session just
            // reattaches. Busy sessions refuse: restarting mid-turn
            // would kill the turn's process underneath the collector.
            if let Some(ms) = state.sessions.get(id).await {
                if ms.busy.load(std::sync::atomic::Ordering::Relaxed) {
                    return Err(RpcError::error(
                        error_code::TURN_IN_PROGRESS,
                        "a turn is already running on this session",
                    ));
                }
                state.sessions.close(id).await?;
            }
            reattach(state, conn, id).await
        }

        "session.list" => {
            let limit = params["limit"].as_u64().unwrap_or(50) as u32;
            let offset = params["offset"].as_u64().unwrap_or(0) as u32;
            let filter = SessionFilter {
                backend: params["backend"].as_str().map(String::from),
                cwd: params["cwd"].as_str().map(String::from),
                tag: params["tag"].as_str().map(String::from),
                project_id: params["projectId"].as_str().map(String::from),
                include_archived: params["includeArchived"].as_bool().unwrap_or(false),
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

        "session.status" => {
            // Live-session introspection: which backend processes are
            // resident right now, which are mid-turn, how long idle.
            // Complements the /metrics gauges with per-session detail.
            let rows = state.sessions.live_status().await;
            let sessions: Vec<Value> = rows
                .into_iter()
                .map(|(id, backend, busy, idle_secs)| {
                    json!({"sessionId": id, "backend": backend,
                           "busy": busy, "idleSecs": idle_secs})
                })
                .collect();
            Ok(json!({"sessions": sessions}))
        }

        "session.watch" => {
            // Subscribe THIS connection to a session's events without
            // creating/resuming/turning on it — the cross-surface
            // notification path. A chat bridge watches a session the
            // web UI started and pings the chat when it finishes or
            // asks for permission. Requires a live session: a detached
            // one has no backend process broadcasting anything. The
            // subscribe replays missed history (replay-tagged), which
            // notification consumers skip.
            let id = req_str(&params, "sessionId")?;
            let ms = state.sessions.get(id).await.ok_or_else(|| {
                RpcError::error(
                    error_code::SESSION_NOT_LIVE,
                    "session not live — resume it before watching",
                )
            })?;
            let replayed = conn.subscribe(state, &ms).await;
            Ok(json!({"sessionId": id, "subscribed": true, "replayed": replayed}))
        }

        "session.unwatch" => {
            let id = req_str(&params, "sessionId")?;
            let removed = conn.unsubscribe(id).await;
            Ok(json!({"sessionId": id, "subscribed": !removed}))
        }

        "session.messages" => {
            let id = req_str(&params, "sessionId")?;
            let limit = params["limit"].as_u64().unwrap_or(200) as u32;
            let offset = params["offset"].as_u64().unwrap_or(0) as u32;
            let msgs = state.store.messages_paged(id, limit, offset).await?;
            Ok(json!({"messages": msgs}))
        }

        "session.export" => {
            let id = req_str(&params, "sessionId")?;
            // Header + transcript + usage in one shot — the wire-side
            // superset of the CLI's client-rendered export.
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
                // Same per-row fields as session.messages minus the
                // redundant session_id — it is in the header.
                "messages": msgs.iter().map(|m| json!({
                    "id": m.id, "role": m.role, "ts": m.ts, "data": m.data,
                })).collect::<Vec<_>>(),
                "usage": {
                    "contextUsed": used, "contextSize": size,
                    "costUsd": cost, "turns": turns,
                },
            }))
        }

        "session.import" => {
            let backend = req_str(&params, "backend")?;
            let clients = state.sessions.clients();
            let client = clients
                .iter()
                .find(|(id, _)| id == backend)
                .map(|(_, c)| c.clone())
                .ok_or_else(|| RpcError::error(error_code::INVALID_PARAMS, "unknown backend"))?;
            let cwd = params["cwd"]
                .as_str()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));
            check_cwd_allowed(state, &cwd)?;
            let found = client.list_importable_sessions(&cwd).await?;
            Ok(json!({"sessions": found}))
        }

        "session.delete" => {
            let id = req_str(&params, "sessionId")?;
            state.sessions.close(id).await.ok();
            state.store.delete_session(id).await?;
            Ok(json!({"deleted": true}))
        }

        "session.close" => {
            let id = req_str(&params, "sessionId")?;
            // Frees a max_sessions slot deterministically: kills the
            // live backend process, keeps the row and history (the
            // next session.resume reattaches). Idempotent — closing a
            // not-live id succeeds without touching anything.
            state.sessions.close(id).await?;
            Ok(json!({"closed": true}))
        }

        "session.set_tags" => {
            let id = req_str(&params, "sessionId")?;
            let tags: Vec<String> =
                serde_json::from_value(params["tags"].clone()).map_err(|_| {
                    RpcError::error(
                        error_code::INVALID_PARAMS,
                        "tags must be an array of strings",
                    )
                })?;
            let ok = state.store.set_tags(id, &tags).await?;
            Ok(json!({"updated": ok}))
        }

        "session.set_pinned" => {
            let id = req_str(&params, "sessionId")?;
            let pinned = params["pinned"].as_bool().ok_or_else(|| {
                RpcError::error(error_code::INVALID_PARAMS, "pinned must be a boolean")
            })?;
            let ok = state.store.set_pinned(id, pinned).await?;
            Ok(json!({"updated": ok}))
        }

        "session.set_archived" => {
            let id = req_str(&params, "sessionId")?;
            let archived = params["archived"].as_bool().ok_or_else(|| {
                RpcError::error(error_code::INVALID_PARAMS, "archived must be a boolean")
            })?;
            let ok = state.store.set_archived(id, archived).await?;
            Ok(json!({"updated": ok}))
        }

        "session.rename" => {
            let id = req_str(&params, "sessionId")?;
            let title = req_str(&params, "title")?;
            let ok = state.store.rename_session(id, title).await?;
            Ok(json!({"renamed": ok}))
        }

        "session.fork" => {
            let id = req_str(&params, "sessionId")?;
            let upto = params["upto"].as_i64();
            let new_id = state.store.fork_session(id, upto).await?;
            Ok(json!({"sessionId": new_id}))
        }

        "session.usage" => {
            let id = params["sessionId"].as_str();
            // `daily` is a global-view mode: with a sessionId it is
            // ignored — the per-session shape carries no day grouping.
            if id.is_none() && params["daily"].as_bool().unwrap_or(false) {
                let days = params["days"].as_u64().unwrap_or(7).max(1) as u32;
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
            match id {
                Some(id) => {
                    let (used, size, cost, turns) = state.store.session_usage(id).await?;
                    Ok(json!({
                        "sessionId": id,
                        "contextUsed": used, "contextSize": size,
                        "costUsd": cost, "turns": turns,
                    }))
                }
                None => {
                    let rows = state
                        .store
                        .usage_summary(params["projectId"].as_str().map(String::from))
                        .await?;
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

        "session.search" => {
            let q = req_str(&params, "query")?;
            let limit = params["limit"].as_u64().unwrap_or(20) as usize;
            // Wire timestamps (RFC3339 or date-only) become unix-ms
            // bounds here; the store stays a pure-ms API.
            let filter = SearchFilter {
                session_id: params["sessionId"].as_str().map(String::from),
                backend: params["backend"].as_str().map(String::from),
                cwd: params["cwd"].as_str().map(String::from),
                project_id: params["projectId"].as_str().map(String::from),
                since_ms: params["since"]
                    .as_str()
                    .map(parse_time_bound)
                    .transpose()
                    .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{e:#}")))?,
                until_ms: params["until"]
                    .as_str()
                    .map(parse_time_bound)
                    .transpose()
                    .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{e:#}")))?,
            };
            let hits = state.store.search_filtered(q, limit, filter).await?;
            let results: Vec<Value> = hits
                .into_iter()
                .map(|(session_id, message_id, snippet)| {
                    json!({"sessionId": session_id, "messageId": message_id, "snippet": snippet})
                })
                .collect();
            Ok(json!({"results": results}))
        }

        "turn.start" => {
            let id = req_str(&params, "sessionId")?;
            let ms = state.sessions.get(id).await.ok_or_else(|| {
                RpcError::error(
                    error_code::SESSION_NOT_LIVE,
                    "session not live — call session.resume first",
                )
            })?;
            // Claim the session exclusively: a second concurrent start
            // would run two collectors on the same broadcast and
            // double-persist every event. swap() is the claim;
            // run_turn's BusyGuard releases it on every exit path.
            if ms.busy.swap(true, std::sync::atomic::Ordering::AcqRel) {
                return Err(RpcError::error(
                    error_code::TURN_IN_PROGRESS,
                    "a turn is already running on this session",
                ));
            }
            // Register disconnect-cancel ownership immediately, before
            // any further await: a client that hangs up right after
            // `turn.start` must not leak an uncancellable turn. The
            // closed-flag check covers a disconnect whose sweep already
            // ran (it found an empty map); detach skips ownership
            // entirely — its turn outlives the connection by design.
            let detach = params["detach"].as_bool().unwrap_or(false);
            let cancel = if detach {
                None
            } else {
                let c = CancellationToken::new();
                let mut owned = conn.owned_turns.lock().await;
                owned.insert(ms.id.clone(), c.clone());
                if conn.closed.load(std::sync::atomic::Ordering::SeqCst) {
                    c.cancel();
                }
                drop(owned);
                Some(c)
            };
            conn.subscribe(state, &ms).await;
            run_turn(state, conn, &ms, params, detach, cancel).await
        }

        "turn.steer" => {
            let id = req_str(&params, "sessionId")?;
            let ms =
                state.sessions.get(id).await.ok_or_else(|| {
                    RpcError::error(error_code::SESSION_NOT_LIVE, "session not live")
                })?;
            let prompt: PromptInput = serde_json::from_value(params["prompt"].clone())
                .map_err(|_| RpcError::error(error_code::INVALID_PARAMS, "prompt required"))?;
            let expected = params["expectedTurn"].as_str().unwrap_or_default();
            let res = ms.session.steer(prompt, expected).await?;
            Ok(json!({"result": res}))
        }

        "turn.cancel" => {
            let id = req_str(&params, "sessionId")?;
            if let Some(ms) = state.sessions.get(id).await {
                ms.session.interrupt().await?;
            }
            Ok(json!({"cancelled": true}))
        }

        "permission.respond" => {
            let id = req_str(&params, "sessionId")?;
            let ms =
                state.sessions.get(id).await.ok_or_else(|| {
                    RpcError::error(error_code::SESSION_NOT_LIVE, "session not live")
                })?;
            let request_id = req_str(&params, "requestId")?;
            let response: PermissionResponse = serde_json::from_value(params["response"].clone())
                .map_err(|_| {
                RpcError::error(error_code::INVALID_PARAMS, "response required")
            })?;
            ms.session
                .respond_to_permission(request_id, response.clone())
                .await?;
            // A deny with `interrupt` kills the whole turn, not just
            // this call — backends only apply the flag to the pending
            // ask, so the daemon drives the follow-up interrupt here.
            // Best-effort: the deny itself already took effect.
            if matches!(
                response,
                PermissionResponse::Deny {
                    interrupt: true,
                    ..
                }
            ) && let Err(e) = ms.session.interrupt().await
            {
                tracing::warn!(error = %e, session = %id, "post-deny interrupt failed");
            }
            Ok(json!({}))
        }

        "session.set_model" => {
            let id = req_str(&params, "sessionId")?;
            let ms =
                state.sessions.get(id).await.ok_or_else(|| {
                    RpcError::error(error_code::SESSION_NOT_LIVE, "session not live")
                })?;
            let model = req_str(&params, "model")?;
            ms.session.set_model(model).await?;
            Ok(json!({}))
        }

        "session.set_mode" => {
            let id = req_str(&params, "sessionId")?;
            let ms =
                state.sessions.get(id).await.ok_or_else(|| {
                    RpcError::error(error_code::SESSION_NOT_LIVE, "session not live")
                })?;
            let mode = req_str(&params, "mode")?;
            ms.session.set_mode(mode).await?;
            Ok(json!({}))
        }

        "catalog.models" => {
            let backend = req_str(&params, "backend")?;
            let clients = state.sessions.clients();
            let client = clients
                .iter()
                .find(|(id, _)| id == backend)
                .map(|(_, c)| c.clone())
                .ok_or_else(|| RpcError::error(error_code::INVALID_PARAMS, "unknown backend"))?;
            let catalog = client.fetch_catalog(None).await?;
            Ok(serde_json::to_value(catalog)?)
        }
        "logs.tail" => {
            let n = params["lines"].as_u64().unwrap_or(200).min(2000) as usize;
            Ok(json!({"lines": crate::logs::ring().recent(n)}))
        }

        "logs.follow" => {
            // Opt this connection into a live push of every new log
            // line (`{"event": "log.line"}` frames — same envelope
            // family as session.event, no sessionId). Works through
            // the relay for the same reason every push does.
            let follow = params["follow"].as_bool().unwrap_or(true);
            let mut slot = conn.log_follow.lock().await;
            if let Some(t) = slot.take() {
                t.cancel();
            }
            if follow {
                let token = CancellationToken::new();
                let mut rx = crate::logs::ring().subscribe();
                let tx = conn.tx.clone();
                let stop = token.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            r = rx.recv() => match r {
                                Ok(line) => {
                                    let frame = json!({
                                        "event": "log.line",
                                        "data": {"line": line},
                                    });
                                    if tx.send(Message::Text(frame.to_string().into()))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }

                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    continue;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            },
                            _ = stop.cancelled() => break,
                        }
                    }
                });
                *slot = Some(token);
            }
            Ok(json!({"following": follow}))
        }

        "file.read" => {
            let id = req_str(&params, "sessionId")?;
            let path = req_str(&params, "path")?;
            let cwd = stored_cwd(state, id).await?;
            let full = resolve_jailed(&cwd, path)?;
            let meta = tokio::fs::metadata(&full)
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{path}: {e}")))?;
            if !meta.is_file() {
                return Err(RpcError::error(
                    error_code::INVALID_PARAMS,
                    format!("{path} is not a regular file"),
                ));
            }
            if meta.len() > FILE_READ_MAX as u64 {
                return Err(RpcError::error(
                    error_code::RESPONSE_TOO_LARGE,
                    format!(
                        "{path} is {} bytes — over the {} KiB read cap",
                        meta.len(),
                        FILE_READ_MAX / 1024
                    ),
                ));
            }
            let bytes = tokio::fs::read(&full)
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{path}: {e}")))?;
            let content = base64::engine::general_purpose::STANDARD.encode(&bytes);
            Ok(json!({"content": content, "bytes": bytes.len(), "path": path}))
        }

        "file.write" => {
            let id = req_str(&params, "sessionId")?;
            let path = req_str(&params, "path")?;
            let content = req_str(&params, "content")?;
            let cwd = stored_cwd(state, id).await?;
            let full = resolve_jailed(&cwd, path)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(content.as_bytes())
                .map_err(|e| {
                    RpcError::error(error_code::INVALID_PARAMS, format!("bad base64: {e}"))
                })?;
            if bytes.len() > FILE_WRITE_MAX {
                return Err(RpcError::error(
                    error_code::INVALID_PARAMS,
                    format!(
                        "decoded {} bytes — over the {} KiB write cap",
                        bytes.len(),
                        FILE_WRITE_MAX / 1024
                    ),
                ));
            }
            tokio::fs::write(&full, &bytes)
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{path}: {e}")))?;
            Ok(json!({"written": bytes.len(), "path": path}))
        }

        "file.list" => {
            let id = req_str(&params, "sessionId")?;
            let path = params["path"].as_str().unwrap_or(".");
            let cwd = stored_cwd(state, id).await?;
            let full = resolve_jailed(&cwd, path)?;
            let meta = tokio::fs::metadata(&full)
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{path}: {e}")))?;
            if !meta.is_dir() {
                return Err(RpcError::error(
                    error_code::INVALID_PARAMS,
                    format!("{path} is not a directory"),
                ));
            }
            let mut entries = tokio::fs::read_dir(&full)
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{path}: {e}")))?;
            let mut out = Vec::new();
            while let Some(e) = entries
                .next_entry()
                .await
                .map_err(|e| RpcError::error(error_code::INVALID_PARAMS, format!("{path}: {e}")))?
            {
                if out.len() >= FILE_LIST_MAX {
                    break;
                }
                let m = e.metadata().await.ok();
                out.push(json!({
                    "name": e.file_name().to_string_lossy(),
                    "dir": m.as_ref().is_some_and(|m| m.is_dir()),
                    "bytes": m.as_ref().map(|m| m.len()),
                }));
            }
            Ok(json!({"entries": out, "path": path}))
        }

        "channel.get_state" => {
            let conv = req_str(&params, "convId")?;
            let key = req_str(&params, "key")?;
            let value = state.store.get_channel_state(conv, key).await?;
            Ok(json!({"value": value}))
        }

        "channel.set_state" => {
            let conv = req_str(&params, "convId")?;
            let key = req_str(&params, "key")?;
            let value = req_str(&params, "value")?;
            state.store.set_channel_state(conv, key, value).await?;
            Ok(json!({"set": true}))
        }

        "channel.delete_state" => {
            let conv = req_str(&params, "convId")?;
            let key = req_str(&params, "key")?;
            state.store.delete_channel_state(conv, key).await?;
            Ok(json!({"deleted": true}))
        }

        other => Err(RpcError::error(
            error_code::METHOD_NOT_FOUND,
            format!("unknown method: {other}"),
        )),
    }
}

fn session_config(params: &Value) -> Result<SessionConfig> {
    let cwd = params["cwd"]
        .as_str()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));
    let mcp = params["mcpServers"]
        .as_object()
        .map(|m| {
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
        .unwrap_or_default();
    Ok(SessionConfig {
        cwd,
        model: params["model"].as_str().map(String::from),
        mode: params["mode"].as_str().map(String::from),
        mcp_servers: mcp,
    })
}

/// Best-effort canonical form for allowlist matching: absolute, with
/// symlinks resolved when the path exists. A missing path still
/// normalizes lexically, so a not-yet-created workspace directory can
/// be pre-authorized in config.
fn canon_cwd(p: &std::path::Path) -> std::path::PathBuf {
    match std::path::absolute(p) {
        Ok(abs) => abs.canonicalize().unwrap_or(abs),
        Err(_) => p.to_path_buf(),
    }
}

/// Fail-closed cwd gate against the hot-reloaded `allowed_dirs`.
/// An empty list keeps the zero-config default — every cwd allowed.
/// A non-empty list admits only cwds inside one of the roots
/// (component-wise prefix: `/a/b` admits `/a/b/c`, not `/a/bc`).
/// Applied at every spawn entry point: session.create, both
/// session.resume paths, and session.import.
use base64::Engine as _;

/// File-read cap: 512 KiB raw → ~683 KiB base64, safely inside the
/// 1 MiB response frame budget.
const FILE_READ_MAX: usize = 512 * 1024;
/// File-write cap, decoded bytes.
const FILE_WRITE_MAX: usize = 1024 * 1024;
/// Directory-listing entry cap.
const FILE_LIST_MAX: usize = 1000;

/// The session's stored cwd — file.* operates relative to (and jailed
/// inside) it. A session that was never created through the store has
/// no jail root and no file access.
async fn stored_cwd(state: &Arc<AppState>, id: &str) -> Result<std::path::PathBuf> {
    state
        .store
        .session_cwd(id)
        .await?
        .map(std::path::PathBuf::from)
        .ok_or_else(|| RpcError::error(error_code::INVALID_PARAMS, format!("unknown session {id}")))
}

/// Lexical absolute path: fold `.`/`..` component-wise without touching
/// the filesystem — the fallback when canonicalization can't run (the
/// target doesn't exist yet).
fn lexical_abs(p: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for c in p.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Resolve `user_path` (relative to the session cwd, or absolute)
/// inside the cwd jail. Symlinks are resolved where the path exists;
/// for a not-yet-existing target (writes), the deepest existing
/// ancestor canonicalizes and the missing tail is re-appended — so a
/// symlinked cwd still admits new files, and every escape (`..`,
/// symlink hop, absolute elsewhere) fails closed.
fn resolve_jailed(cwd: &std::path::Path, user_path: &str) -> Result<std::path::PathBuf> {
    let joined = {
        let p = std::path::Path::new(user_path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            cwd.join(p)
        }
    };
    let resolved = match joined.canonicalize() {
        Ok(c) => c,
        Err(_) => {
            let mut cur = joined.clone();
            let mut tail = Vec::new();
            let mut base = None;
            loop {
                match cur.canonicalize() {
                    Ok(c) => {
                        base = Some(c);
                        break;
                    }
                    Err(_) => match (
                        cur.parent().map(std::path::Path::to_path_buf),
                        cur.file_name().map(|n| n.to_os_string()),
                    ) {
                        (Some(p), Some(n)) => {
                            tail.push(n);
                            cur = p;
                        }
                        _ => break,
                    },
                }
            }
            match base {
                Some(mut b) => {
                    for n in tail.iter().rev() {
                        b.push(n);
                    }
                    b
                }
                None => lexical_abs(&joined),
            }
        }
    };
    let jail = cwd.canonicalize().unwrap_or_else(|_| lexical_abs(cwd));
    if !resolved.starts_with(&jail) {
        return Err(RpcError::error(
            error_code::INVALID_PARAMS,
            format!("{user_path} escapes the session cwd"),
        ));
    }
    Ok(resolved)
}

fn check_cwd_allowed(state: &AppState, cwd: &std::path::Path) -> Result<()> {
    let allowed = state.config.read().allowed_dirs.clone();
    if allowed.is_empty() {
        return Ok(());
    }
    let abs = canon_cwd(cwd);
    if allowed.iter().any(|root| abs.starts_with(canon_cwd(root))) {
        return Ok(());
    }
    Err(RpcError::error(
        error_code::INVALID_PARAMS,
        format!(
            "cwd {} is outside the configured allowed_dirs",
            cwd.display()
        ),
    ))
}

/// Reattach a persisted session: resolve the stored backend handle,
/// gate its cwd through `allowed_dirs`, spawn a fresh backend session,
/// and subscribe this connection with replay. Shared by the plain
/// `session.resume` path and `session.restart`.
async fn reattach(state: &Arc<AppState>, conn: &Arc<Conn>, id: &str) -> Result<Value> {
    let (provider, native) = state
        .store
        .agent_session(id)
        .await?
        .context("no persisted backend session for this id")?;
    let cwd = state
        .store
        .session_cwd(id)
        .await?
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    check_cwd_allowed(state, &cwd)?;
    let handle = PersistenceHandle {
        provider: provider.clone(),
        native_handle: native,
        metadata: Value::Null,
    };
    let ms = state
        .sessions
        .resume(
            id,
            &handle,
            SessionConfig {
                cwd,
                ..Default::default()
            },
        )
        .await?;
    let replayed = conn.subscribe(state, &ms).await;
    Ok(json!({"sessionId": id, "backend": ms.provider, "replayed": replayed}))
}

/// Drive one turn. Blocking (default): forward events to the
/// connection, persist the timeline, honour disconnect-cancel, and
/// resolve with the final result — the client sees events first, then
/// the response. With `detach: true` the response returns immediately
/// and a background collector finishes the turn on a task no connection
/// owns: disconnecting never cancels it (a locked phone drops its
/// socket mid-turn), while `turn.cancel`, a deny-with-interrupt, and
/// `timeoutSecs` still reach it. Both paths persist identically, so a
/// late subscriber replays the outcome from the store or the journal.
async fn run_turn(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    ms: &Arc<ManagedSession>,
    params: Value,
    detach: bool,
    cancel: Option<CancellationToken>,
) -> Result<Value> {
    let started = std::time::Instant::now();
    // Concurrency cap — held for the turn's whole life, whether the
    // collector ends up on this task or a detached one.
    let slot = PROMPT_SLOTS.acquire().await?;
    // The dispatcher claimed `busy` with swap() — this guard releases
    // it on every exit path, including the early parameter errors
    // below, so a rejected turn never wedges the session. A detached
    // turn hands the claim to its background collector instead.
    let mut _busy = BusyGuard::new(ms.clone());
    let prompt: PromptInput = serde_json::from_value(params["prompt"].clone())
        .map_err(|_| RpcError::error(error_code::INVALID_PARAMS, "prompt required"))?;
    let timeout = params["timeoutSecs"]
        .as_u64()
        .map(std::time::Duration::from_secs);

    // Persist the user message up front — a crash mid-turn still leaves
    // the prompt in history.
    let user_text = prompt_text(&prompt);
    if !user_text.is_empty() {
        state
            .store
            .append(&ms.id, "user", &json!({"content": user_text}))
            .await?;
        let _ = state.store.set_title_if_empty(&ms.id, &user_text).await;
    }
    // First use ends the fresh-session reap grace. (`busy` was already
    // claimed by the turn.start dispatcher — only the guard releases.)
    ms.ever_used
        .store(true, std::sync::atomic::Ordering::Relaxed);

    // The registration itself lives in the dispatcher (immediately after
    // the busy claim); this guard removes it again when the turn ends.
    let _guard = if detach {
        None
    } else {
        Some(scopeguard(conn.clone(), ms.id.clone()))
    };

    // Subscribe before start_turn so no early event is missed.
    let events = ms.session.subscribe();
    // start_turn must race the disconnect token: a backend that parks
    // inside its handshake/prompt hand-off (the mock's hang knob, a
    // slow agent boot) would otherwise hold the busy claim with no
    // connection left to answer for it.
    let turn_id = match &cancel {
        Some(c) => tokio::select! {
            r = ms.session.start_turn(prompt) => r?,
            _ = c.cancelled() => {
                let _ = ms.session.interrupt().await;
                return Err(anyhow::anyhow!(
                    "connection dropped before the turn started"
                ));
            }
        },
        None => ms.session.start_turn(prompt).await?,
    };

    if detach {
        let busy = _busy.hand_off();
        let st = state.clone();
        let m = ms.clone();
        tokio::spawn(async move {
            let _slot = slot;
            let _busy = busy;
            if let Err(e) = drive_turn(&st, &m, events, timeout, None, started).await {
                tracing::warn!(session = %m.id, error = %e, "detached turn ended");
            }
        });
        return Ok(json!({"turnId": turn_id, "detached": true}));
    }

    let _slot = slot;
    let (stop_reason, usage) = drive_turn(state, ms, events, timeout, cancel, started).await?;
    Ok(json!({
        "turnId": turn_id,
        "stopReason": stop_reason,
        "usage": usage,
    }))
}

/// Collect one turn's events to its boundary, persist every artifact,
/// and record metrics. Shared by the blocking and detached paths; the
/// `cancel` token is the starting connection's disconnect handle (None
/// for detached turns — nothing but `turn.cancel`, a deny-with-
/// interrupt, or `timeoutSecs` ends them early). Errors cover both a
/// `turn_failed` boundary and persistence failures. Returns the stop
/// reason and the usage snapshot.
#[allow(clippy::type_complexity)]
async fn drive_turn(
    state: &Arc<AppState>,
    ms: &Arc<ManagedSession>,
    mut events: tokio::sync::broadcast::Receiver<StreamEvent>,
    timeout: Option<std::time::Duration>,
    cancel: Option<CancellationToken>,
    started: std::time::Instant,
) -> Result<(&'static str, Option<Usage>)> {
    let mut assistant = String::new();
    let mut reasoning = String::new();
    let mut tools: HashMap<String, ToolCall> = HashMap::new();
    let mut compactions: Vec<String> = Vec::new();
    let mut failure: Option<String> = None;
    let mut usage = None;
    let mut stop_reason = "completed";

    let collect = async {
        loop {
            match events.recv().await {
                Ok(ev) => match &ev.kind {
                    StreamEventKind::Timeline(TimelineItem::AssistantMessage { text }) => {
                        assistant.push_str(text);
                    }
                    StreamEventKind::Timeline(TimelineItem::Reasoning { text }) => {
                        reasoning.push_str(text);
                    }
                    StreamEventKind::Timeline(TimelineItem::ToolCall(t)) => {
                        tools.insert(t.call_id.clone(), t.clone());
                    }
                    StreamEventKind::Timeline(TimelineItem::Compaction { summary }) => {
                        compactions.push(summary.clone());
                    }
                    StreamEventKind::TurnCompleted { usage: u } => {
                        usage = u.clone();
                        break;
                    }
                    StreamEventKind::TurnFailed { error, .. } => {
                        failure = Some(error.clone());
                        stop_reason = "failed";
                        bail!("{error}");
                    }
                    StreamEventKind::TurnCanceled { .. } => {
                        stop_reason = "canceled";
                        break;
                    }
                    _ => {}
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    stop_reason = "closed";
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    // `pending()` stands in for the absent timeout/disconnect arms —
    // each condition folds to "never fires" instead of duplicating the
    // select per combination.
    let outcome = tokio::select! {
        r = collect => r,
        _ = async {
            match timeout {
                Some(t) => tokio::time::sleep(t).await,
                None => std::future::pending().await,
            }
        } => {
            let _ = ms.session.interrupt().await;
            stop_reason = "timeout";
            Ok(())
        }
        _ = async {
            match &cancel {
                Some(c) => c.cancelled().await,
                None => std::future::pending().await,
            }
        } => {
            let _ = ms.session.interrupt().await;
            stop_reason = "canceled";
            Ok(())
        }
    };

    // Persist the assembled timeline — one row per artifact, not per delta.
    if !assistant.is_empty() {
        let _ = state
            .store
            .append(&ms.id, "assistant", &json!({"content": assistant}))
            .await;
    }
    if !reasoning.is_empty() {
        let _ = state
            .store
            .append(&ms.id, "reasoning", &json!({"content": reasoning}))
            .await;
    }
    for t in tools.values() {
        // Best-effort like every row in this phase — one bad row must
        // not skip the remaining artifacts, usage, or the resume-token
        // refresh. ToolCall always serializes; the let-else is for
        // future field changes.
        let Ok(v) = serde_json::to_value(t) else {
            continue;
        };
        let _ = state.store.append(&ms.id, "tool", &v).await;
    }
    // Compaction boundaries and the failure reason are part of the
    // durable transcript — a client that missed the live stream (or a
    // reconnecting one) reads them from session.messages/replay
    // instead of a prompt with no answer and no marker.
    for summary in &compactions {
        let _ = state
            .store
            .append(&ms.id, "compaction", &json!({"content": summary}))
            .await;
    }
    if let Some(err) = &failure {
        let _ = state
            .store
            .append(&ms.id, "error", &json!({"content": err}))
            .await;
    }
    if let Some(u) = &usage {
        let _ = state
            .store
            .record_usage(
                &ms.id,
                "",
                u.context_used.unwrap_or(0),
                u.context_window.unwrap_or(0),
                u.cost_usd.unwrap_or(0.0),
            )
            .await;
    }
    let _ = state.store.touch(&ms.id).await;

    // Refresh the persisted resume token if the backend reported one.
    if let Some(h) = &ms.handle {
        let _ = state
            .store
            .set_agent_session(&ms.id, &h.provider, &h.native_handle)
            .await;
    }

    state
        .metrics
        .record_turn(&ms.provider, stop_reason, started.elapsed());
    outcome.map(|()| (stop_reason, usage))
}

/// Drop-guard: clears the session's busy flag when the turn ends, so the
/// idle sweep can reap it again. The Option lets a detached turn hand
/// the claim to its background collector — the flag must stay set until
/// the turn actually ends, not until run_turn returns its early
/// response.
struct BusyGuard(Option<Arc<ManagedSession>>);
impl BusyGuard {
    fn new(ms: Arc<ManagedSession>) -> Self {
        Self(Some(ms))
    }
    /// Transfer the busy claim to another owner; the receiver's drop
    /// releases the flag.
    fn hand_off(&mut self) -> Self {
        Self(self.0.take())
    }
}
impl Drop for BusyGuard {
    fn drop(&mut self) {
        if let Some(ms) = self.0.take() {
            ms.busy.store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Drop-guard: removes this connection's turn ownership on exit.
fn scopeguard(conn: Arc<Conn>, session_id: String) -> impl Drop {
    struct Guard(Arc<Conn>, String);
    impl Drop for Guard {
        fn drop(&mut self) {
            let conn = self.0.clone();
            let id = self.1.clone();
            tokio::spawn(async move {
                conn.owned_turns.lock().await.remove(&id);
            });
        }
    }
    Guard(conn, session_id)
}

/// Flatten a prompt to plain text for the title/history row.
fn prompt_text(prompt: &PromptInput) -> String {
    match prompt {
        PromptInput::Text(t) => t.clone(),
        PromptInput::Blocks(blocks) => blocks
            .iter()
            .filter_map(|b| match b {
                PromptBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

/// Self-describing list of every method this daemon handles — the wire
/// contract, returned by `hello` and printed by `damond --print-rpc-schema`.
pub fn rpc_methods() -> Vec<Value> {
    vec![
        json!({"name": "hello", "result": {"protocol": "number", "backends": "array", "methods": "array"}}),
        json!({"name": "backend.list", "result": {"backends": "array"}}),
        json!({"name": "session.create", "params": {"backend": "string?", "cwd": "string?", "model": "string?", "mode": "string?", "mcpServers": "object?", "projectId": "string? — scopes the session: root becomes the cwd default, project defaults fill unset params"}, "result": {"sessionId": "string", "backend": "string", "replayed": "number — replay-tagged session.event frames sent for missed history"}}),
        json!({"name": "session.resume", "params": {"sessionId": "string — or", "handle": "{provider, native_handle}", "cwd": "string?", "title": "string?"}, "result": {"sessionId": "string", "backend": "string", "replayed": "number"}}),
        json!({"name": "session.list", "params": {"limit": "number?", "offset": "number?", "backend": "string?", "cwd": "string?", "tag": "string?", "projectId": "string?"}, "result": {"sessions": "array of {sessionId, createdAt, backend, title, cwd, tags}"}}),
        json!({"name": "session.messages", "params": {"sessionId": "string", "limit": "number?", "offset": "number?"}, "result": {"messages": "array of {id, session_id, role, ts, data} — ts is unix ms, 0 when unknown"}}),
        json!({"name": "session.export", "params": {"sessionId": "string"}, "result": {"session": "{sessionId, createdAt, backend, title, tags, cwd}", "messages": "array of {id, role, ts, data}", "usage": "{contextUsed, contextSize, costUsd, turns}"}}),
        json!({"name": "session.import", "params": {"backend": "string", "cwd": "string?"}, "result": {"sessions": "array"}}),
        json!({"name": "session.delete", "params": {"sessionId": "string"}, "result": {"deleted": "boolean"}}),
        json!({"name": "session.close", "params": {"sessionId": "string"}, "result": {"closed": "boolean — kills the live backend process, keeps the row/history; frees a max_sessions slot"}}),
        json!({"name": "session.restart", "params": {"sessionId": "string"}, "result": {"sessionId": "string", "backend": "string", "replayed": "number — kills the live backend (refuses with -32006 mid-turn) and reattaches through the persisted handle"}}),
        json!({"name": "session.status", "params": {}, "result": {"sessions": "array of {sessionId, backend, busy, idleSecs} — live backend sessions only"}}),
        json!({"name": "session.watch", "params": {"sessionId": "string — must be live"}, "result": {"sessionId": "string", "subscribed": "boolean", "replayed": "number — subscribes this connection to the session's events without touching it; the cross-surface notification path"}}),
        json!({"name": "session.unwatch", "params": {"sessionId": "string"}, "result": {"sessionId": "string", "subscribed": "boolean — false when this connection was subscribed and now is not"}}),
        json!({"name": "session.set_pinned", "params": {"sessionId": "string", "pinned": "boolean"}, "result": {"updated": "boolean — pinned rows float to the top of session.list and survive the retention sweep"}}),
        json!({"name": "session.set_archived", "params": {"sessionId": "string", "archived": "boolean"}, "result": {"updated": "boolean — archived rows are hidden from session.list unless includeArchived, and survive the retention sweep"}}),
        json!({"name": "session.set_tags", "params": {"sessionId": "string", "tags": "string[] — replaces the whole list"}, "result": {"updated": "boolean"}}),
        json!({"name": "session.rename", "params": {"sessionId": "string", "title": "string"}, "result": {"renamed": "boolean"}}),
        json!({"name": "session.fork", "params": {"sessionId": "string", "upto": "number?"}, "result": {"sessionId": "string — copies messages, tags, usage history, and the backend resume handle; title suffixed \" (fork)\""}}),
        json!({"name": "session.usage", "params": {"sessionId": "string?", "daily": "boolean? — global view only", "days": "number? — daily window in days, default 7"}, "result": {"daily": "array of {date, turns, costUsd, contextUsed} when daily, else per-session/per-model totals"}}),
        json!({"name": "session.search", "params": {"query": "string", "limit": "number?", "sessionId": "string?", "backend": "string?", "cwd": "string?", "projectId": "string?", "since": "string? — RFC3339 or YYYY-MM-DD", "until": "string?"}, "result": {"results": "array of {sessionId, messageId, snippet}"}}),
        json!({"name": "turn.start", "params": {"sessionId": "string", "prompt": "string|array", "timeoutSecs": "number?", "detach": "boolean? — true resolves immediately {turnId, detached:true} and the turn outlives the connection (not cancelled on disconnect); turn.cancel/timeoutSecs still apply"}, "result": {"turnId": "string", "stopReason": "string", "usage": "object?"}}),
        json!({"name": "turn.steer", "params": {"sessionId": "string", "prompt": "string|array"}, "result": {"result": "string"}}),
        json!({"name": "turn.cancel", "params": {"sessionId": "string"}, "result": {"cancelled": "boolean"}}),
        json!({"name": "permission.respond", "params": {"sessionId": "string", "requestId": "string", "response": "object"}, "result": {}}),
        json!({"name": "session.set_model", "params": {"sessionId": "string", "model": "string"}, "result": {}}),
        json!({"name": "session.set_mode", "params": {"sessionId": "string", "mode": "string"}, "result": {}}),
        json!({"name": "catalog.models", "params": {"backend": "string"}, "result": {"models": "array", "modes": "array"}}),
        json!({"name": "logs.tail", "params": {"lines": "number? — most recent lines, default 200, max 2000"}, "result": {"lines": "string[] — the daemon's recent log lines, oldest first"}}),
        json!({"name": "logs.follow", "params": {"follow": "boolean? — default true; false stops a previous follow"}, "result": {"following": "boolean — when true, every new log line arrives as a log.line push"}}),
        json!({"name": "file.read", "params": {"sessionId": "string", "path": "string — relative to the session cwd or absolute inside it"}, "result": {"content": "string — base64", "bytes": "number", "path": "string"}}),
        json!({"name": "file.write", "params": {"sessionId": "string", "path": "string", "content": "string — base64"}, "result": {"written": "number", "path": "string"}}),
        json!({"name": "file.list", "params": {"sessionId": "string", "path": "string? — default ."}, "result": {"entries": "array of {name, dir, bytes}", "path": "string"}}),
        json!({"name": "channel.get_state", "params": {"convId": "string", "key": "string"}, "result": {"value": "string?"}}),
        json!({"name": "channel.set_state", "params": {"convId": "string", "key": "string", "value": "string"}, "result": {"set": "boolean"}}),
        json!({"name": "channel.delete_state", "params": {"convId": "string", "key": "string"}, "result": {"deleted": "boolean"}}),
        json!({"name": "project.create", "params": {"name": "string?", "root": "string — absolute", "defaults?": "{backend?, model?, mode?, mcpServers?}"}, "result": {"projectId": "string", "name": "string", "root": "string", "defaults": "object"}}),
        json!({"name": "project.list", "params": {}, "result": {"projects": "array of {projectId, name, root, defaults}"}}),
        json!({"name": "project.get", "params": {"projectId": "string"}, "result": {"projectId": "string", "name": "string", "root": "string", "defaults": "object"}}),
        json!({"name": "project.set_defaults", "params": {"projectId": "string", "defaults": "object — replaces the whole set"}, "result": {"updated": "boolean"}}),
        json!({"name": "project.delete", "params": {"projectId": "string"}, "result": {"deleted": "boolean — refuses -32602 while sessions reference the project"}}),
    ]
}

/// Days from 1970-01-01 for a proleptic-Gregorian civil date —
/// Hinnant's `days_from_civil`, the date half of `parse_time_bound`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parse a wire time bound into unix milliseconds. Accepts RFC3339
/// (`2026-09-01T13:30:00Z`, optional fractional seconds, `Z` or
/// `±HH:MM` offset, `T` or space separator) and date-only
/// `YYYY-MM-DD`, which reads as midnight. A stated offset shifts the
/// instant; a naive timestamp reads as UTC — the store's stamps are
/// UTC (`datetime('now')`) and the daemon has no user timezone to
/// honor. Malformed input errors instead of silently matching
/// nothing (a bound nobody wrote must not look like an empty result).
fn parse_time_bound(input: &str) -> Result<i64> {
    let s = input.trim();
    let bad = || anyhow::anyhow!("invalid time bound {input:?} — expected RFC3339 or YYYY-MM-DD");
    let digits = |r: &[u8]| -> Result<i64> {
        if r.is_empty() || !r.iter().all(u8::is_ascii_digit) {
            return Err(bad());
        }
        Ok(r.iter().fold(0i64, |a, &c| a * 10 + (c - b'0') as i64))
    };
    let b = s.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return Err(bad());
    }
    let (year, month, day) = (digits(&b[0..4])?, digits(&b[5..7])?, digits(&b[8..10])?);
    if !(1..=12).contains(&month) {
        return Err(bad());
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let dim = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=dim[(month - 1) as usize]).contains(&day) {
        return Err(bad());
    }
    let mut ms = days_from_civil(year, month, day) * 86_400_000;
    let rest = &s[10..];
    if rest.is_empty() {
        return Ok(ms); // date-only: midnight
    }
    let t = rest.strip_prefix(['T', 't', ' ']).ok_or_else(bad)?;
    let tb = t.as_bytes();
    if tb.len() < 8 || tb[2] != b':' || tb[5] != b':' {
        return Err(bad());
    }
    let (hh, mm, ss) = (digits(&tb[0..2])?, digits(&tb[3..5])?, digits(&tb[6..8])?);
    if hh > 23 || mm > 59 || ss > 59 {
        return Err(bad());
    }
    ms += (hh * 3600 + mm * 60 + ss) * 1000;
    let mut tail = &t[8..];
    if let Some(frac) = tail.strip_prefix('.') {
        let fb = frac.as_bytes();
        let n = fb
            .iter()
            .position(|c| !c.is_ascii_digit())
            .unwrap_or(fb.len());
        if n == 0 {
            return Err(bad());
        }
        // Millisecond precision — extra digits round down; bounds are
        // coarse filters, not clocks.
        let k = n.min(3);
        let frac_ms: i64 = frac[..k].parse().map_err(|_| bad())?;
        ms += frac_ms * 10i64.pow((3 - k) as u32);
        tail = &tail[1 + n..];
    }
    match tail {
        "" | "Z" | "z" => {}
        _ => {
            let ob = tail.as_bytes();
            if ob.len() == 6 && (ob[0] == b'+' || ob[0] == b'-') && ob[3] == b':' {
                let sign = if ob[0] == b'+' { 1 } else { -1 };
                let (oh, om) = (digits(&ob[1..3])?, digits(&ob[4..6])?);
                if oh > 23 || om > 59 {
                    return Err(bad());
                }
                // +02:00 means local = UTC+2 → subtract to reach UTC.
                ms -= sign * (oh * 3600 + om * 60) * 1000;
            } else {
                return Err(bad());
            }
        }
    }
    Ok(ms)
}

#[cfg(test)]
mod tests {
    use super::parse_time_bound;

    /// Known instants: date-only midnight UTC, RFC3339 with Z, the
    /// space separator SQLite writes, fractional seconds, explicit
    /// offsets (both signs), and rejections for malformed input — a
    /// bound nobody can write must error, not match nothing.
    #[test]
    fn time_bounds_parse_to_unix_ms() {
        assert_eq!(parse_time_bound("1970-01-01").unwrap(), 0);
        assert_eq!(parse_time_bound("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(parse_time_bound("1970-01-01T00:00:00.500Z").unwrap(), 500);
        assert_eq!(parse_time_bound("2026-09-01").unwrap(), 1_788_220_800_000);
        assert_eq!(
            parse_time_bound("2026-09-01T13:30:05Z").unwrap(),
            1_788_220_800_000 + (13 * 3600 + 30 * 60 + 5) * 1000
        );
        // Naive timestamps with the space separator read as UTC.
        assert_eq!(
            parse_time_bound("2026-09-01 13:30:05").unwrap(),
            parse_time_bound("2026-09-01T13:30:05Z").unwrap()
        );
        // Explicit offsets: +02:00 local is two hours behind UTC.
        assert_eq!(
            parse_time_bound("2026-09-01T02:00:00+02:00").unwrap(),
            parse_time_bound("2026-09-01T00:00:00Z").unwrap()
        );
        assert_eq!(
            parse_time_bound("1970-01-01T00:00:00-01:00").unwrap(),
            3_600_000
        );
        // Leap day parses in a leap year, refuses otherwise.
        assert!(parse_time_bound("2024-02-29").is_ok());
        assert!(parse_time_bound("2025-02-29").is_err());
        for bad in [
            "not-a-date",
            "2026-13-01",
            "2026-00-10",
            "2026-02-30",
            "2026-9-1",
            "2026-09-01T25:00:00Z",
            "2026-09-01T10:60:00Z",
            "2026-09-01T10:00:60Z",
            "2026-09-01X10:00:00",
            "2026-09-01T10:00",
            "2026-09-01T10:00:00+2:00",
            "2026-09-01T10:00:00.",
        ] {
            assert!(parse_time_bound(bad).is_err(), "accepted {bad:?}");
        }
    }
}
