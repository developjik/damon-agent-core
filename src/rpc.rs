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

/// Per-connection state: outbound frames, session event forwarders,
/// and the turns this connection started (cancelled on disconnect).
struct Conn {
    tx: mpsc::Sender<Message>,
    /// session id → forwarder task. A session's events reach this
    /// connection once it has touched the session.
    subscriptions: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Turns this connection owns — disconnect cancels them.
    owned_turns: Mutex<HashMap<String, CancellationToken>>,
}

impl Conn {
    /// Push one JSON value as a text frame. Bounded channel: a stalled
    /// client drops events rather than stalling the daemon.
    fn send(&self, v: Value) {
        let _ = self.tx.try_send(Message::Text(v.to_string().into()));
    }

    /// Start forwarding a session's event stream to this connection.
    /// Idempotent — one forwarder per (connection, session).
    async fn subscribe(&self, session: &Arc<ManagedSession>) {
        let mut subs = self.subscriptions.lock().await;
        if subs.contains_key(&session.id) {
            return;
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
    // client can't leave an agent burning tokens.
    for (_, token) in conn.owned_turns.lock().await.drain() {
        token.cancel();
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

    for (_, token) in conn.owned_turns.lock().await.drain() {
        token.cancel();
    }
    writer.abort();
}

/// Parse one inbound JSON frame and spawn its dispatch. Requests run
/// concurrently — a long turn never serializes the socket.
fn handle_frame(state: &Arc<AppState>, conn: &Arc<Conn>, text: &str) {
    let Ok(req) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let Some(method) = req["method"].as_str() else {
        return;
    };
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let params = req.get("params").cloned().unwrap_or(json!({}));
    let state = state.clone();
    let conn = conn.clone();
    let method = method.to_string();
    tokio::spawn(async move {
        let result = dispatch(&state, &conn, &method, params).await;
        let frame = match result {
            Ok(v) => json!({"id": id, "result": v}),
            Err(e) => json!({"id": id, "error": {"code": -32000, "message": e.to_string()}}),
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

        "session.create" => {
            let cfg = session_config(&params)?;
            let backend = params["backend"].as_str();
            let ms = state.sessions.create(backend, cfg.clone()).await?;
            state
                .store
                .create_session(&ms.id, &cfg.cwd.to_string_lossy(), Some(&ms.provider))
                .await?;
            conn.subscribe(&ms).await;
            Ok(json!({"sessionId": ms.id, "backend": ms.provider}))
        }

        "session.resume" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            // Already live? Just subscribe and return.
            if let Some(ms) = state.sessions.get(id).await {
                conn.subscribe(&ms).await;
                return Ok(json!({"sessionId": id, "backend": ms.provider}));
            }
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
            conn.subscribe(&ms).await;
            Ok(json!({"sessionId": id, "backend": ms.provider}))
        }

        "session.list" => {
            let limit = params["limit"].as_u64().unwrap_or(50) as u32;
            let offset = params["offset"].as_u64().unwrap_or(0) as u32;
            let rows = state.store.list_sessions_paged(limit, offset).await?;
            let sessions: Vec<Value> = rows
                .into_iter()
                .map(|(id, created_at, backend, title)| {
                    json!({"sessionId": id, "createdAt": created_at, "backend": backend, "title": title})
                })
                .collect();
            Ok(json!({"sessions": sessions}))
        }

        "session.messages" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let limit = params["limit"].as_u64().unwrap_or(200) as u32;
            let offset = params["offset"].as_u64().unwrap_or(0) as u32;
            let msgs = state.store.messages_paged(id, limit, offset).await?;
            Ok(json!({"messages": msgs}))
        }

        "session.import" => {
            let backend = params["backend"].as_str().context("backend required")?;
            let clients = state.sessions.clients();
            let client = clients
                .iter()
                .find(|(id, _)| id == backend)
                .map(|(_, c)| c.clone())
                .context("unknown backend")?;
            let cwd = params["cwd"]
                .as_str()
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));
            let found = client.list_importable_sessions(&cwd).await?;
            Ok(json!({"sessions": found}))
        }

        "session.delete" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            state.sessions.close(id).await.ok();
            state.store.delete_session(id).await?;
            Ok(json!({"deleted": true}))
        }

        "session.rename" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let title = params["title"].as_str().context("title required")?;
            let ok = state.store.rename_session(id, title).await?;
            Ok(json!({"renamed": ok}))
        }

        "session.fork" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let upto = params["upto"].as_i64();
            let new_id = state.store.fork_session(id, upto).await?;
            Ok(json!({"sessionId": new_id}))
        }

        "session.usage" => {
            let id = params["sessionId"].as_str();
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
                    let rows = state.store.usage_summary().await?;
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
            let q = params["query"].as_str().context("query required")?;
            let limit = params["limit"].as_u64().unwrap_or(20) as u32;
            let hits = state
                .store
                .search_filtered(q, limit as usize, None, None, None)
                .await?;
            let results: Vec<Value> = hits
                .into_iter()
                .map(|(session_id, message_id, snippet)| {
                    json!({"sessionId": session_id, "messageId": message_id, "snippet": snippet})
                })
                .collect();
            Ok(json!({"results": results}))
        }

        "turn.start" => {
            let _slot = PROMPT_SLOTS.acquire().await?;
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let ms = state
                .sessions
                .get(id)
                .await
                .context("session not live — call session.resume first")?;
            conn.subscribe(&ms).await;
            run_turn(state, conn, &ms, params).await
        }

        "turn.steer" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let ms = state.sessions.get(id).await.context("session not live")?;
            let prompt: PromptInput =
                serde_json::from_value(params["prompt"].clone()).context("prompt required")?;
            let expected = params["expectedTurn"].as_str().unwrap_or_default();
            let res = ms.session.steer(prompt, expected).await?;
            Ok(json!({"result": res}))
        }

        "turn.cancel" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            if let Some(ms) = state.sessions.get(id).await {
                ms.session.interrupt().await?;
            }
            Ok(json!({"cancelled": true}))
        }

        "permission.respond" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let ms = state.sessions.get(id).await.context("session not live")?;
            let request_id = params["requestId"].as_str().context("requestId required")?;
            let response: PermissionResponse =
                serde_json::from_value(params["response"].clone()).context("response required")?;
            ms.session
                .respond_to_permission(request_id, response)
                .await?;
            Ok(json!({}))
        }

        "session.set_model" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let ms = state.sessions.get(id).await.context("session not live")?;
            let model = params["model"].as_str().context("model required")?;
            ms.session.set_model(model).await?;
            Ok(json!({}))
        }

        "session.set_mode" => {
            let id = params["sessionId"].as_str().context("sessionId required")?;
            let ms = state.sessions.get(id).await.context("session not live")?;
            let mode = params["mode"].as_str().context("mode required")?;
            ms.session.set_mode(mode).await?;
            Ok(json!({}))
        }

        "catalog.models" => {
            let backend = params["backend"].as_str().context("backend required")?;
            let clients = state.sessions.clients();
            let client = clients
                .iter()
                .find(|(id, _)| id == backend)
                .map(|(_, c)| c.clone())
                .context("unknown backend")?;
            let catalog = client.fetch_catalog(None).await?;
            Ok(serde_json::to_value(catalog)?)
        }

        other => bail!("unknown method: {other}"),
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
        ..Default::default()
    })
}

/// Drive one turn to completion: forward events to the connection,
/// persist the timeline, honour disconnect-cancel. Returns the final
/// result payload — the client sees events first, then the response.
async fn run_turn(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    ms: &Arc<ManagedSession>,
    params: Value,
) -> Result<Value> {
    let prompt: PromptInput =
        serde_json::from_value(params["prompt"].clone()).context("prompt required")?;
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

    let cancel = CancellationToken::new();
    conn.owned_turns
        .lock()
        .await
        .insert(ms.id.clone(), cancel.clone());
    let _guard = scopeguard(conn.clone(), ms.id.clone());
    ms.busy.store(true, std::sync::atomic::Ordering::Relaxed);
    let _busy = BusyGuard(ms.clone());

    let mut events = ms.session.subscribe();
    let turn_id = ms.session.start_turn(prompt).await?;

    let mut assistant = String::new();
    let mut reasoning = String::new();
    let mut tools: HashMap<String, ToolCall> = HashMap::new();
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
                    StreamEventKind::TurnCompleted { usage: u } => {
                        usage = u.clone();
                        break;
                    }
                    StreamEventKind::TurnFailed { error, .. } => {
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

    let outcome = if let Some(t) = timeout {
        tokio::select! {
            r = collect => r,
            _ = tokio::time::sleep(t) => {
                let _ = ms.session.interrupt().await;
                stop_reason = "timeout";
                Ok(())
            }
            _ = cancel.cancelled() => {
                let _ = ms.session.interrupt().await;
                stop_reason = "canceled";
                Ok(())
            }
        }
    } else {
        tokio::select! {
            r = collect => r,
            _ = cancel.cancelled() => {
                let _ = ms.session.interrupt().await;
                stop_reason = "canceled";
                Ok(())
            }
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
        let _ = state
            .store
            .append(&ms.id, "tool", &serde_json::to_value(t)?)
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

    outcome?;
    Ok(json!({
        "turnId": turn_id,
        "stopReason": stop_reason,
        "usage": usage,
    }))
}

/// Drop-guard: clears the session's busy flag when the turn ends, so the
/// idle sweep can reap it again.
struct BusyGuard(Arc<ManagedSession>);
impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.0
            .busy
            .store(false, std::sync::atomic::Ordering::Relaxed);
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
        json!({"name": "session.create", "params": {"backend": "string?", "cwd": "string?", "model": "string?", "mode": "string?", "mcpServers": "object?"}, "result": {"sessionId": "string", "backend": "string"}}),
        json!({"name": "session.resume", "params": {"sessionId": "string"}, "result": {"sessionId": "string", "backend": "string"}}),
        json!({"name": "session.list", "params": {"limit": "number?", "offset": "number?"}, "result": {"sessions": "array"}}),
        json!({"name": "session.messages", "params": {"sessionId": "string", "limit": "number?", "before": "number?"}, "result": {"messages": "array"}}),
        json!({"name": "session.import", "params": {"backend": "string", "cwd": "string?"}, "result": {"sessions": "array"}}),
        json!({"name": "session.delete", "params": {"sessionId": "string"}, "result": {"deleted": "boolean"}}),
        json!({"name": "session.rename", "params": {"sessionId": "string", "title": "string"}, "result": {"renamed": "boolean"}}),
        json!({"name": "session.fork", "params": {"sessionId": "string", "upto": "number?"}, "result": {"sessionId": "string"}}),
        json!({"name": "session.usage", "params": {"sessionId": "string?"}, "result": {"contextUsed": "number", "costUsd": "number"}}),
        json!({"name": "session.search", "params": {"query": "string", "limit": "number?"}, "result": {"results": "array"}}),
        json!({"name": "turn.start", "params": {"sessionId": "string", "prompt": "string|array", "timeoutSecs": "number?"}, "result": {"turnId": "string", "stopReason": "string", "usage": "object?"}}),
        json!({"name": "turn.steer", "params": {"sessionId": "string", "prompt": "string|array"}, "result": {"result": "string"}}),
        json!({"name": "turn.cancel", "params": {"sessionId": "string"}, "result": {"cancelled": "boolean"}}),
        json!({"name": "permission.respond", "params": {"sessionId": "string", "response": "object"}, "result": {}}),
        json!({"name": "session.set_model", "params": {"sessionId": "string", "model": "string"}, "result": {}}),
        json!({"name": "session.set_mode", "params": {"sessionId": "string", "mode": "string"}, "result": {}}),
        json!({"name": "catalog.models", "params": {"backend": "string"}, "result": {"models": "array", "modes": "array"}}),
    ]
}
