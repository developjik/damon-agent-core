use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::bail;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::AppState;
use crate::runtime::{self, ClientChannel};

#[derive(Deserialize)]
pub struct WsQuery {
    /// One-shot ticket from POST /v1/ws_ticket — 60s TTL, consumed on use.
    ticket: Option<String>,
}

/// GET /ws — ACP-shaped JSON-RPC over WebSocket.
/// Auth: same bearer token as /v1, via Authorization header or a
/// single-use ?ticket= (query-string tokens are not accepted — they leak
/// into logs and browser history).
pub async fn ws_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<WsQuery>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let authenticated = match state.auth_token().await {
        Some(Ok(expected)) => {
            let header_ok = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .is_some_and(|t| {
                    crate::config::constant_time_eq(t.as_bytes(), expected.as_bytes())
                });
            // Ticket auth: single-use — remove() consumes it so a leaked
            // ticket URL can't be replayed, and a stale one is dead anyway.
            let ticket_ok = match &q.ticket {
                Some(t) => state
                    .ws_tickets
                    .lock()
                    .await
                    .remove(t)
                    .is_some_and(|issued| issued.elapsed() < std::time::Duration::from_secs(60)),
                None => false,
            };
            if !header_ok && !ticket_ok {
                return Err(StatusCode::UNAUTHORIZED);
            }
            true
        }
        // Configured but unresolvable: fail closed, never open.
        Some(Err(_)) => return Err(StatusCode::SERVICE_UNAVAILABLE),
        // A reload may have dropped the token — a non-loopback bind
        // must not silently open the socket.
        None if !state.bind.ip().is_loopback() => {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        None => false,
    };
    // Browser WS handshakes carry Origin and skip CORS preflight — without
    // this check any website could drive an unauthenticated local daemon.
    // Non-browser clients send no Origin. Authenticated connections are
    // gated by the token, so their Origin is unconstrained.
    if !authenticated
        && let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok())
        && !is_localhost_origin(origin)
    {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(ws
        // JSON-RPC text frames need a few KiB; axum's 64 MiB default
        // lets any connected peer force huge allocation/parse spikes.
        .max_frame_size(4 << 20)
        .max_message_size(4 << 20)
        .on_upgrade(move |socket| {
            let (mut writer, mut reader) = socket.split();
            let (tx_in, rx_in) = mpsc::channel::<String>(64);
            let (tx_out, mut rx_out) = mpsc::channel::<String>(64);
            // Pump: socket → tx_in (inbound), rx_out → socket (outbound).
            tokio::spawn(async move {
                while let Some(Ok(msg)) = reader.next().await {
                    if let Message::Text(t) = msg
                        && tx_in.send(t.to_string()).await.is_err()
                    {
                        break;
                    }
                }
            });
            tokio::spawn(async move {
                while let Some(text) = rx_out.recv().await {
                    if writer.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
            });
            handle_socket(rx_in, tx_out, state)
        }))
}

/// Client-facing channel: notifications and server→client requests.
/// Transport-agnostic — writes JSON text frames to `tx`.
/// Server→client request waiter payload: the client's result-or-error.
type PendingReply = oneshot::Sender<Result<Value, Value>>;

/// Global bound on concurrent prompt turns across all sessions and
/// connections — every other resource in this path is capped (channels,
/// frame size, per-session live_prompts), and an unbounded spawn per
/// request would let one client exhaust memory and upstream quota.
static PROMPT_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(64);

struct WsClient {
    tx: mpsc::Sender<String>,
    /// Server→client request waiters: id → the client's result-or-error.
    pending: Arc<Mutex<HashMap<u64, PendingReply>>>,
    next_id: AtomicU64,
    /// Set once a send times out — later notifications drop immediately
    /// instead of each paying the full SEND_TIMEOUT again.
    stalled: std::sync::atomic::AtomicBool,
    /// Response wait bound — permission_timeout_secs + slack, so the
    /// configured permission budget is never silently capped here.
    req_timeout: std::time::Duration,
}

/// Removes a pending waiter if the request future is dropped before the
/// client answers (external cancel/timeout) — without it the entry
/// lingers until the connection dies.
struct PendingGuard {
    pending: Arc<Mutex<HashMap<u64, PendingReply>>>,
    id: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let pending = self.pending.clone();
        let id = self.id;
        // Drop may run outside a runtime context (never here, but stay
        // panic-free); spawn so the async lock can be taken.
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move {
                pending.lock().await.remove(&id);
            });
        }
    }
}

/// Outbound send bound: a client that stops reading must not stall the
/// turn (or the dispatch loop) forever — the message is dropped instead.
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[async_trait::async_trait]
impl ClientChannel for WsClient {
    async fn notify(&self, method: &str, params: Value) {
        // Once a send has timed out the client is stalled — drop later
        // notifications immediately instead of paying SEND_TIMEOUT per
        // chunk for the rest of the turn.
        if self.stalled.load(Ordering::Relaxed) {
            return;
        }
        let msg = json!({"jsonrpc": "2.0", "method": method, "params": params});
        match tokio::time::timeout(SEND_TIMEOUT, self.tx.send(msg.to_string())).await {
            Ok(_) => {}
            Err(_) => {
                self.stalled.store(true, Ordering::Relaxed);
                warn!("dropping notification to stalled client");
            }
        }
    }

    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        // Drop-guard: an external cancel/timeout that drops this future
        // still frees the pending entry.
        let _guard = PendingGuard {
            pending: self.pending.clone(),
            id,
        };
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let sent = tokio::time::timeout(SEND_TIMEOUT, self.tx.send(msg.to_string())).await;
        if !matches!(sent, Ok(Ok(()))) {
            self.pending.lock().await.remove(&id);
            bail!("client connection stalled or closed");
        }
        // A client that never answers must not hang the turn forever or
        // leak the pending entry. The bound is the caller's request_timeout
        // (e.g. permission_timeout_secs + slack) — a fixed cap here would
        // silently override the configured permission budget.
        let wait = self.request_timeout();
        match tokio::time::timeout(wait, rx).await {
            Ok(Ok(Ok(v))) => Ok(v),
            // The client answered with a JSON-RPC error object.
            Ok(Ok(Err(e))) => {
                bail!(
                    "client error on {method}: {}",
                    e["message"].as_str().unwrap_or("unknown")
                )
            }
            Ok(Err(_)) => Err(anyhow::anyhow!("client dropped request")),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                bail!(
                    "client did not respond to {method} within {}s",
                    wait.as_secs()
                )
            }
        }
    }

    fn request_timeout(&self) -> std::time::Duration {
        self.req_timeout
    }
}

impl WsClient {
    async fn respond(&self, id: Value, result: Result<Value, Value>) {
        let msg = match result {
            Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
            Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e}),
        };
        if tokio::time::timeout(SEND_TIMEOUT, self.tx.send(msg.to_string()))
            .await
            .is_err()
        {
            warn!("dropping response to stalled client");
        }
    }
}

/// Run one client session over a generic text transport.
/// `rx` yields inbound JSON text; `tx` carries outbound JSON text.
/// Used by both the local WS handler and the relay tunnel.
/// Process-wide connection counter — identifies which connection owns a
/// live prompt so disconnect can cancel exactly its own turns.
static CONN_ID: AtomicU64 = AtomicU64::new(1);

pub async fn handle_socket(
    rx: mpsc::Receiver<String>,
    tx: mpsc::Sender<String>,
    state: Arc<AppState>,
) {
    let mut rx = rx;
    let conn_id = CONN_ID.fetch_add(1, Ordering::Relaxed);
    // Server→client requests (permission prompts) wait out the configured
    // permission timeout plus slack — the runtime's outer select enforces
    // the real budget; this bound only guards the pending map.
    let req_timeout = state
        .config
        .read()
        .permission_timeout_secs
        .map(std::time::Duration::from_secs)
        .unwrap_or(crate::runtime::PERMISSION_TIMEOUT)
        + std::time::Duration::from_secs(30);
    let client = Arc::new(WsClient {
        tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        next_id: AtomicU64::new(1),
        stalled: std::sync::atomic::AtomicBool::new(false),
        req_timeout,
    });

    info!("ws client connected");
    while let Some(text) = rx.recv().await {
        let Ok(v) = serde_json::from_str::<Value>(&text) else {
            warn!("invalid JSON from client");
            continue;
        };

        // Response to a server-initiated request. A JSON-RPC error object
        // is delivered as Err — collapsing it to result:null would hide
        // the failure and mislead any caller treating null as real data.
        if v.get("method").is_none() && v.get("id").is_some() {
            // JSON-RPC permits string ids — a client answering our
            // request with "5" instead of 5 must still resolve the
            // pending waiter or the request waits out its full timeout.
            let resp_id = v["id"]
                .as_u64()
                .or_else(|| v["id"].as_str().and_then(|s| s.parse::<u64>().ok()));
            if let Some(id) = resp_id
                && let Some(tx) = client.pending.lock().await.remove(&id)
            {
                let payload = match v.get("error") {
                    Some(e) => Err(e.clone()),
                    None => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
                };
                let _ = tx.send(payload);
            }
            continue;
        }

        let Some(method) = v["method"].as_str() else {
            continue;
        };
        let id = v.get("id").cloned();
        let params = v["params"].clone();

        match (method, id) {
            ("initialize", Some(id)) => {
                client
                    .respond(
                        id,
                        Ok(json!({
                            "protocolVersion": 1,
                            "agentCapabilities": {
                                "loadSession": false,
                                "promptCapabilities": {"text": true}
                            },
                            "agentInfo": {"name": "damond", "version": env!("CARGO_PKG_VERSION")}
                        })),
                    )
                    .await;
            }
            ("session/new", Some(id)) => {
                let cwd = params["cwd"].as_str().unwrap_or("").to_string();
                // ACP clients send mcpServers; per-session MCP servers are
                // not supported, so a non-empty list is rejected rather
                // than silently ignored.
                let mcp_nonempty = params["mcpServers"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty());
                if mcp_nonempty {
                    client
                        .respond(
                            id,
                            Err(rpc_error(
                                -32602,
                                "per-session mcpServers are not supported; configure [mcp_servers] in the daemon config",
                            )),
                        )
                        .await;
                    continue;
                }
                let model = params["model"].as_str().map(String::from);
                let session_id = uuid::Uuid::new_v4().to_string();
                match state
                    .store
                    .create_session(&session_id, &cwd, model.as_deref())
                    .await
                {
                    Ok(()) => {
                        client
                            .respond(id, Ok(json!({"sessionId": session_id})))
                            .await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/list", Some(id)) => {
                let limit = params["limit"]
                    .as_u64()
                    .map(|n| u32::try_from(n).unwrap_or(u32::MAX));
                let offset = params["offset"]
                    .as_u64()
                    .map(|n| u32::try_from(n).unwrap_or(u32::MAX));
                let result = if limit.is_some() || offset.is_some() {
                    state
                        .store
                        .list_sessions_paged(limit.unwrap_or(u32::MAX), offset.unwrap_or(0))
                        .await
                } else {
                    state.store.list_sessions().await
                };
                match result {
                    Ok(sessions) => {
                        let list: Vec<Value> = sessions
                            .into_iter()
                            .map(|(sid, created, model)| {
                                json!({"sessionId": sid, "createdAt": created, "model": model})
                            })
                            .collect();
                        client.respond(id, Ok(json!({"sessions": list}))).await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/messages", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                let limit = params["limit"]
                    .as_u64()
                    .map(|n| u32::try_from(n).unwrap_or(u32::MAX));
                let offset = params["offset"]
                    .as_u64()
                    .map(|n| u32::try_from(n).unwrap_or(u32::MAX));
                // Paged reads return the same OpenAI-shaped message objects
                // as the unpaged path — StoredMessage.data is that object.
                let result = if limit.is_some() || offset.is_some() {
                    state
                        .store
                        .messages_paged(&session_id, limit.unwrap_or(u32::MAX), offset.unwrap_or(0))
                        .await
                        .map(|msgs| msgs.into_iter().map(|m| m.data).collect())
                } else {
                    state.store.messages(&session_id).await
                };
                match result {
                    Ok(messages) => {
                        client.respond(id, Ok(json!({"messages": messages}))).await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/delete", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                // Cancel a live prompt first — deleting mid-turn would let
                // the turn keep appending messages to a dead session row.
                if let Some((_, token)) = state.live_prompts.lock().await.get(&session_id) {
                    token.cancel();
                }
                // The wind-down wait runs in a task, not the dispatch loop:
                // up to 10s of polling here would stall this connection's
                // other requests (permission replies, cancels).
                let (state, client) = (state.clone(), client.clone());
                tokio::spawn(async move {
                    // Wait for the turn to release the live_prompts entry
                    // before the rows are deleted. A turn still registered
                    // after 10s is wedged — refuse rather than orphaning
                    // its writes into a dead session row.
                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                    let busy = loop {
                        if !state.live_prompts.lock().await.contains_key(&session_id) {
                            break false;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            break true;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    };
                    if busy {
                        client
                            .respond(id, Err(rpc_error(-32603, "session busy")))
                            .await;
                        return;
                    }
                    // Re-check under the lock: a session/prompt may have
                    // registered between our busy poll and this acquisition.
                    let map = state.live_prompts.lock().await;
                    if map.contains_key(&session_id) {
                        drop(map);
                        client
                            .respond(id, Err(rpc_error(-32603, "session busy")))
                            .await;
                        return;
                    }
                    match state.store.delete_session(&session_id).await {
                        Ok(()) => {
                            // Session-scoped tool approvals die with the session.
                            state.mcp.clear_session(&session_id);
                            drop(map);
                            client.respond(id, Ok(json!({"deleted": true}))).await;
                        }
                        Err(e) => {
                            drop(map);
                            client
                                .respond(id, Err(rpc_error(-32603, &e.to_string())))
                                .await;
                        }
                    }
                });
            }
            ("session/search", Some(id)) => {
                let query = params["query"].as_str().unwrap_or("").to_string();
                let limit = params["limit"].as_u64().unwrap_or(10).min(1000) as usize;
                match state.store.search(&query, limit).await {
                    Ok(hits) => {
                        let results: Vec<Value> = hits
                            .into_iter()
                            .map(|(sid, mid, snippet)| {
                                json!({
                                    "sessionId": sid,
                                    "messageId": mid,
                                    "snippet": snippet,
                                })
                            })
                            .collect();
                        client.respond(id, Ok(json!({"results": results}))).await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/resume", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                match state.store.session_exists(&session_id).await {
                    Ok(true) => {
                        client
                            .respond(id, Ok(json!({"sessionId": session_id})))
                            .await;
                    }
                    Ok(false) => {
                        client
                            .respond(id, Err(rpc_error(-32602, "session not found")))
                            .await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/prompt", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                let text = params["prompt"]
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b["text"].as_str())
                            .collect::<Vec<_>>()
                            // Adjacent text blocks joined with "" merge
                            // words — separate them with a newline.
                            .join("\n")
                    })
                    .unwrap_or_default();
                // An empty prompt would persist an empty user message and
                // burn a full upstream turn — reject it up front.
                if text.trim().is_empty() {
                    client
                        .respond(id, Err(rpc_error(-32602, "empty prompt")))
                        .await;
                    continue;
                }
                let model = params["model"].as_str().map(String::from);
                let cancel = CancellationToken::new();
                // Global in-flight turn cap: every other resource here is
                // bounded, but an unbounded spawn per prompt lets one
                // connection exhaust memory/upstream quota via N sessions.
                let Ok(permit) = PROMPT_SLOTS.try_acquire() else {
                    client
                        .respond(id, Err(rpc_error(-32603, "too many concurrent prompts")))
                        .await;
                    continue;
                };
                // Exists-check + live_prompts insert under ONE lock: a
                // concurrent session/delete holds the same lock across
                // its row delete, so a prompt can never register into a
                // session that is being removed.
                {
                    let mut map = state.live_prompts.lock().await;
                    match state.store.session_exists(&session_id).await {
                        Ok(true) => {}
                        Ok(false) => {
                            drop(map);
                            client
                                .respond(id, Err(rpc_error(-32602, "session not found")))
                                .await;
                            continue;
                        }
                        Err(e) => {
                            drop(map);
                            client
                                .respond(id, Err(rpc_error(-32603, &e.to_string())))
                                .await;
                            continue;
                        }
                    }
                    // One live prompt per session across ALL connections:
                    // a second prompt would interleave writes into the
                    // same persisted history. The entry stays until the
                    // spawned task removes it, so a disconnect-cancelled
                    // turn still blocks a new prompt until it has fully
                    // wound down.
                    if map.contains_key(&session_id) {
                        drop(map);
                        client
                            .respond(
                                id,
                                Err(rpc_error(
                                    -32602,
                                    "session already has a prompt in progress",
                                )),
                            )
                            .await;
                        continue;
                    }
                    map.insert(session_id.clone(), (conn_id, cancel.clone()));
                }
                let (state, client) = (state.clone(), client.clone());
                tokio::spawn(async move {
                    // Hold the permit for the turn's lifetime — the slot
                    // frees when this task exits, however it ends.
                    let _permit = permit;
                    // catch_unwind: a panic inside the turn must not leak
                    // the live_prompts entry — the session would reject
                    // every later prompt as "in progress" until restart.
                    use futures::FutureExt;
                    let result = std::panic::AssertUnwindSafe(Box::pin(runtime::run_prompt(
                        &state,
                        &session_id,
                        &text,
                        model.as_deref(),
                        &(client.clone() as Arc<dyn ClientChannel>),
                        cancel.clone(),
                    )))
                    .catch_unwind()
                    .await;
                    let result = match result {
                        Ok(r) => r,
                        Err(_) => Err(anyhow::anyhow!("internal error: prompt turn panicked")),
                    };
                    // Remove only our own entry — a disconnect may have
                    // cancelled this turn while a newer connection already
                    // started a fresh prompt on the same session.
                    let mut map = state.live_prompts.lock().await;
                    if map.get(&session_id).is_some_and(|(c, _)| *c == conn_id) {
                        map.remove(&session_id);
                    }
                    drop(map);
                    match result {
                        Ok(reason) => {
                            let stop = if cancel.is_cancelled() {
                                "cancelled"
                            } else {
                                match reason {
                                    crate::llm::StopReason::Stop => "end_turn",
                                    crate::llm::StopReason::Length => "max_tokens",
                                    crate::llm::StopReason::ToolCalls => "tool_use",
                                    crate::llm::StopReason::MaxTurnRequests => "max_turn_requests",
                                    crate::llm::StopReason::Other => "end_turn",
                                }
                            };
                            client.respond(id, Ok(json!({"stopReason": stop}))).await;
                        }
                        Err(e) => {
                            client
                                .respond(id, Err(rpc_error(-32603, &format!("{e:#}"))))
                                .await;
                        }
                    }
                });
            }
            ("session/cancel", id) => {
                // Any connected client may cancel a session's turn — the
                // map is shared, so this also reaches prompts started by
                // other connections (e.g. a relay client cancelling a
                // local prompt). ACP sends this as a notification, but a
                // client that includes an id still gets a response —
                // otherwise it waits forever for one.
                if let Some(sid) = params["sessionId"].as_str()
                    && let Some((_, token)) = state.live_prompts.lock().await.get(sid)
                {
                    token.cancel();
                }
                if let Some(id) = id {
                    client.respond(id, Ok(json!({}))).await;
                }
            }
            (_, Some(id)) => {
                client
                    .respond(id, Err(rpc_error(-32601, "method not found")))
                    .await;
            }
            _ => {}
        }
    }
    // Connection ended: cancel every prompt this connection started so
    // no turn keeps running (and executing tools) unattended. Entries
    // stay in the map — the spawned tasks remove them on exit, which
    // keeps the one-prompt-per-session guard until each turn winds down.
    {
        let map = state.live_prompts.lock().await;
        for (_, (owner, token)) in map.iter() {
            if *owner == conn_id {
                token.cancel();
            }
        }
    }
    info!("ws client disconnected");
}

fn rpc_error(code: i64, message: &str) -> Value {
    json!({"code": code, "message": message})
}

/// Whether an Origin header points at a loopback host: `localhost`,
/// `*.localhost`, or any loopback IP (127.0.0.0/8, ::1), any port/scheme.
/// Anything unparseable — including `Origin: null` — is not loopback.
pub(crate) fn is_localhost_origin(origin: &str) -> bool {
    let Ok(uri) = origin.parse::<axum::http::Uri>() else {
        return false;
    };
    let Some(host) = uri.host() else { return false };
    let host = host.trim_end_matches('.');
    if host.eq_ignore_ascii_case("localhost") || host.to_ascii_lowercase().ends_with(".localhost") {
        return true;
    }
    host.trim_matches(|c| c == '[' || c == ']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}
