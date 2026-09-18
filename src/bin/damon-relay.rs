//! `damon-relay` — public WebSocket pipe between a registered daemon and
//! clients. The daemon dials OUT to `/register?name=X`; clients dial
//! `/connect?name=X`. The relay pipes frames between them — it never sees
//! plaintext (daemon and client do an E2E handshake over the pipe).
//! Registration: the daemon dials `/register?name=X` and presents the
//! shared secret as its FIRST frame `{"auth": secret}` — never a URL
//! query, which leaks into access logs. Without the secret configured,
//! only an empty `{"auth": ""}` is accepted and duplicate registrations
//! are refused with an explicit `name_taken` error.

use std::collections::HashMap;
use std::net::IpAddr;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Router;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};

/// (owning daemon name, writer to that client's socket, abort handle
/// for its inbound pump — used to drop laggard clients)
type ClientEntry = (String, mpsc::Sender<String>, tokio::task::AbortHandle);

#[derive(Clone)]
struct RelayState {
    /// daemon name -> writer to that daemon's tunnel socket
    daemons: Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>,
    /// client_id -> owning daemon + client socket writer
    clients: Arc<Mutex<HashMap<u64, ClientEntry>>>,
    next_client: Arc<AtomicU64>,
    /// Optional shared secret required on /register.
    register_secret: Option<String>,
    /// Concurrent /connect sockets per source IP — bounds unauthenticated
    /// resource exhaustion on a public relay.
    conns_per_ip: Arc<Mutex<HashMap<IpAddr, usize>>>,
    /// Global concurrent sockets — per-IP caps are trivially bypassed
    /// with many source IPs (IPv6 /64s), so the relay also needs a
    /// hard ceiling on total unauthenticated connections.
    conns_total: Arc<AtomicU64>,
}

/// Max concurrent client connections from one source IP.
const MAX_CONNS_PER_IP: usize = 8;
/// Max concurrent sockets across all sources — bounds task/map memory
/// on a public relay no matter how the flood is distributed.
const MAX_CONNS_TOTAL: u64 = 1024;

/// Daemon names are map keys and log fields — restrict to a printable
/// charset so an unauthenticated caller can't forge log lines with
/// newlines/control chars.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

#[derive(Deserialize)]
struct Name {
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let bind: std::net::SocketAddr = std::env::var("DAMON_RELAY_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()?;

    let state = RelayState {
        daemons: Arc::new(Mutex::new(HashMap::new())),
        clients: Arc::new(Mutex::new(HashMap::new())),
        next_client: Arc::new(AtomicU64::new(1)),
        register_secret: std::env::var("DAMON_RELAY_SECRET").ok(),
        conns_per_ip: Arc::new(Mutex::new(HashMap::new())),
        conns_total: Arc::new(AtomicU64::new(0)),
    };
    if state.register_secret.is_some() {
        info!("relay registration requires DAMON_RELAY_SECRET");
    }

    let app = Router::new()
        .route("/register", get(register))
        .route("/connect", get(connect))
        .route("/health", get(|| async { "ok" }))
        .with_state(state);

    info!(%bind, "damon-relay listening");
    axum::serve(
        tokio::net::TcpListener::bind(bind).await?,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

/// Daemon's outbound tunnel: `ws://relay/register?name=X`, then a first
/// frame `{"auth": secret}` (empty string when no secret is configured).
/// Keeping the secret out of the URL keeps it out of access logs.
async fn register(
    ws: WebSocketUpgrade,
    Query(Name { name, .. }): Query<Name>,
    State(state): State<RelayState>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
) -> Result<Response, StatusCode> {
    // Daemon names become map keys and log fields — restrict to a
    // printable charset so an unauthenticated caller can't forge log
    // lines with newlines/control chars.
    if !valid_name(&name) {
        return Err(StatusCode::BAD_REQUEST);
    }
    // Same per-IP concurrency bound as /connect — unauthenticated
    // registration attempts (each spawning a socket + auth-timeout task)
    // must not be free.
    {
        let per_ip = state.conns_per_ip.lock().await;
        if per_ip.get(&peer.ip()).copied().unwrap_or(0) >= MAX_CONNS_PER_IP {
            warn!(ip = %peer.ip(), "register refused: per-IP connection cap");
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }
    if state.conns_total.load(Ordering::Relaxed) >= MAX_CONNS_TOTAL {
        warn!(ip = %peer.ip(), "register refused: global connection cap");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    // The slot is taken inside on_upgrade (and released at session end)
    // so an upgrade that never completes can't leak it. The per-IP count
    // is re-checked AFTER incrementing — the pre-upgrade check above is
    // racy (concurrent upgrades all pass before any increments).
    let conns = state.conns_per_ip.clone();
    let total = state.conns_total.clone();
    Ok(ws
        .max_frame_size(4 << 20)
        .max_message_size(4 << 20)
        .on_upgrade(move |socket| async move {
            let over = {
                let mut per_ip = conns.lock().await;
                let n = per_ip.entry(peer.ip()).or_insert(0);
                *n += 1;
                *n > MAX_CONNS_PER_IP
            };
            let t = total.fetch_add(1, Ordering::Relaxed) + 1;
            if over || t > MAX_CONNS_TOTAL {
                release(conns, peer.ip()).await;
                total.fetch_sub(1, Ordering::Relaxed);
                return;
            }
            register_session(socket, name, state).await;
            release(conns, peer.ip()).await;
            total.fetch_sub(1, Ordering::Relaxed);
        }))
}
/// One registered daemon's tunnel session: authenticate, then pipe frames
/// between the daemon socket and its clients until disconnect.
async fn register_session(socket: axum::extract::ws::WebSocket, name: String, state: RelayState) {
    let (mut writer, mut reader) = socket.split();

    // First frame must authenticate: the optional shared secret.
    let auth = tokio::time::timeout(std::time::Duration::from_secs(10), reader.next()).await;
    let authed = match auth {
        Ok(Some(Ok(Message::Text(text)))) => {
            serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v["auth"].as_str().map(String::from))
                .is_some_and(|given| match &state.register_secret {
                    Some(expected) => {
                        damon_core::config::constant_time_eq(given.as_bytes(), expected.as_bytes())
                    }
                    // No secret configured: accept only an EMPTY
                    // auth frame so squatters can't probe whether
                    // the relay is unprotected via a bogus secret.
                    None => given.is_empty(),
                })
        }
        _ => false,
    };
    if !authed {
        warn!(daemon = %name, "registration refused: bad or missing secret");
        let _ = writer
            .send(Message::Text(
                json!({"error": "unauthorized"}).to_string().into(),
            ))
            .await;
        return;
    }

    let (tx, mut rx) = mpsc::channel::<String>(256);
    {
        let mut d = state.daemons.lock().await;
        // A dead tunnel's send_task exits on socket-write failure, which
        // drops its rx — is_closed() then lets the real daemon reclaim
        // its name instead of being refused name_taken forever.
        let stale = d.get(&name).is_some_and(|tx| tx.is_closed());
        if stale {
            warn!(daemon = %name, "replacing dead tunnel registration");
            d.remove(&name);
        }
        if d.contains_key(&name) {
            // A live tunnel already owns this name — refuse the
            // squat EXPLICITLY so the real daemon can detect the
            // takeover attempt instead of retrying blind.
            warn!(daemon = %name, "duplicate registration refused: name already held");
            let _ = writer
                .send(Message::Text(
                    json!({"error": "name_taken"}).to_string().into(),
                ))
                .await;
            return;
        }
        d.insert(name.clone(), tx.clone());
    }
    info!(daemon = %name, "daemon registered");
    // Pump: relay → daemon socket.
    let send_task = tokio::spawn(async move {
        while let Some(text) = rx.recv().await {
            if writer.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    // Pump: daemon socket → relay (route by client id, but only to
    // clients this daemon actually owns).
    let daemons = state.daemons.clone();
    let clients = state.clients.clone();
    let name2 = name.clone();
    let tx2 = tx.clone();
    let recv_task = tokio::spawn(async move {
        // Ping/Pong/Binary keep the link alive — axum yields them to us,
        // so a keepalive ping must not end the pump and drop the tunnel.
        while let Some(msg) = reader.next().await {
            let text = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            let Ok(v) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            let Some(client_id) = v["client"].as_u64() else {
                continue;
            };
            if let Some(data) = v["data"].as_str() {
                let mut map = clients.lock().await;
                let drop_client = match map.get(&client_id) {
                    // Only deliver to clients this daemon owns.
                    Some((owner, _, _)) if owner != &name2 => false,
                    Some((_, tx, _)) => match tx.try_send(json!({"data": data}).to_string()) {
                        Ok(()) => false,
                        // A full queue means the client's socket is
                        // backpressured — dropping frames would hang
                        // its pending RPCs, so disconnect it instead.
                        Err(mpsc::error::TrySendError::Full(_)) => true,
                        Err(mpsc::error::TrySendError::Closed(_)) => true,
                    },
                    None => false,
                };
                if drop_client && let Some((_, _, abort)) = map.remove(&client_id) {
                    warn!(client = client_id, "dropping backpressured client");
                    abort.abort();
                }
            }
        }
        // Remove our own registration only — a newer tunnel for the
        // same name must not be de-registered by a stale exit.
        let mut d = daemons.lock().await;
        if d.get(&name2).is_some_and(|cur| cur.same_channel(&tx2)) {
            d.remove(&name2);
        }
    });
    // Drop our local sender — the map entry and recv_task's clone are
    // the only owners; otherwise send_task never exits on disconnect.
    drop(tx);
    let _ = tokio::join!(send_task, recv_task);
    info!(daemon = %name, "daemon disconnected");
}

/// Client connection: `ws://relay/connect?name=X`. Unauthenticated and
/// unauthenticated-by-design (the E2E handshake happens over the pipe), so
/// per-source-IP concurrency is capped to keep the relay's and the
/// daemon's per-client fanout bounded.
async fn connect(
    ws: WebSocketUpgrade,
    Query(Name { name, .. }): Query<Name>,
    State(state): State<RelayState>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if !valid_name(&name) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // Check the cap without taking the slot — the increment happens inside
    // on_upgrade so an upgrade that never completes can't leak it.
    {
        let per_ip = state.conns_per_ip.lock().await;
        if per_ip.get(&peer.ip()).copied().unwrap_or(0) >= MAX_CONNS_PER_IP {
            warn!(ip = %peer.ip(), "connect refused: per-IP connection cap");
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
    }
    if state.conns_total.load(Ordering::Relaxed) >= MAX_CONNS_TOTAL {
        warn!(ip = %peer.ip(), "connect refused: global connection cap");
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let conns = state.conns_per_ip.clone();
    let total = state.conns_total.clone();
    ws.max_frame_size(4 << 20)
        .max_message_size(4 << 20)
        .on_upgrade(move |socket| async move {
            // Re-check AFTER incrementing — the pre-upgrade check is racy
            // (concurrent upgrades all pass before any increments land).
            let over = {
                let mut per_ip = conns.lock().await;
                let n = per_ip.entry(peer.ip()).or_insert(0);
                *n += 1;
                *n > MAX_CONNS_PER_IP
            };
            let t = total.fetch_add(1, Ordering::Relaxed) + 1;
            if over || t > MAX_CONNS_TOTAL {
                release(conns, peer.ip()).await;
                total.fetch_sub(1, Ordering::Relaxed);
                return;
            }
            relay_client_session(socket, name, state).await;
            release(conns, peer.ip()).await;
            total.fetch_sub(1, Ordering::Relaxed);
        })
}

/// End-of-connection bookkeeping: release the per-IP slot whatever
/// happened inside the session.
async fn release(conns: Arc<Mutex<HashMap<IpAddr, usize>>>, ip: IpAddr) {
    let mut per_ip = conns.lock().await;
    if let Some(n) = per_ip.get_mut(&ip) {
        *n = n.saturating_sub(1);
        if *n == 0 {
            per_ip.remove(&ip);
        }
    }
}

/// Body of one client session: pipe frames between the client socket and
/// the owning daemon's tunnel.
async fn relay_client_session(
    socket: axum::extract::ws::WebSocket,
    name: String,
    state: RelayState,
) {
    let daemon_tx = {
        let d = state.daemons.lock().await;
        match d.get(&name) {
            Some(tx) => tx.clone(),
            None => return, // daemon not registered
        }
    };
    let client_id = state.next_client.fetch_add(1, Ordering::Relaxed);
    let (mut writer, mut reader) = socket.split();
    let (tx, mut rx) = mpsc::channel::<String>(256);

    // Pump: relay → client socket.
    let send_task = tokio::spawn(async move {
        while let Some(text) = rx.recv().await {
            if writer.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    // Pump: client socket → daemon. Registered before the daemon is
    // notified below, so a response can never arrive for an unknown id.
    let daemon_tx2 = daemon_tx.clone();
    let recv_task = tokio::spawn(async move {
        // Ping/Pong/Binary keep the link alive — axum yields them to us,
        // so a keepalive ping must not end the pump and drop the client.
        while let Some(msg) = reader.next().await {
            let text = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            let Ok(v) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            if let Some(data) = v["data"].as_str() {
                let _ = daemon_tx2
                    .send(json!({"client": client_id, "data": data}).to_string())
                    .await;
            }
        }
    });
    // The map entry carries the recv task's abort handle so the daemon
    // pump can disconnect a client whose send queue is backpressured.
    state
        .clients
        .lock()
        .await
        .insert(client_id, (name.clone(), tx, recv_task.abort_handle()));
    info!(daemon = %name, client = client_id, "client connected");

    // Tell the daemon a new client attached.
    let _ = daemon_tx
        .send(json!({"client": client_id}).to_string())
        .await;

    // Either pump ending ends the connection — abort the other so a
    // socket stuck on write can't hang the cleanup (and the daemon's
    // disconnect notice) forever.
    let mut send_task = send_task;
    let mut recv_task = recv_task;
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
    state.clients.lock().await.remove(&client_id);
    // Tell the daemon this client is gone so it can prune the session.
    let _ = daemon_tx
        .send(json!({"client": client_id, "disconnect": true}).to_string())
        .await;
    info!(client = client_id, "client disconnected");
}
