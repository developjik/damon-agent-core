//! `damon-relay` — public WebSocket pipe between a registered daemon and
//! clients. The daemon dials OUT to `/register?name=X`; clients dial
//! `/connect?name=X`. The relay pipes frames between them — it never sees
//! plaintext (daemon and client do an E2E handshake over the pipe).
//!
//! If `DAMON_RELAY_SECRET` is set, `/register` must present it as
//! `?secret=` — otherwise anyone could squat a daemon's name.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tracing::{info, warn};

#[derive(Clone)]
struct RelayState {
    /// daemon name -> writer to that daemon's tunnel socket
    daemons: Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>,
    /// client_id -> (owning daemon name, writer to that client's socket)
    clients: Arc<Mutex<HashMap<u64, (String, mpsc::Sender<String>)>>>,
    next_client: Arc<AtomicU64>,
    /// Optional shared secret required on /register.
    register_secret: Option<String>,
}

#[derive(Deserialize)]
struct Name {
    name: String,
    secret: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
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
    axum::serve(tokio::net::TcpListener::bind(bind).await?, app).await?;
    Ok(())
}

/// Daemon's outbound tunnel: `ws://relay/register?name=X[&secret=S]`
async fn register(
    ws: WebSocketUpgrade,
    Query(Name { name, secret }): Query<Name>,
    State(state): State<RelayState>,
) -> Result<Response, StatusCode> {
    if let Some(expected) = &state.register_secret {
        let ok = secret.as_deref().is_some_and(|s| {
            damon_core::config::constant_time_eq(s.as_bytes(), expected.as_bytes())
        });
        if !ok {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(ws.on_upgrade(move |socket| async move {
        let (mut writer, mut reader) = socket.split();
        let (tx, mut rx) = mpsc::channel::<String>(256);
        {
            let mut d = state.daemons.lock().await;
            if d.contains_key(&name) {
                // A live tunnel already owns this name — refuse the
                // squat instead of silently hijacking it.
                warn!(daemon = %name, "duplicate registration refused");
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
            while let Some(Ok(Message::Text(text))) = reader.next().await {
                let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                let Some(client_id) = v["client"].as_u64() else { continue };
                if let Some(data) = v["data"].as_str() {
                    let map = clients.lock().await;
                    if let Some((owner, tx)) = map.get(&client_id) {
                        if owner == &name2 {
                            let _ = tx.try_send(json!({"data": data}).to_string());
                        }
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
    }))
}

/// Client connection: `ws://relay/connect?name=X`
async fn connect(
    ws: WebSocketUpgrade,
    Query(Name { name, .. }): Query<Name>,
    State(state): State<RelayState>,
) -> Response {
    ws.on_upgrade(move |socket| async move {
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
        state
            .clients
            .lock()
            .await
            .insert(client_id, (name.clone(), tx));
        info!(daemon = %name, client = client_id, "client connected");

        // Tell the daemon a new client attached.
        let _ = daemon_tx.send(json!({"client": client_id}).to_string()).await;

        // Pump: relay → client socket.
        let send_task = tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                if writer.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
        });
        // Pump: client socket → daemon.
        let daemon_tx2 = daemon_tx.clone();
        let recv_task = tokio::spawn(async move {
            while let Some(Ok(Message::Text(text))) = reader.next().await {
                let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                if let Some(data) = v["data"].as_str() {
                    let _ = daemon_tx2
                        .send(json!({"client": client_id, "data": data}).to_string())
                        .await;
                }
            }
        });
        let _ = tokio::join!(send_task, recv_task);
        state.clients.lock().await.remove(&client_id);
        // Tell the daemon this client is gone so it can prune the session.
        let _ = daemon_tx
            .send(json!({"client": client_id, "disconnect": true}).to_string())
            .await;
        info!(client = client_id, "client disconnected");
    })
}
