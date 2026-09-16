//! `damon-relay` — public WebSocket pipe between a registered daemon and
//! clients. The daemon dials OUT to `/register?name=X`; clients dial
//! `/connect?name=X`. The relay pipes frames between them — it never sees
//! plaintext (daemon and client do an E2E handshake over the pipe).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tracing::info;

#[derive(Clone)]
struct RelayState {
    /// daemon name → sender that writes to the daemon's tunnel socket
    daemons: Arc<Mutex<HashMap<String, mpsc::Sender<String>>>>,
    next_client: Arc<AtomicU64>,
}

#[derive(Deserialize)]
struct Name {
    name: String,
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
        next_client: Arc::new(AtomicU64::new(1)),
    };

    let app = Router::new()
        .route("/register", get(register))
        .route("/connect", get(connect))
        .route("/health", get(|| async { "ok" }))
        .with_state(state);

    info!(%bind, "damon-relay listening");
    axum::serve(tokio::net::TcpListener::bind(bind).await?, app).await?;
    Ok(())
}

/// Daemon's outbound tunnel: `ws://relay/register?name=X`
async fn register(
    ws: WebSocketUpgrade,
    Query(Name { name }): Query<Name>,
    State(state): State<RelayState>,
) -> Response {
    ws.on_upgrade(move |socket| async move {
        let (mut writer, mut reader) = socket.split();
        let (tx, mut rx) = mpsc::channel::<String>(256);
        state.daemons.lock().await.insert(name.clone(), tx.clone());
        info!(daemon = %name, "daemon registered");

        // Pump: relay → daemon socket.
        let send_task = tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                if writer.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
        });
        // Pump: daemon socket → relay (route by client id).
        let daemons = state.daemons.clone();
        let name2 = name.clone();
        let recv_task = tokio::spawn(async move {
            while let Some(Ok(Message::Text(text))) = reader.next().await {
                let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                let client_id = v["client"].as_u64().unwrap_or(0);
                if let Some(data) = v["data"].as_str() {
                    // Forward to the client socket — stored in a side map.
                    CLIENTS
                        .lock()
                        .await
                        .get(&client_id)
                        .map(|tx| tx.try_send(json!({"data": data}).to_string()).ok());
                }
            }
            daemons.lock().await.remove(&name2);
        });
        let _ = tokio::join!(send_task, recv_task);
        info!(daemon = %name, "daemon disconnected");
    })
}

/// Client connection: `ws://relay/connect?name=X`
async fn connect(
    ws: WebSocketUpgrade,
    Query(Name { name }): Query<Name>,
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
        CLIENTS.lock().await.insert(client_id, tx);
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
        let recv_task = tokio::spawn(async move {
            while let Some(Ok(Message::Text(text))) = reader.next().await {
                let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                if let Some(data) = v["data"].as_str() {
                    let _ = daemon_tx
                        .send(json!({"client": client_id, "data": data}).to_string())
                        .await;
                }
            }
        });
        let _ = tokio::join!(send_task, recv_task);
        CLIENTS.lock().await.remove(&client_id);
        info!(client = client_id, "client disconnected");
    })
}

/// client_id → sender that writes to that client's socket.
static CLIENTS: std::sync::LazyLock<Mutex<HashMap<u64, mpsc::Sender<String>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
