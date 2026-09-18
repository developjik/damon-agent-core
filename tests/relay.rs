//! E2E: damon-relay server + daemon tunnel + client through the relay.

use std::collections::HashMap;
use std::sync::Arc;

use damon_core::api::AppState;
use damon_core::client::DamonClient;
use damon_core::config::{Config, ProviderConfig};
use damon_core::mcp::McpRegistry;
use damon_core::store::Store;
use serde_json::json;

fn test_config() -> Config {
    let mut providers = std::collections::BTreeMap::new();
    providers.insert(
        "default".into(),
        ProviderConfig {
            api: "openai-completions".into(),
            base_url: Some("http://127.0.0.1:1".into()),
            api_key: Some("test".into()),
            models: vec![],
            default_model: None,
            headers: HashMap::new(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: Some("test-token".into()),
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: Default::default(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
    }
}

#[tokio::test]
async fn relay_e2e() {
    // Start the relay server.
    let relay_state = damon_core::relay::new_relay_state();
    let relay_app = axum::Router::new()
        .route("/register", axum::routing::get(relay_register))
        .route("/connect", axum::routing::get(relay_connect))
        .with_state(relay_state);
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_port = relay_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(relay_listener, relay_app).await.unwrap();
    });
    let relay_url = format!("ws://127.0.0.1:{relay_port}");

    // Start the daemon's tunnel.
    let cfg = test_config();
    let shared = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store, mcp).await;
    tokio::spawn(damon_core::relay::run_tunnel(
        state,
        relay_url.clone(),
        "test-daemon".into(),
        None,
    ));
    // Give the tunnel a moment to register.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Client connects through the relay.
    let client = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        DamonClient::connect_relay(&relay_url, "test-daemon", "test-token"),
    )
    .await
    .expect("connect_relay timed out")
    .unwrap();
    let resp = client.initialize().await.unwrap();
    assert_eq!(resp["agentInfo"]["name"], "damond");
}

// ---------------------------------------------------------------------------
// E2e handshake + frame crypto (channel-level, no relay server needed)
// ---------------------------------------------------------------------------

/// Run the 4-message handshake over an in-memory pipe; returns
/// (daemon_e2e, client_e2e) on success.
async fn established_pair(token: &'static str) -> (damon_core::relay::E2e, damon_core::relay::E2e) {
    let (c2d_tx, mut c2d_rx) = tokio::sync::mpsc::channel::<String>(8);
    let (d2c_tx, mut d2c_rx) = tokio::sync::mpsc::channel::<String>(8);
    let daemon = tokio::spawn(async move {
        damon_core::relay::E2e::daemon_handshake(&d2c_tx, &mut c2d_rx, token).await
    });
    let client = damon_core::relay::E2e::client_handshake(&c2d_tx, &mut d2c_rx, token).await;
    let daemon = daemon.await.unwrap();
    (daemon.unwrap(), client.unwrap())
}

#[tokio::test]
async fn e2e_handshake_and_frame_roundtrip() {
    let (daemon, client) = established_pair("test-token").await;
    // Both directions carry frames with independent sequence counters.
    let ct = client.encrypt(r#"{"jsonrpc":"2.0","id":1}"#).unwrap();
    assert_eq!(daemon.decrypt(&ct).unwrap(), r#"{"jsonrpc":"2.0","id":1}"#);
    let ct = daemon.encrypt(r#"{"jsonrpc":"2.0","result":{}}"#).unwrap();
    assert_eq!(
        client.decrypt(&ct).unwrap(),
        r#"{"jsonrpc":"2.0","result":{}}"#
    );
    // A second frame in each direction still works (seq advanced).
    let ct = client.encrypt("second").unwrap();
    assert_eq!(daemon.decrypt(&ct).unwrap(), "second");
    let ct = daemon.encrypt("second").unwrap();
    assert_eq!(client.decrypt(&ct).unwrap(), "second");
}

#[tokio::test]
async fn e2e_replayed_frame_rejected() {
    let (daemon, client) = established_pair("test-token").await;
    let ct = client.encrypt("hello").unwrap();
    assert_eq!(daemon.decrypt(&ct).unwrap(), "hello");
    // Relay replays the identical frame — seq 0 was already consumed.
    assert!(daemon.decrypt(&ct).is_err());
}

#[tokio::test]
async fn e2e_reordered_frame_rejected() {
    let (daemon, client) = established_pair("test-token").await;
    let ct0 = client.encrypt("first").unwrap();
    let ct1 = client.encrypt("second").unwrap();
    // Relay delivers seq 1 before seq 0 — must fail closed.
    assert!(daemon.decrypt(&ct1).is_err());
    assert_eq!(daemon.decrypt(&ct0).unwrap(), "first");
}

#[tokio::test]
async fn e2e_wrong_token_handshake_fails() {
    let (c2d_tx, mut c2d_rx) = tokio::sync::mpsc::channel::<String>(8);
    let (d2c_tx, mut d2c_rx) = tokio::sync::mpsc::channel::<String>(8);
    let daemon = tokio::spawn(async move {
        damon_core::relay::E2e::daemon_handshake(&d2c_tx, &mut c2d_rx, "test-token").await
    });
    let client =
        damon_core::relay::E2e::client_handshake(&c2d_tx, &mut d2c_rx, "wrong-token").await;
    // The daemon rejects the client's proof (computed with a different
    // token) and drops the pipe before ever sending its own — the client
    // then fails waiting for the daemon proof.
    assert!(client.is_err());
    drop(c2d_tx);
    assert!(daemon.await.unwrap().is_err());
}
async fn relay_register(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
    axum::extract::State(state): axum::extract::State<damon_core::relay::RelayState>,
) -> axum::response::Response {
    let name = q.get("name").cloned().unwrap_or_default();
    ws.on_upgrade(move |socket| async move {
        use futures::{SinkExt, StreamExt};
        // The daemon tunnel presents {"auth": secret} as its first
        // frame; accept it (test relay enforces no secret) and pipe on.
        let (mut writer, mut reader) = socket.split();
        let first = reader.next().await;
        if !matches!(
            first,
            Some(Ok(axum::extract::ws::Message::Text(t))) if t.contains("auth")
        ) {
            return;
        }
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(256);
        state.lock().await.insert(name, tx);
        tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                if writer
                    .send(axum::extract::ws::Message::Text(text.into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        while let Some(Ok(axum::extract::ws::Message::Text(text))) = reader.next().await {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            let client_id = v["client"].as_u64().unwrap_or(0);
            if let Some(data) = v["data"].as_str() {
                TEST_CLIENTS
                    .lock()
                    .await
                    .get(&client_id)
                    .map(|tx| tx.try_send(json!({"data": data}).to_string()).ok());
            }
        }
    })
}

async fn relay_connect(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Query(q): axum::extract::Query<HashMap<String, String>>,
    axum::extract::State(state): axum::extract::State<damon_core::relay::RelayState>,
) -> axum::response::Response {
    let name = q.get("name").cloned().unwrap_or_default();
    ws.on_upgrade(move |socket| async move {
        use futures::{SinkExt, StreamExt};
        let daemon_tx = {
            let d = state.lock().await;
            match d.get(&name) {
                Some(tx) => tx.clone(),
                None => return,
            }
        };
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let client_id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (mut writer, mut reader) = socket.split();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(256);
        TEST_CLIENTS.lock().await.insert(client_id, tx);
        let _ = daemon_tx
            .send(json!({"client": client_id}).to_string())
            .await;
        tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                if writer
                    .send(axum::extract::ws::Message::Text(text.into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        while let Some(Ok(axum::extract::ws::Message::Text(text))) = reader.next().await {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            if let Some(data) = v["data"].as_str() {
                let _ = daemon_tx
                    .send(json!({"client": client_id, "data": data}).to_string())
                    .await;
            }
        }
        TEST_CLIENTS.lock().await.remove(&client_id);
    })
}

static TEST_CLIENTS: std::sync::LazyLock<
    tokio::sync::Mutex<HashMap<u64, tokio::sync::mpsc::Sender<String>>>,
> = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));
