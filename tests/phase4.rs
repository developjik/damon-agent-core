//! Phase 4 tests: TLS serving, non-loopback refusal, Telegram bridge,
//! and real MCP tool call with permission round-trip.

use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::response::Response;
use axum::routing::post;
use damon_core::api::{self, AppState};
use damon_core::config::{Config, McpServerConfig, ProviderConfig};
use damon_core::mcp::McpRegistry;
use damon_core::store::Store;
use damon_core::telegram::{Bridge, TelegramApi};
use serde_json::{Value, json};
use tokio::sync::Mutex;

fn test_config(upstream: &str) -> Config {
    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(upstream.to_string()),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
            relay: None,
    }
}

/// Mock LLM: first call → tool_call for test.ping, second → text.
async fn mock_llm_tool_then_text() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/chat/completions",
        post(move || {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let sse = if n == 0 {
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"function\":{\"name\":\"test.ping\",\"arguments\":\"{}\"}}]}}]}\n\n",
                        "data: [DONE]\n\n"
                    )
                } else {
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"tool done\"}}]}\n\n",
                        "data: [DONE]\n\n"
                    )
                };
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn serve_app(state: Arc<AppState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, api::router(state)).await.unwrap();
    });
    format!("ws://{addr}/ws")
}

// --- Telegram bridge -----------------------------------------------------

struct MockTg {
    updates: Mutex<Vec<Value>>,
    sent: Mutex<Vec<(i64, String)>>,
}

#[async_trait::async_trait]
impl TelegramApi for MockTg {
    async fn get_updates(&self, _o: i64, _t: u64) -> anyhow::Result<Vec<Value>> {
        Ok(std::mem::take(&mut *self.updates.lock().await))
    }
    async fn send_message(&self, chat_id: i64, text: &str) -> anyhow::Result<()> {
        self.sent.lock().await.push((chat_id, text.to_string()));
        Ok(())
    }
}

#[tokio::test]
async fn telegram_bridge_delivers_response() {
    let upstream = mock_llm_tool_then_text().await;
    let mut cfg = test_config(&upstream);
    // No MCP servers → tool call fails fast, second turn returns text.
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg.clone()));
    cfg.mcp_servers.clear();
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store, mcp).await;
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None).await.unwrap();
    let tg = Arc::new(MockTg {
        updates: Mutex::new(vec![json!({
            "update_id": 1,
            "message": {"chat": {"id": 42}, "text": "hi"}
        })]),
        sent: Mutex::new(vec![]),
    });
    let bridge = Bridge::new(tg.clone(), client);
    bridge.client().initialize().await.unwrap();
    bridge.spawn_event_router().await;
    bridge
        .handle_update(json!({
            "update_id": 1,
            "message": {"chat": {"id": 42}, "text": "hi"}
        }))
        .await;

    // Wait for the reply to land.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let sent = tg.sent.lock().await;
        if sent.iter().any(|(_, t)| t.contains("tool done")) {
            break;
        }
        drop(sent);
        assert!(std::time::Instant::now() < deadline, "timed out waiting for reply");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// --- Real MCP + permission round-trip ------------------------------------

#[tokio::test]
async fn mcp_tool_call_with_permission() {
    let upstream = mock_llm_tool_then_text().await;
    let mut cfg = test_config(&upstream);
    let server_py = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/mcp_server.py");
    cfg.mcp_servers.insert(
        "test".to_string(),
        McpServerConfig {
            command: "python3".to_string(),
            args: vec![server_py.to_string()],
            env: HashMap::new(),
            auto_approve: false,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let mcp = {
        let servers = shared.read().mcp_servers.clone();
        McpRegistry::connect_all(&servers).await
    };
    assert!(mcp.has_tool("test.ping"), "MCP tool not registered");
    let state = AppState::new(shared, store.clone(), mcp).await;
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None).await.unwrap();
    client.initialize().await.unwrap();
    let session = client.new_session("/tmp").await.unwrap();
    let mut events = client.events().await;

    client.prompt(&session, "use the tool").await.unwrap();

    // Expect a permission request; answer allow. The turn result arrives
    // as PromptDone on the event stream, ordered after the chunks.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let mut got_permission = false;
    let mut got_text = false;
    loop {
        assert!(std::time::Instant::now() < deadline, "timed out");
        match events.recv().await {
            Some(damon_core::client::ClientEvent::Request { id, method, .. })
                if method == "session/request_permission" =>
            {
                got_permission = true;
                client
                    .respond(id, json!({
                        "outcome": {"outcome": "selected", "optionId": "allow-once"}
                    }))
                    .await
                    .unwrap();
            }
            Some(damon_core::client::ClientEvent::Update(p))
                if p["update"]["content"]["text"] == "tool done" =>
            {
                got_text = true;
            }
            Some(damon_core::client::ClientEvent::PromptDone { result, .. }) => {
                let v = result.map_err(|e| anyhow::anyhow!("{e}")).unwrap();
                assert_eq!(v["stopReason"], "end_turn");
                break;
            }
            _ => {}
        }
    }
    assert!(got_permission, "no permission request received");
    assert!(got_text, "no final text received");

    // Tool result persisted.
    let msgs = store.messages(&session).await.unwrap();
    let tool_msg = msgs.iter().find(|m| m["role"] == "tool").unwrap();
    assert!(tool_msg["content"].as_str().unwrap().contains("content"));
}

// --- Remote access safety -------------------------------------------------

#[tokio::test]
async fn non_loopback_without_token_refuses() {
    let dir = std::env::temp_dir().join(format!("damon-t4-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = dir.join("config.toml");
    std::fs::write(
        &cfg,
        "bind = \"0.0.0.0:19499\"\ndata_dir = \"/tmp/damon-t4\"\n",
    )
    .unwrap();
    let exe = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("damond");
    let out = std::process::Command::new(exe)
        .arg("--config")
        .arg(&cfg)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("auth_token"), "got: {stderr}");
}

#[tokio::test]
async fn tls_serves_https() {
    // Self-signed cert for localhost.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = std::env::temp_dir().join(format!("damon-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();

    let cfg_path = dir.join("config.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "bind = \"127.0.0.1:0\"\ndata_dir = \"{}\"\ntls_cert = \"{}\"\ntls_key = \"{}\"\n",
            dir.display(),
            cert_path.display(),
            key_path.display()
        ),
    )
    .unwrap();

    // Bind a fixed port so we can connect.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    std::fs::write(
        &cfg_path,
        format!(
            "bind = \"127.0.0.1:{port}\"\ndata_dir = \"{}\"\ntls_cert = \"{}\"\ntls_key = \"{}\"\n",
            dir.display(),
            cert_path.display(),
            key_path.display()
        ),
    )
    .unwrap();

    let exe = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("damond");
    let mut child = std::process::Command::new(exe)
        .arg("--config")
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut ok = false;
    while std::time::Instant::now() < deadline {
        match client
            .get(format!("https://127.0.0.1:{port}/health"))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                ok = true;
                break;
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(ok, "TLS /health never became ready");
}
