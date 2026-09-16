//! Channel adapter tests: generic bridge (delivery, per-chat sessions,
//! permission round-trip), Discord/Slack message extraction, REST calls,
//! and the WS handshakes against mock gateway servers.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Json;
use damon_core::api::{self, AppState};
use damon_core::channel::{Bridge, ChannelApi, Incoming};
use damon_core::config::{Config, McpServerConfig, ProviderConfig};
use damon_core::discord::{DiscordApi, DiscordChannel, incoming_from_message};
use damon_core::mcp::McpRegistry;
use damon_core::slack::{SlackApi, SlackChannel, incoming_from_event};
use damon_core::store::Store;
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

/// Mock LLM that always answers with text (no tool calls).
async fn mock_llm_text() -> String {
    let app = Router::new().route(
        "/chat/completions",
        post(|| async {
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"hello back\"}}]}\n\n",
                    "data: [DONE]\n\n"
                )))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

// --- Generic bridge ---------------------------------------------------------

/// Channel whose recv never yields — tests drive handle_message directly.
struct MockChannel {
    sent: Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
impl ChannelApi for MockChannel {
    async fn recv(&self) -> anyhow::Result<Option<Incoming>> {
        std::future::pending().await
    }
    async fn send(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        self.sent
            .lock()
            .await
            .push((chat_id.to_string(), text.to_string()));
        Ok(())
    }
}

async fn wait_for_sent(sent: &Mutex<Vec<(String, String)>>, needle: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if sent.lock().await.iter().any(|(_, t)| t.contains(needle)) {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {needle:?}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn bridge_delivers_response_and_maps_sessions() {
    let upstream = mock_llm_text().await;
    let cfg = test_config(&upstream);
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None).await.unwrap();
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let bridge = Bridge::new(ch.clone(), client);
    bridge.client().initialize().await.unwrap();
    bridge.spawn_event_router().await;


    // Two different chats → two different sessions.
    bridge.handle_message("chat-a".into(), None, "hi".into()).await;
    bridge.handle_message("chat-b".into(), None, "hi".into()).await;

    // Wait until BOTH chats got their reply.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        {
            let sent = ch.sent.lock().await;
            let a = sent.iter().any(|(c, t)| c == "chat-a" && t.contains("hello back"));
            let b = sent.iter().any(|(c, t)| c == "chat-b" && t.contains("hello back"));
            if a && b {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            let sent = ch.sent.lock().await;
            let sessions = store.list_sessions().await.unwrap_or_default();
            let mut dump = String::new();
            for (sid, _) in &sessions {
                let msgs = store.messages(sid).await.unwrap_or_default();
                dump.push_str(&format!("session {sid}: {} msgs\n", msgs.len()));
                for m in msgs {
                    dump.push_str(&format!("  {} {}\n", m["role"], &m.to_string()[..m.to_string().len().min(120)]));
                }
            }
            panic!("timed out waiting for both replies; sent={sent:?}\n{dump}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let sessions = store.list_sessions().await.unwrap();
        if sessions.len() >= 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "expected 2 sessions");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn bridge_permission_reply_allows_tool() {
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
    let mcp_servers = shared.read().mcp_servers.clone();
    let mcp = McpRegistry::connect_all(&mcp_servers).await;
    assert!(mcp.has_tool("test.ping"), "MCP tool not registered");
    let state = AppState::new(shared, store, mcp).await;
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None).await.unwrap();
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let bridge = Bridge::new(ch.clone(), client);
    bridge.client().initialize().await.unwrap();
    bridge.spawn_event_router().await;

    bridge.handle_message("42".into(), None, "use the tool".into()).await;
    // Permission prompt lands in the chat…
    wait_for_sent(&ch.sent, "🔐").await;
    // …and "allow" approves it, letting the turn finish.
    bridge.handle_message("42".into(), None, "allow".into()).await;
    wait_for_sent(&ch.sent, "✅ allowed").await;
    wait_for_sent(&ch.sent, "tool done").await;
}

// --- Discord extraction -------------------------------------------------------

#[test]
fn discord_dm_passes_through() {
    let d = json!({
        "channel_id": "C1",
        "content": "hello",
        "author": {"id": "U1", "bot": false},
    });
    let msg = incoming_from_message(&d, "B1").unwrap();
    assert_eq!(msg.chat_id, "C1");
    assert_eq!(msg.text, "hello");
}

#[test]
fn discord_guild_requires_mention() {
    let base = |content: &str| {
        json!({
            "channel_id": "C1",
            "guild_id": "G1",
            "content": content,
            "author": {"id": "U1"},
        })
    };
    assert!(incoming_from_message(&base("hello"), "B1").is_none());
    let msg = incoming_from_message(&base("<@B1> hello"), "B1").unwrap();
    assert_eq!(msg.text, "hello");
    let msg = incoming_from_message(&base("<@!B1>: status?"), "B1").unwrap();
    assert_eq!(msg.text, "status?");
    // Mention only, no text → ignored.
    assert!(incoming_from_message(&base("<@B1>"), "B1").is_none());
}

#[test]
fn discord_skips_bots() {
    let d = json!({
        "channel_id": "C1",
        "content": "hi",
        "author": {"id": "U2", "bot": true},
    });
    assert!(incoming_from_message(&d, "B1").is_none());
}

// --- Slack extraction ---------------------------------------------------------

#[test]
fn slack_dm_passes_through() {
    let ev = json!({
        "type": "message",
        "channel": "D1",
        "channel_type": "im",
        "user": "U1",
        "text": "hello",
    });
    let msg = incoming_from_event(&ev, "UBOT").unwrap();
    assert_eq!(msg.chat_id, "D1");
    assert_eq!(msg.text, "hello");
}

#[test]
fn slack_channel_requires_mention() {
    let base = |text: &str| {
        json!({
            "type": "message",
            "channel": "C1",
            "channel_type": "channel",
            "user": "U1",
            "text": text,
        })
    };
    assert!(incoming_from_event(&base("hello"), "UBOT").is_none());
    let msg = incoming_from_event(&base("<@UBOT> hello"), "UBOT").unwrap();
    assert_eq!(msg.text, "hello");
}

#[test]
fn slack_skips_subtypes_and_self() {
    let bot_msg = json!({
        "type": "message", "subtype": "bot_message",
        "channel": "D1", "channel_type": "im", "text": "hi",
    });
    assert!(incoming_from_event(&bot_msg, "UBOT").is_none());
    let own = json!({
        "type": "message",
        "channel": "D1", "channel_type": "im",
        "user": "UBOT", "text": "hi",
    });
    assert!(incoming_from_event(&own, "UBOT").is_none());
}

// --- REST APIs against mock servers -------------------------------------------

#[tokio::test]
async fn discord_api_fetches_id_and_posts() {
    let sent = Arc::new(Mutex::new(Vec::<Value>::new()));
    let sent2 = sent.clone();
    let app = Router::new()
        .route("/users/@me", get(|| async { Json(json!({"id": "B1"})) }))
        .route(
            "/channels/{ch}/messages",
            post(move |Path(ch): Path<String>, Json(body): Json<Value>| {
                let sent = sent2.clone();
                async move {
                    sent.lock().await.push(json!({"ch": ch, "body": body}));
                    Json(json!({"id": "m1"}))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let api = DiscordApi::with_base("tok", &format!("http://{addr}"));
    assert_eq!(api.bot_user_id().await.unwrap(), "B1");
    api.send_message("C9", "hi there").await.unwrap();
    let sent = sent.lock().await;
    assert_eq!(sent[0]["ch"], "C9");
    assert_eq!(sent[0]["body"]["content"], "hi there");
}

#[tokio::test]
async fn slack_api_auth_post_and_error() {
    let app = Router::new()
        .route(
            "/auth.test",
            post(|| async { Json(json!({"ok": true, "user_id": "UBOT"})) }),
        )
        .route(
            "/chat.postMessage",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body["channel"], "C1");
                Json(json!({"ok": true, "ts": "1.0"}))
            }),
        )
        .route(
            "/apps.connections.open",
            post(|| async { Json(json!({"ok": true, "url": "wss://sock.example/"})) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let api = SlackApi::with_base("xapp-t", "xoxb-t", &format!("http://{addr}"));
    assert_eq!(api.bot_user_id().await.unwrap(), "UBOT");
    api.post_message("C1", "hi").await.unwrap();
    assert_eq!(api.connections_open().await.unwrap(), "wss://sock.example/");

    // ok:false surfaces as an error.
    let bad = Router::new().route(
        "/auth.test",
        post(|| async { Json(json!({"ok": false, "error": "invalid_auth"})) }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, bad).await.unwrap();
    });
    let api = SlackApi::with_base("x", "y", &format!("http://{addr}"));
    let err = api.bot_user_id().await.unwrap_err().to_string();
    assert!(err.contains("invalid_auth"), "unexpected error: {err}");
}

// --- WS handshakes against mock gateways ---------------------------------------

/// Mock Discord gateway: hello → expect identify → dispatch a DM.
#[tokio::test]
async fn discord_gateway_handshake_and_dispatch() {
    async fn gw(ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(|mut sock: WebSocket| async move {
            sock.send(WsMessage::Text(
                json!({"op": 10, "d": {"heartbeat_interval": 60000}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            // Identify frame.
            let msg = sock.recv().await.unwrap().unwrap();
            let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            assert_eq!(v["op"], 2);
            assert_eq!(v["d"]["token"], "tok");
            // Dispatch a DM MESSAGE_CREATE.
            sock.send(WsMessage::Text(
                json!({
                    "op": 0, "t": "MESSAGE_CREATE", "s": 1,
                    "d": {
                        "channel_id": "C1", "content": "hi bot",
                        "author": {"id": "U1"},
                    },
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        })
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", get(gw)))
            .await
            .unwrap();
    });

    // REST mock: users/@me + gateway/bot pointing at the WS mock.
    let gw_url = format!("ws://{ws_addr}");
    let rest = Router::new()
        .route("/users/@me", get(|| async { Json(json!({"id": "B1"})) }))
        .route(
            "/gateway/bot",
            get(move || {
                let gw_url = gw_url.clone();
                async move { Json(json!({"url": gw_url})) }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, rest).await.unwrap();
    });

    let api = Arc::new(DiscordApi::with_base("tok", &format!("http://{rest_addr}")));
    let ch = DiscordChannel::new(api);
    ch.ready().await.unwrap();
    let msg = ch.recv().await.unwrap().unwrap();
    assert_eq!(msg.chat_id, "C1");
    assert_eq!(msg.text, "hi bot");
}

/// Mock Slack socket: envelope with a DM event → expect ack + Incoming.
#[tokio::test]
async fn slack_socket_envelope_ack_and_dispatch() {
    let acked = Arc::new(Mutex::new(Vec::<String>::new()));
    let acked2 = acked.clone();

    async fn sock_handler(
        ws: WebSocketUpgrade,
        State(acked): State<Arc<Mutex<Vec<String>>>>,
    ) -> Response {
        ws.on_upgrade(move |mut sock: WebSocket| async move {
            sock.send(WsMessage::Text(
                json!({
                    "envelope_id": "e1",
                    "type": "events_api",
                    "payload": {"event": {
                        "type": "message", "channel": "D1",
                        "channel_type": "im", "user": "U1", "text": "yo",
                    }},
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
            // Read the ack frame.
            let msg = sock.recv().await.unwrap().unwrap();
            let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            acked.lock().await.push(v["envelope_id"].as_str().unwrap().to_string());
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        })
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = listener.local_addr().unwrap();
    let sock_app = Router::new()
        .route("/", get(sock_handler))
        .with_state(acked2);
    tokio::spawn(async move {
        axum::serve(listener, sock_app).await.unwrap();
    });

    // REST mock: auth.test + apps.connections.open → ws url.
    let sock_url = format!("ws://{ws_addr}");
    let rest = Router::new()
        .route(
            "/auth.test",
            post(|| async { Json(json!({"ok": true, "user_id": "UBOT"})) }),
        )
        .route(
            "/apps.connections.open",
            post(move || {
                let sock_url = sock_url.clone();
                async move { Json(json!({"ok": true, "url": sock_url})) }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, rest).await.unwrap();
    });

    let api = Arc::new(SlackApi::with_base(
        "xapp-t",
        "xoxb-t",
        &format!("http://{rest_addr}"),
    ));
    let ch = SlackChannel::new(api);
    ch.ready().await.unwrap();
    let msg = ch.recv().await.unwrap().unwrap();
    assert_eq!(msg.chat_id, "D1");
    assert_eq!(msg.text, "yo");
    // Envelope was acked.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while acked.lock().await.is_empty() {
        assert!(std::time::Instant::now() < deadline, "no ack received");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(acked.lock().await[0], "e1");
}
