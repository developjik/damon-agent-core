//! Channel adapter tests: generic bridge (delivery, per-chat sessions,
//! permission round-trip), Discord/Slack message extraction, REST calls,
//! and the WS handshakes against mock gateway servers.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::{get, post};
use damon_core::api::{self, AppState};
use damon_core::channel::{Bridge, ChannelApi, Incoming};
use damon_core::config::Config;
use damon_core::discord::{DiscordApi, DiscordChannel, incoming_from_message};
use damon_core::slack::{SlackApi, SlackChannel, incoming_from_event};
use damon_core::store::Store;
use serde_json::{Value, json};
use tokio::sync::Mutex;

fn test_config() -> Config {
    common::mock_config(None)
}

async fn serve_app(state: Arc<AppState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // ConnectInfo installs the peer IP ws_handler's auth check reads.
        axum::serve(
            listener,
            api::router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("ws://{addr}/ws")
}

mod common;

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
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {needle:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn bridge_delivers_response_and_maps_sessions() {
    let cfg = test_config();
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(shared, store.clone()).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let bridge = Bridge::new(ch.clone(), client);
    bridge.client().hello().await.unwrap();
    bridge.spawn_event_router().await;

    // Two different chats → two different sessions.
    bridge
        .handle_message("chat-a".into(), None, None, "hi".into(), Vec::new())
        .await;
    bridge
        .handle_message("chat-b".into(), None, None, "hi".into(), Vec::new())
        .await;

    // Wait until BOTH chats got their reply.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        {
            let sent = ch.sent.lock().await;
            let a = sent
                .iter()
                .any(|(c, t)| c == "chat-a" && t.contains("echo:"));
            let b = sent
                .iter()
                .any(|(c, t)| c == "chat-b" && t.contains("echo:"));
            if a && b {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            let sent = ch.sent.lock().await;
            let sessions = store
                .list_sessions_paged(u32::MAX, 0)
                .await
                .unwrap_or_default();
            let mut dump = String::new();
            for (sid, ..) in &sessions {
                let msgs = store
                    .messages_paged(sid, u32::MAX, 0)
                    .await
                    .unwrap_or_default();
                dump.push_str(&format!("session {sid}: {} msgs\n", msgs.len()));
                for m in msgs {
                    dump.push_str(&format!(
                        "  {} {}\n",
                        m.data["role"],
                        &m.data.to_string()[..m.data.to_string().len().min(120)]
                    ));
                }
            }
            panic!("timed out waiting for both replies; sent={sent:?}\n{dump}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let sessions = store.list_sessions_paged(u32::MAX, 0).await.unwrap();
        if sessions.len() >= 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "expected 2 sessions");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn bridge_permission_reply_allows_tool() {
    let cfg = test_config();
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(shared, store).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let bridge = Bridge::new(ch.clone(), client);
    bridge.client().hello().await.unwrap();
    bridge.spawn_event_router().await;

    bridge
        .handle_message(
            "42".into(),
            None,
            None,
            "use the perm tool".into(),
            Vec::new(),
        )
        .await;
    // Permission prompt lands in the chat…
    wait_for_sent(&ch.sent, "🔐").await;
    // …and "allow" approves it, letting the turn finish.
    bridge
        .handle_message("42".into(), None, None, "allow".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "✅ allowed").await;
    // The mock emits "allowed" as assistant text once the tool runs —
    // exact match so "✅ allowed" can't satisfy the wait early.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if ch.sent.lock().await.iter().any(|(_, t)| t == "allowed") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "tool never ran");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
    api.post_message("C1", "hi", None).await.unwrap();
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
            acked
                .lock()
                .await
                .push(v["envelope_id"].as_str().unwrap().to_string());
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

/// An unlisted sender must be dropped before any session/prompt work —
/// a public bot without an allowlist is unauthenticated agent access.
#[tokio::test]
async fn bridge_allowlist_drops_unlisted_senders() {
    let cfg = test_config();
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(shared, store.clone()).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let url = serve_app(state).await;

    let client = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let bridge = Bridge::new(ch.clone(), client);
    bridge.set_allowed(["user-ok".to_string()]);
    bridge.client().hello().await.unwrap();
    bridge.spawn_event_router().await;

    // Unlisted sender: dropped before any session is created.
    bridge
        .handle_message(
            "chat-evil".into(),
            None,
            Some("user-evil".into()),
            "hi".into(),
            Vec::new(),
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        ch.sent.lock().await.is_empty(),
        "unlisted sender got a response"
    );
    assert!(
        store
            .list_sessions_paged(u32::MAX, 0)
            .await
            .unwrap()
            .is_empty(),
        "unlisted sender created a session"
    );

    // Listed sender: works normally.
    bridge
        .handle_message(
            "chat-ok".into(),
            None,
            Some("user-ok".into()),
            "hi".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "echo:").await;
}
