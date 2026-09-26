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
use damon_core::channel::{Bridge, ChannelApi, Incoming, MediaOut};
use damon_core::config::Config;
use damon_core::discord::{DiscordApi, DiscordChannel, incoming_from_message};
use damon_core::slack::{SlackApi, SlackChannel, incoming_from_event};
use damon_core::store::{SessionFilter, Store};
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
        if std::time::Instant::now() >= deadline {
            let dump = sent.lock().await.clone();
            panic!("timed out waiting for {needle:?}; sent={dump:?}");
        }
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
                .list_sessions_paged(u32::MAX, 0, Default::default())
                .await
                .unwrap_or_default();
            let mut dump = String::new();
            for s in &sessions {
                let msgs = store
                    .messages_paged(&s.id, u32::MAX, 0)
                    .await
                    .unwrap_or_default();
                dump.push_str(&format!("session {}: {} msgs\n", s.id, msgs.len()));
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
        let sessions = store
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap();
        if sessions.len() >= 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "expected 2 sessions");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Bridge restart no longer resets a conversation: the chat→session
/// mapping and the !cwd/!agent prefs live in daemon-side
/// channel_state rows, so a fresh Bridge (fresh connection, even)
/// reattaches the same session, and a !new after restart still
/// creates with the persisted cwd.
#[tokio::test]
async fn bridge_state_survives_restart() {
    let cfg = test_config();
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(shared, store.clone()).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let url = serve_app(state).await;

    let workdir = std::env::temp_dir().join(format!("damon-chan-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&workdir).unwrap();

    // First incarnation: set the conversation's cwd, run a turn.
    let sent1 = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let client1 = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let bridge1 = Bridge::new(sent1.clone(), client1);
    bridge1.client().hello().await.unwrap();
    bridge1.spawn_event_router().await;
    bridge1
        .handle_message(
            "chat-a".into(),
            None,
            None,
            format!("!cwd {}", workdir.display()),
            Vec::new(),
        )
        .await;
    wait_for_sent(&sent1.sent, "cwd set").await;
    bridge1
        .handle_message("chat-a".into(), None, None, "hi".into(), Vec::new())
        .await;
    wait_for_sent(&sent1.sent, "echo: hi").await;

    let first: Vec<_> = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(first.len(), 1, "one session for the chat");
    let first_id = first[0].id.clone();
    assert_eq!(
        std::path::Path::new(&first[0].cwd),
        workdir.as_path(),
        "!cwd must shape the created session: {:?}",
        first[0].cwd
    );

    // Second incarnation: fresh connection, fresh Bridge — same chat.
    let sent2 = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let client2 = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let bridge2 = Bridge::new(sent2.clone(), client2);
    bridge2.client().hello().await.unwrap();
    bridge2.spawn_event_router().await;
    bridge2
        .handle_message("chat-a".into(), None, None, "again".into(), Vec::new())
        .await;
    wait_for_sent(&sent2.sent, "echo: again").await;

    // The persisted mapping reattached the SAME session — no second
    // row was created for the chat.
    let rows: Vec<_> = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "restart must reattach, not re-create: {rows:?}"
    );
    assert_eq!(rows[0].id, first_id);

    // !new after restart: the persisted prefs still pick the cwd for
    // the fresh session.
    bridge2
        .handle_message("chat-a".into(), None, None, "!new".into(), Vec::new())
        .await;
    wait_for_sent(&sent2.sent, "session reset").await;
    bridge2
        .handle_message("chat-a".into(), None, None, "fresh".into(), Vec::new())
        .await;
    wait_for_sent(&sent2.sent, "echo: fresh").await;
    let rows: Vec<_> = store
        .list_sessions_paged(u32::MAX, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "!new creates a fresh session");
    let fresh = rows.iter().find(|r| r.id != first_id).unwrap();
    assert_eq!(
        std::path::Path::new(&fresh.cwd),
        workdir.as_path(),
        "persisted prefs must shape the post-restart session: {:?}",
        fresh.cwd
    );
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
            .list_sessions_paged(u32::MAX, 0, Default::default())
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

// --- Bounded prompt queueing ---------------------------------------------------

#[tokio::test]
async fn prompts_queue_behind_a_running_turn_and_drain_in_order() {
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

    // A permission-asking turn parks the lane…
    bridge
        .handle_message(
            "chat".into(),
            None,
            Some("u1".into()),
            "use the perm tool".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "🔐").await;
    // …so the next three prompts queue with positions, not rejections…
    for text in ["two", "three", "four"] {
        bridge
            .handle_message("chat".into(), None, None, text.into(), Vec::new())
            .await;
    }
    // …and the fifth is refused — the queue is capped.
    bridge
        .handle_message("chat".into(), None, None, "five".into(), Vec::new())
        .await;
    {
        let sent = ch.sent.lock().await;
        assert!(sent.iter().any(|(_, t)| t == "queued — position 1"));
        assert!(sent.iter().any(|(_, t)| t == "queued — position 2"));
        assert!(sent.iter().any(|(_, t)| t == "queued — position 3"));
        assert!(
            sent.iter()
                .any(|(_, t)| t == "queue full — try again after the running turn")
        );
    }

    // Resolving the permission ends turn 1 and drains the queue in
    // FIFO order — every queued echo arrives, the refused one doesn't.
    bridge
        .handle_message(
            "chat".into(),
            None,
            Some("u1".into()),
            "allow".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "✅ allowed").await;
    wait_for_sent(&ch.sent, "echo: two").await;
    wait_for_sent(&ch.sent, "echo: three").await;
    wait_for_sent(&ch.sent, "echo: four").await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !ch.sent
            .lock()
            .await
            .iter()
            .any(|(_, t)| t.contains("echo: five")),
        "capped prompt ran anyway"
    );
}

#[tokio::test]
async fn cancel_drops_queued_prompts_and_frees_the_lane() {
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

    // A hanging turn parks the lane; one prompt queues behind it.
    bridge
        .handle_message("chat".into(), None, None, "hang".into(), Vec::new())
        .await;
    bridge
        .handle_message("chat".into(), None, None, "stale prompt".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "queued — position 1").await;
    // The hang turn's session must exist before !cancel can cancel it:
    // wait for the store row, then a beat for the bridge to cache the
    // mapping (the store row is written before the map insert).
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if !store
                .list_sessions_paged(u32::MAX, 0, Default::default())
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "hang turn never created a session"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // !cancel stops the turn AND clears its queue.
    bridge
        .handle_message("chat".into(), None, None, "!cancel".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "cancel requested — dropped 1 queued prompt(s)").await;
    // The cancelled turn's end is observable as its stop marker.
    wait_for_sent(&ch.sent, "[canceled]").await;

    // The lane is free again: a fresh prompt runs (never queues), and
    // the cancelled queue's prompt never runs.
    bridge
        .handle_message("chat".into(), None, None, "fresh".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "echo: fresh").await;
    assert!(
        !ch.sent
            .lock()
            .await
            .iter()
            .any(|(_, t)| t.contains("stale")),
        "cancelled queue drained into a turn anyway"
    );
}

// --- !cwd / !agent preferences -------------------------------------------------

#[tokio::test]
async fn cwd_pref_applies_to_the_next_new_session() {
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

    // Relative and non-directory paths are rejected with the reason.
    bridge
        .handle_message(
            "chat".into(),
            None,
            None,
            "!cwd relative/path".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "cwd must be an absolute path").await;
    bridge
        .handle_message(
            "chat".into(),
            None,
            None,
            "!cwd /no/such/dir-xyz".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "not a directory").await;

    // Setting it replies that it applies to the NEXT session…
    let tmp = std::env::temp_dir();
    bridge
        .handle_message(
            "chat".into(),
            None,
            None,
            format!("!cwd {}", tmp.display()),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "cwd set — it applies to the next new session").await;
    // …and !cwd alone reports the effective value.
    bridge
        .handle_message("chat".into(), None, None, "!cwd".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, &format!("cwd: {}", tmp.display())).await;

    // A fresh session is created with that cwd — observable in the
    // store's session rows (filter by cwd).
    bridge
        .handle_message("chat".into(), None, None, "hi".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "echo:").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let n = store
            .list_sessions_paged(
                u32::MAX,
                0,
                SessionFilter {
                    cwd: Some(tmp.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .len();
        if n == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no session created with cwd {}",
            tmp.display()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn agent_pref_lists_validates_and_resets_the_session() {
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

    // Listing shows the daemon's backends (built-in registry plus the
    // injected mock) and the effective default.
    bridge
        .handle_message("chat".into(), None, None, "!agent".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "effective: daemon default").await;
    {
        let sent = ch.sent.lock().await;
        let listing = sent
            .iter()
            .map(|(_, t)| t)
            .find(|t| t.starts_with("backends: "))
            .expect("no backends listing reply");
        assert!(listing.contains("mock"), "mock missing from {listing:?}");
    }
    // Unknown backends are rejected, listing what exists.
    bridge
        .handle_message(
            "chat".into(),
            None,
            None,
            "!agent nosuch".into(),
            Vec::new(),
        )
        .await;
    {
        let sent = ch.sent.lock().await;
        assert!(
            sent.iter()
                .any(|(_, t)| t.starts_with("unknown backend 'nosuch'")),
            "no unknown-backend reply: {sent:?}"
        );
    }

    // Setting a backend resets the session mapping: the next prompt
    // creates a NEW session instead of reusing the old one.
    bridge
        .handle_message("chat".into(), None, None, "first".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "echo: first").await;
    let count_after_first = || async {
        store
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap()
            .len()
    };
    let before = count_after_first().await;
    assert_eq!(before, 1);
    bridge
        .handle_message("chat".into(), None, None, "!agent mock".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "backend set to mock").await;
    bridge
        .handle_message("chat".into(), None, None, "second".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "echo: second").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if count_after_first().await >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "session mapping was not reset by !agent"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// --- Permission buttons through the bridge -------------------------------------

/// A button press resolves the pending lane exactly like a typed reply:
/// the synthesized Incoming is the same shape (word as text, presser as
/// sender), so the requester-identity binding applies to presses too —
/// someone else's ✅ must not approve your tool run.
#[tokio::test]
async fn button_press_resolves_permission_with_requester_identity() {
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
            "chat".into(),
            None,
            Some("u1".into()),
            "use the perm tool".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "🔐").await;

    // Another user's press (or typed allow) is swallowed — no ✅.
    let press_from = |id: &str| Incoming {
        chat_id: "chat".into(),
        thread_id: None,
        sender_id: Some(id.into()),
        text: "allow".into(),
        attachments: Vec::new(),
    };
    bridge
        .handle_message(
            "chat".into(),
            None,
            Some("u2".into()),
            "allow".into(),
            Vec::new(),
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !ch.sent.lock().await.iter().any(|(_, t)| t.contains("✅")),
        "non-requester approved the tool"
    );
    // The requester's press resolves it and the tool runs.
    let press = press_from("u1");
    bridge
        .handle_message(
            press.chat_id,
            press.thread_id,
            press.sender_id,
            press.text,
            press.attachments,
        )
        .await;
    wait_for_sent(&ch.sent, "✅ allowed").await;
    wait_for_sent(&ch.sent, "allowed").await;
}

// --- Cross-surface pickup: !sessions / !resume / !watch ----------------------

/// Serve a daemon with the mock backend, wire a bridge to it, and hand
/// back a second client standing in for ANOTHER surface (web UI, CLI)
/// that owns the "desk" sessions the chat picks up or watches.
async fn serve_with_bridge() -> (
    String,
    Arc<MockChannel>,
    Arc<Bridge>,
    damon_core::client::DamonClient,
    Store,
) {
    let cfg = test_config();
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(shared, store.clone()).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let url = serve_app(state).await;
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let client = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let bridge = Bridge::new(ch.clone(), client.clone());
    bridge.client().hello().await.unwrap();
    bridge.spawn_event_router().await;
    (url, ch, bridge, client, store)
}

/// !sessions lists sessions from every surface and !resume adopts one:
/// the conversation's next prompt lands in the adopted session's
/// history, not in a fresh session.
#[tokio::test]
async fn sessions_and_resume_continue_a_desk_session_in_the_chat() {
    let (_url, ch, bridge, client, store) = serve_with_bridge().await;

    // The chat's own session — one prompt so it exists and has a title.
    bridge
        .handle_message("chat-a".into(), None, None, "hi".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "echo: hi").await;

    // A "desk" session from another surface, given a friendly title.
    let desk = client
        .create_session(Some("mock"), "/tmp", None)
        .await
        .unwrap();
    client.rename_session(&desk, "desk work").await.unwrap();

    bridge
        .handle_message("chat-a".into(), None, None, "!sessions".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "desk work").await;
    wait_for_sent(&ch.sent, "!resume").await;

    // Adopt by title fragment, then prompt — the turn must reach the
    // adopted session.
    bridge
        .handle_message(
            "chat-a".into(),
            None,
            None,
            "!resume desk".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "attached: \"desk work\"").await;
    bridge
        .handle_message(
            "chat-a".into(),
            None,
            None,
            "hello again".into(),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "echo: hello again").await;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let msgs = store.messages_paged(&desk, u32::MAX, 0).await.unwrap();
        let has_user = msgs
            .iter()
            .any(|m| m.role == "user" && m.data["content"] == "hello again");
        let has_echo = msgs
            .iter()
            .any(|m| m.role == "assistant" && m.data["content"] == "echo: hello again");
        if has_user && has_echo {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "prompt never reached the adopted session"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A watched session's completion pings the chat even when the turn
/// was started by another surface — the cross-surface notification.
#[tokio::test]
async fn watch_pings_the_chat_when_another_surface_finishes() {
    let (_url, ch, bridge, client, _store) = serve_with_bridge().await;
    let desk = client
        .create_session(Some("mock"), "/tmp", None)
        .await
        .unwrap();
    client.rename_session(&desk, "long job").await.unwrap();

    bridge
        .handle_message(
            "chat-a".into(),
            None,
            None,
            format!("!watch {}", &desk[..8]),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "watching \"long job\"").await;

    // The desk surface runs the turn; the chat gets the ping.
    client
        .turn_start_blocks(&desk, vec![json!({"type": "text", "text": "go"})])
        .await
        .unwrap();
    wait_for_sent(&ch.sent, "finished a turn").await;
}

/// A watched session's permission ask surfaces in the chat and is
/// answerable there: the reply maps onto the offered actions and the
/// desk turn proceeds to completion.
#[tokio::test]
async fn watch_relayed_permission_ask_is_answerable_from_the_chat() {
    let (_url, ch, bridge, client, store) = serve_with_bridge().await;
    let desk = client
        .create_session(Some("mock"), "/tmp", None)
        .await
        .unwrap();

    bridge
        .handle_message(
            "chat-a".into(),
            None,
            None,
            format!("!watch {}", &desk[..8]),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "watching").await;

    // The desk surface prompts with the mock's permission knob — the
    // ask must surface in the chat.
    client
        .turn_start_blocks(&desk, vec![json!({"type": "text", "text": "perm please"})])
        .await
        .unwrap();
    wait_for_sent(&ch.sent, "reply 'allow' or 'deny'").await;

    bridge
        .handle_message("chat-a".into(), None, None, "allow".into(), Vec::new())
        .await;
    wait_for_sent(&ch.sent, "✅ allowed").await;
    wait_for_sent(&ch.sent, "finished a turn").await;

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let msgs = store.messages_paged(&desk, u32::MAX, 0).await.unwrap();
        if msgs
            .iter()
            .any(|m| m.role == "assistant" && m.data["content"] == "allowed")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "permission never took effect"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// !unwatch stops the pings — the session itself keeps running.
#[tokio::test]
async fn unwatch_stops_the_pings() {
    let (_url, ch, bridge, client, _store) = serve_with_bridge().await;
    let desk = client
        .create_session(Some("mock"), "/tmp", None)
        .await
        .unwrap();

    bridge
        .handle_message(
            "chat-a".into(),
            None,
            None,
            format!("!watch {}", &desk[..8]),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "watching").await;
    bridge
        .handle_message(
            "chat-a".into(),
            None,
            None,
            format!("!unwatch {}", &desk[..8]),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch.sent, "stopped following").await;

    client
        .turn_start_blocks(&desk, vec![json!({"type": "text", "text": "go"})])
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(
        !ch.sent
            .lock()
            .await
            .iter()
            .any(|(_, t)| t.contains("finished a turn")),
        "unwatched session still pinged the chat"
    );
}

/// The watch registry lives daemon-side: a fresh bridge restores it
/// wholesale and keeps pinging after the original bridge is gone.
#[tokio::test]
async fn watches_survive_a_bridge_restart() {
    let (url, ch1, bridge1, client, _store) = serve_with_bridge().await;
    let desk = client
        .create_session(Some("mock"), "/tmp", None)
        .await
        .unwrap();
    client.rename_session(&desk, "long job").await.unwrap();

    bridge1
        .handle_message(
            "chat-a".into(),
            None,
            None,
            format!("!watch {}", &desk[..8]),
            Vec::new(),
        )
        .await;
    wait_for_sent(&ch1.sent, "watching \"long job\"").await;

    // A second bridge incarnation (fresh connection) restores the
    // registry and re-subscribes on its own connection.
    let ch2 = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let client2 = damon_core::client::DamonClient::connect(&url, None)
        .await
        .unwrap();
    let bridge2 = Bridge::new(ch2.clone(), client2);
    bridge2.client().hello().await.unwrap();
    bridge2.restore_watches().await;
    bridge2.spawn_event_router().await;

    client
        .turn_start_blocks(&desk, vec![json!({"type": "text", "text": "go"})])
        .await
        .unwrap();
    wait_for_sent(&ch2.sent, "finished a turn").await;
}

// --- Media + chunking ------------------------------------------------------------

/// Channels without upload support still say something: the default
/// send_media degrades to a text note (consumer-observable through the
/// same send path every message takes).
#[tokio::test]
async fn send_media_default_falls_back_to_a_text_note() {
    let ch = Arc::new(MockChannel {
        sent: Mutex::new(vec![]),
    });
    let media = |name: &str| MediaOut {
        data: vec![1, 2, 3],
        mime: "image/png".into(),
        filename: name.into(),
    };
    ch.send_media("chat", None, None, &[media("a.png"), media("b.png")])
        .await
        .unwrap();
    ch.send_media("chat", None, Some("chart"), &[media("c.png")])
        .await
        .unwrap();
    let sent = ch.sent.lock().await;
    assert_eq!(sent[0], ("chat".into(), "[2 media attachment(s)]".into()));
    assert_eq!(
        sent[1],
        ("chat".into(), "chart\n[1 media attachment(s)]".into())
    );
}

/// A reply longer than the flush threshold arrives as multiple chunks,
/// each within the cap, with code fences kept intact — the chunks
/// reassemble (stripping re-opened fence markers) into the full reply.
#[tokio::test]
async fn long_reply_is_flushed_in_fence_safe_chunks() {
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

    // The mock echoes the prompt, so the reply is a >threshold fenced
    // block. Leading newline keeps the fence on its own line after the
    // "echo: " prefix.
    let fenced = format!("\n```rust\n{}\n```", "let x = 1;\n".repeat(600));
    bridge
        .handle_message("chat".into(), None, None, fenced.clone(), Vec::new())
        .await;
    let full = format!("echo: {fenced}");
    let fences_of = |c: &str| {
        c.lines()
            .filter(|l| l.trim_start_matches(' ').starts_with("```"))
            .count()
    };
    // A chunk that ends mid-fence is followed by one whose first line
    // is the re-opened "```info" marker — strip those heads to rebuild.
    let reassemble = |chunks: &[String]| {
        let mut joined = String::new();
        for (i, c) in chunks.iter().enumerate() {
            if i > 0 && fences_of(&chunks[i - 1]) % 2 == 1 {
                joined.push_str(c.split_once('\n').unwrap().1);
            } else {
                joined.push_str(c);
            }
        }
        joined
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let chunks = loop {
        let chunks: Vec<String> = ch
            .sent
            .lock()
            .await
            .iter()
            .filter(|(c, _)| c == "chat")
            .map(|(_, t)| t.clone())
            .collect();
        if chunks.len() >= 3 && reassemble(&chunks) == full {
            break chunks;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "chunks never reassembled: {chunks:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert!(chunks.len() >= 3, "expected multiple chunks: {chunks:?}");
    for c in &chunks {
        assert!(c.len() <= 3500, "oversized chunk: {}", c.len());
    }
    assert_eq!(reassemble(&chunks), full);
}
