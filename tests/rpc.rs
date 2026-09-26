//! Wire-protocol v2 surface tests: the connect-time hello push and
//! method schema, busy-turn cancel/delete, the permission round-trip,
//! WS ticket auth (issue → single-use consume), session.list /
//! session.messages pagination, resume, fork, idle reaping, and config
//! hot reload.

mod common;

use std::sync::Arc;

use damon_core::api::{self, AppState};
use damon_core::config::SharedConfig;
use damon_core::store::Store;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite;

/// The concrete stream `connect_async` hands back on a plain ws:// URL.
type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn test_config(auth_token: Option<&str>) -> SharedConfig {
    Arc::new(parking_lot::RwLock::new(common::mock_config(auth_token)))
}

/// Build AppState with the in-process mock backend injected, then serve
/// the real router on a loopback socket. `into_make_service_with_connect_info`
/// is required — ws_handler reads the peer IP for auth/rate-limit
/// decisions.
async fn serve(config: SharedConfig) -> (Arc<AppState>, Store, String) {
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(config, store.clone()).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let app = api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (state, store, addr.to_string())
}

async fn read_json(ws: &mut Ws) -> Value {
    loop {
        // Bound each frame wait: a wedged daemon must fail the test,
        // not hang the suite.
        let frame = tokio::time::timeout(std::time::Duration::from_secs(60), ws.next())
            .await
            .expect("timed out waiting for a daemon frame")
            .unwrap()
            .unwrap();
        match frame {
            tungstenite::Message::Text(t) => {
                return serde_json::from_str(&t).unwrap();
            }
            _ => continue,
        }
    }
}

async fn rpc_send(ws: &mut Ws, msg: Value) {
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

/// Connect and consume the connect-time `hello` push — v2 greets every
/// socket before the first request, so tests must drain it or it would
/// be mistaken for a reply.
async fn ws_connect(url: &str) -> (Ws, Value) {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = read_json(&mut ws).await;
    assert!(
        hello["hello"].is_object(),
        "first frame must be the hello push: {hello}"
    );
    (ws, hello)
}

/// Read frames until the response carrying `id` arrives — a turn
/// streams `session.event` pushes ahead of its response, and those
/// must not be mistaken for the reply.
async fn read_reply(ws: &mut Ws, id: u64) -> Value {
    loop {
        let v = read_json(ws).await;
        if v.get("id").and_then(|i| i.as_u64()) == Some(id) {
            return v;
        }
    }
}

/// A turn parked on a hung backend unwinds on turn.cancel — the
/// turn.start request resolves with stopReason "canceled", the busy
/// flag clears, and session.delete then removes the row. A hung
/// backend must not wedge the session permanently.
#[tokio::test]
async fn cancel_hung_turn_unblocks_session_delete() {
    let (state, store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // "hang" parks the mock turn until it is interrupted.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hang"}}),
    )
    .await;

    // Wait until the turn is registered as busy before cancelling.
    // `turn_started` pushes are expected — only a reply for id=2 means
    // the turn ended early.
    let mut live = false;
    for _ in 0..50 {
        if state.sessions.busy_ids().await.contains(&session_id) {
            live = true;
            break;
        }
        if let Ok(v) =
            tokio::time::timeout(std::time::Duration::from_millis(50), read_json(&mut ws)).await
        {
            assert!(v["id"] != 2, "turn ended early: {v}");
        }
    }
    assert!(live, "turn never registered as busy");

    // turn.cancel can land before the mock parks its interrupt sender;
    // retry until the turn actually unwinds. Requests dispatch
    // concurrently, so the cancel and turn replies can arrive in either
    // order.
    let (turn_reply, cancel_reply) =
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut next_id = 3u64;
            let (mut turn, mut cancel) = (None, None);
            loop {
                rpc_send(
                    &mut ws,
                    json!({"id":next_id,"method":"turn.cancel","params":{"sessionId":session_id}}),
                )
                .await;
                next_id += 1;
                match tokio::time::timeout(
                    std::time::Duration::from_millis(200),
                    read_json(&mut ws),
                )
                .await
                {
                    Ok(v) => match v["id"].as_u64() {
                        Some(2) => turn = Some(v),
                        Some(i) if i >= 3 => cancel = Some(v),
                        _ => {}
                    },
                    // No frame within the window — cancel again.
                    Err(_) => continue,
                }
                if let (Some(t), Some(c)) = (turn.clone(), cancel.clone()) {
                    break (t, c);
                }
            }
        })
        .await
        .expect("hung turn never unwound after turn.cancel");

    assert_eq!(
        turn_reply["result"]["stopReason"], "canceled",
        "reply: {turn_reply}"
    );
    assert_eq!(
        cancel_reply["result"]["cancelled"], true,
        "reply: {cancel_reply}"
    );

    // The busy flag cleared — the session is deletable again.
    for _ in 0..50 {
        if !state.sessions.busy_ids().await.contains(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        !state.sessions.busy_ids().await.contains(&session_id),
        "session still busy after the turn unwound"
    );

    rpc_send(
        &mut ws,
        json!({"id":90,"method":"session.delete","params":{"sessionId":session_id}}),
    )
    .await;
    let reply = read_reply(&mut ws, 90).await;
    assert_eq!(reply["result"]["deleted"], true, "reply: {reply}");
    // The row is gone — the session was actually deleted.
    assert!(
        store
            .list_sessions_paged(u32::MAX, 0)
            .await
            .unwrap()
            .iter()
            .all(|(id, ..)| id != &session_id)
    );
    assert!(state.sessions.get(&session_id).await.is_none());
}

/// A permission ask arrives as a `session.event` push
/// (`permission_requested`), `permission.respond` answers it, and the
/// turn completes. The connect-time hello advertises the configured
/// `permission_timeout_secs` — in v2 the client enforces the deny
/// deadline; the daemon only publishes it.
#[tokio::test]
async fn permission_ask_round_trip_and_timeout_advertised() {
    let mut cfg = common::mock_config(None);
    cfg.permission_timeout_secs = Some(7);
    let (_state, _store, addr) = serve(Arc::new(parking_lot::RwLock::new(cfg))).await;

    let (mut ws, hello) = ws_connect(&format!("ws://{addr}/ws")).await;
    assert_eq!(
        hello["hello"]["permissionTimeoutSecs"], 7,
        "hello push must advertise the configured budget: {hello}"
    );

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // "perm" makes the mock raise a permission ask and wait for the
    // answer.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"perm"}}),
    )
    .await;

    // The ask streams in as a push — extract its requestId.
    let request_id = loop {
        let v = tokio::time::timeout(std::time::Duration::from_secs(15), read_json(&mut ws))
            .await
            .expect("permission ask never arrived");
        if v["event"] == "session.event" && v["data"]["type"] == "permission_requested" {
            assert_eq!(v["sessionId"], session_id, "ask for wrong session: {v}");
            break v["data"]["id"].as_str().unwrap().to_string();
        }
    };

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"permission.respond",
               "params":{"sessionId":session_id,"requestId":request_id,
                         "response":{"behavior":"deny","interrupt":false}}}),
    )
    .await;

    // Both replies land in either order; the mock emits "denied" once
    // the answer arrives.
    let mut text = String::new();
    let (mut turn, mut respond) = (None, None);
    while turn.is_none() || respond.is_none() {
        let v = tokio::time::timeout(std::time::Duration::from_secs(15), read_json(&mut ws))
            .await
            .expect("turn never finished after permission.respond");
        let data = &v["data"];
        if v["event"] == "session.event" && data["kind"] == "assistant_message" {
            text.push_str(data["text"].as_str().unwrap_or(""));
        }
        match v["id"].as_u64() {
            Some(2) => turn = Some(v),
            Some(3) => respond = Some(v),
            _ => {}
        }
    }
    let turn = turn.unwrap();
    assert_eq!(turn["result"]["stopReason"], "completed", "reply: {turn}");
    assert!(
        respond.as_ref().unwrap().get("error").is_none(),
        "permission.respond failed: {respond:?}"
    );
    assert_eq!(text, "denied", "deny never reached the backend");
}

/// POST /v1/ws_ticket issues a one-shot ticket that authenticates /ws.
#[tokio::test]
async fn ws_ticket_authenticates() {
    let (_state, _store, addr) = serve(test_config(Some("secret"))).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ws_ticket"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ticket = resp.json::<Value>().await.unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();

    let (mut ws, hello) = ws_connect(&format!("ws://{addr}/ws?ticket={ticket}")).await;
    assert_eq!(hello["hello"]["protocol"], 2, "hello: {hello}");
    assert_eq!(hello["hello"]["daemon"], "damond", "hello: {hello}");

    rpc_send(&mut ws, json!({"id":1,"method":"hello","params":{}})).await;
    let reply = read_reply(&mut ws, 1).await;
    assert_eq!(reply["result"]["protocol"], 2, "reply: {reply}");
    assert_eq!(reply["result"]["daemon"], "damond", "reply: {reply}");
    // The schema artifact is part of the contract — every dispatch arm
    // must be listed.
    let methods = reply["result"]["methods"].as_array().unwrap();
    let names: Vec<&str> = methods
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    for expected in [
        "hello",
        "backend.list",
        "session.create",
        "session.resume",
        "session.list",
        "session.messages",
        "session.import",
        "session.delete",
        "session.rename",
        "session.fork",
        "session.usage",
        "session.search",
        "turn.start",
        "turn.steer",
        "turn.cancel",
        "permission.respond",
        "session.set_model",
        "session.set_mode",
        "catalog.models",
    ] {
        assert!(
            names.contains(&expected),
            "missing method in schema: {expected}"
        );
    }
    // The injected mock backend is advertised.
    let backends = reply["result"]["backends"].as_array().unwrap();
    assert!(
        backends.iter().any(|b| b == "mock"),
        "mock missing from hello backends: {backends:?}"
    );
}

/// A consumed ticket must not authenticate a second connection.
#[tokio::test]
async fn ws_ticket_is_single_use() {
    let (_state, _store, addr) = serve(test_config(Some("secret"))).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ws_ticket"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    let ticket = resp.json::<Value>().await.unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();

    // First use: handshake succeeds (the ticket is consumed here).
    let (_ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
        .await
        .unwrap();

    // Second use of the same ticket: the upgrade is rejected with 401.
    let err = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
        .await
        .unwrap_err();
    match err {
        tungstenite::Error::Http(resp) => {
            assert_eq!(resp.status(), tungstenite::http::StatusCode::UNAUTHORIZED);
        }
        other => panic!("expected HTTP 401 rejection, got {other:?}"),
    }
}

/// DamonClient::connect with a token must exchange it for a ticket and
/// authenticate — the token itself never appears in the WS URL.
#[tokio::test]
async fn client_connect_uses_ticket_auth() {
    let (_state, _store, addr) = serve(test_config(Some("secret"))).await;

    let client =
        damon_core::client::DamonClient::connect(&format!("ws://{addr}/ws"), Some("secret"))
            .await
            .expect("connect with token should succeed via ticket");
    let init = client.hello().await.unwrap();
    assert_eq!(init["daemon"], "damond");
    assert_eq!(init["protocol"], 2);

    // A wrong token must fail at the ticket exchange, before any WS dial.
    let err =
        damon_core::client::DamonClient::connect(&format!("ws://{addr}/ws"), Some("wrong")).await;
    assert!(err.is_err(), "wrong token must not connect");
}

/// backend.list reports the injected mock with its capabilities — the
/// v2 replacement for the old agent listing and status methods.
#[tokio::test]
async fn backend_list_reports_mock() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(&mut ws, json!({"id":1,"method":"backend.list","params":{}})).await;
    let reply = read_reply(&mut ws, 1).await;
    let backends = reply["result"]["backends"].as_array().unwrap();
    let mock = backends
        .iter()
        .find(|b| b["id"] == "mock")
        .expect("mock missing from backend.list");
    assert_eq!(mock["available"], true, "entry: {mock}");
    assert_eq!(
        mock["capabilities"]["session_persistence"], true,
        "entry: {mock}"
    );
}

/// session.list with limit/offset pages instead of returning
/// everything; rows carry {sessionId, createdAt, backend, title}.
#[tokio::test]
async fn session_list_paginates() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("mock"))
        .await
        .unwrap();
    store
        .create_session("s2", "/b", Some("mock"))
        .await
        .unwrap();

    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.list","params":{"limit":1}}),
    )
    .await;
    let page1 = read_reply(&mut ws, 1).await;
    let sessions = page1["result"]["sessions"].as_array().unwrap();
    assert_eq!(
        sessions.len(),
        1,
        "limit=1 must return one session: {page1}"
    );
    assert!(sessions[0]["sessionId"].is_string(), "row: {}", sessions[0]);
    assert!(sessions[0]["createdAt"].is_string(), "row: {}", sessions[0]);
    assert!(sessions[0]["backend"].is_string(), "row: {}", sessions[0]);

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.list","params":{"limit":1,"offset":1}}),
    )
    .await;
    let page2 = read_reply(&mut ws, 2).await;
    let sessions2 = page2["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions2.len(), 1);
    assert_ne!(
        sessions[0]["sessionId"], sessions2[0]["sessionId"],
        "offset=1 must return the other session"
    );

    // No paging params → the full list.
    rpc_send(&mut ws, json!({"id":3,"method":"session.list","params":{}})).await;
    let all = read_reply(&mut ws, 3).await;
    assert_eq!(all["result"]["sessions"].as_array().unwrap().len(), 2);
}

/// session.messages honors limit/offset and returns StoredMessage rows
/// ({id, session_id, role, data}).
#[tokio::test]
async fn session_messages_paginates() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("mock"))
        .await
        .unwrap();
    for i in 0..3 {
        store
            .append("s1", "user", &json!({"content": format!("m{i}")}))
            .await
            .unwrap();
    }

    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.messages",
               "params":{"sessionId":"s1","limit":1,"offset":1}}),
    )
    .await;
    let page = read_reply(&mut ws, 1).await;
    let msgs = page["result"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1, "unexpected page: {page}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["data"]["content"], "m1");

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.messages","params":{"sessionId":"s1"}}),
    )
    .await;
    let all = read_reply(&mut ws, 2).await;
    assert_eq!(all["result"]["messages"].as_array().unwrap().len(), 3);
}

/// session.messages returns the persisted history in stored order —
/// v2 has no push-based replay; clients render these rows directly.
/// An unknown session id returns an empty page, not an error.
#[tokio::test]
async fn session_messages_returns_persisted_history() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("mock"))
        .await
        .unwrap();
    store
        .append("s1", "user", &json!({"content": "hi there"}))
        .await
        .unwrap();
    store
        .append("s1", "assistant", &json!({"content": "working on it"}))
        .await
        .unwrap();
    store
        .append(
            "s1",
            "tool",
            &json!({"name": "fs.read", "status": "completed"}),
        )
        .await
        .unwrap();
    store
        .append("s1", "assistant", &json!({"content": "done"}))
        .await
        .unwrap();

    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.messages","params":{"sessionId":"s1"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    let msgs = reply["result"]["messages"].as_array().unwrap();
    let roles: Vec<&str> = msgs.iter().filter_map(|m| m["role"].as_str()).collect();
    assert_eq!(
        roles,
        vec!["user", "assistant", "tool", "assistant"],
        "stored order wrong: {msgs:?}"
    );
    assert_eq!(msgs[0]["data"]["content"], "hi there");
    assert_eq!(msgs[1]["data"]["content"], "working on it");
    assert_eq!(msgs[2]["data"]["name"], "fs.read");
    assert_eq!(msgs[3]["data"]["content"], "done");

    // Unknown session → empty page, no replay, no error.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.messages","params":{"sessionId":"nope"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert!(reply.get("error").is_none(), "unexpected error: {reply}");
    assert_eq!(reply["result"]["messages"].as_array().unwrap().len(), 0);
}

/// v2 has no session.export — the CLI renders Markdown/JSON client-side
/// from session.messages rows. This proves the wire surface carries
/// everything an exporter needs: both turns' prompts and echo replies,
/// in order, with roles.
#[tokio::test]
async fn session_messages_supports_client_export() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let sid = new["result"]["sessionId"].as_str().unwrap().to_string();

    // Two turns through the mock backend (echo replies).
    for (id, text) in [(2u64, "first turn alpha"), (4u64, "second turn beta")] {
        rpc_send(
            &mut ws,
            json!({"id":id,"method":"turn.start",
                   "params":{"sessionId":sid,"prompt":text}}),
        )
        .await;
        let reply =
            tokio::time::timeout(std::time::Duration::from_secs(30), read_reply(&mut ws, id))
                .await
                .expect("turn timed out");
        assert!(reply.get("error").is_none(), "turn failed: {reply}");
    }

    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.messages","params":{"sessionId":sid}}),
    )
    .await;
    let reply = read_reply(&mut ws, 6).await;
    let msgs = reply["result"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 4, "messages: {msgs:?}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["data"]["content"], "first turn alpha");
    assert_eq!(msgs[1]["role"], "assistant");
    assert_eq!(msgs[1]["data"]["content"], "echo: first turn alpha");
    assert_eq!(msgs[2]["data"]["content"], "second turn beta");
    assert_eq!(msgs[3]["data"]["content"], "echo: second turn beta");
}

/// turn.start streams `session.event` pushes (turn_started → timeline
/// assistant_message → turn_completed) and resolves at turn end with
/// the turn id, stop reason, and usage. Prompt accepts a plain string
/// or content blocks.
#[tokio::test]
async fn turn_start_streams_events_then_resolves() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let sid = new["result"]["sessionId"].as_str().unwrap().to_string();

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hello"}}),
    )
    .await;

    let mut types = Vec::new();
    let mut echo = String::new();
    let reply = loop {
        let v = tokio::time::timeout(std::time::Duration::from_secs(30), read_json(&mut ws))
            .await
            .expect("turn never resolved");
        if v["id"] == 2 {
            break v;
        }
        assert_eq!(v["event"], "session.event", "unexpected frame: {v}");
        assert_eq!(v["sessionId"], sid, "event for wrong session: {v}");
        types.push(v["data"]["type"].as_str().unwrap_or("?").to_string());
        if v["data"]["kind"] == "assistant_message" {
            echo.push_str(v["data"]["text"].as_str().unwrap_or(""));
        }
    };
    assert_eq!(
        types,
        vec!["turn_started", "timeline", "turn_completed"],
        "event stream wrong: {types:?}"
    );
    assert_eq!(echo, "echo: hello");
    assert!(reply["result"]["turnId"].is_string(), "reply: {reply}");
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");
    assert_eq!(
        reply["result"]["usage"]["input_tokens"], 10,
        "reply: {reply}"
    );

    // Block-form prompt is accepted too.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"turn.start",
               "params":{"sessionId":sid,
                         "prompt":[{"type":"text","text":"blocks"}]}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");
}

/// agent_idle_secs reaps an idle session: past the deadline the live
/// mapping is gone, and session.resume reattaches through the persisted
/// backend handle — the next turn works without a fresh session.create.
#[tokio::test]
async fn idle_session_is_reaped_then_resumes() {
    let mut cfg = common::mock_config(None);
    cfg.agent_idle_secs = 1;
    let (state, _store, addr) = serve(Arc::new(parking_lot::RwLock::new(cfg))).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // A turn persists the backend's resume handle — without one there
    // is nothing to reattach to.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hi"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");

    // The sweep reaps ~1s past the last touch; poll for the evidence —
    // the live mapping disappearing — instead of sleeping blind.
    let mut reaped = false;
    for _ in 0..100 {
        if state.sessions.get(&session_id).await.is_none() {
            reaped = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(reaped, "idle session was never reaped");

    // Resume reattaches through the persisted handle…
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.resume","params":{"sessionId":session_id}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert!(reply.get("error").is_none(), "resume failed: {reply}");
    assert_eq!(reply["result"]["sessionId"], session_id);
    assert_eq!(reply["result"]["backend"], "mock", "reply: {reply}");

    // …and a turn on the reattached session succeeds.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"again"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");
}

/// Pick a free port, drop the listener, and let damond bind it
/// (mirrors tests/boot.rs).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Config hot reload reaches the RUNNING daemon: editing the config
/// file changes what new connections are told — here the advertised
/// permissionTimeoutSecs in the hello push — without a restart
/// (watcher → SharedConfig swap).
#[tokio::test]
async fn config_hot_reload_reaches_running_daemon() {
    let dir = std::env::temp_dir().join(format!("damon-hotreload-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let cfg_path = dir.join("config.toml");
    // Literal string (single quotes): a Windows temp path is full of
    // backslashes, which TOML basic strings would read as escapes.
    std::fs::write(
        &cfg_path,
        format!(
            "bind = \"127.0.0.1:{port}\"\ndata_dir = '{}'\npermission_timeout_secs = 300\n",
            dir.display()
        ),
    )
    .unwrap();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_damond"))
        .arg("--config")
        .arg(&cfg_path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn damond");

    let url = format!("ws://127.0.0.1:{port}/ws");
    // Connect once the listener is up; the hello push advertises the
    // configured timeout.
    let mut baseline = None;
    for _ in 0..150 {
        if let Ok((mut wsx, _)) = tokio_tungstenite::connect_async(&url).await {
            let hello = read_json(&mut wsx).await;
            baseline = hello["hello"]["permissionTimeoutSecs"].as_u64();
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(baseline, Some(300), "baseline hello push");

    // Edit the config on disk and poll for the reload to land — each
    // new connection's hello reflects the live config.
    std::fs::write(
        &cfg_path,
        format!(
            "bind = \"127.0.0.1:{port}\"\ndata_dir = '{}'\npermission_timeout_secs = 42\n",
            dir.display()
        ),
    )
    .unwrap();

    let mut landed = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if let Ok((mut wsx, _)) = tokio_tungstenite::connect_async(&url).await {
            let hello = read_json(&mut wsx).await;
            if hello["hello"]["permissionTimeoutSecs"].as_u64() == Some(42) {
                landed = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(landed, "hot reload never reached the hello push");
    child.kill().await.unwrap();
}

/// session.fork copies the session row and its messages into a new
/// session; the fork's backend session is fresh (the backend cannot
/// branch its own context). The original session is untouched.
#[tokio::test]
async fn session_fork_copies_history() {
    let (_state, store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // Seed a message so the fork has something to copy.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hello"}}),
    )
    .await;
    let _ = read_reply(&mut ws, 2).await;

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.fork","params":{"sessionId":session_id}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert!(reply.get("error").is_none(), "fork failed: {reply}");
    let fork_id = reply["result"]["sessionId"].as_str().unwrap().to_string();
    assert_ne!(fork_id, session_id, "fork returned the same session id");

    // The fork exists and has the same message count as the original.
    let orig_msgs = store
        .messages_paged(&session_id, u32::MAX, 0)
        .await
        .unwrap();
    let fork_msgs = store.messages_paged(&fork_id, u32::MAX, 0).await.unwrap();
    assert_eq!(
        orig_msgs.len(),
        fork_msgs.len(),
        "fork did not copy all messages"
    );
    assert!(fork_msgs.len() >= 2, "fork has no messages");

    // The original session is still there.
    assert!(
        store
            .list_sessions_paged(u32::MAX, 0)
            .await
            .unwrap()
            .iter()
            .any(|(id, ..)| id == &session_id)
    );
}

/// session.resume after the daemon-side mapping is lost (daemon
/// restart): the persisted backend handle reattaches a live session,
/// resuming an already-live session is a no-op, and the reattached
/// session actually works.
#[tokio::test]
async fn session_resume_reattaches_backend_session() {
    let (state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new = read_reply(&mut ws, 1).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // A turn persists the backend's resume handle — without one there
    // is nothing to reattach to.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hi"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");

    // Drop the live mapping — the restart case resume exists for.
    state.sessions.detach(&session_id).await;

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.resume","params":{"sessionId":session_id}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert!(reply.get("error").is_none(), "resume failed: {reply}");
    assert_eq!(reply["result"]["sessionId"], session_id);
    assert_eq!(reply["result"]["backend"], "mock", "reply: {reply}");

    // The mapping is warm again — a second resume is a no-op, not a
    // second backend round-trip.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.resume","params":{"sessionId":session_id}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    assert!(
        reply.get("error").is_none(),
        "second resume failed: {reply}"
    );
    assert_eq!(reply["result"]["sessionId"], session_id);

    // The reattached session actually works.
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hi"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 5).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");
}

/// session.resume with a `handle` imports a native session: it mints a
/// Damon row bound to the handle, resumes the backend, and a second
/// resume of the same handle returns the same session — no duplicates.
#[tokio::test]
async fn session_resume_by_handle_imports_native_session() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    let handle = json!({"provider": "mock", "native_handle": "native-42"});
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.resume",
               "params":{"handle":handle,"title":"imported","cwd":"/tmp"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    assert!(
        reply.get("error").is_none(),
        "handle resume failed: {reply}"
    );
    let session_id = reply["result"]["sessionId"].as_str().unwrap().to_string();
    assert_eq!(reply["result"]["backend"], "mock", "reply: {reply}");

    // The imported session is listed with its title.
    rpc_send(&mut ws, json!({"id":2,"method":"session.list","params":{}})).await;
    let reply = read_reply(&mut ws, 2).await;
    let sessions = reply["result"]["sessions"].as_array().unwrap();
    let row = sessions
        .iter()
        .find(|s| s["sessionId"] == session_id)
        .expect("imported session missing from list");
    assert_eq!(row["title"], "imported", "row: {row}");

    // Re-resuming the same handle dedups to the same Damon session.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.resume","params":{"handle":handle}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert_eq!(reply["result"]["sessionId"], session_id, "reply: {reply}");

    // The imported session drives turns like a native one.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hi"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");
}

/// session.resume on a session with no persisted backend handle fails
/// honestly — there is nothing to reattach to, and v2 does not mint a
/// silent fresh session.
#[tokio::test]
async fn session_resume_without_handle_errors() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("mock"))
        .await
        .unwrap();

    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    // A stored row with no backend session handle → honest error.
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.resume","params":{"sessionId":"s1"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    assert!(reply["error"].is_object(), "expected an error: {reply}");
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("no persisted backend session"),
        "error must name the missing handle: {reply}"
    );

    // An unknown session id errors the same way.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.resume","params":{"sessionId":"ghost"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert!(reply["error"].is_object(), "expected an error: {reply}");
}

/// session.resume naming a backend that isn't registered fails
/// honestly instead of falling back to another provider.
#[tokio::test]
async fn session_resume_unknown_backend_errors() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("ghost"))
        .await
        .unwrap();
    store
        .set_agent_session("s1", "ghost", "native-1")
        .await
        .unwrap();

    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.resume","params":{"sessionId":"s1"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    assert!(reply["error"].is_object(), "expected an error: {reply}");
    assert!(
        reply["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("not available"),
        "error must name the missing backend: {reply}"
    );
}
