//! Wire-protocol v2 surface tests: the connect-time hello push and
//! method schema, busy-turn cancel/delete, the permission round-trip,
//! WS ticket auth (issue → single-use consume), session.list /
//! session.messages pagination, list filters and tags, export, search
//! time bounds, the daily usage rollup, resume, fork, idle reaping,
//! and config hot reload.

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
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap()
            .iter()
            .all(|s| s.id != session_id)
    );
    assert!(state.sessions.get(&session_id).await.is_none());
}

/// A detached turn outlives the connection that started it — the whole
/// point of `detach: true`. The start response returns immediately with
/// `{turnId, detached: true}`, dropping the socket does not cancel the
/// hung turn, and a second connection can resume the session and cancel
/// it. The user prompt is persisted either way, so a late subscriber
/// reads the transcript from the store.
#[tokio::test]
async fn detached_turn_survives_disconnect() {
    let (state, store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let session_id = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hang","detach":true}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["detached"], true, "reply: {reply}");
    assert!(
        reply["result"]["turnId"].is_string(),
        "detached start must still mint a turnId: {reply}"
    );

    // Wait for the turn to register as busy before dropping the socket.
    for _ in 0..50 {
        if state.sessions.busy_ids().await.contains(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(state.sessions.busy_ids().await.contains(&session_id));

    // Disconnect: nothing may cancel the detached turn.
    drop(ws);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(
        state.sessions.busy_ids().await.contains(&session_id),
        "detached turn must survive the starting connection's disconnect"
    );

    // A second connection picks the turn up and cancels it.
    let (mut ws2, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    rpc_send(
        &mut ws2,
        json!({"id":1,"method":"session.resume","params":{"sessionId":session_id}}),
    )
    .await;
    read_reply(&mut ws2, 1).await;
    // turn.cancel may land before the mock parks its interrupt sender —
    // retry until the turn actually unwinds.
    let mut cancelled = false;
    for attempt in 0..100u32 {
        rpc_send(
            &mut ws2,
            json!({"id":10 + attempt,"method":"turn.cancel","params":{"sessionId":session_id}}),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if !state.sessions.busy_ids().await.contains(&session_id) {
            cancelled = true;
            break;
        }
    }
    assert!(cancelled, "detached turn never unwound after turn.cancel");

    let rows = store.messages_paged(&session_id, 50, 0).await.unwrap();
    assert!(
        rows.iter().any(|m| m.role == "user"),
        "user row must be persisted: {rows:?}"
    );
}

/// A detached turn that finishes while its client is gone lands in the
/// store and frees the session — a reconnecting client reads the echo
/// from history instead of a lost RPC response.
#[tokio::test]
async fn detached_turn_completes_while_client_gone() {
    let (state, store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let session_id = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hello there","detach":true}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["detached"], true, "reply: {reply}");

    drop(ws);
    for _ in 0..100 {
        if !state.sessions.busy_ids().await.contains(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        !state.sessions.busy_ids().await.contains(&session_id),
        "detached turn never finished"
    );
    let rows = store.messages_paged(&session_id, 50, 0).await.unwrap();
    assert!(
        rows.iter()
            .any(|m| m.role == "assistant" && m.data["content"] == "echo: hello there"),
        "echo must be persisted for the reconnecting client: {rows:?}"
    );
}

/// The default is unchanged: a blocking turn belongs to its connection,
/// and the disconnect of the connection that started it cancels the
/// work — `detach` is an explicit opt-out, not a silent default flip.
#[tokio::test]
async fn disconnect_cancels_blocking_turns() {
    let (state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let session_id = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":session_id,"prompt":"hang"}}),
    )
    .await;
    for _ in 0..50 {
        if state.sessions.busy_ids().await.contains(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(state.sessions.busy_ids().await.contains(&session_id));

    drop(ws);
    for _ in 0..100 {
        if !state.sessions.busy_ids().await.contains(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        !state.sessions.busy_ids().await.contains(&session_id),
        "blocking turn must be cancelled when its connection drops"
    );
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
        "session.export",
        "session.import",
        "session.delete",
        "session.close",
        "session.restart",
        "session.status",
        "session.set_pinned",
        "session.set_archived",
        "session.fork",
        "session.set_tags",
        "session.usage",
        "session.search",
        "turn.start",
        "turn.steer",
        "turn.cancel",
        "permission.respond",
        "session.set_model",
        "session.set_mode",
        "catalog.models",
        "logs.tail",
        "logs.follow",
        "file.read",
        "file.write",
        "file.list",
        "channel.get_state",
        "channel.set_state",
        "channel.delete_state",
        "project.create",
        "project.list",
        "project.get",
        "project.set_defaults",
        "project.delete",
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
/// ({id, session_id, role, ts, data}).
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
/// the pull-based view complements the replay push (new connections
/// get store rows as replay-tagged events; this method pages them
/// explicitly). An unknown session id returns an empty page, not an
/// error.
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

/// A second turn.start on a session with a live turn is rejected with
/// the typed TURN_IN_PROGRESS code instead of running two collectors
/// on the same broadcast (which double-persisted every event).
#[tokio::test]
async fn concurrent_turn_start_is_rejected() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // Both prompts park (the mock hangs on "hang") — whichever
    // dispatch task wins the busy claim runs; the loser fails fast
    // with the typed code. Task ordering is nondeterministic, so the
    // test accepts either id being the rejected one.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hang"}}),
    )
    .await;
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hang"}}),
    )
    .await;
    let (rejected, rejected_id) = loop {
        let v = tokio::time::timeout(std::time::Duration::from_secs(15), read_json(&mut ws))
            .await
            .expect("no reply for either turn.start");
        let id = v.get("id").and_then(|i| i.as_u64()).unwrap_or(0);
        if id == 2 || id == 3 {
            assert!(v["error"].is_object(), "expected rejection: {v}");
            assert_eq!(v["error"]["code"], -32006, "{v}");
            assert!(
                v["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("already running"),
                "{v}"
            );
            break (v, id);
        }
    };
    let _ = rejected;
    let hung_id = if rejected_id == 2 { 3 } else { 2 };

    // Cancel drains the hung turn; the busy claim must be released. The
    // cancel can land before the mock parks its interrupt sender
    // (requests dispatch concurrently), so retry the cancel until the
    // turn actually unwinds — same dance the dedicated cancel test
    // performs. Non-matching frames (cancel acks, turn events) are
    // simply read past.
    let mut next_id = 4u64;
    let done_hung = loop {
        rpc_send(
            &mut ws,
            json!({"id":next_id,"method":"turn.cancel","params":{"sessionId":sid}}),
        )
        .await;
        next_id += 1;
        let v = tokio::time::timeout(std::time::Duration::from_millis(500), read_json(&mut ws))
            .await
            .expect("hung turn never unwound after turn.cancel");
        if v.get("id").and_then(|i| i.as_u64()) == Some(hung_id) {
            break v;
        }
    };
    assert_eq!(done_hung["result"]["stopReason"], "canceled", "{done_hung}");
    let next_prompt_id = next_id;

    // The busy claim is free again — a follow-up turn completes.
    rpc_send(
        &mut ws,
        json!({"id":next_prompt_id,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hello"}}),
    )
    .await;
    let done = read_reply(&mut ws, next_prompt_id).await;
    assert_eq!(done["result"]["stopReason"], "completed", "{done}");
}

/// A failed turn leaves a durable error row: session.messages carries
/// the failure reason, and a fresh connection's replay re-raises it as
/// a timeline `error` item — a prompt with no answer and no marker is
/// the pre-fix behavior.
#[tokio::test]
async fn failed_turn_persists_error_marker() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"please fail"}}),
    )
    .await;
    let failed = read_reply(&mut ws, 2).await;
    assert!(failed["error"].is_object(), "turn must fail: {failed}");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.messages","params":{"sessionId":sid}}),
    )
    .await;
    let msgs = read_reply(&mut ws, 3).await["result"]["messages"].clone();
    let error_rows: Vec<&Value> = msgs
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "error")
        .collect();
    assert_eq!(error_rows.len(), 1, "one error row: {msgs}");
    assert_eq!(error_rows[0]["data"]["content"], "mock failure");

    // A late subscriber sees the marker in its replay, tagged as such.
    let (mut ws2, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    rpc_send(
        &mut ws2,
        json!({"id":1,"method":"session.resume","params":{"sessionId":sid}}),
    )
    .await;
    let mut saw_error = false;
    let resumed = loop {
        let v = tokio::time::timeout(std::time::Duration::from_secs(15), read_json(&mut ws2))
            .await
            .expect("resume never resolved");
        if v.get("id").and_then(|i| i.as_u64()) == Some(1) {
            break v;
        }
        if v["event"] == "session.event"
            && v["data"]["type"] == "timeline"
            && v["data"]["kind"] == "error"
        {
            assert_eq!(v["replay"], true, "replayed marker must be tagged: {v}");
            assert_eq!(v["data"]["message"], "mock failure", "{v}");
            saw_error = true;
        }
    };
    assert!(saw_error, "error marker missing from replay");
    assert!(
        resumed["result"]["replayed"].as_u64().unwrap() >= 2,
        "{resumed}"
    );
}

/// Backend Compaction timeline items persist as `compaction` rows —
/// the context boundary is part of the searchable transcript, not a
/// live-only event.
#[tokio::test]
async fn compaction_event_is_persisted() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"compact now"}}),
    )
    .await;
    let done = read_reply(&mut ws, 2).await;
    assert_eq!(done["result"]["stopReason"], "completed", "{done}");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.messages","params":{"sessionId":sid}}),
    )
    .await;
    let msgs = read_reply(&mut ws, 3).await["result"]["messages"].clone();
    let compactions: Vec<&Value> = msgs
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "compaction")
        .collect();
    assert_eq!(compactions.len(), 1, "one compaction row: {msgs}");
    assert_eq!(compactions[0]["data"]["content"], "compacted 3 messages");
}

/// session.close kills the live backend process but keeps the row:
/// turn.start then fails with SESSION_NOT_LIVE, session.list still
/// shows the session, session.resume reattaches, and closing an
/// unknown id stays idempotent. This is the close the SESSION_LIMIT
/// error message always promised.
#[tokio::test]
async fn session_close_frees_slot_keeps_row() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    // One completed turn persists the backend resume handle — close
    // must keep the row resumable, and resume needs a handle to find.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hello"}}),
    )
    .await;
    let done = read_reply(&mut ws, 2).await;
    assert_eq!(done["result"]["stopReason"], "completed", "{done}");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.close","params":{"sessionId":sid}}),
    )
    .await;
    let closed = read_reply(&mut ws, 3).await;
    assert_eq!(closed["result"]["closed"], true, "{closed}");

    rpc_send(
        &mut ws,
        json!({"id":4,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hi"}}),
    )
    .await;
    let rejected = read_reply(&mut ws, 4).await;
    assert_eq!(rejected["error"]["code"], -32001, "{rejected}");

    rpc_send(&mut ws, json!({"id":5,"method":"session.list","params":{}})).await;
    let listed = read_reply(&mut ws, 5).await;
    assert!(
        listed["result"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["sessionId"] == sid),
        "row must survive close: {listed}"
    );

    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.resume","params":{"sessionId":sid}}),
    )
    .await;
    let resumed = read_reply(&mut ws, 6).await;
    assert_eq!(resumed["result"]["sessionId"], sid, "{resumed}");

    // Idempotent on an id that was never live.
    rpc_send(
        &mut ws,
        json!({"id":7,"method":"session.close",
               "params":{"sessionId":"no-such-session"}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 7).await["result"]["closed"], true);
}

/// allowed_dirs gates every spawn entry point: cwds under a listed
/// root pass, sibling-prefix and outside paths fail with a named
/// INVALID_PARAMS error. Component-wise semantics — `/root-ev` must
/// not sneak under `/root`.
#[tokio::test]
async fn allowed_dirs_gate_session_cwds() {
    let mut cfg = common::mock_config(None);
    cfg.allowed_dirs = vec!["/damon-allowed-root".into(), "/damon-other-root".into()];
    let (_state, _store, addr) = serve(Arc::new(parking_lot::RwLock::new(cfg))).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create",
               "params":{"cwd":"/damon-allowed-root/proj"}}),
    )
    .await;
    let ok = read_reply(&mut ws, 1).await;
    assert!(ok["result"]["sessionId"].is_string(), "{ok}");

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.create",
               "params":{"cwd":"/damon-allowed-root-sibling/proj"}}),
    )
    .await;
    let sibling = read_reply(&mut ws, 2).await;
    assert_eq!(sibling["error"]["code"], -32602, "{sibling}");
    assert!(
        sibling["error"]["message"]
            .as_str()
            .unwrap()
            .contains("allowed_dirs"),
        "{sibling}"
    );

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.create","params":{"cwd":"/etc"}}),
    )
    .await;
    let outside = read_reply(&mut ws, 3).await;
    assert_eq!(outside["error"]["code"], -32602, "{outside}");

    // import lists native sessions under a cwd — same gate.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.import",
               "params":{"backend":"mock","cwd":"/etc"}}),
    )
    .await;
    let import = read_reply(&mut ws, 4).await;
    assert_eq!(import["error"]["code"], -32602, "{import}");
}

/// session.status reports live backend sessions with busy/idle detail;
/// session.close removes one from the live set without touching rows.
#[tokio::test]
async fn session_status_reports_live_sessions() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let s1 = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let s2 = read_reply(&mut ws, 2).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.status","params":{}}),
    )
    .await;
    let status = read_reply(&mut ws, 3).await;
    let rows = status["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{status}");
    for r in rows {
        assert_eq!(r["backend"], "mock", "{status}");
        assert_eq!(r["busy"], false, "{status}");
        assert!(r["idleSecs"].is_u64(), "{status}");
    }
    let ids: Vec<&str> = rows
        .iter()
        .map(|r| r["sessionId"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&s1.as_str()) && ids.contains(&s2.as_str()),
        "{status}"
    );

    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.close","params":{"sessionId":s1}}),
    )
    .await;
    let _ = read_reply(&mut ws, 4).await;
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"session.status","params":{}}),
    )
    .await;
    let status = read_reply(&mut ws, 5).await;
    let rows = status["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "close must shrink the live set: {status}");
    assert_eq!(rows[0]["sessionId"], s2, "{status}");
}

/// session.restart kills the live backend and reattaches through the
/// persisted handle; mid-turn it refuses with TURN_IN_PROGRESS.
#[tokio::test]
async fn session_restart_reattaches_and_refuses_midturn() {
    let (state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    // One turn persists the resume handle.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hello"}}),
    )
    .await;
    let _ = read_reply(&mut ws, 2).await;

    // Mid-turn restart is refused — it would kill the turn's process.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hang"}}),
    )
    .await;
    let mut next_id = 4u64;
    // The hang turn replies only on cancel — wait for the busy flag,
    // same as the dedicated cancel test.
    let mut live = false;
    for _ in 0..100 {
        if state.sessions.busy_ids().await.contains(&sid) {
            live = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(live, "turn never registered as busy");
    rpc_send(
        &mut ws,
        json!({"id":next_id,"method":"session.restart","params":{"sessionId":sid}}),
    )
    .await;
    next_id += 1;
    let refused = read_reply(&mut ws, next_id - 1).await;
    assert_eq!(refused["error"]["code"], -32006, "{refused}");

    // Cancel the hung turn, then restart for real.
    let mut cancel_id = next_id;
    loop {
        rpc_send(
            &mut ws,
            json!({"id":cancel_id,"method":"turn.cancel","params":{"sessionId":sid}}),
        )
        .await;
        cancel_id += 1;
        let v = tokio::time::timeout(std::time::Duration::from_millis(500), read_json(&mut ws))
            .await
            .expect("hung turn never unwound");
        if v.get("id").and_then(|i| i.as_u64()) == Some(3) {
            break; // the hung turn's reply
        }
    }
    let restart_id = cancel_id;
    rpc_send(
        &mut ws,
        json!({"id":restart_id,"method":"session.restart","params":{"sessionId":sid}}),
    )
    .await;
    let restarted = read_reply(&mut ws, restart_id).await;
    assert_eq!(restarted["result"]["sessionId"], sid, "{restarted}");
    assert_eq!(restarted["result"]["backend"], "mock", "{restarted}");

    // The restarted session runs a turn — the crash-recovery story.
    let done_id = restart_id + 1;
    rpc_send(
        &mut ws,
        json!({"id":done_id,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hello"}}),
    )
    .await;
    let done = read_reply(&mut ws, done_id).await;
    assert_eq!(done["result"]["stopReason"], "completed", "{done}");
}

/// Pin/archive round trip: pinned rows float first and carry the flag,
/// archived rows disappear from the default listing and return with
/// includeArchived.
#[tokio::test]
async fn session_pin_and_archive_flags() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let plain = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let pinned = read_reply(&mut ws, 2).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let archived = read_reply(&mut ws, 3).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.set_pinned",
               "params":{"sessionId":pinned,"pinned":true}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 4).await["result"]["updated"], true);
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"session.set_archived",
               "params":{"sessionId":archived,"archived":true}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 5).await["result"]["updated"], true);

    // Default listing: archived hidden, pinned first.
    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.list","params":{"limit":50}}),
    )
    .await;
    let listed = read_reply(&mut ws, 6).await;
    let rows = listed["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{listed}");
    assert_eq!(
        rows[0]["sessionId"], pinned,
        "pinned floats first: {listed}"
    );
    assert_eq!(rows[0]["pinned"], true, "{listed}");
    assert_eq!(rows[1]["sessionId"], plain, "{listed}");

    // includeArchived brings it back, flagged.
    rpc_send(
        &mut ws,
        json!({"id":7,"method":"session.list",
               "params":{"limit":50,"includeArchived":true}}),
    )
    .await;
    let listed = read_reply(&mut ws, 7).await;
    let rows = listed["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "{listed}");
    let arch = rows.iter().find(|r| r["sessionId"] == archived).unwrap();
    assert_eq!(arch["archived"], true, "{listed}");
}

/// logs.tail serves the daemon's recent ring; logs.follow pushes every
/// new line as a log.line event frame on this connection.
#[tokio::test]
async fn logs_tail_and_follow_stream_lines() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    damon_core::logs::ring().push("seed line 1".to_string());
    damon_core::logs::ring().push("seed line 2".to_string());
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"logs.tail","params":{"lines":10}}),
    )
    .await;
    let tail = read_reply(&mut ws, 1).await;
    let lines: Vec<&str> = tail["result"]["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l.as_str().unwrap())
        .collect();
    assert!(
        lines.contains(&"seed line 1") && lines.contains(&"seed line 2"),
        "{tail}"
    );

    // Follow, then emit a line — it must arrive as a log.line push.
    rpc_send(&mut ws, json!({"id":2,"method":"logs.follow","params":{}})).await;
    let ack = read_reply(&mut ws, 2).await;
    assert_eq!(ack["result"]["following"], true, "{ack}");
    damon_core::logs::ring().push("live line".to_string());
    let mut saw_live = false;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        let v = match tokio::time::timeout(std::time::Duration::from_secs(5), read_json(&mut ws))
            .await
        {
            Ok(v) => v,
            Err(_) => break,
        };
        if v["event"] == "log.line" {
            assert_eq!(v["data"]["line"], "live line", "{v}");
            saw_live = true;
            break;
        }
    }
    assert!(saw_live, "log.line push never arrived");

    // Unfollow stops the stream (ack-only, no more pushes expected —
    // validated by the next follow=false ack coming back alone).
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"logs.follow","params":{"follow":false}}),
    )
    .await;
    let ack = read_reply(&mut ws, 3).await;
    assert_eq!(ack["result"]["following"], false, "{ack}");
}

/// file.read/write/list round-trip inside the session cwd jail; every
/// escape (`..`, absolute elsewhere, unknown session) fails closed.
#[tokio::test]
async fn file_api_round_trips_inside_the_cwd_jail() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    let dir = std::env::temp_dir().join(format!("damon-file-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":dir}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    use base64::Engine as _;
    let payload = base64::engine::general_purpose::STANDARD.encode("hello file");
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"file.write",
               "params":{"sessionId":sid,"path":"a.txt","content":payload}}),
    )
    .await;
    let wrote = read_reply(&mut ws, 2).await;
    assert_eq!(wrote["result"]["written"], 10, "{wrote}");
    assert!(dir.join("a.txt").exists());

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"file.read",
               "params":{"sessionId":sid,"path":"a.txt"}}),
    )
    .await;
    let read = read_reply(&mut ws, 3).await;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(read["result"]["content"].as_str().unwrap())
            .unwrap(),
        b"hello file",
        "{read}"
    );

    rpc_send(
        &mut ws,
        json!({"id":4,"method":"file.list","params":{"sessionId":sid}}),
    )
    .await;
    let list = read_reply(&mut ws, 4).await;
    let entry = list["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "a.txt")
        .expect("a.txt must list");
    assert_eq!(entry["dir"], false, "{list}");
    assert_eq!(entry["bytes"], 10, "{list}");

    // Escapes fail closed.
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"file.write",
               "params":{"sessionId":sid,"path":"../escape.txt","content":payload}}),
    )
    .await;
    let esc = read_reply(&mut ws, 5).await;
    assert_eq!(esc["error"]["code"], -32602, "{esc}");
    rpc_send(
        &mut ws,
        json!({"id":6,"method":"file.read",
               "params":{"sessionId":sid,"path":"/etc/hosts"}}),
    )
    .await;
    let esc = read_reply(&mut ws, 6).await;
    assert_eq!(esc["error"]["code"], -32602, "{esc}");
    assert!(
        !dir.parent().unwrap().join("escape.txt").exists(),
        "the escape must not have landed"
    );

    // Unknown session has no jail root and no file access.
    rpc_send(
        &mut ws,
        json!({"id":7,"method":"file.read",
               "params":{"sessionId":"nope","path":"a.txt"}}),
    )
    .await;
    let unknown = read_reply(&mut ws, 7).await;
    assert_eq!(unknown["error"]["code"], -32602, "{unknown}");
}

/// Projects: create/list/get/set_defaults lifecycle, session.create
/// integration (root-as-cwd-default, defaults fill unset params,
/// explicit params win), list/search/usage filters, and delete
/// refusing while sessions are bound.
#[tokio::test]
async fn projects_lifecycle_and_session_scoping() {
    let (_state, store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    let root = std::env::temp_dir()
        .join(format!("damon-proj-{}", uuid::Uuid::new_v4()))
        .to_string_lossy()
        .into_owned();
    std::fs::create_dir_all(&root).unwrap();

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"project.create",
               "params":{"name":"web", "root":root,
                          "defaults":{"backend":"mock", "model":"sonnet"}}}),
    )
    .await;
    let created = read_reply(&mut ws, 1).await;
    let pid = created["result"]["projectId"].as_str().unwrap().to_string();
    assert_eq!(created["result"]["name"], "web", "{created}");
    assert_eq!(
        created["result"]["defaults"]["model"], "sonnet",
        "{created}"
    );

    // Unknown defaults key is rejected up front.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"project.create",
               "params":{"root":root, "defaults":{"colour":"blue"}}}),
    )
    .await;
    let bad = read_reply(&mut ws, 2).await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");

    // set_defaults replaces wholesale.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"project.set_defaults",
               "params":{"projectId":pid, "defaults":{"model":"opus"}}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 3).await["result"]["updated"], true);

    // list + get reflect it.
    rpc_send(&mut ws, json!({"id":4,"method":"project.list","params":{}})).await;
    let listed = read_reply(&mut ws, 4).await;
    assert_eq!(listed["result"]["projects"].as_array().unwrap().len(), 1);
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"project.get","params":{"projectId":pid}}),
    )
    .await;
    let got = read_reply(&mut ws, 5).await;
    assert_eq!(got["result"]["defaults"], json!({"model":"opus"}), "{got}");

    // session.create with the project: cwd defaults to root, model
    // from defaults, explicit backend wins over nothing (defaults have
    // no backend now).
    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.create",
               "params":{"projectId":pid}}),
    )
    .await;
    let s1 = read_reply(&mut ws, 6).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    let row = store
        .list_sessions_paged(10, 0, Default::default())
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == s1)
        .unwrap();
    assert_eq!(row.cwd, root, "project root is the cwd default");
    assert_eq!(row.model.as_deref(), Some("opus"), "defaults fill model");

    // Explicit params win over project defaults.
    rpc_send(
        &mut ws,
        json!({"id":7,"method":"session.create",
               "params":{"projectId":pid, "cwd":"/tmp", "model":"haiku"}}),
    )
    .await;
    let s2 = read_reply(&mut ws, 7).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    let row = store
        .list_sessions_paged(10, 0, Default::default())
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == s2)
        .unwrap();
    assert_eq!(row.cwd, "/tmp");
    assert_eq!(row.model.as_deref(), Some("haiku"));

    // Unknown project is a named error.
    rpc_send(
        &mut ws,
        json!({"id":8,"method":"session.create",
               "params":{"projectId":"nope"}}),
    )
    .await;
    let bad = read_reply(&mut ws, 8).await;
    assert_eq!(bad["error"]["code"], -32602, "{bad}");

    // Content for search: one hit inside the project.
    store
        .append(
            &s1,
            "assistant",
            &json!({"content": "projneedle in project"}),
        )
        .await
        .unwrap();
    store
        .append(
            &s2,
            "assistant",
            &json!({"content": "projneedle outside search"}),
        )
        .await
        .unwrap();
    // An unbound session carries the same needle — the project filter
    // must exclude it.
    let unbound = uuid::Uuid::new_v4().to_string();
    store
        .create_session(&unbound, "/elsewhere", None)
        .await
        .unwrap();
    store
        .append(
            &unbound,
            "assistant",
            &json!({"content": "projneedle unbound"}),
        )
        .await
        .unwrap();
    rpc_send(
        &mut ws,
        json!({"id":9,"method":"session.search",
               "params":{"query":"projneedle", "projectId":pid, "limit":10}}),
    )
    .await;
    let hits = read_reply(&mut ws, 9).await["result"]["results"]
        .as_array()
        .unwrap()
        .clone();
    let ids: Vec<&str> = hits
        .iter()
        .map(|h| h["sessionId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&s1.as_str()), "{hits:?}");
    assert!(
        ids.contains(&s2.as_str()),
        "both project sessions match: {hits:?}"
    );
    assert!(
        !ids.contains(&unbound.as_str()),
        "unbound session excluded: {hits:?}"
    );

    // session.list filter.
    rpc_send(
        &mut ws,
        json!({"id":10,"method":"session.list",
                "params":{"projectId":pid}}),
    )
    .await;
    let rows = read_reply(&mut ws, 10).await["result"]["sessions"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(rows.len(), 2, "{rows:?}");

    // usage with project filter runs (mock has no usage rows; the
    // shape must still answer).
    rpc_send(
        &mut ws,
        json!({"id":11,"method":"session.usage",
                "params":{"projectId":pid}}),
    )
    .await;
    let usage = read_reply(&mut ws, 11).await;
    assert!(usage["result"]["sessions"].is_array(), "{usage}");

    // Delete refuses while sessions are bound.
    rpc_send(
        &mut ws,
        json!({"id":12,"method":"project.delete","params":{"projectId":pid}}),
    )
    .await;
    let refused = read_reply(&mut ws, 12).await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert!(
        refused["error"]["message"]
            .as_str()
            .unwrap()
            .contains("session(s)"),
        "{refused}"
    );

    // Unbind (delete the sessions) → delete succeeds.
    for (i, sid) in [s1, s2].iter().enumerate() {
        rpc_send(
            &mut ws,
            json!({"id":13 + i as u64,"method":"session.delete",
                    "params":{"sessionId":sid}}),
        )
        .await;
        let _ = read_reply(&mut ws, 13 + i as u64).await;
    }
    rpc_send(
        &mut ws,
        json!({"id":20,"method":"project.delete","params":{"projectId":pid}}),
    )
    .await;
    let deleted = read_reply(&mut ws, 20).await;
    assert_eq!(deleted["result"]["deleted"], true, "{deleted}");
    rpc_send(
        &mut ws,
        json!({"id":21,"method":"project.list","params":{}}),
    )
    .await;
    assert_eq!(
        read_reply(&mut ws, 21).await["result"]["projects"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

/// channel.get/set/delete_state round trip — the bridge's persistence
/// substrate.
#[tokio::test]
async fn channel_state_round_trip() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"channel.set_state",
               "params":{"convId":"telegram#42","key":"prefs","value":"{\"cwd\":\"/w\"}"}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 1).await["result"]["set"], true);

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"channel.get_state",
               "params":{"convId":"telegram#42","key":"prefs"}}),
    )
    .await;
    let got = read_reply(&mut ws, 2).await;
    assert_eq!(got["result"]["value"], "{\"cwd\":\"/w\"}", "{got}");

    // Missing key reads null; delete clears.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"channel.get_state",
               "params":{"convId":"telegram#42","key":"session"}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 3).await["result"]["value"], Value::Null);
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"channel.delete_state",
               "params":{"convId":"telegram#42","key":"prefs"}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 4).await["result"]["deleted"], true);
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"channel.get_state",
               "params":{"convId":"telegram#42","key":"prefs"}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 5).await["result"]["value"], Value::Null);
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
            .list_sessions_paged(u32::MAX, 0, Default::default())
            .await
            .unwrap()
            .iter()
            .any(|s| s.id == session_id)
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

/// A connection that attaches to a session late (reconnect, second
/// client) receives everything it missed before live streaming starts:
/// persisted history replayed from the store as `session.event` frames
/// tagged `"replay": true`, then live events untagged.
#[tokio::test]
async fn session_resume_replays_missed_history_as_events() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let url = format!("ws://{addr}/ws");
    let (mut ws, _) = ws_connect(&url).await;
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"hello"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");

    // A brand-new connection attaching to the live session.
    let (mut ws2, _) = ws_connect(&url).await;
    rpc_send(
        &mut ws2,
        json!({"id":3,"method":"session.resume","params":{"sessionId":sid}}),
    )
    .await;
    let mut replayed = Vec::new();
    let reply = loop {
        let v = read_json(&mut ws2).await;
        if v.get("id") == Some(&serde_json::json!(3)) {
            break v;
        }
        replayed.push(v);
    };
    assert!(reply.get("error").is_none(), "resume failed: {reply}");
    let count = reply["result"]["replayed"].as_u64().unwrap_or(0);
    assert!(count >= 2, "expected history replay, got {count}: {reply}");
    assert_eq!(
        replayed.len() as u64,
        count,
        "replay count must match frames"
    );
    let kinds: Vec<(bool, &str, String)> = replayed
        .iter()
        .filter_map(|f| {
            let d = &f["data"];
            Some((
                f["replay"].as_bool().unwrap_or(false),
                d["kind"].as_str()?,
                d["text"].as_str().unwrap_or_default().to_string(),
            ))
        })
        .collect();
    assert!(
        kinds.contains(&(true, "user_message", "hello".to_string())),
        "replayed: {kinds:?}"
    );
    assert!(
        kinds.contains(&(true, "assistant_message", "echo: hello".to_string())),
        "replayed: {kinds:?}"
    );
    // Store rows replay oldest-first.
    let user_at = kinds
        .iter()
        .position(|(r, k, _)| *r && *k == "user_message")
        .unwrap();
    let assistant_at = kinds
        .iter()
        .position(|(r, k, t)| *r && *k == "assistant_message" && t == "echo: hello")
        .unwrap();
    assert!(user_at < assistant_at, "replay order: {kinds:?}");

    // Live streaming after replay is untagged and the session works.
    rpc_send(
        &mut ws2,
        json!({"id":4,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"again"}}),
    )
    .await;
    let mut live = Vec::new();
    let reply = loop {
        let v = read_json(&mut ws2).await;
        if v.get("id") == Some(&serde_json::json!(4)) {
            break v;
        }
        live.push(v);
    };
    assert_eq!(reply["result"]["stopReason"], "completed", "reply: {reply}");
    assert!(
        live.iter().all(|f| f.get("replay").is_none()),
        "live frames must not carry replay: {live:?}"
    );
    assert!(
        live.iter()
            .any(|f| f["data"]["kind"] == "assistant_message"
                && f["data"]["text"] == "echo: again"),
        "live stream missing echo: {live:?}"
    );
}

/// `permission.respond` with a deny carrying `interrupt: true` kills
/// the whole turn, not just the denied call — the turn resolves with
/// stopReason "canceled" (a plain deny lets the turn keep running).
#[tokio::test]
async fn permission_deny_with_interrupt_cancels_the_turn() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"perm"}}),
    )
    .await;

    // The ask streams before the turn result — capture its request id.
    let mut request_id = None;
    while request_id.is_none() {
        let v = read_json(&mut ws).await;
        if v["event"] == "session.event"
            && v["data"]["type"] == "permission_requested"
            && let Some(r) = v["data"]["id"].as_str()
        {
            request_id = Some(r.to_string());
        }
    }
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"permission.respond",
               "params":{"sessionId":sid,"requestId":request_id,
                         "response":{"behavior":"deny","interrupt":true}}}),
    )
    .await;
    let respond = read_reply(&mut ws, 3).await;
    assert!(respond.get("error").is_none(), "respond failed: {respond}");

    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(
        reply["result"]["stopReason"], "canceled",
        "deny+interrupt must cancel the turn: {reply}"
    );
}

/// `max_sessions` caps live sessions: create past the cap fails with a
/// named error, close frees the slot again.
#[tokio::test]
async fn max_sessions_caps_live_sessions() {
    let mut cfg = common::mock_config(None);
    cfg.max_sessions = Some(1);
    let mgr = damon_core::session::SessionManager::from_config(&cfg);
    mgr.insert_client("mock".to_string(), common::mock_client());

    let first = mgr.create(Some("mock"), Default::default()).await.unwrap();
    let err = mgr
        .create(Some("mock"), Default::default())
        .await
        .err()
        .expect("create past the cap must fail");
    assert!(
        err.to_string().contains("session limit"),
        "error must name the limit: {err:#}"
    );

    mgr.close(&first.id).await.unwrap();
    mgr.create(Some("mock"), Default::default())
        .await
        .expect("closed session must free its slot");
}

/// UTC calendar date `offset_days` from today as YYYY-MM-DD — the
/// same clock the daemon's SQLite date('now') uses, for date-only
/// search bounds.
fn utc_date(offset_days: i64) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    // Civil-from-days (Hinnant), inverse of the epoch-day conversion.
    let z = secs.div_euclid(86_400) + offset_days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// session.list filters server-side: backend, cwd, and tag membership
/// narrow the rows; every row now carries cwd and tags alongside the
/// old fields.
#[tokio::test]
async fn session_list_filters_backend_cwd_tag() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/work/a", Some("claude"))
        .await
        .unwrap();
    store
        .create_session("s2", "/work/b", Some("gpt"))
        .await
        .unwrap();
    store.set_tags("s1", &["prod".into()]).await.unwrap();
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(&mut ws, json!({"id":1,"method":"session.list","params":{}})).await;
    let reply = read_reply(&mut ws, 1).await;
    let sessions = reply["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 2, "{reply}");
    let s1 = sessions.iter().find(|s| s["sessionId"] == "s1").unwrap();
    assert_eq!(s1["backend"], "claude", "row: {s1}");
    assert_eq!(s1["cwd"], "/work/a", "row: {s1}");
    assert_eq!(s1["tags"], json!(["prod"]), "row: {s1}");
    let s2 = sessions.iter().find(|s| s["sessionId"] == "s2").unwrap();
    assert_eq!(s2["tags"], json!([]), "untagged row: {s2}");

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.list","params":{"backend":"gpt"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    let rows = reply["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{reply}");
    assert_eq!(rows[0]["sessionId"], "s2");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.list","params":{"cwd":"/work/a"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    let rows = reply["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{reply}");
    assert_eq!(rows[0]["sessionId"], "s1");

    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.list","params":{"tag":"prod"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    let rows = reply["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{reply}");
    assert_eq!(rows[0]["sessionId"], "s1");

    rpc_send(
        &mut ws,
        json!({"id":5,"method":"session.list","params":{"tag":"missing"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 5).await;
    assert_eq!(reply["result"]["sessions"].as_array().unwrap().len(), 0);

    // Filters AND together: nothing matches both.
    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.list",
               "params":{"backend":"claude","cwd":"/work/b"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 6).await;
    assert_eq!(reply["result"]["sessions"].as_array().unwrap().len(), 0);
}

/// session.messages rows carry ts — the unix-ms persistence stamp of
/// each row (0 only for pre-migration history).
#[tokio::test]
async fn session_messages_carry_ts() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("mock"))
        .await
        .unwrap();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    store
        .append("s1", "user", &json!({"content": "stamped"}))
        .await
        .unwrap();
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.messages","params":{"sessionId":"s1"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    let msgs = reply["result"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1, "{reply}");
    let ts = msgs[0]["ts"].as_i64().expect("ts must be a number");
    assert!(ts > 0, "fresh rows are stamped: {ts}");
    assert!(
        (before - 2_000..=after + 2_000).contains(&ts),
        "ts {ts} outside [{before},{after}]"
    );
}

/// session.set_tags replaces the whole list (rename semantics); tags
/// appear in session.list rows and drive the tag filter. A non-array
/// tags param errors honestly.
#[tokio::test]
async fn session_set_tags_round_trip() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/a", Some("mock"))
        .await
        .unwrap();
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.set_tags",
               "params":{"sessionId":"s1","tags":["alpha","beta"]}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    assert_eq!(reply["result"]["updated"], true, "{reply}");

    rpc_send(&mut ws, json!({"id":2,"method":"session.list","params":{}})).await;
    let reply = read_reply(&mut ws, 2).await;
    let row = &reply["result"]["sessions"][0];
    assert_eq!(row["tags"], json!(["alpha", "beta"]), "row: {row}");

    // Replace-all: setting ["gamma"] drops alpha/beta.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.set_tags",
               "params":{"sessionId":"s1","tags":["gamma"]}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert_eq!(reply["result"]["updated"], true, "{reply}");
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.list","params":{"tag":"gamma"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    assert_eq!(reply["result"]["sessions"].as_array().unwrap().len(), 1);
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"session.list","params":{"tag":"alpha"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 5).await;
    assert_eq!(reply["result"]["sessions"].as_array().unwrap().len(), 0);

    // Clearing via [] and an unknown session reporting false.
    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.set_tags",
               "params":{"sessionId":"s1","tags":[]}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 6).await["result"]["updated"], true);
    rpc_send(
        &mut ws,
        json!({"id":7,"method":"session.set_tags",
               "params":{"sessionId":"ghost","tags":["x"]}}),
    )
    .await;
    assert_eq!(read_reply(&mut ws, 7).await["result"]["updated"], false);

    // A non-array tags param errors instead of silently storing.
    rpc_send(
        &mut ws,
        json!({"id":8,"method":"session.set_tags",
               "params":{"sessionId":"s1","tags":"prod"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 8).await;
    assert!(reply["error"].is_object(), "expected error: {reply}");
}

/// session.export returns the header + transcript + usage superset of
/// the CLI's client-side export shape; an unknown session errors.
#[tokio::test]
async fn session_export_returns_superset() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"export me"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "{reply}");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.export","params":{"sessionId":sid}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert!(reply.get("error").is_none(), "export failed: {reply}");
    let session = &reply["result"]["session"];
    assert_eq!(session["sessionId"], sid, "{session}");
    assert_eq!(session["backend"], "mock");
    assert_eq!(session["cwd"], "/tmp");
    assert_eq!(session["tags"], json!([]));
    assert!(session["createdAt"].is_string(), "{session}");
    assert_eq!(session["title"], "export me", "first prompt titles it");

    let msgs = reply["result"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 2, "user + echo reply: {msgs:?}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[0]["data"]["content"], "export me");
    assert!(msgs[0]["ts"].as_i64().unwrap_or(0) > 0, "rows carry ts");
    assert_eq!(msgs[1]["role"], "assistant");
    assert_eq!(msgs[1]["data"]["content"], "echo: export me");

    let usage = &reply["result"]["usage"];
    assert_eq!(usage["turns"], 1, "{usage}");
    assert!(
        (usage["costUsd"].as_f64().unwrap_or(0.0) - 0.001).abs() < 1e-9,
        "mock reports 0.001 per turn: {usage}"
    );
    assert!(usage["contextUsed"].is_u64() && usage["contextSize"].is_u64());

    // Unknown session → honest error.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.export","params":{"sessionId":"ghost"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    assert!(reply["error"].is_object(), "expected error: {reply}");
}

/// session.search accepts backend/cwd narrowing and since/until time
/// bounds — RFC3339 or date-only (midnight UTC) — and malformed
/// bounds error rather than silently matching nothing.
#[tokio::test]
async fn session_search_backend_and_time_bounds() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store
        .create_session("s1", "/work/a", Some("claude"))
        .await
        .unwrap();
    store
        .create_session("s2", "/work/b", Some("gpt"))
        .await
        .unwrap();
    for sid in ["s1", "s2"] {
        store
            .append(sid, "user", &json!({"content": "shared needle"}))
            .await
            .unwrap();
    }
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    async fn count(ws: &mut Ws, id: u64) -> usize {
        let v = read_reply(ws, id).await;
        v["result"]["results"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    }

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.search","params":{"query":"needle"}}),
    )
    .await;
    assert_eq!(count(&mut ws, 1).await, 2);

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.search",
           "params":{"query":"needle","backend":"claude"}}),
    )
    .await;
    let v = read_reply(&mut ws, 2).await;
    let hits = v["result"]["results"].as_array().unwrap();
    assert_eq!(hits.len(), 1, "{v}");
    assert_eq!(hits[0]["sessionId"], "s1");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.search",
           "params":{"query":"needle","cwd":"/work/b"}}),
    )
    .await;
    assert_eq!(count(&mut ws, 3).await, 1);

    // Date-only bounds read as midnight: since=yesterday keeps both
    // (rows are stamped today, after yesterday's midnight)...
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.search",
           "params":{"query":"needle","since":utc_date(-1)}}),
    )
    .await;
    assert_eq!(count(&mut ws, 4).await, 2);
    // ...until=today's midnight excludes them (they are later in the
    // day), until=tomorrow's midnight keeps them.
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"session.search",
           "params":{"query":"needle","until":utc_date(0)}}),
    )
    .await;
    assert_eq!(
        count(&mut ws, 5).await,
        0,
        "date-only until is that day's midnight"
    );
    rpc_send(
        &mut ws,
        json!({"id":6,"method":"session.search",
           "params":{"query":"needle","until":utc_date(1)}}),
    )
    .await;
    assert_eq!(count(&mut ws, 6).await, 2);

    // RFC3339 with a future instant excludes everything; the same
    // instant as a date-only bound behaves identically.
    rpc_send(
        &mut ws,
        json!({"id":7,"method":"session.search",
           "params":{"query":"needle","since":format!("{}T00:00:00Z", utc_date(1))}}),
    )
    .await;
    assert_eq!(count(&mut ws, 7).await, 0);

    // Malformed bounds error — they must not look like empty results.
    rpc_send(
        &mut ws,
        json!({"id":8,"method":"session.search",
           "params":{"query":"needle","since":"not-a-date"}}),
    )
    .await;
    let v = read_reply(&mut ws, 8).await;
    assert!(v["error"].is_object(), "expected error: {v}");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("time bound"),
        "error must name the bound: {v}"
    );
}

/// session.usage with daily:true groups the global usage history by
/// UTC day (turns, summed cost, latest context snapshot) and honors
/// the days window; without it, both existing shapes are untouched.
#[tokio::test]
async fn session_usage_daily_rollup() {
    let (_state, store, addr) = serve(test_config(None)).await;
    store.create_session("a", "/tmp", None).await.unwrap();
    store.create_session("b", "/tmp", None).await.unwrap();
    store.record_usage("a", "m", 10, 100, 0.10).await.unwrap();
    store.record_usage("a", "m", 20, 100, 0.40).await.unwrap();
    store.record_usage("b", "m", 50, 100, 0.10).await.unwrap();
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.usage","params":{"daily":true}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    let daily = reply["result"]["daily"].as_array().unwrap();
    assert_eq!(daily.len(), 1, "all rows are today: {daily:?}");
    let row = &daily[0];
    let date = row["date"].as_str().unwrap();
    assert!(
        date.len() == 10 && date.as_bytes()[4] == b'-' && date.as_bytes()[7] == b'-',
        "date must be YYYY-MM-DD: {date}"
    );
    assert_eq!(row["turns"], 3, "{row}");
    assert!(
        (row["costUsd"].as_f64().unwrap() - 0.50).abs() < 1e-9,
        "0.10 + 0.30 + 0.10 per-turn deltas: {row}"
    );
    assert_eq!(
        row["contextUsed"], 50,
        "latest snapshot is the day's last row"
    );

    // The days window is accepted and covers today either way.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"session.usage","params":{"daily":true,"days":1}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["daily"].as_array().unwrap().len(), 1);

    // Without daily: the per-model summary shape is untouched.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.usage","params":{}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    let rows = reply["result"]["sessions"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{reply}");
    assert_eq!(rows[0]["model"], "m");
    assert_eq!(rows[0]["turns"], 3);

    // With a sessionId: the per-session shape is untouched.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.usage","params":{"sessionId":"a"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    assert_eq!(reply["result"]["sessionId"], "a", "{reply}");
    assert_eq!(reply["result"]["turns"], 2);
    assert!(
        (reply["result"]["costUsd"].as_f64().unwrap() - 0.40).abs() < 1e-9,
        "{reply}"
    );
}

/// session.fork copies the usage history and the suffixed title: the
/// fork's export reports the source's cost/turns under "… (fork)".
#[tokio::test]
async fn session_fork_copies_usage_and_title() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    rpc_send(
        &mut ws,
        json!({"id":1,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 1).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"turn.start",
               "params":{"sessionId":sid,"prompt":"pricey question"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["stopReason"], "completed", "{reply}");

    rpc_send(
        &mut ws,
        json!({"id":3,"method":"session.fork","params":{"sessionId":sid}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    assert!(reply.get("error").is_none(), "fork failed: {reply}");
    let fork_id = reply["result"]["sessionId"].as_str().unwrap().to_string();

    rpc_send(
        &mut ws,
        json!({"id":4,"method":"session.export","params":{"sessionId":fork_id}}),
    )
    .await;
    let reply = read_reply(&mut ws, 4).await;
    let session = &reply["result"]["session"];
    assert_eq!(session["title"], "pricey question (fork)", "{session}");
    let usage = &reply["result"]["usage"];
    assert_eq!(usage["turns"], 1, "usage copied: {usage}");
    assert!(
        (usage["costUsd"].as_f64().unwrap() - 0.001).abs() < 1e-9,
        "usage copied: {usage}"
    );
    let msgs = reply["result"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 2, "transcript copied: {msgs:?}");

    // The original keeps its own unsuffixed title.
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"session.export","params":{"sessionId":sid}}),
    )
    .await;
    let reply = read_reply(&mut ws, 5).await;
    assert_eq!(reply["result"]["session"]["title"], "pricey question");
}

/// Errors carry typed JSON-RPC codes: the spec-reserved range for
/// frame/param/method failures, Damon server codes for state a client
/// can recover from (resume on SESSION_NOT_LIVE, hide pickers on
/// NOT_SUPPORTED, pick another backend on BACKEND_UNAVAILABLE). The
/// message strings stay as they were — codes are additive.
#[tokio::test]
async fn error_codes_are_typed() {
    let (_state, _store, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    // Garbage frame → parse error with a null id (JSON-RPC 2.0).
    rpc_send_raw(&mut ws, "{not json").await;
    let v = read_json(&mut ws).await;
    assert!(v["error"].is_object(), "expected error frame: {v}");
    assert_eq!(v["error"]["code"], -32700, "{v}");
    assert!(v["id"].is_null(), "parse errors carry a null id: {v}");

    // Valid JSON, an id, but no method → invalid request. A frame
    // without an id (notification-shaped) stays silently dropped.
    rpc_send_raw(&mut ws, r#"{"id":7,"params":{}}"#).await;
    let v = read_reply(&mut ws, 7).await;
    assert_eq!(v["error"]["code"], -32600, "{v}");

    // Unknown method → method not found.
    rpc_send(
        &mut ws,
        json!({"id":8,"method":"session.ponder","params":{}}),
    )
    .await;
    let v = read_reply(&mut ws, 8).await;
    assert_eq!(v["error"]["code"], -32601, "{v}");

    // Missing required param → invalid params, historical message.
    rpc_send(
        &mut ws,
        json!({"id":9,"method":"session.messages","params":{}}),
    )
    .await;
    let v = read_reply(&mut ws, 9).await;
    assert_eq!(v["error"]["code"], -32602, "{v}");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("sessionId required"),
        "{v}"
    );

    // Unknown session on turn.start → SESSION_NOT_LIVE (client: resume).
    rpc_send(
        &mut ws,
        json!({"id":10,"method":"turn.start",
               "params":{"sessionId":"ghost","prompt":"hi"}}),
    )
    .await;
    let v = read_reply(&mut ws, 10).await;
    assert_eq!(v["error"]["code"], -32001, "{v}");

    // Backend the daemon doesn't have → BACKEND_UNAVAILABLE.
    rpc_send(
        &mut ws,
        json!({"id":11,"method":"session.create",
               "params":{"backend":"nope"}}),
    )
    .await;
    let v = read_reply(&mut ws, 11).await;
    assert_eq!(v["error"]["code"], -32003, "{v}");

    // Capability the mock lacks (default trait impl) → NOT_SUPPORTED.
    rpc_send(
        &mut ws,
        json!({"id":12,"method":"session.create","params":{"cwd":"/tmp"}}),
    )
    .await;
    let sid = read_reply(&mut ws, 12).await["result"]["sessionId"]
        .as_str()
        .unwrap()
        .to_string();
    rpc_send(
        &mut ws,
        json!({"id":13,"method":"session.set_model",
               "params":{"sessionId":sid,"model":"big"}}),
    )
    .await;
    let v = read_reply(&mut ws, 13).await;
    assert_eq!(v["error"]["code"], -32002, "{v}");
}

/// Metrics record what actually happened: one completed mock turn
/// lands in the per-backend histogram, and every dispatched frame —
/// errors included — counts toward the RPC counters.
#[tokio::test]
async fn metrics_record_turns_and_rpc_counters() {
    let (state, _store, addr) = serve(test_config(None)).await;
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
               "params":{"sessionId":sid,"prompt":"measure me"}}),
    )
    .await;
    let reply = tokio::time::timeout(std::time::Duration::from_secs(30), read_reply(&mut ws, 2))
        .await
        .expect("turn timed out");
    assert!(reply.get("error").is_none(), "turn failed: {reply}");

    // An errored request on the same socket so the error counter has
    // something to count.
    rpc_send(&mut ws, json!({"id":3,"method":"nope.nope","params":{}})).await;
    let _ = read_reply(&mut ws, 3).await;

    let body = state.metrics.render(0, 0);
    assert!(
        body.contains("damon_turns_total{backend=\"mock\",status=\"completed\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("damon_turn_seconds_count{backend=\"mock\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("damon_rpc_errors_total{code=\"-32601\"} 1"),
        "{body}"
    );
    let rpc_total = body
        .lines()
        .find(|l| l.starts_with("damon_rpc_requests_total"))
        .unwrap();
    let n: u64 = rpc_total
        .split_whitespace()
        .last()
        .unwrap()
        .parse()
        .unwrap();
    assert!(n >= 3, "three frames were dispatched: {body}");
}

/// Send a raw string frame (not JSON-serialized params) — the parse
/// and invalid-request paths must see the exact bytes.
async fn rpc_send_raw(ws: &mut Ws, text: &str) {
    ws.send(tungstenite::Message::Text(text.into()))
        .await
        .unwrap();
}
