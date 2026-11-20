//! `#16` session state: the live phase surface and the crash-restore
//! checkpoint, over the real wire.
//!
//! `session.status` reports what each live session is doing (`idle`,
//! `busy`, `awaiting_input`) and — after a daemon restart — which
//! sessions the previous incarnation left mid-turn or parked on an
//! answer. The checkpoint is a file next to the store, so these tests use
//! a real store file rather than the in-memory one.

mod common;

use std::sync::Arc;

use damon_core::api::{self, AppState};
use damon_core::config::SharedConfig;
use damon_core::store::Store;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn test_config() -> SharedConfig {
    Arc::new(parking_lot::RwLock::new(common::mock_config(None)))
}

/// A store directory that outlives one daemon incarnation, plus the
/// database path inside it.
fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "damon-session-state-{tag}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Boot a daemon over the given store and serve its router on loopback.
async fn boot(db: &std::path::Path) -> (Arc<AppState>, String) {
    let store = Store::open(db).await.unwrap();
    let state = AppState::new(test_config(), store).await;
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
    (state, format!("ws://{addr}/ws"))
}

async fn read_json(ws: &mut Ws) -> Value {
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(60), ws.next())
            .await
            .expect("timed out waiting for a daemon frame")
            .unwrap()
            .unwrap();
        if let tungstenite::Message::Text(t) = frame {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn rpc_send(ws: &mut Ws, msg: Value) {
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

async fn ws_connect(url: &str) -> (Ws, Value) {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = read_json(&mut ws).await;
    assert!(hello["hello"].is_object(), "missing hello push: {hello}");
    (ws, hello)
}

/// Read until the reply to `id`, skipping event pushes.
async fn read_reply(ws: &mut Ws, id: u64) -> Value {
    loop {
        let v = read_json(ws).await;
        if v["id"].as_u64() == Some(id) {
            return v;
        }
    }
}

async fn status(ws: &mut Ws) -> Value {
    rpc_send(
        ws,
        json!({"id": 90, "method": "session.status", "params": {}}),
    )
    .await;
    let reply = read_reply(ws, 90).await;
    assert!(
        reply.get("error").is_none(),
        "session.status failed: {reply}"
    );
    reply["result"].clone()
}

/// `session.status` names what each live session is doing: a parked
/// permission ask is `awaiting_input` (the turn is not reapable, and the
/// client can show the request), and answering it returns the session to
/// `busy` with the turn's own outcome on completion.
#[tokio::test]
async fn session_status_reports_phase_and_the_parked_ask() {
    let dir = temp_dir("phase");
    let (_state, url) = boot(&dir.join("damon.db")).await;
    let (mut ws, hello) = ws_connect(&url).await;
    assert_eq!(
        hello["hello"]["capabilities"]["sessionPhase"], true,
        "hello must advertise the phase surface: {hello}"
    );

    rpc_send(
        &mut ws,
        json!({"id": 1, "method": "session.create", "params": {"cwd": "/tmp"}}),
    )
    .await;
    let created = read_reply(&mut ws, 1).await;
    let session_id = created["result"]["sessionId"].as_str().unwrap().to_string();

    // Idle right after create, with no turn and no ask.
    let idle = status(&mut ws).await;
    let row = idle["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["sessionId"] == session_id.as_str())
        .unwrap_or_else(|| panic!("live session missing from session.status: {idle}"));
    assert_eq!(row["phase"], "idle");
    assert_eq!(row["busy"], false);
    assert!(row.get("pendingAsk").is_none());
    assert_eq!(idle["recovered"].as_array().unwrap().len(), 0);

    // "perm" parks the turn on a permission answer.
    rpc_send(
        &mut ws,
        json!({"id": 2, "method": "turn.start",
               "params": {"sessionId": session_id, "prompt": "perm"}}),
    )
    .await;
    let request_id = loop {
        let v = read_json(&mut ws).await;
        if v["event"] == "session.event" && v["data"]["type"] == "permission_requested" {
            break v["data"]["id"].as_str().unwrap().to_string();
        }
    };

    let parked = status(&mut ws).await;
    let row = parked["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["sessionId"] == session_id.as_str())
        .unwrap_or_else(|| panic!("live session missing while parked: {parked}"));
    assert_eq!(row["phase"], "awaiting_input", "row: {row}");
    assert_eq!(row["busy"], true, "a parked turn is not reapable");
    assert_eq!(row["pendingAsk"]["requestId"], request_id.as_str());
    assert_eq!(row["pendingAsk"]["kind"], "permission");

    rpc_send(
        &mut ws,
        json!({"id": 3, "method": "permission.respond",
               "params": {"sessionId": session_id, "requestId": request_id,
                          "response": {"behavior": "deny", "interrupt": false}}}),
    )
    .await;
    let turn = read_reply(&mut ws, 2).await;
    assert_eq!(turn["result"]["stopReason"], "completed", "turn: {turn}");

    let done = status(&mut ws).await;
    let row = done["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["sessionId"] == session_id.as_str())
        .unwrap();
    assert_eq!(row["phase"], "idle", "a finished turn frees the session");
    assert!(row.get("pendingAsk").is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The crash test: a daemon dies with a turn parked on an answer. The
/// next incarnation reads the checkpoint, converges that row to idle and
/// reports it as `recovered` with the phase and turn it was in — never as
/// a live session, never as silently idle.
#[tokio::test]
async fn a_killed_daemon_returns_its_parked_sessions_as_recovered() {
    let dir = temp_dir("crash");
    let db = dir.join("damon.db");
    let checkpoint = dir.join("sessions.state.json");

    let session_id = {
        let (_state, url) = boot(&db).await;
        let (mut ws, _hello) = ws_connect(&url).await;
        rpc_send(
            &mut ws,
            json!({"id": 1, "method": "session.create", "params": {"cwd": "/tmp"}}),
        )
        .await;
        let created = read_reply(&mut ws, 1).await;
        let session_id = created["result"]["sessionId"].as_str().unwrap().to_string();
        // Detached: the turn outlives the connection, so dropping the
        // socket is a daemon crash, not a client cancel. (A non-detached
        // turn would be cancelled by the disconnect and end up idle.)
        rpc_send(
            &mut ws,
            json!({"id": 2, "method": "turn.start",
                   "params": {"sessionId": session_id, "prompt": "plan review", "detach": true}}),
        )
        .await;
        // The reply and the ask race — read both in one pass instead of
        // filtering, or the ask could be skipped while waiting for id 2.
        // The reply and the ask race on this socket — collect both in one
        // pass so neither is dropped while waiting for the other.
        let (started, ask) = {
            let mut started: Option<Value> = None;
            let mut ask: Option<Value> = None;
            while started.is_none() || ask.is_none() {
                let v = read_json(&mut ws).await;
                if v["id"].as_u64() == Some(2) {
                    started = Some(v);
                } else if v["event"] == "session.event"
                    && v["data"]["type"] == "permission_requested"
                {
                    ask = Some(v);
                }
            }
            (started.unwrap(), ask.unwrap())
        };
        assert_eq!(started["result"]["detached"], true, "turn: {started}");
        assert_eq!(ask["sessionId"], session_id.as_str(), "ask: {ask}");
        // The daemon "dies" here: the socket, the state, and the running
        // turn are dropped without an answer. Nothing is shut down
        // gracefully.
        drop(ws);
        session_id
    };

    let written = std::fs::read_to_string(&checkpoint)
        .unwrap_or_else(|e| panic!("no checkpoint at {}: {e}", checkpoint.display()));
    assert!(
        written.contains("\"awaitingInput\"") || written.contains("\"awaiting_input\""),
        "checkpoint must record the parked phase: {written}"
    );

    // Second incarnation on the same store.
    let (state2, url2) = boot(&db).await;
    assert_eq!(
        state2.sessions.live_count().await,
        0,
        "a restart must not resurrect a backend process"
    );
    let recovered = state2.sessions.recovered_sessions();
    assert_eq!(recovered.len(), 1, "recovered: {recovered:?}");
    assert_eq!(recovered[0].id, session_id);
    assert_eq!(recovered[0].turns_interrupted, 1);

    let (mut ws, _hello) = ws_connect(&url2).await;
    let status2 = status(&mut ws).await;
    assert_eq!(
        status2["sessions"].as_array().unwrap().len(),
        0,
        "no ghost session is reported live: {status2}"
    );
    let row = &status2["recovered"].as_array().unwrap()[0];
    assert_eq!(row["sessionId"], session_id.as_str());
    assert_eq!(row["backend"], "mock");
    assert_eq!(row["phase"], "awaiting_input", "row: {row}");

    assert_eq!(state2.sessions.session_counts().active(), 0);
    drop(ws);
    drop(state2);

    // A third boot finds the converged document: the same interruption is
    // reported once, never counted twice, and still no ghost is live.
    let (state3, _url3) = boot(&db).await;
    assert_eq!(state3.sessions.live_count().await, 0);
    let again = state3.sessions.recovered_sessions();
    assert_eq!(again.len(), 1, "recovered: {again:?}");
    assert_eq!(again[0].id, session_id);
    assert_eq!(
        again[0].turns_interrupted, 1,
        "an already converged session is not interrupted a second time"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
