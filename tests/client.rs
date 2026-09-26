//! Integration tests for `DamonClient` auto-reconnect.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::routing::get;
use damon_core::client::{ClientEvent, DamonClient};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;

struct ServerState {
    /// Close the socket right after answering the first request.
    close_after_reply: AtomicBool,
    /// How many WS connections this server has accepted.
    greetings: AtomicU64,
    /// Fires once the handler has closed the socket.
    done: Notify,
    /// Test-side gate: the handler waits for this before returning, so the
    /// accept loop can be torn down before the client redials — otherwise
    /// the reconnect would land on the still-listening old server.
    proceed: Notify,
    /// Every request the server has answered, oldest first — the wrapper
    /// tests assert the exact v2 param shapes hit the wire.
    requests: parking_lot::Mutex<Vec<(String, Value)>>,
}

/// WS endpoint: greets each connection with a `session.event` push, then
/// answers every request with `{"echo": <method>}`.
async fn ws_handler(
    State(state): State<Arc<ServerState>>,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<ServerState>) {
    let n = state.greetings.fetch_add(1, Ordering::SeqCst) + 1;
    let _ = socket
        .send(Message::Text(
            json!({"event": "session.event", "sessionId": "test", "data": {"greeting": n}})
                .to_string()
                .into(),
        ))
        .await;
    while let Some(Ok(Message::Text(text))) = socket.recv().await {
        let Ok(v) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let Some(id) = v.get("id").and_then(|i| i.as_u64()) else {
            continue;
        };
        state.requests.lock().push((
            v["method"].as_str().unwrap_or_default().to_string(),
            v["params"].clone(),
        ));
        // The steer/set wrappers get real v2 result shapes; everything
        // else keeps the `{"echo": method}` reply the reconnect tests
        // assert on.
        let result = match v["method"].as_str() {
            Some("turn.steer") => json!({"result": "accepted"}),
            Some("session.set_model") | Some("session.set_mode") => json!({}),
            _ => json!({"echo": v["method"].clone()}),
        };
        let reply = json!({
            "id": id,
            "result": result,
        });
        if socket
            .send(Message::Text(reply.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
        if state.close_after_reply.load(Ordering::SeqCst) {
            let _ = socket.send(Message::Close(None)).await;
            state.done.notify_one();
            state.proceed.notified().await;
            return;
        }
    }
}

/// Serve the WS endpoint on an already-bound listener.
fn serve(listener: TcpListener, state: Arc<ServerState>) -> JoinHandle<()> {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    })
}

/// Pull the next `session.event` push off the client's event stream.
/// Connection-state mirror events pass through — reconnects re-emit
/// `Connected`, which must not be mistaken for payload.
async fn recv_greeting(events: &mut tokio::sync::mpsc::Receiver<ClientEvent>) -> u64 {
    loop {
        let ev = timeout(Duration::from_secs(15), events.recv())
            .await
            .expect("timed out waiting for greeting")
            .expect("event stream closed");
        match ev {
            ClientEvent::Event { event, .. } => {
                return event["greeting"].as_u64().expect("no greeting");
            }
            ClientEvent::Connected | ClientEvent::Disconnected => continue,
            other => panic!("expected Event, got {other:?}"),
        }
    }
}

/// Server drops the connection after the first reply; once it restarts on
/// the same port the client must redial and serve requests again.
#[tokio::test]
async fn reconnects_after_server_restart() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    let mut events = client.events().await;
    assert_eq!(recv_greeting(&mut events).await, 1);

    // First request succeeds, then the server closes the socket.
    let v = client.request("hello", json!({})).await.unwrap();
    assert_eq!(v["echo"], "hello");
    state.done.notified().await;

    // Tear down the accept loop, release the handler, rebind the port.
    server.abort();
    let _ = server.await;
    state.proceed.notify_one();
    let listener = TcpListener::bind(addr).await.unwrap();
    let _server2 = serve(listener, state.clone());

    // The client redials in the background; this request may span the gap.
    let v = timeout(
        Duration::from_secs(15),
        client.request("session.list", json!({})),
    )
    .await
    .expect("request did not complete after reconnect")
    .expect("request failed after reconnect");
    assert_eq!(v["echo"], "session.list");
    assert_eq!(state.greetings.load(Ordering::SeqCst), 2);
}

/// After a reconnect the client auto-resubscribes: a session it
/// touched earlier is resumed on the new link (fire-and-forget), so
/// events keep flowing without the caller re-issuing session.resume.
/// Both mock servers share one request log, so the resume is asserted
/// there regardless of which incarnation the redial lands on.
#[tokio::test]
async fn reconnect_auto_resumes_touched_sessions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    let mut events = client.events().await;
    assert_eq!(recv_greeting(&mut events).await, 1);

    // Touch a session — the mock's answer is its one allowed reply, so
    // the link dies right after (by design).
    const SID: &str = "sid-auto-resume";
    client.turn_start(SID, "hi").await.unwrap();
    state.done.notified().await;
    server.abort();
    let _ = server.await;
    state.proceed.notify_one();
    let listener = TcpListener::bind(addr).await.unwrap();
    let _server2 = serve(listener, state.clone());

    // The redial (with its auto-resume) lands on some incarnation —
    // poll the shared request log for the resume.
    let deadline = Duration::from_secs(15);
    let started = std::time::Instant::now();
    loop {
        let reqs = state.requests.lock().clone();
        let resumes = reqs
            .iter()
            .filter(|(m, p)| m == "session.resume" && p["sessionId"] == SID)
            .count();
        if resumes >= 1 {
            break;
        }
        assert!(
            started.elapsed() < deadline,
            "auto-resume never hit the wire; requests so far: {reqs:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// auto_resume: false opts out — no session.resume is sent on redial.
#[tokio::test]
async fn auto_resume_can_be_disabled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let server = serve(listener, state.clone());

    let opts = damon_core::client::ConnectOptions {
        auto_resume: false,
        ..Default::default()
    };
    let client = DamonClient::connect_with_options(&format!("ws://{addr}/ws"), None, opts)
        .await
        .unwrap();
    let mut events = client.events().await;
    assert_eq!(recv_greeting(&mut events).await, 1);

    // turn_start's reply is the mock's one allowed answer — the link
    // dies right after, by design.
    let _ = client.turn_start("sid-x", "hi").await;
    state.done.notified().await;
    server.abort();
    let _ = server.await;
    state.proceed.notify_one();
    let listener = TcpListener::bind(addr).await.unwrap();
    let _server2 = serve(listener, state.clone());

    // Drive a request to completion so the redial has landed (retry —
    // a churn window right after the restart is not what we test).
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(Ok(_)) = timeout(Duration::from_secs(5), client.request("ping", json!({}))).await
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "client never completed a request after restart"
        );
    }
    // Any (unwanted) resume had ample opportunity — none may exist.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let reqs = state.requests.lock().clone();
    assert!(
        !reqs.iter().any(|(m, _)| m == "session.resume"),
        "auto_resume=false must not resume; requests: {reqs:?}"
    );
}

/// With the server gone for good, a request issued while disconnected
/// waits for a reconnect that never comes and errors after ~10s.
#[tokio::test]
async fn request_times_out_when_server_stays_down() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    client.request("hello", json!({})).await.unwrap();
    state.done.notified().await;

    // No restart: release the handler and kill the accept loop for good.
    server.abort();
    let _ = server.await;
    state.proceed.notify_one();

    // Give the reader task a moment to observe the close and mark the
    // client disconnected, so the request below exercises the wait path.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let start = Instant::now();
    let err = client
        .request("session.list", json!({}))
        .await
        .expect_err("request must fail while the server is down");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_secs(9),
        "request returned after {elapsed:?}, expected ~10s reconnect wait"
    );
    assert!(
        err.to_string().contains("reconnect"),
        "unexpected error: {err}"
    );
}

/// Events keep flowing across a reconnect: the second server greets the
/// redialed connection and the event stream delivers it.
#[tokio::test]
async fn notifications_resume_after_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    let mut events = client.events().await;
    assert_eq!(recv_greeting(&mut events).await, 1);

    client.request("hello", json!({})).await.unwrap();
    state.done.notified().await;

    server.abort();
    let _ = server.await;
    state.proceed.notify_one();
    let listener = TcpListener::bind(addr).await.unwrap();
    let _server2 = serve(listener, state.clone());

    // The redialed connection is greeted again — proof the event
    // path survived the reconnect.
    assert_eq!(recv_greeting(&mut events).await, 2);
}

/// Killing and restarting the server is observable as a
/// Connected → Disconnected → Connected sequence, both on the
/// `conn_state()` watch (authoritative) and as ClientEvent mirrors on
/// the event stream.
#[tokio::test]
async fn conn_state_and_events_track_server_lifecycle() {
    use damon_core::client::ConnState;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    let mut events = client.events().await;
    let mut conn = client.conn_state();
    assert_eq!(*conn.borrow(), ConnState::Connected);
    assert_eq!(recv_greeting(&mut events).await, 1);

    // Server closes the socket after its first reply.
    client.request("hello", json!({})).await.unwrap();
    state.done.notified().await;
    server.abort();
    let _ = server.await;
    state.proceed.notify_one();

    // Disconnected, on both surfaces.
    let ev = timeout(Duration::from_secs(15), events.recv())
        .await
        .expect("timed out waiting for Disconnected")
        .expect("event stream closed");
    assert!(matches!(ev, ClientEvent::Disconnected));
    let deadline = Instant::now() + Duration::from_secs(15);
    while *conn.borrow() != ConnState::Disconnected {
        assert!(
            Instant::now() < deadline,
            "watch never flipped to Disconnected"
        );
        conn.changed().await.expect("conn watch closed");
    }

    // Restart on the same port; the supervisor redials.
    let listener = TcpListener::bind(addr).await.unwrap();
    let _server2 = serve(listener, state.clone());
    let deadline = Instant::now() + Duration::from_secs(15);
    while *conn.borrow() != ConnState::Connected {
        assert!(
            Instant::now() < deadline,
            "watch never flipped back to Connected"
        );
        conn.changed().await.expect("conn watch closed");
    }
    // The Connected mirror arrives before the second greeting (emitted
    // by the supervisor right after the successful redial).
    let ev = timeout(Duration::from_secs(15), events.recv())
        .await
        .expect("timed out waiting for Connected")
        .expect("event stream closed");
    assert!(matches!(ev, ClientEvent::Connected));
    assert_eq!(state.greetings.load(Ordering::SeqCst), 2);
}

/// The steer/model/mode wrappers send the exact v2 param shapes and
/// surface the daemon's result: `turn.steer`
/// `{sessionId, prompt, expectedTurn?}` → the SteerResult object,
/// `session.set_model`/`set_mode` → unit on `{}`.
#[tokio::test]
async fn steer_and_set_wrappers_send_v2_param_shapes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(false),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
        requests: Default::default(),
    });
    let _server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();

    // `expectedTurn: null` steers whatever turn is live — the daemon
    // reads it as an optional string (absent and null behave alike).
    let v = client.turn_steer("s1", "keep going", None).await.unwrap();
    assert_eq!(v, json!({"result": "accepted"}));
    let v = client.turn_steer("s2", "pivot", Some("t7")).await.unwrap();
    assert_eq!(v, json!({"result": "accepted"}));
    client
        .set_model("s1", "anthropic/claude-opus")
        .await
        .unwrap();
    client.set_mode("s1", "plan").await.unwrap();

    let reqs = state.requests.lock();
    assert_eq!(reqs.len(), 4, "expected exactly the four wrapper calls");
    assert_eq!(
        reqs[0],
        (
            "turn.steer".to_string(),
            json!({"sessionId": "s1", "prompt": "keep going", "expectedTurn": null})
        )
    );
    assert_eq!(
        reqs[1],
        (
            "turn.steer".to_string(),
            json!({"sessionId": "s2", "prompt": "pivot", "expectedTurn": "t7"})
        )
    );
    assert_eq!(
        reqs[2],
        (
            "session.set_model".to_string(),
            json!({"sessionId": "s1", "model": "anthropic/claude-opus"})
        )
    );
    assert_eq!(
        reqs[3],
        (
            "session.set_mode".to_string(),
            json!({"sessionId": "s1", "mode": "plan"})
        )
    );
}
