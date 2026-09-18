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
}

/// WS endpoint: greets each connection with a notification, then answers
/// every request with `{"echo": <method>}`.
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
            json!({"method": "session/update", "params": {"greeting": n}})
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
        let reply = json!({
            "id": id,
            "result": {"echo": v["method"].clone()},
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

/// Pull the next `session/update` event off the client's event stream.
async fn recv_greeting(events: &mut tokio::sync::mpsc::Receiver<ClientEvent>) -> u64 {
    let ev = timeout(Duration::from_secs(15), events.recv())
        .await
        .expect("timed out waiting for greeting")
        .expect("event stream closed");
    match ev {
        ClientEvent::Update(params) => params["greeting"].as_u64().expect("no greeting"),
        other => panic!("expected Update, got {other:?}"),
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
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    let mut events = client.events().await;
    assert_eq!(recv_greeting(&mut events).await, 1);

    // First request succeeds, then the server closes the socket.
    let v = client.request("initialize", json!({})).await.unwrap();
    assert_eq!(v["echo"], "initialize");
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
        client.request("session/list", json!({})),
    )
    .await
    .expect("request did not complete after reconnect")
    .expect("request failed after reconnect");
    assert_eq!(v["echo"], "session/list");
    assert_eq!(state.greetings.load(Ordering::SeqCst), 2);
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
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    client.request("initialize", json!({})).await.unwrap();
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
        .request("session/list", json!({}))
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

/// Notifications keep flowing across a reconnect: the second server
/// greets the redialed connection and the event stream delivers it.
#[tokio::test]
async fn notifications_resume_after_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = Arc::new(ServerState {
        close_after_reply: AtomicBool::new(true),
        greetings: AtomicU64::new(0),
        done: Notify::new(),
        proceed: Notify::new(),
    });
    let server = serve(listener, state.clone());

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    let mut events = client.events().await;
    assert_eq!(recv_greeting(&mut events).await, 1);

    client.request("initialize", json!({})).await.unwrap();
    state.done.notified().await;

    server.abort();
    let _ = server.await;
    state.proceed.notify_one();
    let listener = TcpListener::bind(addr).await.unwrap();
    let _server2 = serve(listener, state.clone());

    // The redialed connection is greeted again — proof the notification
    // path survived the reconnect.
    assert_eq!(recv_greeting(&mut events).await, 2);
}
