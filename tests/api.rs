//! HTTP surface tests: /health, the token gate, /metrics, ws tickets,
//! rate-limit bucket, and auth-token cache behavior.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

mod common;

async fn app(token: Option<&str>) -> Router {
    let cfg = common::mock_config(token);
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(shared, store).await;
    state
        .sessions
        .insert_client("mock".to_string(), common::mock_client());
    damon_core::api::router(state)
}

#[tokio::test]
async fn health_is_open() {
    let app = app(None).await;
    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"], "ok");
    assert!(
        v["backends"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a == "mock")
    );
}

#[tokio::test]
async fn token_gate_blocks_and_allows() {
    let app = app(Some("secret-tok")).await;
    let resp = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", "Bearer secret-tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn metrics_requires_token_and_exposes_counters() {
    let app = app(Some("secret-tok")).await;
    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", "Bearer secret-tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("damon_requests_total"));
}

#[tokio::test]
async fn ws_ticket_requires_token_and_returns_hex() {
    let app = app(Some("secret-tok")).await;
    let resp = app
        .clone()
        .oneshot(Request::post("/v1/ws_ticket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let resp = app
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("authorization", "Bearer secret-tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ticket = v["ticket"].as_str().unwrap();
    assert!(!ticket.is_empty() && ticket.len() == 36, "ticket: {ticket}");
}

#[test]
fn token_bucket_allows_burst_then_throttles() {
    let now = std::time::Instant::now();
    let mut bucket = (20.0, now);
    for _ in 0..20 {
        assert!(damon_core::api::bucket_allow(&mut bucket, now));
    }
    assert!(!damon_core::api::bucket_allow(&mut bucket, now));
    // Refill over a full minute at the configured rate.
    let later = now + std::time::Duration::from_secs(61);
    assert!(damon_core::api::bucket_allow(&mut bucket, later));
}

#[tokio::test]
async fn auth_token_cache_re_resolves_after_ttl() {
    // env-var-backed token: flip the var, prove the cached value is
    // served within the TTL and the new one after it expires.
    // SAFETY: test-only; unique var name, removed before returning.
    unsafe {
        std::env::set_var("DAMON_TEST_TOK_CACHE", "first");
    }
    let mut cfg = common::mock_config(Some("env:DAMON_TEST_TOK_CACHE"));
    cfg.auth_token = Some("env:DAMON_TEST_TOK_CACHE".into());
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(shared, store).await;
    let first = state
        .auth_token_cached(std::time::Duration::from_secs(60))
        .await;
    assert_eq!(first, Some(Ok("first".to_string())));
    // SAFETY: see above.
    unsafe {
        std::env::set_var("DAMON_TEST_TOK_CACHE", "second");
    }
    let cached = state
        .auth_token_cached(std::time::Duration::from_secs(60))
        .await;
    assert_eq!(cached, Some(Ok("first".to_string())));
    let ttl_expired = std::time::Duration::from_secs(0);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let fresh = state.auth_token_cached(ttl_expired).await;
    assert_eq!(fresh, Some(Ok("second".to_string())));
    // SAFETY: see above.
    unsafe {
        std::env::remove_var("DAMON_TEST_TOK_CACHE");
    }
}

/// The full device-pairing loop over a real server: wrong key refused,
/// key spent exactly once (consumed + rotated), pending device cannot
/// mint a ticket, approval unlocks ticket minting, the ticket opens a
/// WebSocket (hello arrives), revocation kills the token immediately,
/// and the master-token path is byte-for-byte unchanged.
#[tokio::test]
async fn web_device_pairing_full_flow() {
    use futures::StreamExt;
    use tokio_tungstenite::tungstenite::Message;

    let cfg = common::mock_config(Some("master-tok"));
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(shared, store.clone()).await;
    state
        .sessions
        .insert_client("mock".to_string(), common::mock_client());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            damon_core::api::router(server_state)
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    let base = format!("http://{addr}");
    // Router with the auth middleware for one-shot requests.
    // The oneshot router MUST share the state above — same store, same
    // pairing keys (app() would build a second in-memory store).
    let gated = damon_core::api::router(state.clone());

    // Master regression, both directions.
    let resp = gated
        .clone()
        .clone()
        .oneshot(Request::post("/v1/ws_ticket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "no bearer, no device"
    );
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("authorization", "Bearer master-tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "master path unchanged");

    // Pairing: wrong key refused.
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/pair")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"pairingKey":"NOPE","name":"pixel"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // The desktop shows a key (store call — the RPC arm wraps this).
    let key = store.pairing_key_ensure().await.unwrap();

    // Pair: the key is spent exactly once.
    let resp = gated
        .clone()
        .clone()
        .oneshot(
            Request::post("/v1/pair")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"pairingKey":"{key}","name":"pixel"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let paired: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let device_id = paired["deviceId"].as_str().unwrap().to_string();
    let device_token = paired["token"].as_str().unwrap().to_string();
    assert_eq!(paired["status"], "pending");
    assert!(!device_token.is_empty());

    // The spent key is dead — consumed AND rotated in one step.
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/pair")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"pairingKey":"{key}","name":"again"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let fresh = store.pairing_key_ensure().await.unwrap();
    assert_ne!(fresh, key, "a fresh key rotated in");

    // Pending device: no ticket.
    let resp = gated
        .clone()
        .clone()
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"deviceToken":"{device_token}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "pending mints nothing"
    );

    // Pair status: pending while waiting.
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/pair/status")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"deviceId":"{device_id}","token":"{device_token}"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"], "pending", "{v}");

    // Approve (the device.* RPC arm is a thin store call) → ticket mints.
    assert!(store.approve_device(&device_id).await.unwrap());
    let resp = gated
        .clone()
        .clone()
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"deviceToken":"{device_token}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let ticket = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();

    // The ticket OPENS a WebSocket — hello arrives.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
        .await
        .unwrap();
    let hello = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let hello: serde_json::Value = serde_json::from_slice(
        match hello {
            Message::Text(t) => t.as_bytes().to_vec(),
            other => panic!("expected text frame: {other:?}"),
        }
        .as_slice(),
    )
    .unwrap();
    assert_eq!(hello["hello"]["protocol"], 2, "{hello}");

    // Revoke → the token is dead immediately; pair status reads revoked.
    assert!(store.revoke_device(&device_id).await.unwrap());
    let resp = gated
        .clone()
        .clone()
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"deviceToken":"{device_token}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "revoked mints nothing"
    );
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/pair/status")
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"deviceId":"{device_id}","token":"{device_token}"}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"], "revoked", "{v}");

    // Master path still fine after everything.
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("authorization", "Bearer master-tok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = base;
}

/// Revocation reaches ALREADY-OPEN sockets: the device's live /ws
/// connection is cut the moment `device.revoke` lands (the row dies,
/// the unspent tickets die, and the socket — riding a now-dead
/// credential — is closed instead of lingering until it happens to
/// disconnect). A master connection is untouched.
#[tokio::test]
async fn device_revoke_cuts_the_live_socket() {
    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let cfg = common::mock_config(Some("master-tok"));
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(shared, store.clone()).await;
    state
        .sessions
        .insert_client("mock".to_string(), common::mock_client());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            damon_core::api::router(server_state)
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    let gated = damon_core::api::router(state.clone());

    // Pair + approve a device directly through the store (the HTTP
    // loop is covered by web_device_pairing_full_flow).
    let token = format!("dev-{}", uuid::Uuid::new_v4());
    let device_id = store
        .create_device(
            "pixel",
            &damon_core::cli_lifecycle::hash_device_token(&token),
        )
        .await
        .unwrap();
    assert!(store.approve_device(&device_id).await.unwrap());

    // Device mints a ticket and OPENS a WebSocket with it.
    let resp = gated
        .clone()
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"deviceToken":"{token}"}}"#)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let ticket = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    let (mut device_ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
            .await
            .unwrap();
    let _hello = tokio::time::timeout(std::time::Duration::from_secs(5), device_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // A master connection revokes the device over the same surface a
    // real trusted client uses — the RPC arm, not the store call.
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    req.headers_mut()
        .insert("Authorization", "Bearer master-tok".parse().unwrap());
    let (mut admin_ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let _admin_hello = tokio::time::timeout(std::time::Duration::from_secs(5), admin_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    admin_ws
        .send(Message::Text(
            serde_json::json!({
                "id": 7,
                "method": "device.revoke",
                "params": {"deviceId": device_id}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    // Collect the revoke reply (skipping any pushed frames).
    let reply = loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), admin_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(t) = frame
            && serde_json::from_slice::<serde_json::Value>(t.as_bytes()).unwrap()["id"] == 7
        {
            break serde_json::from_slice::<serde_json::Value>(t.as_bytes()).unwrap();
        }
    };
    assert_eq!(reply["result"]["revoked"], true, "{reply}");
    assert_eq!(
        reply["result"]["socketsCut"], 1,
        "the device's live socket must be counted and cut: {reply}"
    );

    // The device socket actually dies — promptly, not at the next TCP
    // timeout. Close frame, reset, or clean EOF all count.
    let closed = tokio::time::timeout(std::time::Duration::from_secs(5), device_ws.next())
        .await
        .expect("revoked device socket must close");
    let dead = match closed {
        None => true,
        Some(Err(_)) => true,
        Some(Ok(Message::Close(_))) => true,
        Some(Ok(other)) => panic!("expected closure, got another frame: {other:?}"),
    };
    assert!(dead);

    // A second revoke reports nothing left to cut.
    admin_ws
        .send(Message::Text(
            serde_json::json!({
                "id": 8,
                "method": "device.revoke",
                "params": {"deviceId": device_id}
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let reply = loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), admin_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(t) = frame
            && serde_json::from_slice::<serde_json::Value>(t.as_bytes()).unwrap()["id"] == 8
        {
            break serde_json::from_slice::<serde_json::Value>(t.as_bytes()).unwrap();
        }
    };
    assert_eq!(reply["result"]["revoked"], false, "{reply}");
    assert_eq!(reply["result"]["socketsCut"], 0, "{reply}");
}
