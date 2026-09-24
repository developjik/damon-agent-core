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
