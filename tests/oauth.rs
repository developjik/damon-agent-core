//! OAuth refresh flow tests. `DAMON_TEST_TOKEN_DIR` swaps the keychain for
//! a temp dir and `DAMON_TEST_TOKEN_URL` points the refresh at a local mock
//! — both are process-global, so every test holds `ENV_LOCK`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::post;
use damon_core::oauth::{self, OAuthTokens};

/// Serializes tests that mutate the process-wide env vars.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Mock {
    hits: AtomicUsize,
    status: StatusCode,
    body: serde_json::Value,
    last_request: parking_lot::Mutex<Option<serde_json::Value>>,
}

async fn token_handler(
    State(mock): State<Arc<Mock>>,
    Json(req): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    mock.hits.fetch_add(1, Ordering::SeqCst);
    *mock.last_request.lock() = Some(req);
    (mock.status, Json(mock.body.clone()))
}

/// Spawn a mock token endpoint; returns (base_url, mock state).
async fn mock_server(status: StatusCode, body: serde_json::Value) -> (String, Arc<Mock>) {
    let mock = Arc::new(Mock {
        hits: AtomicUsize::new(0),
        status,
        body,
        last_request: parking_lot::Mutex::new(None),
    });
    let app = axum::Router::new()
        .route("/token", post(token_handler))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/token"), mock)
}

/// Point the token store at a fresh temp dir and the endpoint at `url`.
/// Returns the temp dir path (caller removes it).
fn setup_env(provider: &str, url: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("damon-oauth-test-{provider}"));
    // SAFETY: serialized by ENV_LOCK; no other thread reads these vars
    // while a test holds it.
    unsafe {
        std::env::set_var("DAMON_TEST_TOKEN_DIR", &dir);
        std::env::set_var("DAMON_TEST_TOKEN_URL", url);
    }
    dir
}

fn seed_tokens(dir: &std::path::Path, provider: &str, tokens: &OAuthTokens) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(
        dir.join(format!("{provider}.json")),
        serde_json::to_string(tokens).unwrap(),
    )
    .unwrap();
}

fn expired() -> OAuthTokens {
    OAuthTokens {
        access_token: "old-access".into(),
        refresh_token: "rt-1".into(),
        expires_at: 0,
    }
}

#[tokio::test]
async fn expired_token_refreshes_and_stores() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "test-refresh";
    let (url, mock) = mock_server(
        StatusCode::OK,
        serde_json::json!({
            "access_token": "new-access",
            "refresh_token": "rt-2",
            "expires_in": 3600,
        }),
    )
    .await;
    let dir = setup_env(provider, &url);
    seed_tokens(&dir, provider, &expired());

    let token = oauth::access_token(provider).await.unwrap();
    assert_eq!(token, "new-access");
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);

    // The refresh request carried the old refresh_token.
    let req = mock.last_request.lock().clone().unwrap();
    assert_eq!(req["grant_type"], "refresh_token");
    assert_eq!(req["refresh_token"], "rt-1");

    // Rotated tokens were persisted.
    let stored = oauth::load(provider).unwrap().unwrap();
    assert_eq!(stored.access_token, "new-access");
    assert_eq!(stored.refresh_token, "rt-2");
    assert!(!stored.is_expired());

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn invalid_grant_returns_err() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "test-grant";
    let (url, mock) = mock_server(
        StatusCode::BAD_REQUEST,
        serde_json::json!({"error": "invalid_grant"}),
    )
    .await;
    let dir = setup_env(provider, &url);
    seed_tokens(&dir, provider, &expired());

    let err = oauth::access_token(provider).await.unwrap_err();
    assert!(err.to_string().contains("invalid_grant"), "{err}");
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn concurrent_refresh_is_single_flight() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "test-flight";
    let (url, mock) = mock_server(
        StatusCode::OK,
        serde_json::json!({
            "access_token": "flight-access",
            "expires_in": 3600,
        }),
    )
    .await;
    let dir = setup_env(provider, &url);
    seed_tokens(&dir, provider, &expired());

    let (a, b) = tokio::join!(oauth::access_token(provider), oauth::access_token(provider));
    assert_eq!(a.unwrap(), "flight-access");
    assert_eq!(b.unwrap(), "flight-access");
    // The second caller waited on the lock, re-loaded the fresh tokens,
    // and never hit the endpoint.
    assert_eq!(mock.hits.load(Ordering::SeqCst), 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn fresh_token_skips_refresh() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "test-fresh";
    let (url, mock) = mock_server(StatusCode::OK, serde_json::json!({})).await;
    let dir = setup_env(provider, &url);
    seed_tokens(
        &dir,
        provider,
        &OAuthTokens {
            access_token: "still-good".into(),
            refresh_token: "rt-1".into(),
            expires_at: i64::MAX,
        },
    );

    let token = oauth::access_token(provider).await.unwrap();
    assert_eq!(token, "still-good");
    assert_eq!(mock.hits.load(Ordering::SeqCst), 0);

    std::fs::remove_dir_all(&dir).ok();
}
