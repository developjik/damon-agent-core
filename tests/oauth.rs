//! OAuth refresh flow tests. `DAMON_TEST_TOKEN_DIR` swaps the keychain for
//! a temp dir and `DAMON_TEST_TOKEN_URL` points the refresh at a local mock
//! — both are process-global, so every test holds `ENV_LOCK`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::post;
use base64::Engine;
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
        account_id: None,
    }
}

/// Fake id_token JWT whose payload carries the given ChatGPT account id.
fn id_token_with_account(account: &str) -> String {
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let header = b64(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = b64(&serde_json::to_vec(&serde_json::json!({
        "https://api.openai.com/auth": {"chatgpt_account_id": account},
    }))
    .unwrap());
    format!("{header}.{payload}.c2ln")
}

#[tokio::test]
async fn expired_token_refreshes_and_stores() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "anthropic";
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
    let provider = "anthropic";
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
    let provider = "anthropic";
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
    let provider = "anthropic";
    let (url, mock) = mock_server(StatusCode::OK, serde_json::json!({})).await;
    let dir = setup_env(provider, &url);
    seed_tokens(
        &dir,
        provider,
        &OAuthTokens {
            access_token: "still-good".into(),
            refresh_token: "rt-1".into(),
            expires_at: i64::MAX,
            account_id: None,
        },
    );

    let token = oauth::access_token(provider).await.unwrap();
    assert_eq!(token, "still-good");
    assert_eq!(mock.hits.load(Ordering::SeqCst), 0);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn openai_refresh_carries_account_id_and_scope() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "openai";
    let (url, mock) = mock_server(
        StatusCode::OK,
        serde_json::json!({
            "access_token": "new-access",
            "refresh_token": "rt-2",
            "expires_in": 3600,
            "id_token": id_token_with_account("acc-new"),
        }),
    )
    .await;
    let dir = setup_env(provider, &url);
    seed_tokens(&dir, provider, &expired());

    let cred = oauth::credential(provider).await.unwrap();
    assert_eq!(cred.access_token, "new-access");
    assert_eq!(cred.account_id.as_deref(), Some("acc-new"));

    // The refresh request speaks the Codex dialect.
    let req = mock.last_request.lock().clone().unwrap();
    assert_eq!(req["grant_type"], "refresh_token");
    assert_eq!(req["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");
    assert_eq!(req["scope"], "openid profile email");

    // Account id was persisted alongside the rotated tokens.
    let stored = oauth::load(provider).unwrap().unwrap();
    assert_eq!(stored.account_id.as_deref(), Some("acc-new"));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn openai_refresh_keeps_previous_account_id_without_claim() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "openai";
    let (url, _mock) = mock_server(
        StatusCode::OK,
        serde_json::json!({
            "access_token": "new-access",
            "expires_in": 3600,
        }),
    )
    .await;
    let dir = setup_env(provider, &url);
    seed_tokens(
        &dir,
        provider,
        &OAuthTokens {
            access_token: "old".into(),
            refresh_token: "rt-1".into(),
            expires_at: 0,
            account_id: Some("acc-old".into()),
        },
    );

    let cred = oauth::credential(provider).await.unwrap();
    assert_eq!(cred.access_token, "new-access");
    // No id_token in the response → the stored account id survives.
    assert_eq!(cred.account_id.as_deref(), Some("acc-old"));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn openai_exchange_accepts_pasted_callback_url() {
    let _guard = ENV_LOCK.lock().await;
    let provider = "openai";
    let (url, mock) = mock_server(
        StatusCode::OK,
        serde_json::json!({
            "access_token": "at",
            "refresh_token": "rt",
            "expires_in": 3600,
            "id_token": id_token_with_account("acc-login"),
        }),
    )
    .await;
    let dir = setup_env(provider, &url);

    let (_, verifier) = oauth::authorize_url(provider).unwrap();
    oauth::exchange(
        provider,
        "http://localhost:1455/auth/callback?code=xyz&state=WRONG",
        &verifier,
    )
    .await
    .unwrap_err(); // state must match the verifier we sent

    oauth::exchange(
        provider,
        &format!("http://localhost:1455/auth/callback?code=xyz&state={verifier}"),
        &verifier,
    )
    .await
    .unwrap();

    // The token request speaks the Codex dialect.
    let req = mock.last_request.lock().clone().unwrap();
    assert_eq!(req["grant_type"], "authorization_code");
    assert_eq!(req["code"], "xyz");
    assert_eq!(req["code_verifier"], verifier);
    assert_eq!(req["redirect_uri"], "http://localhost:1455/auth/callback");
    assert_eq!(req["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");

    let stored = oauth::load(provider).unwrap().unwrap();
    assert_eq!(stored.access_token, "at");
    assert_eq!(stored.account_id.as_deref(), Some("acc-login"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn authorize_url_rejects_unknown_providers() {
    assert!(oauth::authorize_url("nope").is_err());
}

// ─── RFC 8628 device flows: kimi-code, xai-oauth, github-copilot ───

use axum::extract::State as ExtractState;

const KIMI_CLIENT: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
const XAI_CLIENT: &str = "b1a00492-073a-47ea-816f-4c329264a828";
const GITHUB_CLIENT: &str = "Ov23li8tweQw6odWQebz";

struct DeviceMock {
    device_hits: AtomicUsize,
    token_hits: AtomicUsize,
    token_bodies: Vec<serde_json::Value>,
    last_device_request: parking_lot::Mutex<Option<String>>,
    last_token_request: parking_lot::Mutex<Option<String>>,
}

async fn device_mock(
    device_resp: serde_json::Value,
    token_bodies: Vec<serde_json::Value>,
) -> (String, std::sync::Arc<DeviceMock>) {
    let mock = std::sync::Arc::new(DeviceMock {
        device_hits: AtomicUsize::new(0),
        token_hits: AtomicUsize::new(0),
        token_bodies,
        last_device_request: parking_lot::Mutex::new(None),
        last_token_request: parking_lot::Mutex::new(None),
    });
    let s = mock.clone();
    let app = axum::Router::new()
        .route(
            "/device",
            axum::routing::post(move |body: String| {
                let s = s.clone();
                async move {
                    s.device_hits.fetch_add(1, Ordering::SeqCst);
                    *s.last_device_request.lock() = Some(body);
                    Json(device_resp.clone())
                }
            }),
        )
        .route(
            "/token",
            post(
                move |State(mock): ExtractState<std::sync::Arc<DeviceMock>>, body: String| {
                    let s = mock.clone();
                    async move {
                        let n = s.token_hits.fetch_add(1, Ordering::SeqCst);
                        *s.last_token_request.lock() = Some(body);
                        let body = s
                            .token_bodies
                            .get(n)
                            .unwrap_or_else(|| s.token_bodies.last().unwrap());
                        (StatusCode::OK, Json(body.clone()))
                    }
                },
            ),
        )
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), mock)
}

fn wire_device_env(base: &str, dir: &str) {
    // SAFETY: callers hold ENV_LOCK; vars removed by clear_device_env.
    unsafe {
        std::env::set_var("DAMON_TEST_DEVICE_URL", format!("{base}/device"));
        std::env::set_var("DAMON_TEST_TOKEN_URL", format!("{base}/token"));
        std::env::set_var("DAMON_TEST_TOKEN_DIR", std::env::temp_dir().join(dir));
    }
}

fn clear_device_env(dir: &str) {
    // SAFETY: callers hold ENV_LOCK.
    unsafe {
        std::env::remove_var("DAMON_TEST_DEVICE_URL");
        std::env::remove_var("DAMON_TEST_TOKEN_URL");
        std::env::remove_var("DAMON_TEST_TOKEN_DIR");
    }
    std::fs::remove_dir_all(std::env::temp_dir().join(dir)).ok();
}

/// Decode a urlencoded form body into (key, decoded-value) pairs.
fn form_pairs(body: &str) -> Vec<(String, String)> {
    body.split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((
                k.to_string(),
                v.replace("%20", " ").replace('+', " ").replace("%3A", ":"),
            ))
        })
        .collect()
}

fn device_offer() -> serde_json::Value {
    serde_json::json!({
        "user_code": "ABCD-EFGH",
        "device_code": "dc-1",
        "verification_uri": "https://auth.example/device",
        "verification_uri_complete": "https://auth.example/device?code=ABCD-EFGH",
        "expires_in": 600,
        "interval": 0,
    })
}

#[allow(dead_code)]
fn form_get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

#[test]
fn device_flavors_are_device_only() {
    assert!(oauth::is_device_flow("kimi-code"));
    assert!(oauth::is_device_flow("xai-oauth"));
    assert!(oauth::is_device_flow("github-copilot"));
    assert!(!oauth::is_device_flow("anthropic"));
    assert!(!oauth::is_device_flow("openai"));
}

#[tokio::test]
async fn kimi_device_flow_logs_in_and_refreshes() {
    let _guard = ENV_LOCK.lock().await;
    let dir = "damon-oauth-device-kimi";
    let (base, mock) = device_mock(
        device_offer(),
        vec![
            serde_json::json!({"error": "authorization_pending"}),
            serde_json::json!({
                "access_token": "k-ac", "refresh_token": "k-rt",
                "expires_in": 3600, "scope": "openid", "token_type": "bearer",
            }),
            serde_json::json!({
                "access_token": "k-ac2", "refresh_token": "k-rt2",
                "expires_in": 3600, "scope": "openid", "token_type": "bearer",
            }),
        ],
    )
    .await;
    wire_device_env(&base, dir);

    let offer = oauth::device_authorization("kimi-code").await.unwrap();
    assert_eq!(offer.user_code, "ABCD-EFGH");
    assert_eq!(offer.device_code, "dc-1");
    let dev_req = mock.last_device_request.lock().clone().unwrap();
    let pairs = form_pairs(&dev_req);
    assert_eq!(form_get(&pairs, "client_id"), Some(KIMI_CLIENT));

    let cred = oauth::device_poll("kimi-code", &offer).await.unwrap();
    assert_eq!(cred.access_token, "k-ac");

    let tok_req = mock.last_token_request.lock().clone().unwrap();
    let pairs = form_pairs(&tok_req);
    assert_eq!(
        form_get(&pairs, "grant_type"),
        Some("urn:ietf:params:oauth:grant-type:device_code")
    );
    assert_eq!(form_get(&pairs, "client_id"), Some(KIMI_CLIENT));
    assert_eq!(form_get(&pairs, "device_code"), Some("dc-1"));

    // Rotated refresh goes through the same token endpoint.
    let c2 = oauth::force_credential("kimi-code").await.unwrap();
    assert_eq!(c2.access_token, "k-ac2");
    let tok_req = mock.last_token_request.lock().clone().unwrap();
    let pairs = form_pairs(&tok_req);
    assert_eq!(form_get(&pairs, "grant_type"), Some("refresh_token"));
    assert_eq!(form_get(&pairs, "refresh_token"), Some("k-rt"));

    let stored = oauth::load("kimi-code").unwrap().unwrap();
    assert_eq!(stored.access_token, "k-ac2");

    clear_device_env(dir);
}

#[tokio::test]
async fn copilot_device_flow_stores_long_lived_token() {
    let _guard = ENV_LOCK.lock().await;
    let dir = "damon-oauth-device-copilot";
    let (base, mock) = device_mock(
        device_offer(),
        vec![serde_json::json!({
            "access_token": "gho_copilot", "token_type": "bearer", "scope": "read:user",
        })],
    )
    .await;
    wire_device_env(&base, dir);

    let offer = oauth::device_authorization("github-copilot").await.unwrap();
    let dev_req = mock.last_device_request.lock().clone().unwrap();
    let pairs = form_pairs(&dev_req);
    assert_eq!(form_get(&pairs, "client_id"), Some(GITHUB_CLIENT));
    assert_eq!(form_get(&pairs, "scope"), Some("read:user"));

    let cred = oauth::device_poll("github-copilot", &offer).await.unwrap();
    assert_eq!(cred.access_token, "gho_copilot");

    let stored = oauth::load("github-copilot").unwrap().unwrap();
    assert!(!stored.is_expired(), "copilot tokens are long-lived");
    // force_refresh cannot rotate a GitHub token — hands back the stored one.
    let again = oauth::force_credential("github-copilot").await.unwrap();
    assert_eq!(again.access_token, "gho_copilot");
    assert_eq!(mock.token_hits.load(Ordering::SeqCst), 1);

    clear_device_env(dir);
}

#[tokio::test]
async fn xai_device_flow_uses_scope_and_refresh() {
    let _guard = ENV_LOCK.lock().await;
    let dir = "damon-oauth-device-xai";
    let (base, mock) = device_mock(
        device_offer(),
        vec![
            serde_json::json!({
                "access_token": "x-ac", "refresh_token": "x-rt",
                "expires_in": 3600, "token_type": "bearer",
            }),
            serde_json::json!({
                "access_token": "x-ac2", "refresh_token": "x-rt2",
                "expires_in": 3600, "token_type": "bearer",
            }),
        ],
    )
    .await;
    wire_device_env(&base, dir);

    let offer = oauth::device_authorization("xai-oauth").await.unwrap();
    let dev_req = mock.last_device_request.lock().clone().unwrap();
    let pairs = form_pairs(&dev_req);
    assert_eq!(form_get(&pairs, "client_id"), Some(XAI_CLIENT));
    assert_eq!(
        form_get(&pairs, "scope"),
        Some("openid profile email offline_access grok-cli:access api:access")
    );

    oauth::device_poll("xai-oauth", &offer).await.unwrap();
    // Expire the stored set so the next credential() refreshes.
    let dir_p = std::env::temp_dir().join(dir);
    std::fs::write(
        dir_p.join("xai-oauth.json"),
        serde_json::to_string(&OAuthTokens {
            access_token: "x-ac".into(),
            refresh_token: "x-rt".into(),
            expires_at: 0,
            account_id: None,
        })
        .unwrap(),
    )
    .unwrap();
    let cred = oauth::credential("xai-oauth").await.unwrap();
    assert_eq!(cred.access_token, "x-ac2");
    let tok_req = mock.last_token_request.lock().clone().unwrap();
    let pairs = form_pairs(&tok_req);
    assert_eq!(form_get(&pairs, "grant_type"), Some("refresh_token"));

    clear_device_env(dir);
}
