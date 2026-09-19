use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use damon_core::api::{self, AppState};
use damon_core::config::{Config, ProviderConfig};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn test_config(base_url: &str, auth_token: Option<&str>) -> damon_core::config::SharedConfig {
    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(base_url.to_string()),
            api_key: Some("env:DAMON_TEST_KEY".to_string()),
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let cfg = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: auth_token.map(String::from),
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
    };
    Arc::new(parking_lot::RwLock::new(cfg))
}

/// Mock upstream that echoes the Authorization header and the model name
/// it received in the request body — the wire model is what routing
/// assertions read back.
async fn mock_upstream() -> String {
    let app = Router::new().route(
        "/chat/completions",
        post(|req: Request<Body>| async move {
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let bytes = req.into_body().collect().await.unwrap().to_bytes();
            let v: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({}));
            let model = v["model"].as_str().unwrap_or("").to_string();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(format!(
                    "data: {{\"auth\":\"{auth}\",\"model\":\"{model}\"}}\n\ndata: [DONE]\n\n"
                )))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

async fn app(base_url: &str, token: Option<&str>) -> Router {
    let shared = test_config(base_url, token);
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store, mcp).await;
    api::router(state)
}

#[tokio::test]
async fn health_is_open() {
    let app = app("http://127.0.0.1:1", None).await;
    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn passthrough_injects_provider_key() {
    unsafe { std::env::set_var("DAMON_TEST_KEY", "sk-test-123") };
    let upstream = mock_upstream().await;
    let app = app(&format!("http://{upstream}"), None).await;

    let resp = app
        .oneshot(
            Request::post("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("Bearer sk-test-123"), "got: {text}");
}
/// The passthrough branch must forward the RESOLVED upstream model, not
/// the client's `provider/model:level` string — prefix routing and
/// thinking-suffix selection previously reached the upstream verbatim
/// and were rejected as unknown models.
#[tokio::test]
async fn passthrough_rewrites_routed_model() {
    unsafe { std::env::set_var("DAMON_TEST_KEY", "sk-test-123") };
    let upstream = mock_upstream().await;
    let app = app(&format!("http://{upstream}"), None).await;

    let resp = app
        .oneshot(
            Request::post("/v1/chat/completions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"model":"default/gpt-4o:high","messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("\"model\":\"gpt-4o\""), "got: {text}");
    assert!(!text.contains("default/gpt-4o"), "got: {text}");
}

#[tokio::test]
async fn token_gate_blocks_and_allows() {
    unsafe { std::env::set_var("DAMON_TEST_KEY", "sk-test-123") };
    let upstream = mock_upstream().await;
    let app = app(&format!("http://{upstream}"), Some("secret-token")).await;

    // No token → 401
    let resp = app
        .clone()
        .oneshot(
            Request::post("/v1/chat/completions")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Wrong token → 401
    let resp = app
        .clone()
        .oneshot(
            Request::post("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer wrong")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Right token → 200
    let resp = app
        .oneshot(
            Request::post("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer secret-token")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn no_provider_returns_503_openai_error() {
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers: BTreeMap::new(),
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
    }));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let app = api::router(AppState::new(shared, store, mcp).await);
    let resp = app
        .oneshot(
            Request::post("/v1/chat/completions")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["type"], "server_error");
}

#[test]
fn literal_api_key_is_rejected() {
    let toml = r#"
[providers.default]
base_url = "http://x"
api_key = "sk-literal-secret"
"#;
    let dir = std::env::temp_dir().join(format!("damon-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bad.toml");
    std::fs::write(&path, toml).unwrap();
    let err = format!("{:#}", Config::load(&path).unwrap_err());
    assert!(
        err.contains("env:") || err.contains("keychain:"),
        "got: {err}"
    );
}

#[tokio::test]
async fn metrics_requires_token_and_exposes_counters() {
    let app = app("http://127.0.0.1:1", Some("secret-token")).await;

    // No token → 401
    let resp = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Token → 200 with Prometheus text
    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header(header::AUTHORIZATION, "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    for name in [
        "damon_requests_total",
        "damon_prompts_total",
        "damon_tokens_input_total",
        "damon_tokens_output_total",
        "damon_active_sessions",
    ] {
        assert!(text.contains(name), "missing {name} in: {text}");
    }
}

#[tokio::test]
async fn ws_ticket_requires_token_and_returns_hex() {
    let app = app("http://127.0.0.1:1", Some("secret-token")).await;

    // No token → 401
    let resp = app
        .clone()
        .oneshot(Request::post("/v1/ws_ticket").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Token → 200 with a 32-byte hex ticket
    let resp = app
        .oneshot(
            Request::post("/v1/ws_ticket")
                .header(header::AUTHORIZATION, "Bearer secret-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ticket = json["ticket"].as_str().expect("ticket field");
    assert_eq!(ticket.len(), 64);
    assert!(ticket.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn token_bucket_allows_burst_then_throttles() {
    let mut bucket = (20.0, std::time::Instant::now());
    for _ in 0..20 {
        assert!(api::bucket_allow(&mut bucket));
    }
    assert!(!api::bucket_allow(&mut bucket));
    // A minute of elapsed time refills the bucket.
    if let Some(past) = bucket.1.checked_sub(std::time::Duration::from_secs(60)) {
        bucket.1 = past;
        assert!(api::bucket_allow(&mut bucket));
    }
}

#[tokio::test]
async fn auth_token_cache_re_resolves_after_ttl() {
    // A `!cmd` secret whose output changes between resolutions.
    let dir = std::env::temp_dir().join(format!("damon-ttl-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("token");
    std::fs::write(&path, "first").unwrap();

    let shared = test_config(
        "http://127.0.0.1:1",
        Some(&format!("!cat {}", path.display())),
    );

    // Within the TTL the cached value is served even though the
    // command's output changed. The window is wide (30s) so no realistic
    // scheduler pause between the two awaits can outlast it.
    let state = AppState::new(
        shared.clone(),
        damon_core::store::Store::in_memory().await.unwrap(),
        damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await,
    )
    .await;
    let ttl = std::time::Duration::from_secs(30);
    let tok1 = state.auth_token_cached(ttl).await.unwrap().unwrap();
    assert_eq!(tok1, "first");
    std::fs::write(&path, "second").unwrap();
    let tok2 = state.auth_token_cached(ttl).await.unwrap().unwrap();
    assert_eq!(tok2, "first");

    // After expiry the secret is re-resolved. Fresh state and a tiny
    // ttl: expiry is carried by an at-least sleep (100ms ≫ 10ms), never
    // by an un-sleepable gap between two awaits — the naive version of
    // this arm flaked on loaded CI runners.
    let state = AppState::new(
        shared,
        damon_core::store::Store::in_memory().await.unwrap(),
        damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await,
    )
    .await;
    let ttl = std::time::Duration::from_millis(10);
    let tok3 = state.auth_token_cached(ttl).await.unwrap().unwrap();
    assert_eq!(tok3, "second");
    std::fs::write(&path, "third").unwrap();
    tokio::time::sleep(ttl * 20).await;
    let tok4 = state.auth_token_cached(ttl).await.unwrap().unwrap();
    assert_eq!(tok4, "third");
}
