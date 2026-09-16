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
    };
    Arc::new(parking_lot::RwLock::new(cfg))
}

/// Mock upstream that echoes the Authorization header it received.
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
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(format!("data: {{\"auth\":\"{auth}\"}}\n\ndata: [DONE]\n\n")))
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
    assert!(err.contains("env:") || err.contains("keychain:"), "got: {err}");
}
