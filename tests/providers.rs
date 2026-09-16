//! Provider adapter tests: translation correctness and model routing.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::body::Body;
use axum::response::Response;
use axum::routing::post;
use axum::{Json, Router};
use damon_core::config::{Config, ProviderConfig, glob_match};
use serde_json::{Value, json};

fn provider(kind: &str, base_url: &str) -> ProviderConfig {
    ProviderConfig {
        api: kind.to_string(),
        base_url: Some(base_url.to_string()),
        api_key: None,
        models: vec![],
        default_model: None,
        headers: Default::default(),
        compat: Default::default(),
        discovery: None,
        context_promotion_target: None,
    }
}

#[test]
fn glob_routing() {
    assert!(glob_match("claude-*", "claude-sonnet-4"));
    assert!(glob_match("gpt-*", "gpt-4o"));
    assert!(!glob_match("claude-*", "gpt-4o"));
    assert!(glob_match("*", "anything"));
    assert!(glob_match("gpt-4?", "gpt-4o"));
}

#[test]
fn route_model_prefix_and_glob() {
    let mut providers = BTreeMap::new();
    let mut claude = provider("anthropic-messages", "https://api.anthropic.com");
    claude.models = vec!["claude-*".to_string()];
    providers.insert("claude".to_string(), claude);
    providers.insert(
        "default".to_string(),
        provider("openai-completions", "https://api.openai.com/v1"),
    );
    let cfg = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
    };

    // Explicit prefix wins.
    let (name, _, upstream) = cfg.route_model("claude/claude-sonnet-4").unwrap();
    assert_eq!(name, "claude");
    assert_eq!(upstream, "claude-sonnet-4");

    // Glob match.
    let (name, _, _) = cfg.route_model("claude-opus-4").unwrap();
    assert_eq!(name, "claude");

    // No match → default.
    let (name, _, _) = cfg.route_model("gpt-4o").unwrap();
    assert_eq!(name, "default");
}

/// Mock Anthropic endpoint: captures the request body, returns a fixed
/// Messages API response.
async fn mock_anthropic() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/v1/messages",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                captured
                    .lock()
                    .await
                    .push(serde_json::from_str(&body).unwrap());
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"content":[{"type":"text","text":"hi from claude"}],"usage":{}}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn anthropic_translates_request_and_response() {
    let (base, captured) = mock_anthropic().await;
    let p = damon_core::provider::Provider::new("claude", &provider("anthropic-messages", &base))
        .unwrap();

    let body = json!({
        "model": "claude-sonnet-4",
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
        ],
        "tools": [{
            "type": "function",
            "function": {"name": "fs.read", "description": "read", "parameters": {}}
        }],
    });
    let resp = p.chat(body).await.unwrap();

    // Request translation: system extracted, tool name mangled.
    let sent = &captured.lock().await[0];
    assert_eq!(sent["system"][0]["text"], "be brief");
    assert_eq!(sent["system"][0]["cache_control"]["type"], "ephemeral");
    assert_eq!(sent["tools"][0]["name"], "fs__read");
    assert_eq!(sent["messages"][0]["role"], "user");

    // Response translation: OpenAI shape.
    assert_eq!(resp["choices"][0]["message"]["content"], "hi from claude");
}

/// A persisted thinking block must be replayed verbatim (signature
/// included) ahead of the assistant's text on the next request.
#[tokio::test]
async fn anthropic_replays_thinking_blocks() {
    let (base, captured) = mock_anthropic().await;
    let p = damon_core::provider::Provider::new("claude", &provider("anthropic-messages", &base))
        .unwrap();

    let body = json!({
        "model": "claude-sonnet-4",
        "messages": [
            {"role": "user", "content": "hi"},
            {
                "role": "assistant",
                "content": "ok",
                "thinking": [{
                    "type": "thinking",
                    "thinking": "let me think",
                    "signature": "sig123",
                }],
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "fs.read", "arguments": "{}"},
                }],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "data"},
        ],
    });
    let _ = p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    let assistant = &sent["messages"][1];
    assert_eq!(assistant["role"], "assistant");
    // Thinking block first, then text, then tool_use.
    assert_eq!(assistant["content"][0]["type"], "thinking");
    assert_eq!(assistant["content"][0]["thinking"], "let me think");
    assert_eq!(assistant["content"][0]["signature"], "sig123");
    assert_eq!(assistant["content"][1]["type"], "text");
    assert_eq!(assistant["content"][2]["type"], "tool_use");
}

/// Mock Gemini endpoint.
async fn mock_gemini() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/v1beta/models/{*path}",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                captured.lock().await.push(serde_json::from_str(&body).unwrap());
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"candidates":[{"content":{"parts":[{"text":"hi from gemini"}]},"finishReason":"STOP"}],"usageMetadata":{}}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn gemini_translates_request_and_response() {
    let (base, captured) = mock_gemini().await;
    let p = damon_core::provider::Provider::new("gemini", &provider("gemini", &base)).unwrap();

    let body = json!({
        "model": "gemini-2.5-flash",
        "messages": [{"role": "user", "content": "hi"}],
    });
    let resp = p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    assert_eq!(sent["contents"][0]["role"], "user");
    assert_eq!(resp["choices"][0]["message"]["content"], "hi from gemini");
}

/// End-to-end: /v1/chat/completions with model routing to anthropic.
#[tokio::test]
async fn v1_routes_to_anthropic_by_model() {
    let (base, captured) = mock_anthropic().await;
    let mut providers = BTreeMap::new();
    let mut claude = provider("anthropic-messages", &base);
    claude.models = vec!["claude-*".to_string()];
    providers.insert("claude".to_string(), claude);
    providers.insert(
        "default".to_string(),
        provider("openai-completions", "http://127.0.0.1:1"),
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
    }));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let app = damon_core::api::router(damon_core::api::AppState::new(shared, store, mcp).await);

    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let resp = app
        .oneshot(
            axum::http::Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"claude-sonnet-4","messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "hi from claude");
    // Verify the request actually went to the anthropic mock.
    assert_eq!(captured.lock().await.len(), 1);
}

/// Mock OpenAI Responses endpoint: captures the request, returns a fixed
/// responses-API payload.
async fn mock_responses() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/responses",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                captured.lock().await.push(serde_json::from_str(&body).unwrap());
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"hi from responses"}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn responses_translates_request_and_response() {
    let (base, captured) = mock_responses().await;
    let p =
        damon_core::provider::Provider::new("o3", &provider("openai-responses", &base)).unwrap();

    let body = json!({
        "model": "o3",
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": "hello", "tool_calls": [{
                "id": "call_1", "type": "function",
                "function": {"name": "fs.read", "arguments": "{}"}
            }]},
            {"role": "tool", "tool_call_id": "call_1", "content": "file contents"},
        ],
        "tools": [{
            "type": "function",
            "function": {"name": "fs.read", "description": "read", "parameters": {}}
        }],
        "max_tokens": 1024,
    });
    let resp = p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    // system → instructions
    assert_eq!(sent["instructions"], "be brief");
    // user → input message
    assert_eq!(sent["input"][0]["type"], "message");
    assert_eq!(sent["input"][0]["role"], "user");
    // assistant tool_call → function_call item
    assert_eq!(sent["input"][2]["type"], "function_call");
    assert_eq!(sent["input"][2]["call_id"], "call_1");
    // tool result → function_call_output
    assert_eq!(sent["input"][3]["type"], "function_call_output");
    // max_tokens → max_output_tokens
    assert_eq!(sent["max_output_tokens"], 1024);
    // tools flattened
    assert_eq!(sent["tools"][0]["type"], "function");
    assert_eq!(sent["tools"][0]["name"], "fs.read");

    // Response translated back to OpenAI shape.
    assert_eq!(
        resp["choices"][0]["message"]["content"],
        "hi from responses"
    );
    assert_eq!(resp["usage"]["prompt_tokens"], 1);
}

/// Mock chat-completions endpoint that captures the request body.
async fn mock_completions() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                captured.lock().await.push(serde_json::from_str(&body).unwrap());
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn compat_flags_shape_request() {
    let (base, captured) = mock_completions().await;
    let mut cfg = provider("openai-completions", &base);
    cfg.compat = damon_core::config::ProviderCompat {
        supports_store: true,
        supports_developer_role: true,
        max_tokens_field: Some("max_completion_tokens".into()),
        requires_tool_result_name: true,
        requires_mistral_tool_ids: true,
        extra_body: [("gateway".into(), json!("m1-01"))].into_iter().collect(),
        ..Default::default()
    };
    let p = damon_core::provider::Provider::new("test", &cfg).unwrap();

    let body = json!({
        "model": "test-model",
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_abc123def456", "type": "function",
                "function": {"name": "fs.read", "arguments": "{}"}
            }]},
            {"role": "tool", "tool_call_id": "call_abc123def456", "content": "data"},
        ],
        "max_tokens": 512,
    });
    p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    // store: false injected
    assert_eq!(sent["store"], false);
    // max_tokens renamed
    assert_eq!(sent["max_completion_tokens"], 512);
    assert!(sent.get("max_tokens").is_none());
    // extra_body merged
    assert_eq!(sent["gateway"], "m1-01");
    // system → developer
    assert_eq!(sent["messages"][0]["role"], "developer");
    // tool_call id normalized to 9 chars
    let id = sent["messages"][2]["tool_calls"][0]["id"].as_str().unwrap();
    assert_eq!(id.len(), 9);
    assert!(id.chars().all(|c| c.is_ascii_alphanumeric()));
    // tool result got name + matching id
    assert_eq!(sent["messages"][3]["name"], "fs.read");
    assert_eq!(sent["messages"][3]["tool_call_id"].as_str().unwrap(), id);
}

#[tokio::test]
async fn compat_coalesces_system_messages() {
    let (base, captured) = mock_completions().await;
    let mut cfg = provider("openai-completions", &base);
    cfg.compat = damon_core::config::ProviderCompat {
        supports_multiple_system_messages: false,
        ..Default::default()
    };
    let p = damon_core::provider::Provider::new("test", &cfg).unwrap();

    let body = json!({
        "model": "m",
        "messages": [
            {"role": "system", "content": "one"},
            {"role": "system", "content": "two"},
            {"role": "user", "content": "hi"},
        ],
    });
    p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    assert_eq!(sent["messages"].as_array().unwrap().len(), 2);
    assert_eq!(sent["messages"][0]["content"], "one\ntwo");
}

#[test]
fn command_secret_resolves() {
    let r = damon_core::config::SecretRef::parse("!printf testkey123").unwrap();
    assert_eq!(r.resolve().unwrap(), "testkey123");
    // Failing command is an error, not a silent empty key.
    assert!(
        damon_core::config::SecretRef::parse("!false")
            .unwrap()
            .resolve()
            .is_err()
    );
}

/// Mock /models endpoint for discovery.
async fn mock_models_list() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new()
        .route(
            "/models",
            axum::routing::get(|| async {
                Json(json!({"object":"list","data":[{"id":"local-model-7b"}]}))
            }),
        )
        .route(
            "/chat/completions",
            post(move |body: String| {
                let captured = captured2.clone();
                async move {
                    captured.lock().await.push(serde_json::from_str(&body).unwrap());
                    Response::builder()
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"local ok"},"finish_reason":"stop"}]}"#,
                        ))
                        .unwrap()
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

/// Discovered model ids route to their provider without a `models` glob.
#[tokio::test]
async fn discovered_model_routes_to_provider() {
    let (base, captured) = mock_models_list().await;
    let mut local = provider("openai-completions", &base);
    local.discovery = Some("openai-models-list".to_string());
    let mut providers = BTreeMap::new();
    providers.insert("local".to_string(), local);
    // Default provider is a dead endpoint — if routing falls through to it,
    // the request fails. Discovery must claim "local-model-7b" first.
    providers.insert(
        "default".to_string(),
        provider("openai-completions", "http://127.0.0.1:1"),
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
    }));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let app = damon_core::api::router(damon_core::api::AppState::new(shared, store, mcp).await);

    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let resp = app
        .oneshot(
            axum::http::Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"local-model-7b","messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "local ok");
    assert_eq!(captured.lock().await.len(), 1);
}

/// Ollama discovery probe: /api/tags → model names.
#[tokio::test]
async fn ollama_discovery_reads_tags() {
    let app = Router::new().route(
        "/api/tags",
        axum::routing::get(|| async {
            Json(json!({"models":[{"name":"llama3.2:latest"},{"name":"qwen3:8b"}]}))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");

    let mut cfg = provider("openai-completions", &base);
    cfg.discovery = Some("ollama".to_string());
    let mut providers = BTreeMap::new();
    providers.insert("ollama".to_string(), cfg);
    let config = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
    };
    let mut built: std::collections::HashMap<
        String,
        std::sync::Arc<damon_core::provider::Provider>,
    > = std::collections::HashMap::new();
    let found = damon_core::provider::discovery::discover_all(&config, &mut built).await;
    assert_eq!(
        found["ollama"],
        vec!["llama3.2:latest".to_string(), "qwen3:8b".to_string()]
    );
}

#[test]
fn partial_json_repairs_truncated_args() {
    use damon_core::llm::parse_partial_json;
    // Complete
    assert_eq!(parse_partial_json(r#"{"a":1}"#)["a"], 1);
    // Truncated mid-string
    assert_eq!(parse_partial_json(r#"{"path":"/tmp/fi"#)["path"], "/tmp/fi");
    // Truncated mid-object
    assert_eq!(parse_partial_json(r##"{"a":1,"b":""##)["a"], 1);
    // Empty → {}
    assert_eq!(parse_partial_json(""), json!({}));
    // Garbage → {}
    assert_eq!(parse_partial_json("not json"), json!({}));
}

/// Mock completions endpoint that 400s on strict tools, then succeeds.
async fn mock_strict_fallback() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                let v: Value = serde_json::from_str(&body).unwrap();
                captured.lock().await.push(v.clone());
                let has_strict = v["tools"].as_array().is_some_and(|t| {
                    t.iter().any(|t| t["strict"].as_bool() == Some(true))
                });
                if has_strict {
                    return Response::builder()
                        .status(400)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{"error":{"message":"strict mode not supported"}}"#,
                        ))
                        .unwrap();
                }
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn strict_400_retries_without_strict() {
    let (base, captured) = mock_strict_fallback().await;
    let p = damon_core::provider::Provider::new("test", &provider("openai-completions", &base))
        .unwrap();

    let body = json!({
        "model": "m",
        "messages": [{"role": "user", "content": "hi"}],
        "tools": [{"type": "function", "strict": true,
            "function": {"name": "f", "parameters": {}}}],
    });
    let resp = p.chat(body).await.unwrap();
    assert_eq!(resp["choices"][0]["message"]["content"], "ok");
    // Two requests: first with strict, second without.
    let sent = captured.lock().await;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["tools"][0]["strict"], true);
    assert!(sent[1]["tools"][0].get("strict").is_none());
}

/// Mock SSE endpoint emitting text + reasoning + usage + finish_reason.
async fn mock_sse_rich() -> String {
    let app = Router::new().route(
        "/chat/completions",
        post(|| async {
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking...\"}}]}\n\n",
                        "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
                        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3}}\n\n",
                        "data: [DONE]\n\n",
                    ),
                ))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn sse_emits_thinking_usage_and_stop_reason() {
    use futures::StreamExt;
    let base = mock_sse_rich().await;
    let p = damon_core::provider::Provider::new("test", &provider("openai-completions", &base))
        .unwrap();

    let body = json!({"model": "m", "messages": [{"role":"user","content":"hi"}]});
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let mut events = Vec::new();
    while let Some(ev) = stream.next().await {
        events.push(ev.unwrap());
    }
    use damon_core::llm::StreamEvent;
    assert!(matches!(&events[0], StreamEvent::Thinking(t) if t == "thinking..."));
    assert!(matches!(&events[1], StreamEvent::Text(t) if t == "hello"));
    assert!(matches!(
        &events[2],
        StreamEvent::Usage {
            input: 5,
            output: 3
        }
    ));
    assert!(matches!(
        &events[3],
        StreamEvent::Done(damon_core::llm::StopReason::Stop)
    ));
}

#[tokio::test]
async fn sse_utf8_chunk_boundary_not_corrupted() {
    use damon_core::provider::openai::sse_events;
    use futures::StreamExt;

    // A multi-byte char (한 = E2 95 9C) split across chunk boundaries.
    // Old code did from_utf8_lossy per chunk → U+FFFD corruption.
    let payload = "data: {\"choices\":[{\"delta\":{\"content\":\"한\"}}]}\n\ndata: [DONE]\n\n";
    let bytes = payload.as_bytes();
    // Find the '한' byte offset and split inside it.
    let han = payload.find("한").unwrap();
    let chunks: Vec<std::io::Result<bytes::Bytes>> = vec![
        Ok(bytes::Bytes::from(&bytes[..han + 1])), // E2 (first byte of 한)
        Ok(bytes::Bytes::from(&bytes[han + 1..])), // 95 9C + rest
    ];
    let stream = futures::stream::iter(chunks);
    let mut events = Vec::new();
    let mut s = std::pin::pin!(sse_events(Box::pin(stream)));
    while let Some(ev) = s.next().await {
        events.push(ev.unwrap());
    }
    use damon_core::llm::StreamEvent;
    assert!(
        matches!(&events[0], StreamEvent::Text(t) if t == "한"),
        "expected intact '한', got {events:?}"
    );
}

#[tokio::test]
async fn sse_multi_data_line_event() {
    use damon_core::provider::openai::sse_events;
    use futures::StreamExt;

    // SSE spec: multiple data: lines in one event are joined with \n.
    // Split at a JSON whitespace point so the joined result parses.
    let payload = concat!(
        "data: {\"choices\":[{\"delta\":\n",
        "data: {\"content\":\"hi\"}}]}\n\n",
        "data: [DONE]\n\n"
    );
    let chunks: Vec<std::io::Result<bytes::Bytes>> = vec![Ok(bytes::Bytes::from(payload))];
    let stream = futures::stream::iter(chunks);
    let mut events = Vec::new();
    let mut s = std::pin::pin!(sse_events(Box::pin(stream)));
    while let Some(ev) = s.next().await {
        events.push(ev.unwrap());
    }
    use damon_core::llm::StreamEvent;
    assert!(
        matches!(&events[0], StreamEvent::Text(t) if t == "hi"),
        "expected joined multi-line data, got {events:?}"
    );
}

#[test]
fn thinking_level_splits_and_maps() {
    use damon_core::config::Config;
    let (m, t) = Config::split_thinking_level("claude-sonnet-4:high");
    assert_eq!(m, "claude-sonnet-4");
    assert_eq!(t, Some("high"));
    let (m, t) = Config::split_thinking_level("gpt-4o");
    assert_eq!(m, "gpt-4o");
    assert_eq!(t, None);
    // Unknown suffix is not a level.
    let (m, t) = Config::split_thinking_level("model:latest");
    assert_eq!(m, "model:latest");
    assert_eq!(t, None);
}

#[tokio::test]
async fn thinking_maps_per_provider() {
    // Anthropic: _thinking → thinking.budget_tokens + max_tokens bump.
    let (base, captured) = mock_anthropic().await;
    let p = damon_core::provider::Provider::new("claude", &provider("anthropic-messages", &base))
        .unwrap();
    let body = json!({
        "model": "claude-sonnet-4",
        "_thinking": "high",
        "messages": [{"role":"user","content":"hi"}],
    });
    let _ = p.chat(body).await;
    let sent = &captured.lock().await[0];
    assert_eq!(sent["thinking"]["type"], "enabled");
    assert_eq!(sent["thinking"]["budget_tokens"], 32768);
    assert!(sent["max_tokens"].as_u64().unwrap() > 32768);

    // OpenAI completions: _thinking → reasoning_effort.
    let (base, captured) = mock_completions().await;
    let p = damon_core::provider::Provider::new("openai", &provider("openai-completions", &base))
        .unwrap();
    let body = json!({
        "model": "o3",
        "_thinking": "low",
        "messages": [{"role":"user","content":"hi"}],
    });
    let _ = p.chat(body).await;
    let sent = &captured.lock().await[0];
    assert_eq!(sent["reasoning_effort"], "low");
    assert!(sent.get("_thinking").is_none());
}

#[tokio::test]
async fn inband_and_thinking_coexist() {
    // inband_tools strips `tools` and renders them as a prompt; _thinking
    // still maps to reasoning_effort. No 400 from the combination.
    let (base, captured) = mock_completions().await;
    let mut cfg = provider("openai-completions", &base);
    cfg.compat.inband_tools = true;
    let p = damon_core::provider::Provider::new("local", &cfg).unwrap();
    let body = json!({
        "model": "qwen3:8b",
        "_thinking": "medium",
        "messages": [{"role":"user","content":"hi"}],
        "tools": [{"type":"function","function":{"name":"fs.read","description":"r","parameters":{}}}],
    });
    let _ = p.chat(body).await;
    let sent = &captured.lock().await[0];
    assert_eq!(sent["reasoning_effort"], "medium");
    assert!(sent.get("tools").is_none(), "inband must strip tools");
    assert!(
        sent["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["content"].as_str().unwrap_or("").contains("<tool_call>")),
        "inband must render tools into prompt"
    );
}

#[test]
fn inband_extracts_tool_calls() {
    use damon_core::provider::inband::extract_tool_calls;
    let (clean, calls) = extract_tool_calls(
        "Let me check.\n<tool_call>{\"name\":\"fs.read\",\"arguments\":{\"path\":\"/tmp\"}}</tool_call>\nDone.",
    );
    assert_eq!(clean, "Let me check.\n\nDone.");
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "fs.read");
    assert!(calls[0].1.contains("/tmp"));
    // Malformed block stays in text.
    let (clean, calls) = extract_tool_calls("<tool_call>not json</tool_call>");
    assert!(clean.contains("not json"));
    assert!(calls.is_empty());
}

/// Mock endpoint that 400s on model "small" with context overflow, 200s on "big".
async fn mock_promotion() -> (String, Arc<tokio::sync::Mutex<Vec<Value>>>) {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                let v: Value = serde_json::from_str(&body).unwrap();
                captured.lock().await.push(v.clone());
                if v["model"].as_str() == Some("small") {
                    return Response::builder()
                        .status(400)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            r#"{"error":{"message":"context_length_exceeded: too many tokens"}}"#,
                        ))
                        .unwrap();
                }
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"promoted ok"},"finish_reason":"stop"}]}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

#[tokio::test]
async fn context_overflow_promotes_to_target() {
    let (base, captured) = mock_promotion().await;
    let mut cfg = provider("openai-completions", &base);
    cfg.context_promotion_target = Some("big".to_string());
    let mut providers = BTreeMap::new();
    providers.insert("default".to_string(), cfg);
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        data_dir: None,
        tls_cert: None,
        tls_key: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
    }));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let app = damon_core::api::router(damon_core::api::AppState::new(shared, store, mcp).await);

    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let resp = app
        .oneshot(
            axum::http::Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"small","messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "promoted ok");
    // Two requests: "small" (400) then "big" (200).
    let sent = captured.lock().await;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0]["model"], "small");
    assert_eq!(sent[1]["model"], "big");
}

/// In-band tools end-to-end: model emits <tool_call> in text, we parse it.
#[tokio::test]
async fn inband_tools_roundtrip() {
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
    let captured2 = captured.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move |body: String| {
            let captured = captured2.clone();
            async move {
                captured.lock().await.push(serde_json::from_str(&body).unwrap());
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"choices":[{"index":0,"message":{"role":"assistant","content":"I'll read it.\n<tool_call>{\"name\":\"fs.read\",\"arguments\":{\"path\":\"/tmp/x\"}}</tool_call>"},"finish_reason":"stop"}]}"#,
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut cfg = provider("openai-completions", &format!("http://{addr}"));
    cfg.compat.inband_tools = true;
    let p = damon_core::provider::Provider::new("local", &cfg).unwrap();

    let body = json!({
        "model": "m",
        "messages": [{"role": "user", "content": "read /tmp/x"}],
        "tools": [{"type": "function", "function": {"name": "fs.read", "description": "read", "parameters": {}}}],
    });
    let resp = p.chat(body).await.unwrap();

    // Request had no tools field — rendered into system prompt instead.
    let sent = &captured.lock().await[0];
    assert!(sent.get("tools").is_none());
    assert!(
        sent["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("fs.read")
    );

    // Response has real tool_calls.
    let tc = &resp["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(tc["function"]["name"], "fs.read");
    assert!(
        tc["function"]["arguments"]
            .as_str()
            .unwrap()
            .contains("/tmp/x")
    );
    assert_eq!(resp["choices"][0]["finish_reason"], "tool_calls");
}
