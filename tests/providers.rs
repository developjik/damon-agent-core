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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
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

/// Parallel tool calls persist one role:"tool" row per call; the
/// translated Anthropic request must fold them into ONE user message —
/// consecutive same-role messages are a 400 ("roles must alternate").
#[tokio::test]
async fn anthropic_folds_parallel_tool_results_into_one_user_message() {
    let (base, captured) = mock_anthropic().await;
    let p = damon_core::provider::Provider::new("claude", &provider("anthropic-messages", &base))
        .unwrap();

    let body = json!({
        "model": "claude-sonnet-4",
        "messages": [
            {"role": "user", "content": "run both"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "call_a", "type": "function",
                 "function": {"name": "fs.read", "arguments": "{\"path\":\"a\"}"}},
                {"id": "call_b", "type": "function",
                 "function": {"name": "fs.read", "arguments": "{\"path\":\"b\"}"}},
            ]},
            {"role": "tool", "tool_call_id": "call_a", "content": "A"},
            {"role": "tool", "tool_call_id": "call_b", "content": "B"},
        ],
    });
    p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    let msgs = sent["messages"].as_array().unwrap();
    for w in msgs.windows(2) {
        assert_ne!(
            w[0]["role"], w[1]["role"],
            "consecutive same-role: {msgs:?}"
        );
    }
    // The tool turn: one assistant message carrying both tool_use blocks,
    // then one user message carrying both tool_result blocks.
    let assistant = &msgs[1];
    assert_eq!(assistant["role"], "assistant");
    assert_eq!(assistant["content"].as_array().unwrap().len(), 2);
    assert_eq!(assistant["content"][0]["type"], "tool_use");
    assert_eq!(assistant["content"][1]["type"], "tool_use");
    let user = &msgs[2];
    assert_eq!(user["role"], "user");
    let results = user["content"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["type"], "tool_result");
    assert_eq!(results[0]["tool_use_id"], "call_a");
    assert_eq!(results[1]["tool_use_id"], "call_b");
}

/// Same fold for Gemini: parallel tool results land as ONE user turn
/// with every functionResponse part (Gemini rejects consecutive
/// same-role contents).
#[tokio::test]
async fn gemini_folds_parallel_tool_results_into_one_user_turn() {
    let (base, captured) = mock_gemini().await;
    let p = damon_core::provider::Provider::new("gemini", &provider("gemini", &base)).unwrap();

    let body = json!({
        "model": "gemini-2.5-flash",
        "messages": [
            {"role": "user", "content": "run both"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "call_a", "type": "function",
                 "function": {"name": "fs.read", "arguments": "{}"}},
                {"id": "call_b", "type": "function",
                 "function": {"name": "fs.read", "arguments": "{}"}},
            ]},
            {"role": "tool", "tool_call_id": "call_a", "content": "A"},
            {"role": "tool", "tool_call_id": "call_b", "content": "B"},
        ],
    });
    p.chat(body).await.unwrap();

    let sent = &captured.lock().await[0];
    let contents = sent["contents"].as_array().unwrap();
    for w in contents.windows(2) {
        assert_ne!(
            w[0]["role"], w[1]["role"],
            "consecutive same-role: {contents:?}"
        );
    }
    let model_turn = &contents[1];
    assert_eq!(model_turn["role"], "model");
    assert_eq!(model_turn["parts"].as_array().unwrap().len(), 2);
    let user_turn = &contents[2];
    assert_eq!(user_turn["role"], "user");
    let parts = user_turn["parts"].as_array().unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0]["functionResponse"]["name"], "fs__read");
    assert_eq!(parts[1]["functionResponse"]["name"], "fs__read");
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
    }));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let app = damon_core::api::router(damon_core::api::AppState::new(shared, store, mcp).await);

    use http_body_util::BodyExt;
    use tower::ServiceExt;
    // Prefix + thinking-suffix routing: the wire model must be the bare
    // upstream id and the suffix must enable thinking.
    let resp = app
        .oneshot(
            axum::http::Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"claude/claude-sonnet-4:high","messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "hi from claude");
    // Verify the request actually went to the anthropic mock, with the
    // rewritten model and the thinking budget from the :high suffix.
    let sent = captured.lock().await[0].clone();
    assert_eq!(sent["model"], "claude-sonnet-4", "wire model: {sent}");
    assert_eq!(sent["thinking"]["type"], "enabled", "wire: {sent}");
}

/// M2 regression: an OpenAI-wire tool replay (assistant tool_calls, no
/// thinking blocks — /v1 clients cannot echo them) must disable thinking
/// instead of building a request Anthropic 400s on every later turn.
#[tokio::test]
async fn v1_tool_history_without_thinking_disables_thinking() {
    let (base, captured) = mock_anthropic().await;
    let mut providers = BTreeMap::new();
    let mut claude = provider("anthropic-messages", &base);
    claude.models = vec!["claude-*".to_string()];
    providers.insert("claude".to_string(), claude);
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
    }));
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let mcp = damon_core::mcp::McpRegistry::connect_all(&HashMap::new()).await;
    let app = damon_core::api::router(damon_core::api::AppState::new(shared, store, mcp).await);

    use tower::ServiceExt;
    let resp = app
        .oneshot(
            axum::http::Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"claude/claude-sonnet-4:high","messages":[
                        {"role":"user","content":"run it"},
                        {"role":"assistant","tool_calls":[{"id":"call_1","type":"function",
                          "function":{"name":"fs","arguments":"{}"}}]},
                        {"role":"tool","tool_call_id":"call_1","content":"ok"},
                        {"role":"user","content":"thanks"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let sent = captured.lock().await[0].clone();
    assert!(
        sent.get("thinking").is_none(),
        "thinking must be dropped for un-replayable tool history: {sent}"
    );
}

/// M1 regression: the translated SSE stream must carry a finish_reason
/// chunk BEFORE [DONE] — OpenAI SDK tool loops key on it.
#[tokio::test]
async fn v1_stream_emits_finish_reason_before_done() {
    let base = mock_anthropic_sse(concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"fs__read\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    ))
    .await;
    let mut providers = BTreeMap::new();
    let mut claude = provider("anthropic-messages", &base);
    claude.models = vec!["claude-*".to_string()];
    providers.insert("claude".to_string(), claude);
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
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
                    r#"{"model":"claude-sonnet-4","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    let fr = text
        .find(r#""finish_reason":"tool_calls""#)
        .expect("no finish_reason chunk in SSE");
    let done = text.find("data: [DONE]").expect("no [DONE] in SSE");
    assert!(fr < done, "finish_reason must precede [DONE]: {text}");
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
    // tool result got name + matching id — the dotted name is mangled
    // for the wire (OpenAI forbids '.' in function names).
    assert_eq!(sent["messages"][3]["name"], "fs__read");
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
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

// ---------------------------------------------------------------------------
// Regression tests for sparse tool-call indices and mid-stream errors.
// ---------------------------------------------------------------------------

/// Mock Anthropic SSE: a text block precedes the tool_use block, so the
/// tool call arrives at content-block index 1 — not 0.
async fn mock_anthropic_sse(sse: &'static str) -> String {
    let app = Router::new().route(
        "/v1/messages",
        post(move || async move {
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(sse))
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
async fn anthropic_text_before_tool_use_yields_single_call() {
    use damon_core::llm::{StreamEvent, ToolCallAccumulator};
    use futures::StreamExt;
    // [text@0, tool_use@1]: the tool call's content-block index is sparse.
    // Old code used it as the accumulator index → a fake empty call at
    // slot 0 was persisted, poisoning the session history.
    let base = mock_anthropic_sse(concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"let me check\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"fs__read\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"p\\\":1}\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    ))
    .await;
    let p = damon_core::provider::Provider::new("claude", &provider("anthropic-messages", &base))
        .unwrap();
    // The upstream echoes the mangled name — the request must declare
    // the tool so the mangle map restores `fs.read`. An unknown name is
    // no longer guessed (a visible unknown-tool error beats a silent
    // wrong call).
    let body = json!({
        "model": "claude-sonnet-4",
        "messages": [{"role":"user","content":"hi"}],
        "tools": [{"type":"function","function":{"name":"fs.read","parameters":{}}}],
    });
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let mut acc = ToolCallAccumulator::default();
    let mut saw_text = false;
    while let Some(ev) = stream.next().await {
        match ev.unwrap() {
            StreamEvent::Text(_) => saw_text = true,
            StreamEvent::ToolCallDelta {
                index,
                id,
                name,
                arguments,
            } => acc.push(index, id, name, &arguments),
            _ => {}
        }
    }
    let calls = acc.finish();
    assert!(saw_text);
    assert_eq!(
        calls.len(),
        1,
        "sparse index must not create empty calls: {calls:?}"
    );
    assert_eq!(calls[0].id, "toolu_1");
    assert_eq!(calls[0].name, "fs.read");
    assert_eq!(calls[0].arguments, "{\"p\":1}");
}

#[tokio::test]
async fn anthropic_midstream_error_propagates() {
    use futures::StreamExt;
    // An `error` event mid-stream must surface as Err, not end the turn
    // as if it completed.
    let base = mock_anthropic_sse(concat!(
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
        "data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
    ))
    .await;
    let p = damon_core::provider::Provider::new("claude", &provider("anthropic-messages", &base))
        .unwrap();
    let body = json!({"model": "claude-sonnet-4", "messages": [{"role":"user","content":"hi"}]});
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let mut saw_err = false;
    while let Some(ev) = stream.next().await {
        if let Err(e) = ev {
            // overloaded_error mid-stream maps to RateLimited so the
            // runtime's backoff path engages, same as an HTTP 429/529.
            assert!(
                e.downcast_ref::<damon_core::provider::RateLimited>()
                    .is_some(),
                "got {e}"
            );
            saw_err = true;
        }
    }
    assert!(saw_err, "mid-stream error event must surface as Err");
}

/// Mock Responses SSE endpoint.
async fn mock_responses_sse(sse: &'static str) -> String {
    let app = Router::new().route(
        "/responses",
        post(move || async move {
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(sse))
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
async fn responses_reasoning_before_function_call_yields_single_call() {
    use damon_core::llm::{StreamEvent, ToolCallAccumulator};
    use futures::StreamExt;
    // reasoning@0 then function_call@1: output_index is sparse.
    let base = mock_responses_sse(concat!(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\"}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"call_id\":\"fc_1\",\"name\":\"fs.read\"}}\n\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\\\"p\\\":1}\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"function_call\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n",
    ))
    .await;
    let p =
        damon_core::provider::Provider::new("o3", &provider("openai-responses", &base)).unwrap();
    let body = json!({"model": "o3", "messages": [{"role":"user","content":"hi"}]});
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let mut acc = ToolCallAccumulator::default();
    while let Some(ev) = stream.next().await {
        if let StreamEvent::ToolCallDelta {
            index,
            id,
            name,
            arguments,
        } = ev.unwrap()
        {
            acc.push(index, id, name, &arguments);
        }
    }
    let calls = acc.finish();
    assert_eq!(
        calls.len(),
        1,
        "sparse output_index must not create empty calls: {calls:?}"
    );
    assert_eq!(calls[0].id, "fc_1");
    assert_eq!(calls[0].name, "fs.read");
}

#[tokio::test]
async fn responses_failed_propagates_error() {
    use futures::StreamExt;
    let base = mock_responses_sse("data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"model exploded\"}}}\n\n")
    .await;
    let p =
        damon_core::provider::Provider::new("o3", &provider("openai-responses", &base)).unwrap();
    let body = json!({"model": "o3", "messages": [{"role":"user","content":"hi"}]});
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let mut saw_err = false;
    while let Some(ev) = stream.next().await {
        if let Err(e) = ev {
            assert!(e.to_string().contains("model exploded"), "got {e}");
            saw_err = true;
        }
    }
    assert!(saw_err, "response.failed must surface as Err");
}

/// Mock Gemini SSE endpoint.
async fn mock_gemini_sse(sse: &'static str) -> String {
    let app = Router::new().route(
        "/v1beta/models/g:streamGenerateContent",
        post(move || async move {
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from(sse))
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
async fn gemini_midstream_error_propagates() {
    use futures::StreamExt;
    let base = mock_gemini_sse(concat!(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n\n",
        "data: {\"error\":{\"code\":500,\"message\":\"backend exploded\"}}\n\n",
    ))
    .await;
    let p = damon_core::provider::Provider::new("g", &provider("gemini", &base)).unwrap();
    let body = json!({"model": "g", "messages": [{"role":"user","content":"hi"}]});
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let mut saw_err = false;
    while let Some(ev) = stream.next().await {
        if let Err(e) = ev {
            assert!(e.to_string().contains("backend exploded"), "got {e}");
            saw_err = true;
        }
    }
    assert!(saw_err, "gemini error payload must surface as Err");
}

#[tokio::test]
async fn anthropic_nonstream_tool_call_finish_reason() {
    // A non-streaming response with tool_use must report
    // finish_reason "tool_calls" — a hardcoded "stop" breaks the
    // tool loop for /v1 clients.
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<Value>::new()));
    let app = Router::new().route(
        "/v1/messages",
        post(move || async move {
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"content":[{"type":"tool_use","id":"t1","name":"fs__read","input":{"p":1}}],"stop_reason":"tool_use","usage":{}}"#,
                ))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let _ = captured;
    let p = damon_core::provider::Provider::new(
        "claude",
        &provider("anthropic-messages", &format!("http://{addr}")),
    )
    .unwrap();
    // The upstream echoes the mangled name — the request must declare
    // the tool so the mangle map restores `fs.read`.
    let body = json!({
        "model": "claude-sonnet-4",
        "messages": [{"role":"user","content":"hi"}],
        "tools": [{"type":"function","function":{"name":"fs.read","parameters":{}}}],
    });
    let resp = p.chat(body).await.unwrap();
    assert_eq!(resp["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(
        resp["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "fs.read"
    );
}

#[tokio::test]
async fn accumulator_drops_empty_slots() {
    // Direct unit coverage: a sparse index must not leave a fake call.
    use damon_core::llm::ToolCallAccumulator;
    let mut acc = ToolCallAccumulator::default();
    acc.push(1, Some("id1".into()), Some("n".into()), "{}");
    let calls = acc.finish();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].id, "id1");
}

// ─── ChatGPT subscription OAuth: openai-responses + api_key = "oauth" ───
//
// The oauth path flips the same process-global DAMON_TEST_* hooks the
// tests/oauth.rs binary uses (a separate test binary, so no cross-file
// contention) — but the two tests here run in parallel with each other,
// so they serialize on a local lock.

use std::sync::atomic::{AtomicUsize, Ordering};

/// (authorization, chatgpt-account-id, body) per /responses call.
type CapturedRequest = (Option<String>, Option<String>, Value);
static OAUTH_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Fake id_token JWT carrying the given ChatGPT account id claim.
fn fake_id_token(account: &str) -> String {
    use base64::Engine;
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let header = b64(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = b64(&serde_json::to_vec(&json!({
        "https://api.openai.com/auth": {"chatgpt_account_id": account},
    }))
    .unwrap());
    format!("{header}.{payload}.c2ln")
}

struct OAuthMock {
    /// Origin serving both /responses and /token.
    base: String,
    /// Captured /responses calls, in order.
    requests: Arc<tokio::sync::Mutex<Vec<CapturedRequest>>>,
    upstream_hits: Arc<AtomicUsize>,
    token_hits: Arc<AtomicUsize>,
}

/// Spawn one mock origin: /responses answers the first call with 401 and
/// later calls with a completed SSE stream; /token rotates the access
/// token on every hit (rotated-1, rotated-2, …) for account acc-2.
async fn oauth_mocks(dir_name: &str) -> OAuthMock {
    use axum::http::{HeaderMap, StatusCode};

    let requests: Arc<tokio::sync::Mutex<Vec<CapturedRequest>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let upstream_hits = Arc::new(AtomicUsize::new(0));
    let token_hits = Arc::new(AtomicUsize::new(0));

    let req_state = requests.clone();
    let up_hits = upstream_hits.clone();
    let tok_hits = token_hits.clone();
    let app = Router::new()
        .route(
            "/responses",
            post(move |headers: HeaderMap, body: String| {
                let requests = req_state.clone();
                let hits = up_hits.clone();
                async move {
                    let n = hits.fetch_add(1, Ordering::SeqCst);
                    requests.lock().await.push((
                        headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok().map(String::from)),
                        headers
                            .get("chatgpt-account-id")
                            .and_then(|v| v.to_str().ok().map(String::from)),
                        serde_json::from_str(&body).unwrap(),
                    ));
                    if n == 0 {
                        return Response::builder()
                            .status(StatusCode::UNAUTHORIZED)
                            .header("content-type", "application/json")
                            .body(Body::from(r#"{"error":"stale token"}"#))
                            .unwrap();
                    }
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from(concat!(
                            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"he\"}\n\n",
                            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",",
                            "\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"he\"}]}],",
                            "\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
                        )))
                        .unwrap()
                }
            }),
        )
        .route(
            "/token",
            post(move |body: String| {
                let hits = tok_hits.clone();
                async move {
                    let n = hits.fetch_add(1, Ordering::SeqCst);
                    let req: Value = serde_json::from_str(&body).unwrap();
                    assert_eq!(req["grant_type"], "refresh_token");
                    assert_eq!(req["client_id"], "app_EMoamEEZ73f0CkXaXp7hrann");
                    Json(json!({
                        "access_token": format!("rotated-{}", n + 1),
                        "refresh_token": format!("rt-{}", n + 2),
                        "expires_in": 3600,
                        "id_token": fake_id_token("acc-2"),
                    }))
                }
            }),
        )
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Seed an expired token set for acc-1: the proactive refresh rotates
    // to rotated-1/acc-2 before the first upstream call.
    let dir = std::env::temp_dir().join(dir_name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("openai.json"),
        serde_json::to_string(&json!({
            "access_token": "stale-access",
            "refresh_token": "rt-1",
            "expires_at": 0,
            "account_id": "acc-1",
        }))
        .unwrap(),
    )
    .unwrap();

    OAuthMock {
        base: format!("http://{addr}"),
        requests,
        upstream_hits,
        token_hits,
    }
}

fn wire_env(base: &str, dir_name: &str) {
    // SAFETY: callers hold OAUTH_ENV_LOCK.
    unsafe {
        std::env::set_var("DAMON_TEST_TOKEN_URL", format!("{base}/token"));
        std::env::set_var("DAMON_TEST_TOKEN_DIR", std::env::temp_dir().join(dir_name));
    }
}

fn clear_env(dir_name: &str) {
    // SAFETY: callers hold OAUTH_ENV_LOCK.
    unsafe {
        std::env::remove_var("DAMON_TEST_TOKEN_DIR");
        std::env::remove_var("DAMON_TEST_TOKEN_URL");
    }
    std::fs::remove_dir_all(std::env::temp_dir().join(dir_name)).ok();
}

fn oauth_provider_config(base: &str) -> ProviderConfig {
    ProviderConfig {
        api: "openai-responses".to_string(),
        base_url: Some(base.to_string()),
        api_key: Some("oauth".to_string()),
        models: vec![],
        default_model: None,
        headers: Default::default(),
        compat: Default::default(),
        discovery: None,
        context_promotion_target: None,
    }
}

#[tokio::test]
async fn responses_oauth_stream_authenticates_and_retries_on_401() {
    use futures::StreamExt;
    let _guard = OAUTH_ENV_LOCK.lock().await;
    const DIR: &str = "damon-prov-oauth-stream";
    let m = oauth_mocks(DIR).await;
    wire_env(&m.base, DIR);

    let p =
        damon_core::provider::Provider::new("chatgpt", &oauth_provider_config(&m.base)).unwrap();
    let body = json!({"model": "gpt-5.1", "messages": [{"role": "user", "content": "hi"}]});
    let mut stream = std::pin::pin!(p.chat_stream(body).await.unwrap());
    let (mut text, mut done) = (String::new(), false);
    while let Some(ev) = stream.next().await {
        match ev.unwrap() {
            damon_core::llm::StreamEvent::Text(t) => text.push_str(&t),
            damon_core::llm::StreamEvent::Done(_) => done = true,
            _ => {}
        }
    }
    assert_eq!(text, "he");
    assert!(done, "terminal Done event must arrive");

    // Proactive refresh (expired seed) + forced refresh (upstream 401).
    assert_eq!(m.token_hits.load(Ordering::SeqCst), 2);
    assert_eq!(m.upstream_hits.load(Ordering::SeqCst), 2);
    let reqs = m.requests.lock().await;
    // First call: the rotated-at-load token, retry: the forced one.
    assert_eq!(reqs[0].0.as_deref(), Some("Bearer rotated-1"));
    assert_eq!(reqs[0].1.as_deref(), Some("acc-2"));
    assert_eq!(reqs[1].0.as_deref(), Some("Bearer rotated-2"));
    assert_eq!(reqs[1].1.as_deref(), Some("acc-2"));
    // Codex dialect: SSE-only, stateless, instructions defaulted in.
    assert_eq!(reqs[1].2["stream"], json!(true));
    assert_eq!(reqs[1].2["store"], json!(false));
    assert_eq!(reqs[1].2["instructions"], json!(""));
    assert_eq!(reqs[1].2["model"], json!("gpt-5.1"));

    // The rotated set was persisted for the next turn.
    let stored = damon_core::oauth::load("openai").unwrap().unwrap();
    assert_eq!(stored.access_token, "rotated-2");
    assert_eq!(stored.account_id.as_deref(), Some("acc-2"));

    clear_env(DIR);
}

#[tokio::test]
async fn responses_oauth_nonstream_folds_sse_into_json() {
    let _guard = OAUTH_ENV_LOCK.lock().await;
    const DIR: &str = "damon-prov-oauth-nonstream";
    let m = oauth_mocks(DIR).await;
    wire_env(&m.base, DIR);

    let p =
        damon_core::provider::Provider::new("chatgpt", &oauth_provider_config(&m.base)).unwrap();
    let body = json!({"model": "gpt-5.1", "messages": [{"role": "user", "content": "hi"}]});
    // No "stream" flag on the inbound request — the oauth backend still
    // only speaks SSE, so the provider folds it back into JSON.
    let resp = p.chat(body).await.unwrap();
    assert_eq!(resp["choices"][0]["message"]["content"], "he");
    assert_eq!(resp["choices"][0]["finish_reason"], "stop");
    assert_eq!(resp["usage"]["prompt_tokens"], 1);
    assert_eq!(resp["usage"]["completion_tokens"], 2);

    assert_eq!(m.upstream_hits.load(Ordering::SeqCst), 2);
    let reqs = m.requests.lock().await;
    assert_eq!(reqs[0].2["stream"], json!(true), "outbound must be SSE");

    clear_env(DIR);
}

// ─── omp wire shapes: Azure deployments, Vertex paths, Bedrock bearer ───

/// Captured-request tuple types for the wire-shape mocks below.
type AzureCapture = (String, Option<String>, Option<String>, Value);
type AuthHeaders = (Option<String>, Option<String>);
type VertexCapture = (String, Option<String>);

fn compat_provider(
    kind: &str,
    base: &str,
    compat: damon_core::config::ProviderCompat,
) -> ProviderConfig {
    ProviderConfig {
        api: kind.to_string(),
        base_url: Some(base.to_string()),
        api_key: None,
        models: vec![],
        default_model: None,
        headers: Default::default(),
        compat,
        discovery: None,
        context_promotion_target: None,
    }
}

/// Azure OpenAI: the deployment rides in the path with an `api-version`
/// query, and the key rides in the `api-key` header — not Bearer.
#[tokio::test]
async fn azure_deployment_url_and_api_key_header() {
    let captured: Arc<tokio::sync::Mutex<Vec<AzureCapture>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let state = captured.clone();
    let app = Router::new().fallback(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap, body: String| {
            let captured = state.clone();
            async move {
                captured.lock().await.push((
                    uri.to_string(),
                    headers
                        .get("api-key")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                    headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                    serde_json::from_str(&body).unwrap(),
                ));
                Json(json!({
                    "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                    "usage": {},
                }))
            }
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let compat = damon_core::config::ProviderCompat {
        azure_deployment_urls: true,
        azure_api_version: Some("2024-10-21".into()),
        ..Default::default()
    };
    let mut cfg = compat_provider(
        "openai-completions",
        &format!("http://{addr}/openai"),
        compat,
    );
    cfg.api_key = None;
    cfg.headers.insert("api-key".into(), "azkey".into());
    let p = damon_core::provider::Provider::new("azure", &cfg).unwrap();

    let resp = p
        .chat(json!({"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}))
        .await
        .unwrap();
    assert_eq!(resp["choices"][0]["message"]["content"], "ok");

    let reqs = captured.lock().await;
    let (uri, api_key, bearer, body) = &reqs[0];
    assert_eq!(
        uri, "/openai/deployments/gpt-4o/chat/completions?api-version=2024-10-21",
        "deployment in path, api-version in query"
    );
    assert_eq!(api_key.as_deref(), Some("azkey"));
    assert!(bearer.is_none(), "azure does not use Bearer");
    assert_eq!(body["model"], "gpt-4o");
}

/// Anthropic-compatible bearer surfaces (Bedrock mantle): the key goes
/// out as `Authorization: Bearer`, never `x-api-key`.
#[tokio::test]
async fn anthropic_bearer_auth_compat() {
    let captured: Arc<tokio::sync::Mutex<Vec<AuthHeaders>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let state = captured.clone();
    let app = Router::new().route(
        "/v1/messages",
        post(move |headers: axum::http::HeaderMap| {
            let captured = state.clone();
            async move {
                captured.lock().await.push((
                    headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                    headers
                        .get("x-api-key")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                ));
                Json(json!({"content":[{"type":"text","text":"ok"}],"usage":{}}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // SAFETY: single-threaded test; removed after the assertions.
    unsafe {
        std::env::set_var("DAMON_TEST_BEARER_KEY", "bedrock-bearer");
    }
    let mut cfg = compat_provider(
        "anthropic-messages",
        &format!("http://{addr}"),
        damon_core::config::ProviderCompat {
            bearer_auth: true,
            ..Default::default()
        },
    );
    cfg.api_key = Some("env:DAMON_TEST_BEARER_KEY".into());
    let p = damon_core::provider::Provider::new("mantle", &cfg).unwrap();
    let resp = p
        .chat(json!({"model": "claude-x", "messages": [{"role": "user", "content": "hi"}]}))
        .await
        .unwrap();
    assert_eq!(resp["choices"][0]["message"]["content"], "ok");

    let reqs = captured.lock().await;
    assert_eq!(reqs[0].0.as_deref(), Some("Bearer bedrock-bearer"));
    assert!(reqs[0].1.is_none(), "x-api-key must be absent");
    // SAFETY: set above; no other test reads this var.
    unsafe {
        std::env::remove_var("DAMON_TEST_BEARER_KEY");
    }
}

/// Vertex AI: `{base}/models/{model}:generateContent` paths on a
/// project/location base, key still via `x-goog-api-key`.
#[tokio::test]
async fn vertex_url_shape() {
    let captured: Arc<tokio::sync::Mutex<Vec<VertexCapture>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let state = captured.clone();
    let app = Router::new().fallback(
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap, body: String| {
            let captured = state.clone();
            async move {
                captured.lock().await.push((
                    uri.to_string(),
                    headers
                        .get("x-goog-api-key")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                ));
                let _ = body;
                Json(json!({
                    "candidates": [{"content": {"parts": [{"text": "ok"}]}}],
                    "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
                }))
            }
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // SAFETY: single-threaded test; removed after the assertions.
    unsafe {
        std::env::set_var("DAMON_TEST_VERTEX_KEY", "vk");
    }
    let mut cfg = compat_provider(
        "gemini",
        &format!("http://{addr}/v1/projects/p/locations/us-central1/publishers/google"),
        damon_core::config::ProviderCompat {
            vertex: true,
            ..Default::default()
        },
    );
    cfg.api_key = Some("env:DAMON_TEST_VERTEX_KEY".into());
    let p = damon_core::provider::Provider::new("vertex", &cfg).unwrap();
    let resp = p
        .chat(json!({"model": "gemini-2-0", "messages": [{"role": "user", "content": "hi"}]}))
        .await
        .unwrap();
    assert_eq!(resp["choices"][0]["message"]["content"], "ok");

    let reqs = captured.lock().await;
    assert_eq!(
        reqs[0].0,
        "/v1/projects/p/locations/us-central1/publishers/google/models/gemini-2-0:generateContent"
    );
    assert_eq!(reqs[0].1.as_deref(), Some("vk"));
    // SAFETY: set above; no other test reads this var.
    unsafe {
        std::env::remove_var("DAMON_TEST_VERTEX_KEY");
    }
}

/// Presets auto-register from env vars; keyless local engines always do.
#[tokio::test]
async fn presets_auto_register_from_env() {
    // SAFETY: single-threaded env mutation; removed before returning.
    unsafe {
        std::env::set_var("OPENROUTER_API_KEY", "sk-or");
    }
    let providers = BTreeMap::new();
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
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
    };
    let (built, errors) = damon_core::provider::build_providers(&cfg);
    assert!(errors.is_empty(), "{errors:?}");
    // Env-keyed preset registered.
    assert!(built.contains_key("openrouter"));
    // Keyless local engines always register.
    assert!(built.contains_key("lm-studio"));
    assert!(built.contains_key("llama.cpp"));
    unsafe {
        std::env::remove_var("OPENROUTER_API_KEY");
    }
}

/// The `oauth:<flavor>` sentinel only pairs with its own api kind, and
/// bare `oauth` on a multi-flavor kind is rejected (ambiguous).
#[test]
fn oauth_sentinel_flavor_validation() {
    let pc = |kind: &str, key: &str| ProviderConfig {
        api: kind.to_string(),
        base_url: Some("http://127.0.0.1:1".into()),
        api_key: Some(key.into()),
        models: vec![],
        default_model: None,
        headers: Default::default(),
        compat: Default::default(),
        discovery: None,
        context_promotion_target: None,
    };
    // Bare oauth infers from api — openai-completions carries several
    // flavors, so the inference is ambiguous and rejected.
    assert!(damon_core::provider::Provider::new("x", &pc("openai-completions", "oauth")).is_err());
    // Flavor must match the transport.
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-completions", "oauth:openai"))
            .is_err()
    );
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-completions", "oauth:anthropic"))
            .is_err()
    );
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-responses", "oauth:kimi-code"))
            .is_err()
    );
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-completions", "oauth:nonsense"))
            .is_err()
    );
    // Valid pairings build without touching the keychain.
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-completions", "oauth:github-copilot"))
            .is_ok()
    );
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-completions", "oauth:kimi-code"))
            .is_ok()
    );
    assert!(
        damon_core::provider::Provider::new("x", &pc("openai-responses", "oauth:xai-oauth"))
            .is_ok()
    );
    assert!(
        damon_core::provider::Provider::new("x", &pc("anthropic-messages", "oauth:anthropic"))
            .is_ok()
    );
}

/// A copilot-flavored provider sends the bearer token AND the integration
/// headers the Copilot API expects (Editor-Version, Copilot-Integration-Id).
#[tokio::test]
async fn copilot_oauth_provider_sends_integration_headers() {
    let _guard = OAUTH_ENV_LOCK.lock().await;
    const DIR: &str = "damon-prov-copilot";
    // SAFETY: guarded by OAUTH_ENV_LOCK; removed below.
    unsafe {
        std::env::set_var("DAMON_TEST_TOKEN_DIR", std::env::temp_dir().join(DIR));
    }
    let dir = std::env::temp_dir().join(DIR);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("github-copilot.json"),
        serde_json::to_string(&json!({
            "access_token": "gho_1",
            "refresh_token": "gho_1",
            "expires_at": 4102444800_i64,
            "account_id": null,
        }))
        .unwrap(),
    )
    .unwrap();

    let captured: Arc<tokio::sync::Mutex<Vec<AuthHeaders>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let state = captured.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move |headers: axum::http::HeaderMap| {
            let captured = state.clone();
            async move {
                captured.lock().await.push((
                    headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                    headers
                        .get("editor-version")
                        .and_then(|v| v.to_str().ok().map(String::from)),
                ));
                Json(json!({
                    "choices": [{"message": {"role": "assistant", "content": "ok"}}],
                    "usage": {},
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let cfg = ProviderConfig {
        api: "openai-completions".to_string(),
        base_url: Some(format!("http://{addr}")),
        api_key: Some("oauth:github-copilot".into()),
        models: vec![],
        default_model: None,
        headers: Default::default(),
        compat: Default::default(),
        discovery: None,
        context_promotion_target: None,
    };
    let p = damon_core::provider::Provider::new("copilot", &cfg).unwrap();
    let resp = p
        .chat(json!({"model": "gpt-5", "messages": [{"role": "user", "content": "hi"}]}))
        .await
        .unwrap();
    assert_eq!(resp["choices"][0]["message"]["content"], "ok");

    let reqs = captured.lock().await;
    assert_eq!(reqs[0].0.as_deref(), Some("Bearer gho_1"));
    assert_eq!(reqs[0].1.as_deref(), Some("copilot/1.0.82"));

    // SAFETY: set above; no other test reads this var name.
    unsafe {
        std::env::remove_var("DAMON_TEST_TOKEN_DIR");
    }
    std::fs::remove_dir_all(&dir).ok();
}
