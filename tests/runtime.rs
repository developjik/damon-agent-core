//! End-to-end runtime test: WS client → session/prompt → mock LLM streams
//! a tool call → permission round-trip → tool result → final text.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::response::Response;
use axum::routing::post;
use damon_core::api::{self, AppState};
use damon_core::config::{Config, ProviderConfig};
use damon_core::mcp::McpRegistry;
use damon_core::store::Store;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite;

/// Mock LLM upstream: first call returns a tool_call, second returns text.
async fn mock_llm() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/chat/completions",
        post(move |body: String| {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let sse = if n == 0 {
                    // First turn: emit a tool call.
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"echo.ping\",\"arguments\":\"{\\\"x\\\":1}\"}}]}}]}\n\n",
                        "data: [DONE]\n\n"
                    )
                } else {
                    // Second turn: plain text.
                    let _ = body;
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"done!\"}}]}\n\n",
                        "data: [DONE]\n\n"
                    )
                };
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

#[tokio::test]
async fn ws_prompt_streams_and_persists() {
    let upstream = mock_llm().await;

    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;
    let app = api::router(state);

    // Serve the app on a real socket for the WS client.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    // initialize
    ws.send(tungstenite::Message::Text(
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let init: Value = read_json(&mut ws).await;
    assert_eq!(init["result"]["agentInfo"]["name"], "damond");

    // session/new
    ws.send(tungstenite::Message::Text(
        json!({"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let new: Value = read_json(&mut ws).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // session/prompt — the mock LLM requests tool "echo.ping" which is not
    // registered in MCP, so the tool call fails and the loop continues to
    // the second LLM turn which returns text.
    ws.send(tungstenite::Message::Text(
        json!({
            "jsonrpc":"2.0","id":3,"method":"session/prompt",
            "params":{"sessionId":session_id,"prompt":[{"type":"text","text":"hi"}]}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    // Collect notifications until the prompt response (id=3) arrives.
    let mut saw_text = false;
    let mut saw_tool_update = false;
    let mut stop_reason = String::new();
    for _ in 0..50 {
        let msg: Value = read_json(&mut ws).await;
        if msg["id"] == 3 {
            stop_reason = msg["result"]["stopReason"]
                .as_str()
                .unwrap_or("")
                .to_string();
            break;
        }
        let update = &msg["params"]["update"];
        if update["sessionUpdate"] == "agent_message_chunk" && update["content"]["text"] == "done!"
        {
            saw_text = true;
        }
        if update["sessionUpdate"] == "tool_call_update" {
            saw_tool_update = true;
        }
    }
    assert_eq!(stop_reason, "end_turn");
    assert!(saw_text, "expected agent_message_chunk notification");
    assert!(saw_tool_update, "expected tool_call_update notification");

    // Session history persisted: user + assistant(tool_calls) + tool + assistant.
    let msgs = store.messages(&session_id).await.unwrap();
    assert_eq!(msgs.len(), 4, "got {msgs:?}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[1]["tool_calls"][0]["function"]["name"], "echo.ping");
    assert_eq!(msgs[2]["role"], "tool");
    assert_eq!(msgs[3]["content"], "done!");
}

/// Mock LLM: first call returns two tool calls, second returns text.
async fn mock_llm_two_tools() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/chat/completions",
        post(move |_body: String| {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let sse = if n == 0 {
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"a\",\"arguments\":\"{}\"}},{\"index\":1,\"id\":\"call_2\",\"function\":{\"name\":\"b\",\"arguments\":\"{}\"}}]}}]}\n\n",
                        "data: [DONE]\n\n"
                    )
                } else {
                    "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"
                };
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

/// Mock LLM: first call emits a tool call to the registered test.sleep
/// tool; later calls return text (unreached when the cancel lands as
/// expected).
async fn mock_llm_registered_tool() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/chat/completions",
        post(move |_body: String| {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let sse = if n == 0 {
                    concat!(
                        "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"test.sleep\",\"arguments\":\"{\\\"secs\\\":5}\"}}]}}]}\n\n",
                        "data: [DONE]\n\n"
                    )
                } else {
                    "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"
                };
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

/// ClientChannel that cancels the turn on the first tool_call_update
/// notification — the in_progress notify fires before any call executes,
/// so the token is already set when the tool loop starts.
struct CancelOnToolUpdate {
    cancel: tokio_util::sync::CancellationToken,
}

#[async_trait::async_trait]
impl damon_core::runtime::ClientChannel for CancelOnToolUpdate {
    async fn notify(&self, _method: &str, params: Value) {
        if params["update"]["sessionUpdate"] == "tool_call_update" {
            self.cancel.cancel();
        }
    }
    async fn request(&self, _method: &str, _params: Value) -> anyhow::Result<Value> {
        Ok(json!({"outcome": {"optionId": "allow-once"}}))
    }
}

#[tokio::test]
async fn cancel_does_not_mask_real_tool_errors() {
    let upstream = mock_llm_two_tools().await;

    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;

    let session_id = "test-session";
    store
        .create_session(session_id, "/tmp", None)
        .await
        .unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(CancelOnToolUpdate {
        cancel: cancel.clone(),
    });

    // The cancel token is set by the first tool_call_update notification,
    // before either call executes. Both calls fail with genuine
    // unknown-tool errors, and the assertions prove those real errors are
    // persisted verbatim — a cancelled turn must not rewrite genuine tool
    // failures as "cancelled". Not covered here: the Cancelled-sentinel
    // path (a call in flight when the token fires → row says
    // "cancelled"), reached by
    // cancel_reaches_inflight_tool_and_persists_cancelled below, nor the
    // append-failure repair loop that backfills orphan tool_calls.
    let _ =
        damon_core::runtime::run_prompt(&state, session_id, &json!("hi"), None, &client, cancel)
            .await;

    let msgs = store.messages(session_id).await.unwrap();
    // user + assistant(tool_calls) + tool(error) + tool(error)
    assert_eq!(msgs.len(), 4, "got {msgs:?}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[1]["tool_calls"].as_array().unwrap().len(), 2);
    assert_eq!(msgs[2]["role"], "tool");
    assert_eq!(msgs[2]["tool_call_id"], "call_1");
    assert_eq!(msgs[3]["role"], "tool");
    assert_eq!(msgs[3]["tool_call_id"], "call_2");
    assert_eq!(msgs[3]["content"], "error: unknown tool b");
}

#[tokio::test]
async fn cancel_reaches_inflight_tool_and_persists_cancelled() {
    let upstream = mock_llm_registered_tool().await;

    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let server_py = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/mcp_server.py");
    let mut mcp_servers = HashMap::new();
    mcp_servers.insert(
        "test".to_string(),
        damon_core::config::McpServerConfig {
            command: "python3".to_string(),
            args: vec![server_py.to_string()],
            env: HashMap::new(),
            auto_approve: true,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers,
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = {
        let servers = shared.read().mcp_servers.clone();
        McpRegistry::connect_all(&servers).await
    };
    assert!(mcp.has_tool("test.sleep"), "MCP tool not registered");
    let state = AppState::new(shared, store.clone(), mcp).await;

    let session_id = "test-session";
    store
        .create_session(session_id, "/tmp", None)
        .await
        .unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(CancelOnToolUpdate {
        cancel: cancel.clone(),
    });

    // The in_progress tool_call_update sets the token before execute_tool
    // runs, so the registered call bails with the Cancelled sentinel and
    // is persisted as "cancelled" — a cancel must still leave a
    // well-formed tool row, not drop it (a missing row would orphan the
    // assistant tool_calls and 400 every later turn).
    let _ =
        damon_core::runtime::run_prompt(&state, session_id, &json!("hi"), None, &client, cancel)
            .await;

    let msgs = store.messages(session_id).await.unwrap();
    // user + assistant(tool_calls) + tool(cancelled); the cancelled turn
    // stops before a second LLM call.
    assert_eq!(msgs.len(), 3, "got {msgs:?}");
    assert_eq!(msgs[1]["tool_calls"].as_array().unwrap().len(), 1);
    assert_eq!(msgs[2]["role"], "tool");
    assert_eq!(msgs[2]["tool_call_id"], "call_1");
    assert_eq!(msgs[2]["content"], "cancelled");
}

/// Mock LLM: first call emits one SSE chunk then stalls forever;
/// later calls return text immediately.
async fn mock_llm_stall_then_text() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/chat/completions",
        post(move || {
            let calls = calls.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    let s = futures::stream::once(async {
                        Ok::<_, std::io::Error>(bytes::Bytes::from_static(
                            b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
                        ))
                    })
                    .chain(futures::stream::pending());
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(s))
                        .unwrap()
                } else {
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from(
                            "data: {\"choices\":[{\"delta\":{\"content\":\"done!\"}}]}\n\ndata: [DONE]\n\n",
                        ))
                        .unwrap()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

async fn ws_connect(
    addr: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap()
        .0
}

/// Build a WS handshake request with an explicit Origin header.
fn ws_request(addr: &str, origin: Option<&str>) -> tungstenite::http::Request<()> {
    let mut b = tungstenite::http::Request::builder()
        .uri(format!("ws://{addr}/ws"))
        .header("host", addr)
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header(
            "sec-websocket-key",
            tungstenite::handshake::client::generate_key(),
        );
    if let Some(o) = origin {
        b = b.header("origin", o);
    }
    b.body(()).unwrap()
}

/// Shared app state for handler-level tests: one openai-completions
/// provider pointed at `upstream`, no auth token.
async fn test_state(upstream: &str) -> (Arc<AppState>, Store) {
    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    (AppState::new(shared, store.clone(), mcp).await, store)
}

async fn serve(state: Arc<AppState>) -> String {
    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

/// Without an auth token, a browser WS handshake from a non-loopback
/// Origin must be rejected — otherwise any website could drive the agent.
#[tokio::test]
async fn ws_rejects_foreign_origin_without_token() {
    let upstream = mock_llm().await;
    let (state, _store) = test_state(&upstream).await;
    let addr = serve(state).await;

    // Foreign origin → 403.
    let err = tokio_tungstenite::connect_async(ws_request(&addr, Some("https://evil.example")))
        .await
        .unwrap_err();
    assert!(
        matches!(err, tungstenite::Error::Http(ref r) if r.status() == tungstenite::http::StatusCode::FORBIDDEN),
        "expected 403, got {err:?}"
    );

    // Loopback origins and no origin still work.
    for origin in [
        Some("http://localhost:3000"),
        Some("http://127.0.0.1:5173"),
        None,
    ] {
        tokio_tungstenite::connect_async(ws_request(&addr, origin))
            .await
            .unwrap_or_else(|e| panic!("origin {origin:?} rejected: {e}"));
    }
}

/// session/new validates mcpServers entries (malformed → -32602, a
/// well-formed but unspawnable command still creates the session —
/// connect failures are logged, not fatal), and session/prompt must
/// reject unknown sessions.
#[tokio::test]
async fn session_new_validates_mcp_servers_and_prompt_unknown_session() {
    let upstream = mock_llm().await;
    let (state, _store) = test_state(&upstream).await;
    let addr = serve(state).await;
    let mut ws = ws_connect(&addr).await;

    // Malformed entry — no command — is rejected before any spawn.
    send_rpc(
        &mut ws,
        1,
        "session/new",
        json!({"cwd": "/tmp", "mcpServers": [{"name": "x"}]}),
    )
    .await;
    let resp = read_json(&mut ws).await;
    assert_eq!(resp["error"]["code"], -32602, "got {resp}");
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("command")
    );

    // A well-formed entry is accepted even when the command itself is
    // unspawnable — connect failures are logged, not fatal (same rule as
    // global [mcp_servers]).
    send_rpc(
        &mut ws,
        2,
        "session/new",
        json!({"cwd": "/tmp", "mcpServers": [{"name": "x", "command": "definitely-not-a-real-binary-damon"}]}),
    )
    .await;
    let resp = read_json(&mut ws).await;
    assert!(resp["result"]["sessionId"].is_string(), "got {resp}");

    send_rpc(
        &mut ws,
        3,
        "session/prompt",
        json!({"sessionId": "no-such", "prompt": [{"type": "text", "text": "hi"}]}),
    )
    .await;
    let resp = read_json(&mut ws).await;
    assert_eq!(resp["error"]["code"], -32602, "got {resp}");
    assert_eq!(resp["error"]["message"], "session not found");
}

/// session/new's model becomes the session default; session/prompt's
/// model overrides it for that turn. Both must reach the upstream body.
#[tokio::test]
async fn session_and_prompt_model_reach_upstream() {
    let bodies = Arc::new(parking_lot::Mutex::new(Vec::<Value>::new()));
    let b2 = bodies.clone();
    let app = Router::new().route(
        "/chat/completions",
        post(move |body: String| {
            let b2 = b2.clone();
            async move {
                b2.lock().push(serde_json::from_str(&body).unwrap());
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n",
                    ))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (state, _store) = test_state(&upstream).await;
    let addr = serve(state).await;
    let mut ws = ws_connect(&addr).await;

    send_rpc(
        &mut ws,
        1,
        "session/new",
        json!({"cwd": "/tmp", "model": "session-model"}),
    )
    .await;
    let resp = read_json(&mut ws).await;
    let session_id = resp["result"]["sessionId"].as_str().unwrap().to_string();

    // Turn 1: no prompt model → session default applies.
    send_rpc(
        &mut ws,
        2,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "a"}]}),
    )
    .await;
    // Turn 2: prompt model overrides the session default.
    loop {
        let m = read_json(&mut ws).await;
        if m["id"] == 2 {
            break;
        }
    }
    send_rpc(
        &mut ws,
        3,
        "session/prompt",
        json!({"sessionId": session_id, "model": "turn-model", "prompt": [{"type": "text", "text": "b"}]}),
    )
    .await;
    loop {
        let m = read_json(&mut ws).await;
        if m["id"] == 3 {
            break;
        }
    }

    let bodies = bodies.lock();
    assert_eq!(bodies.len(), 2, "got {bodies:?}");
    assert_eq!(bodies[0]["model"], "session-model");
    assert_eq!(bodies[1]["model"], "turn-model");
}

async fn send_rpc(
    ws: &mut (impl SinkExt<tungstenite::Message, Error = tungstenite::Error> + Unpin),
    id: u64,
    method: &str,
    params: Value,
) {
    ws.send(tungstenite::Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
}

/// A prompt started by a connection that then disconnects must be
/// cancelled — not orphaned — and the session-level one-prompt guard
/// must hold across connections.
#[tokio::test]
async fn disconnect_cancels_prompt_and_frees_session() {
    let upstream = mock_llm_stall_then_text().await;

    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;
    let app = api::router(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // conn1: create a session and start a prompt that stalls upstream.
    let mut ws1 = ws_connect(&addr.to_string()).await;
    send_rpc(&mut ws1, 1, "session/new", json!({"cwd": "/tmp"})).await;
    let new: Value = read_json(&mut ws1).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();
    send_rpc(
        &mut ws1,
        2,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "hi"}]}),
    )
    .await;

    // Wait until the turn is registered as live.
    for _ in 0..100 {
        if state.live_prompts.lock().await.contains_key(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(state.live_prompts.lock().await.contains_key(&session_id));

    // conn2: a second prompt on the same session is rejected — the guard
    // is per-session, not per-connection.
    let mut ws2 = ws_connect(&addr.to_string()).await;
    send_rpc(
        &mut ws2,
        1,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "again"}]}),
    )
    .await;
    let rejected: Value = read_json(&mut ws2).await;
    assert_eq!(rejected["id"], 1);
    assert_eq!(rejected["error"]["code"], -32602, "got {rejected}");

    // conn1 drops: its in-flight turn must be cancelled and wound down.
    drop(ws1);
    for _ in 0..500 {
        if !state.live_prompts.lock().await.contains_key(&session_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !state.live_prompts.lock().await.contains_key(&session_id),
        "orphaned prompt still registered after disconnect"
    );

    // The session is free again: conn2's prompt now runs to completion.
    send_rpc(
        &mut ws2,
        2,
        "session/prompt",
        json!({"sessionId": session_id, "prompt": [{"type": "text", "text": "again"}]}),
    )
    .await;
    // Read until the id=2 response arrives (turn events interleave with
    // it). Deadline-bound rather than message-count-bound: a loaded CI
    // runner can trickle events arbitrarily slowly, and an error reply
    // must surface verbatim, not as an empty stopReason.
    let reply = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let msg: Value = read_json(&mut ws2).await;
            if msg["id"] == 2 {
                return msg;
            }
        }
    })
    .await
    .expect("id=2 prompt response never arrived");
    assert_eq!(
        reply["result"]["stopReason"], "end_turn",
        "unexpected reply: {reply}"
    );
}

async fn read_json(
    ws: &mut (impl StreamExt<Item = Result<tungstenite::Message, tungstenite::Error>> + Unpin),
) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            tungstenite::Message::Text(t) => {
                return serde_json::from_str(&t).unwrap();
            }
            _ => continue,
        }
    }
}

/// Regression: a dead connection must fail pending requests, not hang
/// them. The server accepts the WS upgrade then closes immediately.
#[tokio::test]
async fn client_request_fails_on_disconnect() {
    use axum::extract::ws::{WebSocket, WebSocketUpgrade};
    use axum::routing::get;
    use damon_core::client::DamonClient;

    async fn close_immediately(ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(|mut sock: WebSocket| async move {
            let _ = sock.close().await;
        })
    }
    let app = Router::new().route("/ws", get(close_immediately));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = DamonClient::connect(&format!("ws://{addr}/ws"), None)
        .await
        .unwrap();
    // The request must resolve with an error once the socket dies —
    // before the fix it waited on a response that could never arrive.
    // The deadline must exceed request()'s documented worst case, not
    // the healthy-path latency: up to 3 attempts, each parking as long
    // as RECONNECT_WAIT (10s) for a reconnect that keeps half-arriving
    // (this peer accepts then closes), plus the redial backoff chain.
    // A pre-fix hang trips any finite deadline; a healthy runner
    // resolves in milliseconds.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        client.request("session/list", json!({})),
    )
    .await
    .expect("request hung after disconnect");
    assert!(result.is_err(), "expected error, got {result:?}");
}

/// Regression: a failed compaction summary must NOT record a compaction —
/// the placeholder used to permanently drop half the session's context.
#[tokio::test]
async fn compaction_failure_keeps_full_history() {

    // Mock upstream: non-streaming (the summarizer) 500s, streaming
    // (the actual turn) succeeds.
    let app = Router::new().route(
        "/chat/completions",
        post(|body: String| async move {
            let v: Value = serde_json::from_str(&body).unwrap();
            if v["stream"].as_bool() == Some(true) {
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n",
                    ))
                    .unwrap()
            } else {
                Response::builder()
                    .status(500)
                    .body(Body::from("boom"))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{addr}")),
            api_key: None,
            models: vec![],
            default_model: Some("m".into()),
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    // Tiny context window forces compaction to run on this turn.
    let mut models = BTreeMap::new();
    models.insert(
        "m".to_string(),
        damon_core::config::ModelMeta {
            context_window: Some(1),
            ..Default::default()
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models,
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;

    store.create_session("s1", "", None).await.unwrap();
    // Seed enough history that the dropped half is non-empty.
    store
        .append(
            "s1",
            "user",
            &json!({"role":"user","content":"earlier message"}),
        )
        .await
        .unwrap();
    store
        .append(
            "s1",
            "assistant",
            &json!({"role":"assistant","content":"earlier reply"}),
        )
        .await
        .unwrap();

    struct Noop;
    #[async_trait::async_trait]
    impl damon_core::runtime::ClientChannel for Noop {
        async fn notify(&self, _m: &str, _p: Value) {}
        async fn request(&self, _m: &str, _p: Value) -> anyhow::Result<Value> {
            anyhow::bail!("no client")
        }
    }
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(Noop);
    let cancel = tokio_util::sync::CancellationToken::new();
    damon_core::runtime::run_prompt(&state, "s1", &json!("new question"), None, &client, cancel)
        .await
        .unwrap();

    // The failed summary must not have been recorded — the cutoff stays 0
    // and messages() still returns the full history.
    let (through, summary) = store.compaction("s1").await.unwrap();
    assert_eq!(through, 0, "compaction was recorded despite failure");
    assert!(summary.is_none());
    let msgs = store.messages("s1").await.unwrap();
    assert!(
        msgs.iter().any(|m| m["content"] == "earlier message"),
        "history was dropped: {msgs:?}"
    );
}

/// Regression: the old token estimate priced a 3-byte CJK char at 0.75
/// tokens (len/4), so a Korean session only reached the 85% compaction
/// threshold at ~1.13× the real window and 400ed before ever
/// compacting. Weighting non-ASCII bytes puts a CJK char at ~1 token:
/// the same history that stayed under the threshold with the old
/// formula must now trigger compaction.
#[tokio::test]
async fn compaction_triggers_on_cjk_history() {

    // Mock upstream: non-streaming (the summarizer) returns a summary,
    // streaming (the actual turn) succeeds.
    let app = Router::new().route(
        "/chat/completions",
        post(|body: String| async move {
            let v: Value = serde_json::from_str(&body).unwrap();
            if v["stream"].as_bool() == Some(true) {
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n",
                    ))
                    .unwrap()
            } else {
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        "{\"choices\":[{\"message\":{\"content\":\"cjk summary\"}}]}",
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

    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{addr}")),
            api_key: None,
            models: vec![],
            default_model: Some("m".into()),
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    // Window sits between the old and new estimates of the CJK history:
    // 100 Korean chars = 300 bytes → old 300/4 = 75 tokens, weighted
    // (300 + 300/3)/4 = 100 tokens. Threshold 85% of 100 = 85: the old
    // formula (75) must NOT compact, the weighted one (100) must.
    let mut models = BTreeMap::new();
    models.insert(
        "m".to_string(),
        damon_core::config::ModelMeta {
            context_window: Some(100),
            ..Default::default()
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models,
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;

    store.create_session("s1", "", None).await.unwrap();
    store
        .append(
            "s1",
            "user",
            &json!({"role":"user","content":"한".repeat(100)}),
        )
        .await
        .unwrap();

    struct Noop;
    #[async_trait::async_trait]
    impl damon_core::runtime::ClientChannel for Noop {
        async fn notify(&self, _m: &str, _p: Value) {}
        async fn request(&self, _m: &str, _p: Value) -> anyhow::Result<Value> {
            anyhow::bail!("no client")
        }
    }
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(Noop);
    let cancel = tokio_util::sync::CancellationToken::new();
    damon_core::runtime::run_prompt(&state, "s1", &json!("hi"), None, &client, cancel)
        .await
        .unwrap();

    let (through, summary) = store.compaction("s1").await.unwrap();
    assert!(through > 0, "CJK history did not trigger compaction");
    assert_eq!(summary.as_deref(), Some("cjk summary"));
}

/// Mock LLM: the first `tool_turns` calls each emit one tool call for
/// `tool`, later calls return text. Used to drive the permission and
/// iteration-cap paths against a real MCP tool.
async fn mock_llm_tool(tool: &str, tool_turns: usize) -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = tool.to_string();
    let app = Router::new().route(
        "/chat/completions",
        post(move |_body: String| {
            let calls = calls.clone();
            let tool = tool.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                let sse = if n < tool_turns {
                    format!(
                        "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call_{n}\",\"function\":{{\"name\":\"{tool}\",\"arguments\":\"{{}}\"}}}}]}}}}]}}\n\ndata: [DONE]\n\n"
                    )
                } else {
                    "data: {\"choices\":[{\"delta\":{\"content\":\"done!\"}}]}\n\ndata: [DONE]\n\n"
                        .to_string()
                };
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr.to_string()
}

/// App state wired to `upstream` plus the python `mcp_server.py` stdio
/// server (tool `test.ping`). `auto_approve` controls whether calls skip
/// the permission round-trip.
async fn mcp_state(
    upstream: &str,
    auto_approve: bool,
    permission_timeout_secs: Option<u64>,
) -> (Arc<AppState>, Store) {
    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let server_py = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/mcp_server.py");
    let mut mcp_servers = HashMap::new();
    mcp_servers.insert(
        "test".to_string(),
        damon_core::config::McpServerConfig {
            command: "python3".to_string(),
            args: vec![server_py.to_string()],
            env: HashMap::new(),
            auto_approve,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers,
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let servers = shared.read().mcp_servers.clone();
    let mcp = McpRegistry::connect_all(&servers).await;
    assert!(mcp.has_tool("test.ping"), "MCP tool not registered");
    (AppState::new(shared, store.clone(), mcp).await, store)
}

/// ClientChannel whose permission requests never resolve — the runtime
/// must deny the tool once permission_timeout_secs elapses.
struct SilentClient;

#[async_trait::async_trait]
impl damon_core::runtime::ClientChannel for SilentClient {
    async fn notify(&self, _method: &str, _params: Value) {}
    async fn request(&self, _method: &str, _params: Value) -> anyhow::Result<Value> {
        futures::future::pending().await
    }
}

#[tokio::test]
async fn permission_prompt_timeout_denies_tool() {
    let upstream = mock_llm_tool("test.ping", 1).await;
    let (state, store) = mcp_state(&upstream, false, Some(1)).await;
    store.create_session("s1", "/tmp", None).await.unwrap();

    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(SilentClient);
    let cancel = tokio_util::sync::CancellationToken::new();
    // The client never answers; without the timeout this hangs forever.
    damon_core::runtime::run_prompt(&state, "s1", &json!("hi"), None, &client, cancel)
        .await
        .unwrap();

    let msgs = store.messages("s1").await.unwrap();
    let tool_row = msgs.iter().find(|m| m["role"] == "tool").unwrap();
    assert!(
        tool_row["content"]
            .as_str()
            .unwrap()
            .contains("permission denied"),
        "expected denial, got {tool_row}"
    );
}

/// ClientChannel that answers every permission prompt with
/// "allow-always" and counts how many it saw.
struct AlwaysAllowClient {
    requests: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl damon_core::runtime::ClientChannel for AlwaysAllowClient {
    async fn notify(&self, _method: &str, _params: Value) {}
    async fn request(&self, _method: &str, _params: Value) -> anyhow::Result<Value> {
        self.requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(json!({"outcome": {"optionId": "allow-always"}}))
    }
}

#[tokio::test]
async fn always_allow_skips_later_prompts() {
    // Two tool-call turns: the first prompts, the second must not.
    let upstream = mock_llm_tool("test.ping", 2).await;
    let (state, store) = mcp_state(&upstream, false, None).await;
    store.create_session("s1", "/tmp", None).await.unwrap();

    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(AlwaysAllowClient {
        requests: requests.clone(),
    });
    let cancel = tokio_util::sync::CancellationToken::new();
    damon_core::runtime::run_prompt(&state, "s1", &json!("hi"), None, &client, cancel)
        .await
        .unwrap();

    assert_eq!(
        requests.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "second call to test.ping should have been auto-approved"
    );
    // Both tool rows carry real results, not permission errors.
    let msgs = store.messages("s1").await.unwrap();
    let tool_rows: Vec<&Value> = msgs.iter().filter(|m| m["role"] == "tool").collect();
    assert_eq!(tool_rows.len(), 2, "got {msgs:?}");
    for row in tool_rows {
        assert!(
            !row["content"]
                .as_str()
                .unwrap()
                .contains("permission denied"),
            "unexpected denial: {row}"
        );
    }
}

#[tokio::test]
async fn max_iterations_returns_max_turn_requests() {
    // The model never stops calling tools — the loop must give up with
    // MaxTurnRequests after MAX_ITERATIONS rounds.
    let upstream = mock_llm_tool("test.ping", usize::MAX).await;
    let (state, store) = mcp_state(&upstream, true, None).await;
    store.create_session("s1", "/tmp", None).await.unwrap();

    struct Noop;
    #[async_trait::async_trait]
    impl damon_core::runtime::ClientChannel for Noop {
        async fn notify(&self, _m: &str, _p: Value) {}
        async fn request(&self, _m: &str, _p: Value) -> anyhow::Result<Value> {
            anyhow::bail!("no client")
        }
    }
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(Noop);
    let cancel = tokio_util::sync::CancellationToken::new();
    let stop = damon_core::runtime::run_prompt(&state, "s1", &json!("hi"), None, &client, cancel)
        .await
        .unwrap();
    assert_eq!(
        stop.stop_reason,
        damon_core::llm::StopReason::MaxTurnRequests
    );
}

#[tokio::test]
async fn compaction_reevaluates_after_tool_results_grow_history() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Mock upstream: streaming calls emit one tool call then text;
    // non-streaming calls are the compaction summarizer — counted, and
    // answered with a summary large enough to keep the tail over the
    // (tiny) context window so compaction must re-trigger next iteration.
    let summarize_calls = Arc::new(AtomicUsize::new(0));
    let stream_calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/chat/completions",
        post({
            let summarize_calls = summarize_calls.clone();
            let stream_calls = stream_calls.clone();
            move |body: String| {
                let summarize_calls = summarize_calls.clone();
                let stream_calls = stream_calls.clone();
                async move {
                    let v: Value = serde_json::from_str(&body).unwrap();
                    if v["stream"].as_bool() == Some(true) {
                        let n = stream_calls.fetch_add(1, Ordering::SeqCst);
                        let sse = if n == 0 {
                            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"test.ping\",\"arguments\":\"{}\"}}]}}]}\n\ndata: [DONE]\n\n"
                        } else {
                            "data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\ndata: [DONE]\n\n"
                        };
                        Response::builder()
                            .header("content-type", "text/event-stream")
                            .body(Body::from(sse))
                            .unwrap()
                    } else {
                        summarize_calls.fetch_add(1, Ordering::SeqCst);
                        Response::builder()
                            .header("content-type", "application/json")
                            .body(Body::from(
                                json!({"choices":[{"message":{"content": "x".repeat(2000)}}]})
                                    .to_string(),
                            ))
                            .unwrap()
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Real MCP tool so the call executes and its result row appends.
    let (state, store) = mcp_state(&addr.to_string(), true, None).await;
    // Tiny context window: compaction triggers on every evaluation.
    {
        let mut models = BTreeMap::new();
        models.insert(
            "default".to_string(),
            damon_core::config::ModelMeta {
                context_window: Some(1),
                ..Default::default()
            },
        );
        state.config.write().models = models;
    }
    store.create_session("s1", "", None).await.unwrap();
    store
        .append(
            "s1",
            "user",
            &json!({"role":"user","content":"earlier message"}),
        )
        .await
        .unwrap();
    store
        .append(
            "s1",
            "assistant",
            &json!({"role":"assistant","content":"earlier reply"}),
        )
        .await
        .unwrap();

    struct Noop;
    #[async_trait::async_trait]
    impl damon_core::runtime::ClientChannel for Noop {
        async fn notify(&self, _m: &str, _p: Value) {}
        async fn request(&self, _m: &str, _p: Value) -> anyhow::Result<Value> {
            anyhow::bail!("no client")
        }
    }
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(Noop);
    let cancel = tokio_util::sync::CancellationToken::new();
    damon_core::runtime::run_prompt(&state, "s1", &json!("new question"), None, &client, cancel)
        .await
        .unwrap();

    // Two model iterations ran (tool call, then text). The tool result
    // appended between them grew the history, so compaction must have
    // been evaluated twice — once per iteration. The old flag stayed
    // false after the first check and summarized only once.
    assert_eq!(
        summarize_calls.load(Ordering::SeqCst),
        2,
        "compaction did not re-evaluate after tool results grew history"
    );
}

/// session/compact forces a compaction pass regardless of the estimate
/// threshold — the manual escape hatch for a wedged session. The mock
/// answers non-streaming calls (the summarizer) with a JSON summary.
/// The provider model has no context_window metadata, so this also
/// covers the forced path where the threshold can't even be computed.
#[tokio::test]
async fn session_compact_forces_summarization() {
    let app = Router::new().route(
        "/chat/completions",
        post(|_body: String| async move {
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"choices":[{"message":{"content":"summary of earlier turns"}}]})
                        .to_string(),
                ))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (state, store) = test_state(&upstream.to_string()).await;
    let addr = serve(state).await;
    let mut ws = ws_connect(&addr).await;

    send_rpc(&mut ws, 1, "session/new", json!({"cwd": "/tmp"})).await;
    let resp = read_json(&mut ws).await;
    let sid = resp["result"]["sessionId"].as_str().unwrap().to_string();

    // Seed history directly — four rows so the oldest half is droppable.
    for i in 0..4 {
        store
            .append(
                &sid,
                if i % 2 == 0 { "user" } else { "assistant" },
                &json!({"role": if i % 2 == 0 { "user" } else { "assistant" }, "content": format!("msg {i}")}),
            )
            .await
            .unwrap();
    }

    send_rpc(&mut ws, 2, "session/compact", json!({"sessionId": sid})).await;
    let resp = read_json(&mut ws).await;
    assert_eq!(resp["result"]["compacted"], true, "got {resp}");
    assert!(resp["result"]["compactedThrough"].as_i64().unwrap() > 0);

    // The summary is now what the model sees for the compacted range.
    let (_, summary) = store.compaction(&sid).await.unwrap();
    assert_eq!(summary.as_deref(), Some("summary of earlier turns"));

    // Unknown session → -32602, same as session/prompt.
    send_rpc(
        &mut ws,
        3,
        "session/compact",
        json!({"sessionId": "no-such"}),
    )
    .await;
    let resp = read_json(&mut ws).await;
    assert_eq!(resp["error"]["code"], -32602, "got {resp}");
}

/// The first user prompt becomes the session title; a second prompt must
/// not overwrite it, and an explicit rename survives later prompts.
#[tokio::test]
async fn first_prompt_sets_title_and_rename_sticks() {
    let upstream = mock_llm_tool("noop.never", 0).await; // always text reply
    let mut providers = BTreeMap::new();
    providers.insert(
        "default".to_string(),
        ProviderConfig {
            api: "openai-completions".to_string(),
            base_url: Some(format!("http://{upstream}")),
            api_key: None,
            models: vec![],
            default_model: None,
            headers: Default::default(),
            compat: Default::default(),
            discovery: None,
            context_promotion_target: None,
        },
    );
    let shared: damon_core::config::SharedConfig = Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: None,
        tls_cert: None,
        tls_key: None,
        data_dir: None,
        mcp_servers: HashMap::new(),
        providers,
        models: BTreeMap::new(),
        relay: None,
        permission_timeout_secs: None,
        max_tool_output: None,
        summary_model: None,
        session_retention_days: None,
        builtin_tools: Default::default(),
    }));
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(shared, store.clone(), mcp).await;

    struct Noop;
    #[async_trait::async_trait]
    impl damon_core::runtime::ClientChannel for Noop {
        async fn notify(&self, _m: &str, _p: Value) {}
        async fn request(&self, _m: &str, _p: Value) -> anyhow::Result<Value> {
            anyhow::bail!("no client")
        }
    }
    let client: Arc<dyn damon_core::runtime::ClientChannel> = Arc::new(Noop);

    store.create_session("s1", "", None).await.unwrap();
    damon_core::runtime::run_prompt(
        &state,
        "s1",
        &json!("summarize this repository please"),
        None,
        &client,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.list_sessions().await.unwrap()[0].3,
        "summarize this repository please"
    );

    // Second prompt must not overwrite the title.
    damon_core::runtime::run_prompt(
        &state,
        "s1",
        &json!("a different question entirely"),
        None,
        &client,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        store.list_sessions().await.unwrap()[0].3,
        "summarize this repository please"
    );

    // Explicit rename survives subsequent prompts.
    store.rename_session("s1", "repo work").await.unwrap();
    damon_core::runtime::run_prompt(
        &state,
        "s1",
        &json!("third question"),
        None,
        &client,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(store.list_sessions().await.unwrap()[0].3, "repo work");
}
