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

/// ClientChannel that cancels the turn on the first tool_call_update
/// notification — after call_1's result is persisted, before call_2 runs.
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
async fn cancel_mid_tool_loop_repairs_orphan_calls() {
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

    // call_1 executes (fails: unknown tool), its tool_call_update fires
    // cancel → call_2 never runs → must get a "cancelled" tool row.
    let _ = damon_core::runtime::run_prompt(&state, session_id, "hi", None, &client, cancel).await;

    let msgs = store.messages(session_id).await.unwrap();
    // user + assistant(tool_calls) + tool(error) + tool(cancelled)
    assert_eq!(msgs.len(), 4, "got {msgs:?}");
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[1]["tool_calls"].as_array().unwrap().len(), 2);
    assert_eq!(msgs[2]["role"], "tool");
    assert_eq!(msgs[2]["tool_call_id"], "call_1");
    assert_eq!(msgs[3]["role"], "tool");
    assert_eq!(msgs[3]["tool_call_id"], "call_2");
    assert_eq!(msgs[3]["content"], "cancelled");
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

/// session/new must reject a non-empty mcpServers list instead of
/// silently ignoring it, and session/prompt must reject unknown sessions.
#[tokio::test]
async fn session_new_rejects_mcp_servers_and_prompt_unknown_session() {
    let upstream = mock_llm().await;
    let (state, _store) = test_state(&upstream).await;
    let addr = serve(state).await;
    let mut ws = ws_connect(&addr).await;

    send_rpc(
        &mut ws,
        1,
        "session/new",
        json!({"cwd": "/tmp", "mcpServers": [{"name": "x", "command": "y"}]}),
    )
    .await;
    let resp = read_json(&mut ws).await;
    assert_eq!(resp["error"]["code"], -32602, "got {resp}");
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mcpServers")
    );

    send_rpc(
        &mut ws,
        2,
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
    let mut stop_reason = String::new();
    for _ in 0..50 {
        let msg: Value = read_json(&mut ws2).await;
        if msg["id"] == 2 {
            stop_reason = msg["result"]["stopReason"]
                .as_str()
                .unwrap_or("")
                .to_string();
            break;
        }
    }
    assert_eq!(stop_reason, "end_turn");
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
