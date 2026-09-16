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

    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
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
            stop_reason = msg["result"]["stopReason"].as_str().unwrap_or("").to_string();
            break;
        }
        let update = &msg["params"]["update"];
        if update["sessionUpdate"] == "agent_message_chunk"
            && update["content"]["text"] == "done!"
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
    store.create_session(session_id, "/tmp").await.unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let client: Arc<dyn damon_core::runtime::ClientChannel> =
        Arc::new(CancelOnToolUpdate { cancel: cancel.clone() });

    // call_1 executes (fails: unknown tool), its tool_call_update fires
    // cancel → call_2 never runs → must get a "cancelled" tool row.
    let _ = damon_core::runtime::run_prompt(&state, session_id, "hi", &client, cancel).await;

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
