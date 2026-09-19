//! JSON-RPC surface tests: busy session/delete, WS ticket auth
//! (issue → single-use consume), and session/list pagination.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::response::Response;
use axum::routing::post;
use damon_core::api::{self, AppState};
use damon_core::config::{Config, ProviderConfig, SharedConfig};
use damon_core::mcp::McpRegistry;
use damon_core::store::Store;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite;

fn test_config(auth_token: Option<&str>, upstream: Option<String>) -> SharedConfig {
    let mut providers = BTreeMap::new();
    if let Some(base) = upstream {
        providers.insert(
            "default".to_string(),
            ProviderConfig {
                api: "openai-completions".to_string(),
                base_url: Some(base),
                api_key: None,
                models: vec![],
                default_model: None,
                headers: Default::default(),
                compat: Default::default(),
                discovery: None,
                context_promotion_target: None,
            },
        );
    }
    Arc::new(parking_lot::RwLock::new(Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        auth_token: auth_token.map(String::from),
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
    }))
}

/// Build AppState + serve the real router on a loopback socket.
async fn serve(config: SharedConfig) -> (Arc<AppState>, Store, String) {
    let store = Store::in_memory().await.unwrap();
    let mcp = McpRegistry::connect_all(&HashMap::new()).await;
    let state = AppState::new(config, store.clone(), mcp).await;
    let app = api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (state, store, addr.to_string())
}

/// Mock LLM upstream that accepts the request but never responds — the
/// turn parks inside `chat_stream` until cancelled (the connect await is
/// wrapped in the cancel select, so cancel must abort it).
async fn mock_llm_hangs() -> String {
    let app = Router::new().route(
        "/chat/completions",
        post(|| async move { std::future::pending::<Response<Body>>().await }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
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

async fn rpc_send<S>(ws: &mut S, msg: Value)
where
    S: futures::Sink<tungstenite::Message, Error = tungstenite::Error> + Unpin,
{
    ws.send(tungstenite::Message::Text(msg.to_string().into()))
        .await
        .unwrap();
}

/// session/delete against a session whose turn is parked on a hung
/// upstream: the delete must CANCEL the turn (prompt id=2 fails with the
/// cancellation error), free the live_prompts entry, and delete the row —
/// a hung provider must not wedge the session permanently.
#[tokio::test]
async fn delete_busy_session_cancels_turn_and_deletes() {
    let upstream = mock_llm_hangs().await;
    let (state, store, addr) = serve(test_config(None, Some(upstream))).await;

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}),
    )
    .await;
    let new: Value = read_json(&mut ws).await;
    let session_id = new["result"]["sessionId"].as_str().unwrap().to_string();

    // The prompt parks forever — the mock upstream never answers.
    rpc_send(
        &mut ws,
        json!({
            "jsonrpc":"2.0","id":2,"method":"session/prompt",
            "params":{"sessionId":session_id,"prompt":[{"type":"text","text":"hi"}]}
        }),
    )
    .await;

    // Wait until the turn is registered as live before deleting.
    let mut live = false;
    for _ in 0..50 {
        if state.live_prompts.lock().await.contains_key(&session_id) {
            live = true;
            break;
        }
        if let Ok(v) =
            tokio::time::timeout(std::time::Duration::from_millis(50), read_json(&mut ws)).await
        {
            panic!("prompt turn ended early: {v}");
        }
    }
    assert!(live, "prompt turn never registered in live_prompts");

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":3,"method":"session/delete","params":{"sessionId":session_id}}),
    )
    .await;
    // First the prompt turn must unwind with its cancellation error...
    let prompt_reply = tokio::time::timeout(std::time::Duration::from_secs(30), read_json(&mut ws))
        .await
        .expect("prompt turn never unwound after delete");
    assert_eq!(prompt_reply["id"], 2, "unexpected reply: {prompt_reply}");
    // Cancel unwinds the turn as a normal result — the runtime maps a
    // cancelled turn to stopReason:"cancelled" rather than an RPC error.
    assert_eq!(
        prompt_reply["result"]["stopReason"]
            .as_str()
            .unwrap_or_default(),
        "cancelled",
        "expected cancelled stopReason: {prompt_reply}"
    );
    // ...then the delete itself succeeds.
    let reply = tokio::time::timeout(std::time::Duration::from_secs(30), read_json(&mut ws))
        .await
        .expect("session/delete reply never arrived");
    assert!(reply.get("error").is_none(), "unexpected error: {reply}");
    // The row is gone — the session was actually deleted.
    assert!(!store.session_exists(&session_id).await.unwrap());
}

/// POST /v1/ws_ticket issues a one-shot ticket that authenticates /ws.
#[tokio::test]
async fn ws_ticket_authenticates() {
    let (_state, _store, addr) = serve(test_config(Some("secret"), None)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ws_ticket"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ticket = resp.json::<Value>().await.unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
        .await
        .unwrap();
    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
    )
    .await;
    let init: Value = read_json(&mut ws).await;
    assert_eq!(init["result"]["agentInfo"]["name"], "damond");
}

/// A consumed ticket must not authenticate a second connection.
#[tokio::test]
async fn ws_ticket_is_single_use() {
    let (_state, _store, addr) = serve(test_config(Some("secret"), None)).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/ws_ticket"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    let ticket = resp.json::<Value>().await.unwrap()["ticket"]
        .as_str()
        .unwrap()
        .to_string();

    // First use: handshake succeeds (the ticket is consumed here).
    let (_ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
        .await
        .unwrap();

    // Second use of the same ticket: the upgrade is rejected with 401.
    let err = tokio_tungstenite::connect_async(format!("ws://{addr}/ws?ticket={ticket}"))
        .await
        .unwrap_err();
    match err {
        tungstenite::Error::Http(resp) => {
            assert_eq!(resp.status(), tungstenite::http::StatusCode::UNAUTHORIZED);
        }
        other => panic!("expected HTTP 401 rejection, got {other:?}"),
    }
}

/// DamonClient::connect with a token must exchange it for a ticket and
/// authenticate — the token itself never appears in the WS URL.
#[tokio::test]
async fn client_connect_uses_ticket_auth() {
    let (_state, _store, addr) = serve(test_config(Some("secret"), None)).await;

    let client =
        damon_core::client::DamonClient::connect(&format!("ws://{addr}/ws"), Some("secret"))
            .await
            .expect("connect with token should succeed via ticket");
    let init = client.initialize().await.unwrap();
    assert_eq!(init["agentInfo"]["name"], "damond");

    // A wrong token must fail at the ticket exchange, before any WS dial.
    let err =
        damon_core::client::DamonClient::connect(&format!("ws://{addr}/ws"), Some("wrong")).await;
    assert!(err.is_err(), "wrong token must not connect");
}

/// session/list with limit/offset pages instead of returning everything.
#[tokio::test]
async fn session_list_paginates() {
    let (_state, store, addr) = serve(test_config(None, None)).await;
    store.create_session("s1", "/a", None).await.unwrap();
    store.create_session("s2", "/b", None).await.unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"session/list","params":{"limit":1}}),
    )
    .await;
    let page1: Value = read_json(&mut ws).await;
    let sessions = page1["result"]["sessions"].as_array().unwrap();
    assert_eq!(
        sessions.len(),
        1,
        "limit=1 must return one session: {page1}"
    );

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":2,"method":"session/list","params":{"limit":1,"offset":1}}),
    )
    .await;
    let page2: Value = read_json(&mut ws).await;
    let sessions2 = page2["result"]["sessions"].as_array().unwrap();
    assert_eq!(sessions2.len(), 1);
    assert_ne!(
        sessions[0]["sessionId"], sessions2[0]["sessionId"],
        "offset=1 must return the other session"
    );

    // No paging params → the full list.
    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":3,"method":"session/list","params":{}}),
    )
    .await;
    let all: Value = read_json(&mut ws).await;
    assert_eq!(all["result"]["sessions"].as_array().unwrap().len(), 2);
}

/// An unpaged session/messages response that would exceed the client's
/// 4 MiB receive cap must come back as an explicit JSON-RPC error (telling
/// the client to page) instead of an oversized frame that kills the link —
/// and the connection must stay usable afterwards.
#[tokio::test]
async fn oversized_messages_response_fails_loud_and_link_survives() {
    let (_state, store, addr) = serve(test_config(None, None)).await;
    store.create_session("s1", "/a", None).await.unwrap();
    // One prompt larger than the 4 MiB frame cap.
    store
        .append(
            "s1",
            "user",
            &json!({"role":"user","content":"x".repeat(5 << 20)}),
        )
        .await
        .unwrap();
    store.create_session("s2", "/b", None).await.unwrap();
    store
        .append("s2", "user", &json!({"role":"user","content":"small"}))
        .await
        .unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"session/messages","params":{"sessionId":"s1"}}),
    )
    .await;
    let resp: Value = read_json(&mut ws).await;
    assert_eq!(resp["id"], 1, "unexpected frame: {resp}");
    assert!(
        resp["error"].is_object(),
        "oversized response must be an error: {resp}"
    );
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("limit/offset")
    );
    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":2,"method":"session/messages",
               "params":{"sessionId":"s2","limit":1}}),
    )
    .await;
    let page: Value = read_json(&mut ws).await;
    assert_eq!(page["id"], 2);
    assert!(page["result"]["messages"].as_array().unwrap().len() == 1);
}

/// session/messages honors limit/offset and returns OpenAI-shaped rows.
#[tokio::test]
async fn session_messages_paginates() {
    let (_state, store, addr) = serve(test_config(None, None)).await;
    store.create_session("s1", "/a", None).await.unwrap();
    for i in 0..3 {
        store
            .append(
                "s1",
                "user",
                &json!({"role":"user","content":format!("m{i}")}),
            )
            .await
            .unwrap();
    }

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":1,"method":"session/messages",
               "params":{"sessionId":"s1","limit":1,"offset":1}}),
    )
    .await;
    let page: Value = read_json(&mut ws).await;
    let msgs = page["result"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1, "unexpected page: {page}");
    assert_eq!(msgs[0]["content"], "m1");

    rpc_send(
        &mut ws,
        json!({"jsonrpc":"2.0","id":2,"method":"session/messages","params":{"sessionId":"s1"}}),
    )
    .await;
    let all: Value = read_json(&mut ws).await;
    assert_eq!(all["result"]["messages"].as_array().unwrap().len(), 3);
}
