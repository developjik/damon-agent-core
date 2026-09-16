use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::AppState;
use crate::runtime::{self, ClientChannel};

#[derive(Deserialize)]
pub struct WsQuery {
    token: Option<String>,
}

/// GET /ws — ACP-shaped JSON-RPC over WebSocket.
/// Auth: same bearer token as /v1, via Authorization header or ?token=.
pub async fn ws_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<WsQuery>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    if let Some(expected) = state.auth_token() {
        let header_ok = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| t == expected);
        let query_ok = q.token.as_deref() == Some(expected.as_str());
        if !header_ok && !query_ok {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(ws.on_upgrade(move |socket| {
        let (mut writer, mut reader) = socket.split();
        let (tx_in, rx_in) = mpsc::channel::<String>(64);
        let (tx_out, mut rx_out) = mpsc::channel::<String>(64);
        // Pump: socket → tx_in (inbound), rx_out → socket (outbound).
        tokio::spawn(async move {
            while let Some(Ok(msg)) = reader.next().await {
                if let Message::Text(t) = msg {
                    if tx_in.send(t.to_string()).await.is_err() {
                        break;
                    }
                }
            }
        });
        tokio::spawn(async move {
            while let Some(text) = rx_out.recv().await {
                if writer.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
        });
        handle_socket(rx_in, tx_out, state)
    }))
}

/// Client-facing channel: notifications and server→client requests.
/// Transport-agnostic — writes JSON text frames to `tx`.
struct WsClient {
    tx: mpsc::Sender<String>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    next_id: AtomicU64,
}

#[async_trait::async_trait]
impl ClientChannel for WsClient {
    async fn notify(&self, method: &str, params: Value) {
        let msg = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let _ = self.tx.send(msg.to_string()).await;
    }

    async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if self.tx.send(msg.to_string()).await.is_err() {
            self.pending.lock().await.remove(&id);
            bail!("client connection closed");
        }
        // A client that never answers must not hang the turn forever or
        // leak the pending entry.
        match tokio::time::timeout(std::time::Duration::from_secs(120), rx).await {
            Ok(r) => r.context("client dropped request"),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                bail!("client did not respond to {method} within 120s")
            }
        }
    }
}

impl WsClient {
    async fn respond(&self, id: Value, result: Result<Value, Value>) {
        let msg = match result {
            Ok(r) => json!({"jsonrpc": "2.0", "id": id, "result": r}),
            Err(e) => json!({"jsonrpc": "2.0", "id": id, "error": e}),
        };
        let _ = self.tx.send(msg.to_string()).await;
    }
}

/// Run one client session over a generic text transport.
/// `rx` yields inbound JSON text; `tx` carries outbound JSON text.
/// Used by both the local WS handler and the relay tunnel.
pub async fn handle_socket(
    rx: mpsc::Receiver<String>,
    tx: mpsc::Sender<String>,
    state: Arc<AppState>,
) {
    let mut rx = rx;
    let client = Arc::new(WsClient {
        tx,
        pending: Arc::new(Mutex::new(HashMap::new())),
        next_id: AtomicU64::new(1),
    });
    let cancels: Arc<Mutex<HashMap<String, CancellationToken>>> =
        Arc::new(Mutex::new(HashMap::new()));

    info!("ws client connected");
    while let Some(text) = rx.recv().await {
        let Ok(v) = serde_json::from_str::<Value>(&text) else {
            warn!("invalid JSON from client");
            continue;
        };

        // Response to a server-initiated request.
        if v.get("method").is_none() && v.get("id").is_some() {
            if let Some(id) = v["id"].as_u64() {
                if let Some(tx) = client.pending.lock().await.remove(&id) {
                    let _ = tx.send(v.get("result").cloned().unwrap_or(Value::Null));
                }
            }
            continue;
        }

        let Some(method) = v["method"].as_str() else { continue };
        let id = v.get("id").cloned();
        let params = v["params"].clone();

        match (method, id) {
            ("initialize", Some(id)) => {
                client
                    .respond(
                        id,
                        Ok(json!({
                            "protocolVersion": 1,
                            "agentCapabilities": {
                                "loadSession": false,
                                "promptCapabilities": {"text": true}
                            },
                            "agentInfo": {"name": "damond", "version": env!("CARGO_PKG_VERSION")}
                        })),
                    )
                    .await;
            }
            ("session/new", Some(id)) => {
                let cwd = params["cwd"].as_str().unwrap_or("").to_string();
                let session_id = uuid::Uuid::new_v4().to_string();
                match state.store.create_session(&session_id, &cwd).await {
                    Ok(()) => {
                        client
                            .respond(id, Ok(json!({"sessionId": session_id})))
                            .await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/list", Some(id)) => {
                match state.store.list_sessions().await {
                    Ok(sessions) => {
                        let list: Vec<Value> = sessions
                            .into_iter()
                            .map(|(sid, created)| {
                                json!({"sessionId": sid, "createdAt": created})
                            })
                            .collect();
                        client.respond(id, Ok(json!({"sessions": list}))).await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/delete", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                match state.store.delete_session(&session_id).await {
                    Ok(()) => {
                        client.respond(id, Ok(json!({"deleted": true}))).await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/search", Some(id)) => {
                let query = params["query"].as_str().unwrap_or("").to_string();
                let limit = params["limit"].as_u64().unwrap_or(10) as usize;
                match state.store.search(&query, limit).await {
                    Ok(hits) => {
                        let results: Vec<Value> = hits
                            .into_iter()
                            .map(|(sid, mid, snippet)| {
                                json!({
                                    "sessionId": sid,
                                    "messageId": mid,
                                    "snippet": snippet,
                                })
                            })
                            .collect();
                        client.respond(id, Ok(json!({"results": results}))).await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/resume", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                match state.store.session_exists(&session_id).await {
                    Ok(true) => {
                        client
                            .respond(id, Ok(json!({"sessionId": session_id})))
                            .await;
                    }
                    Ok(false) => {
                        client
                            .respond(id, Err(rpc_error(-32602, "session not found")))
                            .await;
                    }
                    Err(e) => {
                        client
                            .respond(id, Err(rpc_error(-32603, &e.to_string())))
                            .await;
                    }
                }
            }
            ("session/prompt", Some(id)) => {
                let session_id = params["sessionId"].as_str().unwrap_or("").to_string();
                let text = params["prompt"]
                    .as_array()
                    .map(|blocks| {
                        blocks
                            .iter()
                            .filter_map(|b| b["text"].as_str())
                            .collect::<Vec<_>>()
                            .join("")
                    })
                    .unwrap_or_default();
                let cancel = CancellationToken::new();
                cancels
                    .lock()
                    .await
                    .insert(session_id.clone(), cancel.clone());
                let (state, client, cancels) =
                    (state.clone(), client.clone(), cancels.clone());
                tokio::spawn(async move {
                    let result = runtime::run_prompt(
                        &state,
                        &session_id,
                        &text,
                        &(client.clone() as Arc<dyn ClientChannel>),
                        cancel.clone(),
                    )
                    .await;
                    cancels.lock().await.remove(&session_id);
                    match result {
                        Ok(reason) => {
                            let stop = if cancel.is_cancelled() {
                                "cancelled"
                            } else {
                                match reason {
                                    crate::llm::StopReason::Stop => "end_turn",
                                    crate::llm::StopReason::Length => "max_tokens",
                                    crate::llm::StopReason::ToolCalls => "tool_use",
                                    crate::llm::StopReason::Other => "end_turn",
                                }
                            };
                            client
                                .respond(id, Ok(json!({"stopReason": stop})))
                                .await;
                        }
                        Err(e) => {
                            client
                                .respond(id, Err(rpc_error(-32603, &format!("{e:#}"))))
                                .await;
                        }
                    }
                });
            }
            ("session/cancel", _) => {
                if let Some(sid) = params["sessionId"].as_str() {
                    if let Some(token) = cancels.lock().await.get(sid) {
                        token.cancel();
                    }
                }
            }
            (_, Some(id)) => {
                client
                    .respond(id, Err(rpc_error(-32601, "method not found")))
                    .await;
            }
            _ => {}
        }
    }
    info!("ws client disconnected");
}

fn rpc_error(code: i64, message: &str) -> Value {
    json!({"code": code, "message": message})
}
