//! Reference client for the damond WS/JSON-RPC (ACP-shaped) API.
//! This module doubles as the integration example — see docs/integration.md.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Events pushed from the daemon outside of request/response.
#[derive(Debug)]
pub enum ClientEvent {
    /// `session/update` notification params.
    Update(Value),
    /// Server-initiated request (e.g. session/request_permission).
    /// Answer with `client.respond(id, result)`.
    Request { id: u64, method: String, params: Value },
    /// Response to a `prompt` call, routed through the event stream so it
    /// stays ordered after that session's notifications.
    PromptDone { session_id: String, result: Result<Value, Value> },
}

/// How a pending RPC response should be delivered.
enum Pending {
    /// Direct oneshot reply (normal requests).
    Direct(oneshot::Sender<Result<Value, Value>>),
    /// Route into the event stream (prompt — keeps ordering vs notifications).
    ToEvents { session_id: String },
}

/// A connected client. Cheap to clone; all clones share the connection.
#[derive(Clone)]
pub struct DamonClient {
    writer: Arc<Mutex<Box<dyn TextWriter>>>,
    pending: Arc<Mutex<HashMap<u64, Pending>>>,
    next_id: Arc<AtomicU64>,
    events: Arc<Mutex<mpsc::Receiver<ClientEvent>>>,
}

/// Anything that can send a JSON text frame to the daemon.
#[async_trait::async_trait]
trait TextWriter: Send + Sync {
    async fn send_text(&mut self, text: String) -> anyhow::Result<()>;
}

/// Adapter: a tungstenite SplitSink.
struct WsWriter(futures::stream::SplitSink<Ws, Message>);
#[async_trait::async_trait]
impl TextWriter for WsWriter {
    async fn send_text(&mut self, text: String) -> anyhow::Result<()> {
        self.0.send(Message::Text(text.into())).await.context("ws send")
    }
}

/// Adapter: an mpsc channel (relay tunnel).
struct ChannelWriter(mpsc::Sender<String>);
#[async_trait::async_trait]
impl TextWriter for ChannelWriter {
    async fn send_text(&mut self, text: String) -> anyhow::Result<()> {
        self.0.send(text).await.context("channel send")
    }
}

impl DamonClient {
    /// Connect to `ws://host:port/ws`. `token` is sent as ?token= when set.
    pub async fn connect(url: &str, token: Option<&str>) -> anyhow::Result<Self> {
        let url = match token {
            Some(t) => format!("{url}?token={}", urlencoding(t)),
            None => url.to_string(),
        };
        let (ws, _) = tokio_tungstenite::connect_async(&url)
            .await
            .with_context(|| format!("cannot connect to {url}"))?;
        let (writer, mut reader) = ws.split();

        let pending: Arc<Mutex<HashMap<u64, Pending>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = mpsc::channel(64);

        let pending2 = pending.clone();
        tokio::spawn(async move {
            while let Some(Ok(Message::Text(text))) = reader.next().await {
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                match (v.get("method"), v.get("id")) {
                    // Server-initiated request.
                    (Some(method), Some(id)) => {
                        if let Some(id) = id.as_u64() {
                            let _ = event_tx
                                .send(ClientEvent::Request {
                                    id,
                                    method: method.as_str().unwrap_or("").to_string(),
                                    params: v["params"].clone(),
                                })
                                .await;
                        }
                    }
                    // Notification.
                    (Some(_method), None) => {
                        let _ = event_tx
                            .send(ClientEvent::Update(v["params"].clone()))
                            .await;
                    }
                    // Response to one of our requests.
                    (None, Some(id)) => {
                        if let Some(id) = id.as_u64() {
                            if let Some(p) = pending2.lock().await.remove(&id) {
                                let result = if let Some(err) = v.get("error") {
                                    Err(err.clone())
                                } else {
                                    Ok(v["result"].clone())
                                };
                                match p {
                                    Pending::Direct(tx) => {
                                        let _ = tx.send(result);
                                    }
                                    Pending::ToEvents { session_id } => {
                                        let _ = event_tx
                                            .send(ClientEvent::PromptDone {
                                                session_id,
                                                result,
                                            })
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(Self {
            writer: Arc::new(Mutex::new(Box::new(WsWriter(writer)))),
            pending,
            next_id: Arc::new(AtomicU64::new(1)),
            events: Arc::new(Mutex::new(event_rx)),
        })
    }

    /// Connect through a `damon-relay` server. `relay_url` is the relay's
    /// ws:// address, `name` the daemon's registered name, `token` the
    /// daemon's auth_token (proves identity in the E2E handshake).
    pub async fn connect_relay(
        relay_url: &str,
        name: &str,
        token: &str,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = crate::relay::client_connect(relay_url, name, token).await?;
        Self::from_channels(tx, rx)
    }

    /// Build a client over a generic (tx, rx) text transport.
    fn from_channels(
        tx: mpsc::Sender<String>,
        mut rx: mpsc::Receiver<String>,
    ) -> anyhow::Result<Self> {
        let pending: Arc<Mutex<HashMap<u64, Pending>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = mpsc::channel(64);
        let pending2 = pending.clone();
        tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                match (v.get("method"), v.get("id")) {
                    (Some(method), Some(id)) => {
                        if let Some(id) = id.as_u64() {
                            let _ = event_tx
                                .send(ClientEvent::Request {
                                    id,
                                    method: method.as_str().unwrap_or("").to_string(),
                                    params: v["params"].clone(),
                                })
                                .await;
                        }
                    }
                    (Some(_method), None) => {
                        let _ = event_tx
                            .send(ClientEvent::Update(v["params"].clone()))
                            .await;
                    }
                    (None, Some(id)) => {
                        if let Some(id) = id.as_u64() {
                            if let Some(p) = pending2.lock().await.remove(&id) {
                                let r = if let Some(e) = v.get("error") {
                                    Err(e.clone())
                                } else {
                                    Ok(v.get("result").cloned().unwrap_or(Value::Null))
                                };
                                match p {
                                    Pending::Direct(tx) => {
                                        let _ = tx.send(r);
                                    }
                                    Pending::ToEvents { session_id } => {
                                        let _ = event_tx
                                            .send(ClientEvent::PromptDone {
                                                session_id,
                                                result: r,
                                            })
                                            .await;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        });
        // Wrap `tx` in a writer-like adapter so `request` can use it.
        let (writer_tx, mut writer_rx) = mpsc::channel::<String>(64);
        tokio::spawn(async move {
            while let Some(text) = writer_rx.recv().await {
                if tx.send(text).await.is_err() { break; }
            }
        });
        Ok(Self {
            writer: Arc::new(Mutex::new(Box::new(ChannelWriter(writer_tx)))),
            pending,
            next_id: Arc::new(AtomicU64::new(1)),
            events: Arc::new(Mutex::new(event_rx)),
        })
    }

    /// Generic JSON-RPC request. Resolves with the result or the error object.
    pub async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, Pending::Direct(tx));
        let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.writer
            .lock()
            .await
            .send_text(msg.to_string())
            .await
            .context("send failed")?;
        match rx.await.context("connection closed")? {
            Ok(v) => Ok(v),
            Err(e) => bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
        }
    }

    /// Respond to a server-initiated request (ClientEvent::Request).
    pub async fn respond(&self, id: u64, result: Value) -> anyhow::Result<()> {
        let msg = json!({"jsonrpc": "2.0", "id": id, "result": result});
        self.writer
            .lock()
            .await
            .send_text(msg.to_string())
            .await
            .context("send failed")
    }

    /// Stream of server events. Only one consumer — takes the receiver.
    pub async fn events(&self) -> mpsc::Receiver<ClientEvent> {
        std::mem::replace(
            &mut *self.events.lock().await,
            mpsc::channel(1).1,
        )
    }

    // --- Convenience wrappers -------------------------------------------

    pub async fn initialize(&self) -> anyhow::Result<Value> {
        self.request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await
    }

    pub async fn new_session(&self, cwd: &str) -> anyhow::Result<String> {
        let v = self
            .request("session/new", json!({"cwd": cwd, "mcpServers": []}))
            .await?;
        v["sessionId"]
            .as_str()
            .map(String::from)
            .context("no sessionId in response")
    }

    /// All sessions as `(sessionId, createdAt)` pairs.
    pub async fn list_sessions(&self) -> anyhow::Result<Vec<(String, String)>> {
        let v = self.request("session/list", json!({})).await?;
        Ok(v["sessions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|s| {
                        (
                            s["sessionId"].as_str().unwrap_or("").to_string(),
                            s["createdAt"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Delete a session and its history.
    pub async fn delete_session(&self, id: &str) -> anyhow::Result<()> {
        self.request("session/delete", json!({"sessionId": id}))
            .await?;
        Ok(())
    }

    /// Verify a session exists so the client can prompt into it.
    pub async fn resume_session(&self, id: &str) -> anyhow::Result<String> {
        let v = self
            .request("session/resume", json!({"sessionId": id}))
            .await?;
        v["sessionId"]
            .as_str()
            .map(String::from)
            .context("no sessionId in response")
    }

    /// Full-text search over all session history.
    /// Returns `(sessionId, messageId, snippet)` triples.
    pub async fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<(String, i64, String)>> {
        let v = self
            .request("session/search", json!({"query": query, "limit": limit}))
            .await?;
        Ok(v["results"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|r| {
                        (
                            r["sessionId"].as_str().unwrap_or("").to_string(),
                            r["messageId"].as_i64().unwrap_or(0),
                            r["snippet"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Start a prompt turn. The response arrives as `ClientEvent::PromptDone`
    /// on the event stream — ordered after that session's chunk notifications,
    /// so consumers never lose trailing chunks to a race.
    pub async fn prompt(&self, session_id: &str, text: &str) -> anyhow::Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().await.insert(
            id,
            Pending::ToEvents {
                session_id: session_id.to_string(),
            },
        );
        let msg = json!({
            "jsonrpc": "2.0", "id": id, "method": "session/prompt",
            "params": {"sessionId": session_id, "prompt": [{"type": "text", "text": text}]},
        });
        self.writer
            .lock()
            .await
            .send_text(msg.to_string())
            .await
            .context("send failed")
    }

    pub async fn cancel(&self, session_id: &str) -> anyhow::Result<()> {
        let msg = json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id},
        });
        self.writer
            .lock()
            .await
            .send_text(msg.to_string())
            .await
            .context("send failed")
    }
}

/// Percent-encode a query-param value (unreserved chars pass through).
fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
