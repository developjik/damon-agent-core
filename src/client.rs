//! Reference client for the damond WS/JSON-RPC (ACP-shaped) API.
//! This module doubles as the integration example — see docs/integration.md.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, bail};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
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
    Request {
        id: u64,
        method: String,
        params: Value,
    },
    /// Response to a `prompt` call, routed through the event stream so it
    /// stays ordered after that session's notifications.
    PromptDone {
        session_id: String,
        result: Result<Value, Value>,
    },
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
    /// Connection liveness, driven by the reconnect supervisor.
    conn: watch::Receiver<ConnState>,
    /// Update notifications dropped because the event channel was full —
    /// a slow consumer loses streamed chunks; poll this to detect it.
    dropped: Arc<AtomicU64>,
}

/// Tunables for `connect_with_options` / `connect_relay_with_options`.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// Capacity of the ClientEvent channel. Update notifications are
    /// dropped (counted via `dropped_events()`) when a slow consumer lets
    /// this fill — raise it for consumers that batch-render.
    pub event_capacity: usize,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self { event_capacity: 64 }
    }
}

/// Whether the transport is currently usable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ConnState {
    Connected,
    Disconnected,
}

/// How long `request()` waits for a reconnect before giving up.
const RECONNECT_WAIT: Duration = Duration::from_secs(10);
/// First reconnect delay; doubles each failure up to `RECONNECT_MAX`.
const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

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
        self.0
            .send(Message::Text(text.into()))
            .await
            .context("ws send")
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
    /// Connect to `ws://host:port/ws`. When `token` is set, a single-use
    /// ticket is fetched from `POST /v1/ws_ticket` (Bearer auth) and sent
    /// as `?ticket=` — the token never appears in a URL.
    /// If the connection drops, a background supervisor redials with
    /// exponential backoff (100ms doubling to 5s) until the daemon returns;
    /// `request()` waits for the link instead of failing outright.
    pub async fn connect(url: &str, token: Option<&str>) -> anyhow::Result<Self> {
        Self::connect_with_options(url, token, ConnectOptions::default()).await
    }

    /// `connect` with tunables — see `ConnectOptions`.
    pub async fn connect_with_options(
        url: &str,
        token: Option<&str>,
        opts: ConnectOptions,
    ) -> anyhow::Result<Self> {
        let ws_url = ticketed_url(url, token).await?;
        let (ws, _) =
            tokio_tungstenite::connect_async_with_config(&ws_url, Some(ws_connect_config()), false)
                .await
                .with_context(|| format!("cannot connect to {url}"))?;
        let (writer, rx) = ws_transport(ws);

        // Redial closure: fresh ticket + WS → (writer, inbound-text channel).
        let base_url = url.to_string();
        let tok = token.map(String::from);
        let redial: Redial = Box::new(move || {
            let (url, tok) = (base_url.clone(), tok.clone());
            Box::pin(async move {
                let u = ticketed_url(&url, tok.as_deref()).await?;
                let (ws, _) = tokio_tungstenite::connect_async_with_config(
                    &u,
                    Some(ws_connect_config()),
                    false,
                )
                .await?;
                Ok(ws_transport(ws))
            })
        });
        Ok(Self::start(writer, rx, Some(redial), opts.event_capacity))
    }
    /// Reconnects like `connect` — a dropped tunnel is redialed, which
    /// re-runs the E2E handshake.
    pub async fn connect_relay(relay_url: &str, name: &str, token: &str) -> anyhow::Result<Self> {
        Self::connect_relay_with_options(relay_url, name, token, ConnectOptions::default()).await
    }

    /// `connect_relay` with tunables — see `ConnectOptions`.
    pub async fn connect_relay_with_options(
        relay_url: &str,
        name: &str,
        token: &str,
        opts: ConnectOptions,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = crate::relay::client_connect(relay_url, name, token).await?;
        let (u, n, t) = (relay_url.to_string(), name.to_string(), token.to_string());
        let redial: Redial = Box::new(move || {
            let (u, n, t) = (u.clone(), n.clone(), t.clone());
            Box::pin(async move {
                let (tx, rx) = crate::relay::client_connect(&u, &n, &t).await?;
                Ok((Box::new(ChannelWriter(tx)) as Box<dyn TextWriter>, rx))
            })
        });
        Ok(Self::start(
            Box::new(ChannelWriter(tx)),
            rx,
            Some(redial),
            opts.event_capacity,
        ))
    }

    /// Shared constructor: spawn the read/supervisor task and build Self.
    /// `redial` = None for transports that cannot be redialed (tests,
    /// embedders) — the link then stays Connected until the reader ends.
    fn start(
        writer: Box<dyn TextWriter>,
        rx: mpsc::Receiver<String>,
        redial: Option<Redial>,
        event_capacity: usize,
    ) -> Self {
        let pending: Arc<Mutex<HashMap<u64, Pending>>> = Arc::new(Mutex::new(HashMap::new()));
        let (event_tx, event_rx) = mpsc::channel(event_capacity.max(1));
        let (conn_tx, conn_rx) = watch::channel(ConnState::Connected);
        let writer: Arc<Mutex<Box<dyn TextWriter>>> = Arc::new(Mutex::new(writer));
        let dropped = Arc::new(AtomicU64::new(0));

        tokio::spawn(
            Reconnect {
                redial,
                writer: writer.clone(),
                pending: pending.clone(),
                event_tx,
                conn_tx,
                dropped: dropped.clone(),
            }
            .run(rx),
        );

        Self {
            writer,
            pending,
            next_id: Arc::new(AtomicU64::new(1)),
            events: Arc::new(Mutex::new(event_rx)),
            conn: conn_rx,
            dropped,
        }
    }

    /// Generic JSON-RPC request. Resolves with the result or the error object.
    /// While the link is down this waits for the reconnect supervisor —
    /// up to `RECONNECT_WAIT` — before failing. Link deaths are retried
    /// on the reconnected link (bounded); this makes requests
    /// at-least-once — a response lost in transit means the daemon may
    /// have executed the call — acceptable because Direct calls are
    /// metadata ops (list/new/delete/search), and prompt turns never
    /// take this path.
    pub async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        for attempt in 0..3 {
            self.wait_connected().await?;
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            self.pending.lock().await.insert(id, Pending::Direct(tx));
            let msg = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
            let sent = self.writer.lock().await.send_text(msg.to_string()).await;
            if let Err(e) = sent {
                // The frame never reached the wire: drop the waiter so a
                // retry can't leave dead entries in the pending map.
                self.pending.lock().await.remove(&id);
                if attempt == 2 {
                    return Err(e.context("send failed"));
                }
                continue;
            }
            match rx.await {
                Ok(Ok(v)) => return Ok(v),
                // The server answered — even with an error, don't retry.
                Ok(Err(e)) => bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
                // Link died before the response (dropped waiter): the
                // supervisor is redialing — wait and resend.
                Err(_) => continue,
            }
        }
        bail!("connection closed")
    }

    /// Block until the transport is connected, or `RECONNECT_WAIT` elapses.
    async fn wait_connected(&self) -> anyhow::Result<()> {
        let mut rx = self.conn.clone();
        let deadline = tokio::time::Instant::now() + RECONNECT_WAIT;
        loop {
            if *rx.borrow() == ConnState::Connected {
                return Ok(());
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                // State changed — re-check it.
                Ok(Ok(())) => {}
                // The supervisor is gone — no reconnect will ever arrive.
                Ok(Err(_)) => bail!("connection closed"),
                Err(_) => bail!("timed out waiting for reconnect"),
            }
        }
    }

    /// Update notifications dropped because the event channel filled —
    /// nonzero means a slow consumer lost streamed chunks. Raise
    /// `ConnectOptions::event_capacity` or consume faster.
    pub fn dropped_events(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
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

    /// Answer a server-initiated request with a JSON-RPC error — used
    /// for methods the client doesn't implement so the daemon isn't
    /// left waiting on a response that never comes.
    pub async fn respond_error(&self, id: u64, code: i64, message: &str) -> anyhow::Result<()> {
        let msg = json!({
            "jsonrpc": "2.0", "id": id,
            "error": {"code": code, "message": message}
        });
        self.writer
            .lock()
            .await
            .send_text(msg.to_string())
            .await
            .context("send failed")
    }

    /// Stream of server events. Only one consumer — takes the receiver.
    pub async fn events(&self) -> mpsc::Receiver<ClientEvent> {
        std::mem::replace(&mut *self.events.lock().await, mpsc::channel(1).1)
    }

    // --- Convenience wrappers -------------------------------------------

    pub async fn initialize(&self) -> anyhow::Result<Value> {
        self.request(
            "initialize",
            json!({"protocolVersion": 1, "clientCapabilities": {}}),
        )
        .await
    }

    /// Create a session. `model` becomes the session's default model —
    /// a per-prompt override still wins. Accepts `provider/model`,
    /// a glob-routed id, a discovered id, or `model:level` thinking suffix.
    pub async fn new_session(&self, cwd: &str, model: Option<&str>) -> anyhow::Result<String> {
        let v = self
            .request(
                "session/new",
                json!({"cwd": cwd, "mcpServers": [], "model": model}),
            )
            .await?;
        v["sessionId"]
            .as_str()
            .map(String::from)
            .context("no sessionId in response")
    }

    /// All sessions as `(sessionId, createdAt, model, title)` tuples —
    /// model/title are empty strings when unset.
    pub async fn list_sessions(&self) -> anyhow::Result<Vec<(String, String, String, String)>> {
        let v = self.request("session/list", json!({})).await?;
        Ok(v["sessions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|s| {
                        (
                            s["sessionId"].as_str().unwrap_or("").to_string(),
                            s["createdAt"].as_str().unwrap_or("").to_string(),
                            s["model"].as_str().unwrap_or("").to_string(),
                            s["title"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Rename a session (sets its title).
    pub async fn rename_session(&self, id: &str, title: &str) -> anyhow::Result<()> {
        self.request("session/rename", json!({"sessionId": id, "title": title}))
            .await?;
        Ok(())
    }

    /// Set or clear (None) the session's default model override.
    pub async fn set_session_model(&self, id: &str, model: Option<&str>) -> anyhow::Result<()> {
        self.request(
            "session/set_model",
            json!({"sessionId": id, "model": model}),
        )
        .await?;
        Ok(())
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

    /// ACP session/load: attach to an existing session — the daemon
    /// replays its history as session/update notifications on the event
    /// stream, so callers should already be draining events.
    pub async fn load_session(&self, id: &str) -> anyhow::Result<()> {
        self.request("session/load", json!({"sessionId": id}))
            .await?;
        Ok(())
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
    /// Start a prompt turn. `model` overrides the session's stored default
    /// for this turn only. The response arrives as `ClientEvent::PromptDone`
    /// on the event stream — ordered after that session's chunk notifications,
    /// so consumers never lose trailing chunks to a race.
    pub async fn prompt(
        &self,
        session_id: &str,
        text: &str,
        model: Option<&str>,
    ) -> anyhow::Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().await.insert(
            id,
            Pending::ToEvents {
                session_id: session_id.to_string(),
            },
        );
        let msg = json!({
            "jsonrpc": "2.0", "id": id, "method": "session/prompt",
            "params": {"sessionId": session_id, "model": model, "prompt": [{"type": "text", "text": text}]},
        });
        let sent = self.writer.lock().await.send_text(msg.to_string()).await;
        if let Err(e) = sent {
            // Mirror request(): a failed send must not leave a stale
            // pending entry that can never resolve.
            self.pending.lock().await.remove(&id);
            return Err(e).context("send failed");
        }
        Ok(())
    }

    /// Like `prompt`, but sends raw ACP content blocks — use for
    /// multimodal prompts (image/resource blocks alongside text).
    pub async fn prompt_blocks(
        &self,
        session_id: &str,
        blocks: Vec<Value>,
        model: Option<&str>,
    ) -> anyhow::Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().await.insert(
            id,
            Pending::ToEvents {
                session_id: session_id.to_string(),
            },
        );
        let msg = json!({
            "jsonrpc": "2.0", "id": id, "method": "session/prompt",
            "params": {"sessionId": session_id, "model": model, "prompt": blocks},
        });
        let sent = self.writer.lock().await.send_text(msg.to_string()).await;
        if let Err(e) = sent {
            self.pending.lock().await.remove(&id);
            return Err(e).context("send failed");
        }
        Ok(())
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

    /// Force a context compaction on the session — the manual escape
    /// hatch when a session is wedged against the real context window
    /// while the daemon's estimate still reads under the threshold.
    /// Returns (compacted, compacted_through, reason).
    pub async fn compact_session(
        &self,
        session_id: &str,
    ) -> anyhow::Result<(bool, Option<i64>, Option<String>)> {
        let v = self
            .request("session/compact", json!({"sessionId": session_id}))
            .await?;
        Ok((
            v["compacted"].as_bool().unwrap_or(false),
            v["compactedThrough"].as_i64(),
            v["reason"].as_str().map(String::from),
        ))
    }
}

/// Exchange the bearer token for a single-use WS ticket and return the
/// `?ticket=` URL. No token → the URL unchanged (open localhost daemon).
async fn ticketed_url(ws_url: &str, token: Option<&str>) -> anyhow::Result<String> {
    let Some(token) = token else {
        return Ok(ws_url.to_string());
    };
    // ws(s)://host/ws → http(s)://host/v1/ws_ticket. Trailing slashes
    // must go first — ws://host/ws/ would otherwise 404 on /ws/v1/ws_ticket.
    let http = ws_url
        .trim_end_matches('/')
        .strip_suffix("/ws")
        .unwrap_or(ws_url.trim_end_matches('/'))
        .replacen("wss://", "https://", 1)
        .replacen("ws://", "http://", 1);
    // Fresh client per call, but bounded: without explicit timeouts a
    // hung daemon would stall connect() — and every redial, which shares
    // this helper — forever.
    let resp = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(15))
        .build()
        .context("ws_ticket http client build failed")?
        .post(format!("{http}/v1/ws_ticket"))
        .bearer_auth(token)
        .send()
        .await
        .context("ws_ticket request failed")?;
    if !resp.status().is_success() {
        anyhow::bail!("ws_ticket rejected: {}", resp.status());
    }
    let ticket = resp.json::<Value>().await?["ticket"]
        .as_str()
        .context("ws_ticket response missing ticket")?
        .to_string();
    let sep = if ws_url.contains('?') { '&' } else { '?' };
    Ok(format!("{ws_url}{sep}ticket={ticket}"))
}

/// WS dial config: cap inbound messages/frames at 4 MiB — the same
/// budget the daemon and relay enforce (the default ~64 MiB lets a
/// hostile peer force huge allocations into the session channels).
fn ws_connect_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(4 << 20))
        .max_frame_size(Some(4 << 20))
}

/// A redial attempt: produce a fresh (writer, inbound-text channel) pair.
type Redial = Box<
    dyn Fn() -> futures::future::BoxFuture<
            'static,
            anyhow::Result<(Box<dyn TextWriter>, mpsc::Receiver<String>)>,
        > + Send
        + Sync,
>;

/// Adapt a WS into (writer, inbound-text channel): a pump task forwards
/// text frames; the channel closing signals the connection's death.
fn ws_transport(ws: Ws) -> (Box<dyn TextWriter>, mpsc::Receiver<String>) {
    let (writer, mut reader) = ws.split();
    let (tx, rx) = mpsc::channel::<String>(64);
    tokio::spawn(async move {
        while let Some(msg) = reader.next().await {
            let text = match msg {
                // Ping/Pong/Binary keep the link alive — treat them like
                // the daemon side does: skip, never tear down the
                // connection (keepalive intermediaries would otherwise
                // cause a permanent redial churn).
                Ok(Message::Text(text)) => text,
                // A Close frame (or transport error) ends the link — the
                // reconnect supervisor must see the channel close.
                Ok(Message::Close(_)) => break,
                Ok(_) => continue,
                Err(_) => break,
            };
            if tx.send(text.to_string()).await.is_err() {
                break;
            }
        }
    });
    (Box::new(WsWriter(writer)), rx)
}

/// Owns the inbound-text channel and redials when it dies.
/// On reconnect the writer is swapped and `conn` flips back to Connected,
/// releasing any `request()` calls parked in `wait_connected`.
struct Reconnect {
    redial: Option<Redial>,
    writer: Arc<Mutex<Box<dyn TextWriter>>>,
    pending: Arc<Mutex<HashMap<u64, Pending>>>,
    event_tx: mpsc::Sender<ClientEvent>,
    conn_tx: watch::Sender<ConnState>,
    dropped: Arc<AtomicU64>,
}

impl Reconnect {
    async fn run(self, mut rx: mpsc::Receiver<String>) {
        let mut backoff = RECONNECT_MIN;
        loop {
            while let Some(text) = rx.recv().await {
                handle_frame(&text, &self.pending, &self.event_tx, &self.dropped).await;
            }
            // Connection ended: fail every pending request so callers
            // don't wait on a response that can never arrive.
            drain_pending(&self.pending, &self.event_tx).await;
            if self.conn_tx.send(ConnState::Disconnected).is_err() {
                return; // every client handle dropped
            }
            let Some(redial) = &self.redial else {
                return; // transport cannot be redialed
            };

            // Redial until the daemon answers; backoff doubles per failure.
            loop {
                match redial().await {
                    Ok((w, r)) => {
                        // Swap the writer before announcing Connected so a
                        // released request() never sends on the dead sink.
                        *self.writer.lock().await = w;
                        rx = r;
                        backoff = RECONNECT_MIN;
                        if self.conn_tx.send(ConnState::Connected).is_err() {
                            return;
                        }
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(backoff).await;
                        backoff = Ord::min(backoff * 2, RECONNECT_MAX);
                    }
                }
            }
        }
    }
}

/// Route one inbound text frame: server request, notification, or a
/// response that resolves a pending call.
async fn handle_frame(
    text: &str,
    pending: &Mutex<HashMap<u64, Pending>>,
    event_tx: &mpsc::Sender<ClientEvent>,
    dropped: &AtomicU64,
) {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return;
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
        // Notification. Updates are lossy by design — a slow consumer
        // must never backpressure the pump into stalling RPC responses.
        // Every drop is counted on `dropped_events()`; warn once per
        // power of two so a wedged consumer doesn't spam the log.
        (Some(_method), None)
            if event_tx
                .try_send(ClientEvent::Update(v["params"].clone()))
                .is_err() =>
        {
            let n = dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_power_of_two() {
                tracing::warn!(
                    dropped = n,
                    "event channel full; dropping update notifications — slow consumer"
                );
            }
        }
        (Some(_method), None) => {}
        // Response to one of our requests.
        (None, Some(id)) => {
            if let Some(id) = id.as_u64()
                && let Some(p) = pending.lock().await.remove(&id)
            {
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
                            .send(ClientEvent::PromptDone { session_id, result })
                            .await;
                    }
                }
            }
        }
        _ => {}
    }
}

/// Fail every pending request — used when the transport dies so callers
/// don't wait on a response that can never arrive. Direct waiters see
/// the dropped oneshot — the closed channel IS the link-death signal, so
/// `request()` can retry on the reconnected link; prompt waiters get an
/// error `PromptDone` on the event stream, the only surface their
/// consumers read.
async fn drain_pending(
    pending: &Mutex<HashMap<u64, Pending>>,
    event_tx: &mpsc::Sender<ClientEvent>,
) {
    // Take the waiters out under the lock, then send without it: the
    // event channel is bounded, and a slow consumer must not pin the
    // pending mutex across that await — request()/prompt()/response
    // routing all contend on it. Waiters that race in after the
    // snapshot either fail their send on the dead sink (the caller
    // removes the entry) or resolve on the reconnected link.
    let drained: Vec<Pending> = {
        let mut map = pending.lock().await;
        map.drain().map(|(_, p)| p).collect()
    };
    for p in drained {
        match p {
            Pending::Direct(tx) => {
                drop(tx);
            }
            Pending::ToEvents { session_id } => {
                let _ = event_tx
                    .send(ClientEvent::PromptDone {
                        session_id,
                        result: Err(json!({"message": "connection closed"})),
                    })
                    .await;
            }
        }
    }
}
