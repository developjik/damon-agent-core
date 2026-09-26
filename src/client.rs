//! Reference client for the damond WS/JSON API (protocol v2).
//! This module doubles as the integration example — see docs/protocol-v2.md.

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
    /// A `session.event` push: `event` is the StreamEvent payload
    /// (`{"type": "…", …}` — see docs/protocol-v2.md).
    Event { session_id: String, event: Value },
    /// Response to a `turn_start` call, routed through the event stream
    /// so it stays ordered after that session's `session.event` pushes.
    /// `result` is the turn result (`{turnId, stopReason, usage?}`) or
    /// the error object.
    TurnDone {
        session_id: String,
        result: Result<Value, Value>,
    },
    /// The transport connected (initially implied; re-emitted after
    /// each reconnect). Mirrored from the reconnect supervisor —
    /// best-effort: a full event channel drops it rather than blocking
    /// the supervisor. `conn_state()` is the authoritative source.
    Connected,
    /// The transport died; the supervisor is redialing. Same mirroring
    /// rules as [`ClientEvent::Connected`].
    Disconnected,
}

/// How a pending RPC response should be delivered.
enum Pending {
    /// Direct oneshot reply (normal requests).
    Direct(oneshot::Sender<Result<Value, Value>>),
    /// Route into the event stream (turn.start — keeps ordering vs events).
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
    /// The daemon's permission timeout, learned from the `hello` push
    /// (`permissionTimeoutSecs`); defaults until then.
    permission_timeout_secs: Arc<AtomicU64>,
}

/// Tunables for `connect_with_options` / `connect_relay_with_options`.
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    /// Capacity of the ClientEvent channel. Session events are dropped
    /// (warn-once logged) when a slow consumer lets this fill — raise it
    /// for consumers that batch-render.
    pub event_capacity: usize,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self { event_capacity: 64 }
    }
}
/// Whether the transport is currently usable. Public so embedders can
/// watch connection liveness via [`DamonClient::conn_state`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConnState {
    Connected,
    Disconnected,
}

/// How long `request()` waits for a reconnect before giving up.
const RECONNECT_WAIT: Duration = Duration::from_secs(10);
/// First reconnect delay; doubles each failure up to `RECONNECT_MAX`.
const RECONNECT_MIN: Duration = Duration::from_millis(100);
const RECONNECT_MAX: Duration = Duration::from_secs(5);

/// Fallback for `permission_timeout()` before the `hello` push reports
/// the daemon's configured value — mirrors the daemon's own default.
const DEFAULT_PERMISSION_TIMEOUT_SECS: u64 = 300;

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
        let permission_timeout_secs = Arc::new(AtomicU64::new(DEFAULT_PERMISSION_TIMEOUT_SECS));

        tokio::spawn(
            Reconnect {
                redial,
                writer: writer.clone(),
                pending: pending.clone(),
                event_tx,
                conn_tx,
                dropped,
                permission_timeout_secs: permission_timeout_secs.clone(),
            }
            .run(rx),
        );

        Self {
            writer,
            pending,
            next_id: Arc::new(AtomicU64::new(1)),
            events: Arc::new(Mutex::new(event_rx)),
            conn: conn_rx,
            permission_timeout_secs,
        }
    }

    /// Generic request. Resolves with the result or the error object.
    /// While the link is down this waits for the reconnect supervisor —
    /// up to `RECONNECT_WAIT` — before failing. Link deaths are retried
    /// on the reconnected link (bounded); this makes requests
    /// at-least-once — a response lost in transit means the daemon may
    /// have executed the call — acceptable because Direct calls are
    /// metadata ops (list/create/delete/search), and turns never
    /// take this path.
    pub async fn request(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        for attempt in 0..3 {
            self.wait_connected().await?;
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            self.pending.lock().await.insert(id, Pending::Direct(tx));
            let msg = json!({"id": id, "method": method, "params": params});
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

    /// Live connection state — a watch channel driven by the reconnect
    /// supervisor. Clone and `changed().await` it, or `borrow()` for a
    /// point-in-time check. This is the authoritative liveness source;
    /// the `ClientEvent::Connected`/`Disconnected` events are a
    /// best-effort mirror for event-loop consumers.
    pub fn conn_state(&self) -> watch::Receiver<ConnState> {
        self.conn.clone()
    }

    /// Stream of server events. Only one consumer — takes the receiver.
    pub async fn events(&self) -> mpsc::Receiver<ClientEvent> {
        std::mem::replace(&mut *self.events.lock().await, mpsc::channel(1).1)
    }

    // --- Convenience wrappers -------------------------------------------

    /// Handshake: `{protocol, daemon, version, backends[], methods[]}`.
    /// The daemon also pushes the same shape (minus backends/methods)
    /// on connect — that push is what feeds `permission_timeout()`.
    pub async fn hello(&self) -> anyhow::Result<Value> {
        self.request("hello", json!({})).await
    }

    /// How long the daemon waits on an unanswered permission prompt
    /// before denying it — from the `hello` push, or the 300s default
    /// until the first push arrives.
    pub fn permission_timeout(&self) -> Duration {
        Duration::from_secs(self.permission_timeout_secs.load(Ordering::Relaxed))
    }

    /// Create a session. `backend` picks the agent (None = daemon
    /// default); `model` becomes the session's default model.
    pub async fn create_session(
        &self,
        backend: Option<&str>,
        cwd: &str,
        model: Option<&str>,
    ) -> anyhow::Result<String> {
        let v = self
            .request(
                "session.create",
                json!({"backend": backend, "cwd": cwd, "model": model}),
            )
            .await?;
        v["sessionId"]
            .as_str()
            .map(String::from)
            .context("no sessionId in response")
    }

    /// Sessions as `(sessionId, createdAt, backend, title)` tuples —
    /// backend/title are empty strings when unset. `limit`/`offset`
    /// page the result; None uses the daemon's default page.
    pub async fn list_sessions(
        &self,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> anyhow::Result<Vec<(String, String, String, String)>> {
        let v = self
            .request("session.list", json!({"limit": limit, "offset": offset}))
            .await?;
        Ok(v["sessions"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|s| {
                        (
                            s["sessionId"].as_str().unwrap_or("").to_string(),
                            s["createdAt"].as_str().unwrap_or("").to_string(),
                            s["backend"].as_str().unwrap_or("").to_string(),
                            s["title"].as_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Stored messages for a session (StoredMessage rows), ordered by
    /// message id; `limit`/`offset` page the result.
    pub async fn session_messages(
        &self,
        id: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> anyhow::Result<Vec<Value>> {
        let v = self
            .request(
                "session.messages",
                json!({"sessionId": id, "limit": limit, "offset": offset}),
            )
            .await?;
        Ok(v["messages"].as_array().cloned().unwrap_or_default())
    }

    /// Sessions a backend can import from `cwd` (native history).
    pub async fn import_sessions(
        &self,
        backend: &str,
        cwd: Option<&str>,
    ) -> anyhow::Result<Vec<Value>> {
        let v = self
            .request("session.import", json!({"backend": backend, "cwd": cwd}))
            .await?;
        Ok(v["sessions"].as_array().cloned().unwrap_or_default())
    }

    /// Resume a native session by its persistence handle — the import
    /// path for sessions the backend made outside the daemon. The
    /// daemon mints (or reuses) a Damon session id bound to the handle.
    pub async fn resume_by_handle(
        &self,
        handle: &Value,
        title: Option<&str>,
        cwd: Option<&str>,
    ) -> anyhow::Result<String> {
        let v = self
            .request(
                "session.resume",
                json!({"handle": handle, "title": title, "cwd": cwd}),
            )
            .await?;
        v["sessionId"]
            .as_str()
            .map(String::from)
            .context("no sessionId in response")
    }

    /// Rename a session (sets its title).
    pub async fn rename_session(&self, id: &str, title: &str) -> anyhow::Result<()> {
        self.request("session.rename", json!({"sessionId": id, "title": title}))
            .await?;
        Ok(())
    }

    /// Usage: per-session totals when `id` is given, per-model rollup
    /// (model, contextUsed, contextSize, costUsd, turns) otherwise.
    pub async fn usage(&self, id: Option<&str>) -> anyhow::Result<Value> {
        self.request("session.usage", json!({"sessionId": id}))
            .await
    }

    /// Delete a session and its history.
    pub async fn delete_session(&self, id: &str) -> anyhow::Result<()> {
        self.request("session.delete", json!({"sessionId": id}))
            .await?;
        Ok(())
    }

    /// Resume a persisted session: brings it live (or attaches to the
    /// already-live one) and subscribes this connection to its events.
    pub async fn resume_session(&self, id: &str) -> anyhow::Result<String> {
        let v = self
            .request("session.resume", json!({"sessionId": id}))
            .await?;
        v["sessionId"]
            .as_str()
            .map(String::from)
            .context("no sessionId in response")
    }

    /// Fork a session: copy the session row and its messages into a new
    /// session, optionally only up to and including `upto` (an original
    /// message id). The agent's own session id is NOT copied — the fork
    /// attaches a fresh agent session on first prompt.
    pub async fn fork_session(&self, id: &str, upto: Option<i64>) -> anyhow::Result<String> {
        let v = self
            .request("session.fork", json!({"sessionId": id, "upto": upto}))
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
            .request("session.search", json!({"query": query, "limit": limit}))
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

    /// Start a turn. The response arrives as `ClientEvent::TurnDone` on
    /// the event stream — ordered after that session's `session.event`
    /// pushes, so consumers never lose trailing events to a race.
    pub async fn turn_start(&self, session_id: &str, text: &str) -> anyhow::Result<()> {
        self.turn_start_params(session_id, json!(text)).await
    }

    /// Like `turn_start`, but sends raw content blocks — use for
    /// multimodal prompts (image/resource blocks alongside text).
    pub async fn turn_start_blocks(
        &self,
        session_id: &str,
        blocks: Vec<Value>,
    ) -> anyhow::Result<()> {
        self.turn_start_params(session_id, json!(blocks)).await
    }

    /// Shared turn.start send: register the pending waiter before the
    /// frame goes out so the response can never arrive to no waiter.
    async fn turn_start_params(&self, session_id: &str, prompt: Value) -> anyhow::Result<()> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.pending.lock().await.insert(
            id,
            Pending::ToEvents {
                session_id: session_id.to_string(),
            },
        );
        let msg = json!({
            "id": id, "method": "turn.start",
            "params": {"sessionId": session_id, "prompt": prompt},
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

    /// Cancel the session's running turn.
    pub async fn turn_cancel(&self, session_id: &str) -> anyhow::Result<()> {
        self.request("turn.cancel", json!({"sessionId": session_id}))
            .await?;
        Ok(())
    }

    /// Answer a `permission_requested` event. `request_id` is the
    /// event's `id`; `response` is a PermissionResponse —
    /// `{behavior:"allow",action_id?,updated_input?}` or
    /// `{behavior:"deny",action_id?,message?,interrupt}`.
    pub async fn respond_to_permission(
        &self,
        session_id: &str,
        request_id: &str,
        response: Value,
    ) -> anyhow::Result<()> {
        self.request(
            "permission.respond",
            json!({"sessionId": session_id, "requestId": request_id, "response": response}),
        )
        .await?;
        Ok(())
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
    permission_timeout_secs: Arc<AtomicU64>,
}

impl Reconnect {
    async fn run(self, mut rx: mpsc::Receiver<String>) {
        let mut backoff = RECONNECT_MIN;
        loop {
            while let Some(text) = rx.recv().await {
                handle_frame(
                    &text,
                    &self.pending,
                    &self.event_tx,
                    &self.dropped,
                    &self.permission_timeout_secs,
                )
                .await;
            }
            // Connection ended: fail every pending request so callers
            // don't wait on a response that can never arrive.
            drain_pending(&self.pending, &self.event_tx).await;
            // Mirror the state flip onto the event stream — best-effort
            // (a full channel drops it); the watch is authoritative.
            let _ = self.event_tx.try_send(ClientEvent::Disconnected);
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
                        // A waiter that slipped in after the first
                        // drain_pending may have sent on the dead sink —
                        // drain again so it can't hang forever.
                        drain_pending(&self.pending, &self.event_tx).await;
                        rx = r;
                        backoff = RECONNECT_MIN;
                        if self.conn_tx.send(ConnState::Connected).is_err() {
                            return;
                        }
                        // Same best-effort mirror as the Disconnected flip.
                        let _ = self.event_tx.try_send(ClientEvent::Connected);
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

/// Route one inbound text frame: the `hello` push, a `session.event`
/// push, or a response that resolves a pending call.
async fn handle_frame(
    text: &str,
    pending: &Mutex<HashMap<u64, Pending>>,
    event_tx: &mpsc::Sender<ClientEvent>,
    dropped: &AtomicU64,
    permission_timeout_secs: &AtomicU64,
) {
    let Ok(v) = serde_json::from_str::<Value>(text) else {
        return;
    };
    // The connect-time hello carries the daemon's permission budget —
    // cache it so `permission_timeout()` can be a synchronous getter.
    if let Some(hello) = v.get("hello")
        && let Some(secs) = hello["permissionTimeoutSecs"].as_u64()
    {
        permission_timeout_secs.store(secs, Ordering::Relaxed);
        return;
    }
    if v.get("event").is_some() {
        // Events are lossy by design — a slow consumer must never
        // backpressure the pump into stalling RPC responses. Every drop
        // is counted; warn once per power of two so a wedged consumer
        // doesn't spam the log.
        let ev = ClientEvent::Event {
            session_id: v["sessionId"].as_str().unwrap_or("").to_string(),
            event: v["data"].clone(),
        };
        if event_tx.try_send(ev).is_err() {
            let n = dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_power_of_two() {
                tracing::warn!(
                    dropped = n,
                    "event channel full; dropping session events — slow consumer"
                );
            }
        }
        return;
    }
    // Response to one of our requests. v2 daemons never send
    // server-initiated requests; a `method` key means it isn't a reply.
    if v.get("method").is_none()
        && let Some(id) = v["id"].as_u64()
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
                    .send(ClientEvent::TurnDone { session_id, result })
                    .await;
            }
        }
    }
}

/// Fail every pending request — used when the transport dies so callers
/// don't wait on a response that can never arrive. Direct waiters see
/// the dropped oneshot — the closed channel IS the link-death signal, so
/// `request()` can retry on the reconnected link; turn waiters get an
/// error `TurnDone` on the event stream, the only surface their
/// consumers read.
async fn drain_pending(
    pending: &Mutex<HashMap<u64, Pending>>,
    event_tx: &mpsc::Sender<ClientEvent>,
) {
    // Take the waiters out under the lock, then send without it: the
    // event channel is bounded, and a slow consumer must not pin the
    // pending mutex across that await — request()/turn_start()/response
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
                    .send(ClientEvent::TurnDone {
                        session_id,
                        result: Err(json!({"message": "connection closed"})),
                    })
                    .await;
            }
        }
    }
}
