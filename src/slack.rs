//! Slack channel: Socket Mode WebSocket for inbound events, Web API for
//! sends. Channel messages require a @bot mention; DMs are taken as-is.
//! Requires an app-level token (xapp-, connections:write) for Socket Mode
//! and a bot token (xoxb-, chat:write + im:history + channels:history…)
//! for the Web API.

use std::sync::Arc;

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::channel::{ChannelApi, Incoming};

const API_BASE: &str = "https://slack.com/api";

/// Slack Web API surface — injectable base URL for tests.
pub struct SlackApi {
    base: String,
    http: reqwest::Client,
    /// App-level token (xapp-) for apps.connections.open.
    app_token: String,
    /// Bot token (xoxb-) for everything else.
    bot_token: String,
}

impl SlackApi {
    pub fn new(app_token: &str, bot_token: &str) -> Self {
        Self::with_base(app_token, bot_token, API_BASE)
    }

    pub fn with_base(app_token: &str, bot_token: &str, base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            // A stalled TCP connection must not park a send forever —
            // the bridge's run_turn would hang and brick the chat.
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            app_token: app_token.to_string(),
            bot_token: bot_token.to_string(),
        }
    }

    async fn post(&self, method: &str, token: &str, body: &Value) -> anyhow::Result<Value> {
        let send = || {
            self.http
                .post(format!("{}/{method}", self.base))
                .bearer_auth(token)
                .json(body)
        };
        let resp = send().send().await?;
        // A 429 mid-split must not lose the remaining chunks — honor
        // Retry-After and retry the failed request once.
        let resp = if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let wait = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(1.0);
            tokio::time::sleep(std::time::Duration::from_secs_f64(wait.min(30.0))).await;
            send().send().await?
        } else {
            resp
        };
        let resp: Value = resp.error_for_status()?.json().await?;
        // Slack returns 200 with {"ok": false, "error": "..."} on API errors.
        anyhow::ensure!(
            resp["ok"].as_bool().unwrap_or(false),
            "slack {method} failed: {}",
            resp["error"].as_str().unwrap_or("unknown")
        );
        Ok(resp)
    }

    /// Socket Mode WebSocket URL (app-level token).
    pub async fn connections_open(&self) -> anyhow::Result<String> {
        let resp = self
            .post("apps.connections.open", &self.app_token, &json!({}))
            .await?;
        resp["url"]
            .as_str()
            .map(String::from)
            .context("apps.connections.open returned no url")
    }

    /// Bot's own user id — used to detect mentions and skip self messages.
    pub async fn bot_user_id(&self) -> anyhow::Result<String> {
        let resp = self.post("auth.test", &self.bot_token, &json!({})).await?;
        resp["user_id"]
            .as_str()
            .map(String::from)
            .context("auth.test returned no user_id")
    }

    /// Slack messages effectively cap ~4000 chars — split instead of
    /// truncating so long agent replies aren't silently lost.
    pub async fn post_message(&self, channel: &str, text: &str) -> anyhow::Result<()> {
        const MAX: usize = 3900;
        let mut rest = text;
        while !rest.is_empty() {
            let end = if rest.len() > MAX {
                rest.floor_char_boundary(MAX)
            } else {
                rest.len()
            };
            self.post(
                "chat.postMessage",
                &self.bot_token,
                &json!({
                    "channel": channel,
                    "text": &rest[..end],
                    // Agent output must never ping — a reply containing
                    // <!channel>/<!here> would otherwise notify the room.
                    "parse": "none"
                }),
            )
            .await?;
            rest = &rest[end..];
        }
        Ok(())
    }
}

/// Extract (channel, text) from a Socket Mode events_api message event.
/// Skips bot/system subtypes and the bot's own messages; non-DM messages
/// must mention the bot (mention is stripped).
pub fn incoming_from_event(ev: &Value, bot_user_id: &str) -> Option<Incoming> {
    if ev["type"] != "message" || ev.get("subtype").is_some() {
        return None;
    }
    if ev["user"].as_str() == Some(bot_user_id) {
        return None;
    }
    let channel = ev["channel"].as_str()?.to_string();
    let text = ev["text"].as_str()?.to_string();
    if ev["channel_type"].as_str() != Some("im") {
        let mention = format!("<@{bot_user_id}>");
        if !text.contains(&mention) {
            return None;
        }
        let stripped = text.replace(&mention, "");
        let stripped = stripped
            .trim_start_matches([' ', ',', ':'])
            .trim()
            .to_string();
        if stripped.is_empty() {
            return None;
        }
        return Some(Incoming {
            chat_id: channel,
            sender_id: ev["user"].as_str().map(String::from),
            text: stripped,
        });
    }
    Some(Incoming {
        chat_id: channel,
        sender_id: ev["user"].as_str().map(String::from),
        text,
    })
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct SocketConn {
    read: futures::stream::SplitStream<WsStream>,
    write: Arc<Mutex<futures::stream::SplitSink<WsStream, Message>>>,
}

/// Slack transport for the generic bridge.
pub struct SlackChannel {
    api: Arc<SlackApi>,
    bot_id: Mutex<Option<String>>,
    conn: Mutex<Option<SocketConn>>,
    /// Bounded dedup of processed envelope_ids — Slack retries an
    /// envelope when our ack is late, and a retry we never received
    /// must not be dropped as a duplicate.
    seen_envelopes: Mutex<std::collections::HashSet<String>>,
}

impl SlackChannel {
    pub fn new(api: Arc<SlackApi>) -> Self {
        Self {
            api,
            bot_id: Mutex::new(None),
            conn: Mutex::new(None),
            seen_envelopes: Mutex::new(std::collections::HashSet::new()),
        }
    }

    async fn connect(&self) -> anyhow::Result<SocketConn> {
        let url = self.api.connections_open().await?;
        let (ws, _) = tokio_tungstenite::connect_async(&url)
            .await
            .context("socket mode connect failed")?;
        let (write, read) = ws.split();
        Ok(SocketConn {
            read,
            write: Arc::new(Mutex::new(write)),
        })
    }
}

#[async_trait::async_trait]
impl ChannelApi for SlackChannel {
    async fn ready(&self) -> anyhow::Result<()> {
        let id = self.api.bot_user_id().await?;
        info!(bot_id = %id, "slack bot ready");
        *self.bot_id.lock().await = Some(id);
        Ok(())
    }

    async fn recv(&self) -> anyhow::Result<Option<Incoming>> {
        loop {
            if self.conn.lock().await.is_none() {
                match self.connect().await {
                    Ok(c) => *self.conn.lock().await = Some(c),
                    Err(e) => {
                        warn!(error = %e, "slack socket connect failed; retry in 5s");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                }
            }

            let mut guard = self.conn.lock().await;
            let conn = guard.as_mut().unwrap();
            // Slack sends keepalive pings on idle sockets — no frame at
            // all for 60s means a half-open connection (NAT timeout,
            // silent drop); drop it and reconnect.
            match tokio::time::timeout(std::time::Duration::from_secs(60), conn.read.next()).await {
                Err(_) => {
                    warn!("slack socket silent for 60s; reconnecting");
                    *guard = None;
                }
                Ok(msg) => match msg {
                    Some(Ok(Message::Text(txt))) => {
                        let v: Value = match serde_json::from_str(&txt) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        // Every envelope must be acked, even ones we skip.
                        if let Some(eid) = v["envelope_id"].as_str() {
                            let ack = json!({"envelope_id": eid});
                            let _ = conn
                                .write
                                .lock()
                                .await
                                .send(Message::Text(ack.to_string().into()))
                                .await;
                        }
                        match v["type"].as_str() {
                            // Slack asked us to reconnect.
                            Some("disconnect") => *guard = None,
                            Some("events_api") => {
                                // Dedup by envelope_id, not retry_attempt:
                                // a retry we never received must be
                                // processed, a retry of a SEEN envelope must
                                // not run twice.
                                if let Some(eid) = v["envelope_id"].as_str() {
                                    let mut seen = self.seen_envelopes.lock().await;
                                    // Bound the set — clear at the cap rather
                                    // than grow forever (a cleared entry just
                                    // reprocesses one late retry).
                                    if seen.len() >= 4096 {
                                        seen.clear();
                                    }
                                    if !seen.insert(eid.to_string()) {
                                        continue;
                                    }
                                }
                                let ev = &v["payload"]["event"];
                                let bot_id = self.bot_id.lock().await.clone().unwrap_or_default();
                                if let Some(msg) = incoming_from_event(ev, &bot_id) {
                                    return Ok(Some(msg));
                                }
                            }
                            _ => {} // hello, interactive, slash_commands…
                        }
                    }
                    Some(Ok(_)) => {} // ping/pong/binary
                    Some(Err(e)) => {
                        warn!(error = %e, "slack socket read error; reconnecting");
                        *guard = None;
                    }
                    None => {
                        warn!("slack socket closed; reconnecting");
                        *guard = None;
                    }
                },
            }
            drop(guard);
        }
    }

    async fn send(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        self.api.post_message(chat_id, text).await
    }
}
