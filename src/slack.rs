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

/// Dedup memory for retried Socket Mode envelopes.
const SEEN_ENVELOPE_CAP: usize = 4096;

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
    pub async fn post_message(
        &self,
        channel: &str,
        text: &str,
        thread_ts: Option<&str>,
    ) -> anyhow::Result<()> {
        const MAX: usize = 3900;
        let mut rest = text;
        while !rest.is_empty() {
            let end = if rest.len() > MAX {
                rest.floor_char_boundary(MAX)
            } else {
                rest.len()
            };
            let mut body = json!({
                "channel": channel,
                "text": &rest[..end],
                // Agent output must never ping — a reply containing
                // <!channel>/<!here> would otherwise notify the room.
                "parse": "none"
            });
            if let Some(ts) = thread_ts {
                body["thread_ts"] = json!(ts);
            }
            self.post("chat.postMessage", &self.bot_token, &body)
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
    // Thread replies carry thread_ts — scope them to their own session
    // lane so a thread is its own conversation.
    let thread_id = ev["thread_ts"].as_str().map(String::from);
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
            thread_id,
            sender_id: ev["user"].as_str().map(String::from),
            text: stripped,
            attachments: Vec::new(),
        });
    }
    Some(Incoming {
        chat_id: channel,
        thread_id,
        sender_id: ev["user"].as_str().map(String::from),
        text,
        attachments: Vec::new(),
    })
}

/// True when a message event is addressed to the bot but carries only
/// files — no usable text. recv answers these with a notice rather
/// than dropping them (channel) or prompting on "" (DM). Slack files
/// arrive as `file_share` subtypes, which incoming_from_event skips.
fn attachment_only(ev: &Value, bot_user_id: &str) -> bool {
    if ev["type"] != "message" {
        return false;
    }
    if ev["subtype"].as_str().is_some_and(|s| s != "file_share") {
        return false;
    }
    if ev["user"].as_str() == Some(bot_user_id) {
        return false;
    }
    if ev["files"].as_array().is_none_or(|f| f.is_empty()) {
        return false;
    }
    let text = ev["text"].as_str().unwrap_or("");
    if ev["channel_type"].as_str() != Some("im") {
        let mention = format!("<@{bot_user_id}>");
        if !text.contains(&mention) {
            return false;
        }
        return text
            .replace(&mention, "")
            .trim_start_matches([' ', ',', ':'])
            .trim()
            .is_empty();
    }
    text.trim().is_empty()
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct SocketConn {
    read: futures::stream::SplitStream<WsStream>,
    write: Arc<Mutex<futures::stream::SplitSink<WsStream, Message>>>,
}

/// FIFO-bounded dedup for processed envelope_ids: at the cap the OLDEST
/// id is evicted, so hitting the cap reprocesses at most one late retry
/// instead of clearing every recently seen envelope.
#[derive(Default)]
struct SeenEnvelopes {
    /// Envelope ids, oldest first — eviction order.
    order: std::collections::VecDeque<String>,
    /// Same ids as `order`, for membership tests.
    set: std::collections::HashSet<String>,
}

impl SeenEnvelopes {
    /// Records an id; false means it was already seen (a retried
    /// envelope that must not run twice).
    fn insert(&mut self, eid: String) -> bool {
        if !self.set.insert(eid.clone()) {
            return false;
        }
        if self.order.len() >= SEEN_ENVELOPE_CAP
            && let Some(oldest) = self.order.pop_front()
        {
            self.set.remove(&oldest);
        }
        self.order.push_back(eid);
        true
    }
}

/// Slack transport for the generic bridge.
pub struct SlackChannel {
    api: Arc<SlackApi>,
    bot_id: Mutex<Option<String>>,
    conn: Mutex<Option<SocketConn>>,
    /// Bounded dedup of processed envelope_ids — Slack retries an
    /// envelope when our ack is late, and a retry we never received
    /// must not be dropped as a duplicate.
    seen_envelopes: Mutex<SeenEnvelopes>,
}

impl SlackChannel {
    pub fn new(api: Arc<SlackApi>) -> Self {
        Self {
            api,
            bot_id: Mutex::new(None),
            conn: Mutex::new(None),
            seen_envelopes: Mutex::new(SeenEnvelopes::default()),
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
                        match v["type"].as_str() {
                            // Slack asked us to reconnect — still ack
                            // the envelope like every other one.
                            Some("disconnect") => {
                                if let Some(eid) = v["envelope_id"].as_str() {
                                    let ack = json!({"envelope_id": eid});
                                    let _ = conn
                                        .write
                                        .lock()
                                        .await
                                        .send(Message::Text(ack.to_string().into()))
                                        .await;
                                }
                                *guard = None;
                            }
                            Some("events_api") => {
                                // Dedup by envelope_id, not retry_attempt:
                                // a retry we never received must be
                                // processed, a retry of a SEEN envelope must
                                // not run twice.
                                if let Some(eid) = v["envelope_id"].as_str() {
                                    let mut seen = self.seen_envelopes.lock().await;
                                    if !seen.insert(eid.to_string()) {
                                        // Still ack the retry — an
                                        // unacked duplicate would just
                                        // be retried again.
                                        let ack = json!({"envelope_id": eid});
                                        let _ = conn
                                            .write
                                            .lock()
                                            .await
                                            .send(Message::Text(ack.to_string().into()))
                                            .await;
                                        continue;
                                    }
                                }
                                let ev = &v["payload"]["event"];
                                let bot_id = self.bot_id.lock().await.clone().unwrap_or_default();
                                // Resolve the event BEFORE acking: the
                                // ack is Slack's signal that the event
                                // was handled, so the decision to
                                // process, answer, or drop comes first.
                                let msg = incoming_from_event(ev, &bot_id);
                                let notice = attachment_only(ev, &bot_id);
                                if let Some(eid) = v["envelope_id"].as_str() {
                                    let ack = json!({"envelope_id": eid});
                                    let _ = conn
                                        .write
                                        .lock()
                                        .await
                                        .send(Message::Text(ack.to_string().into()))
                                        .await;
                                }
                                if let Some(msg) = msg {
                                    return Ok(Some(msg));
                                }
                                // File-only message for the bot: say so
                                // instead of dropping it (channel) or
                                // erroring on an empty prompt (DM).
                                if notice {
                                    let channel = ev["channel"].as_str().unwrap_or_default();
                                    let thread = ev["thread_ts"].as_str();
                                    let _ = self
                                        .api
                                        .post_message(
                                            channel,
                                            "attachments aren't supported on this channel yet",
                                            thread,
                                        )
                                        .await;
                                }
                            }
                            _ => {
                                // hello, interactive, slash_commands… —
                                // every envelope must still be acked.
                                if let Some(eid) = v["envelope_id"].as_str() {
                                    let ack = json!({"envelope_id": eid});
                                    let _ = conn
                                        .write
                                        .lock()
                                        .await
                                        .send(Message::Text(ack.to_string().into()))
                                        .await;
                                }
                            }
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
        self.api.post_message(chat_id, text, None).await
    }

    async fn send_in_thread(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        text: &str,
    ) -> anyhow::Result<()> {
        self.api.post_message(chat_id, text, thread_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seen_envelopes_evicts_oldest_at_cap() {
        let mut seen = SeenEnvelopes::default();
        for i in 0..SEEN_ENVELOPE_CAP {
            assert!(seen.insert(format!("e{i}")));
        }
        // Still full: the oldest id is retained until a new one arrives.
        assert!(!seen.insert("e0".into()));
        assert!(seen.insert(format!("e{SEEN_ENVELOPE_CAP}")));
        // Exactly one entry was evicted — the oldest.
        assert_eq!(seen.order.len(), SEEN_ENVELOPE_CAP);
        assert!(!seen.set.contains("e0"));
        assert!(seen.set.contains(&format!("e{SEEN_ENVELOPE_CAP}")));
        // Retained ids still dedup.
        assert!(!seen.insert("e1".into()));
    }
}
