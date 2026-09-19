//! Discord channel: Gateway WebSocket for inbound messages, REST for
//! sends. Guild messages require a @bot mention; DMs are taken as-is.
//! Reconnects by re-identifying — no session resume (v1).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::channel::{ChannelApi, Incoming};

const API_BASE: &str = "https://discord.com/api/v10";

/// GUILD_MESSAGES | DIRECT_MESSAGES | MESSAGE_CONTENT.
/// MESSAGE_CONTENT is a privileged intent — enable it in the dev portal.
const INTENTS: u64 = (1 << 9) | (1 << 12) | (1 << 15);

/// Discord REST surface — injectable base URL for tests.
pub struct DiscordApi {
    base: String,
    http: reqwest::Client,
    token: String,
}

impl DiscordApi {
    pub fn new(token: &str) -> Self {
        Self::with_base(token, API_BASE)
    }

    pub fn with_base(token: &str, base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            // A stalled TCP connection must not park a send forever —
            // the bridge's run_turn would hang and brick the chat.
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            token: token.to_string(),
        }
    }

    /// Bot user's own id — used to detect mentions and skip self messages.
    pub async fn bot_user_id(&self) -> anyhow::Result<String> {
        let resp: Value = self
            .http
            .get(format!("{}/users/@me", self.base))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        resp["id"]
            .as_str()
            .map(String::from)
            .context("users/@me returned no id")
    }

    /// WebSocket gateway URL for this bot.
    pub async fn gateway_url(&self) -> anyhow::Result<String> {
        let resp: Value = self
            .http
            .get(format!("{}/gateway/bot", self.base))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        resp["url"]
            .as_str()
            .map(String::from)
            .context("gateway/bot returned no url")
    }

    /// Discord caps messages at 2000 chars — split instead of
    /// truncating so long agent replies aren't silently lost.
    pub async fn send_message(&self, channel_id: &str, text: &str) -> anyhow::Result<()> {
        const MAX: usize = 1990;
        let mut rest = text;
        while !rest.is_empty() {
            let end = if rest.len() > MAX {
                rest.floor_char_boundary(MAX)
            } else {
                rest.len()
            };
            // A 429 mid-split must not lose the remaining chunks —
            // honor Retry-After and retry the failed chunk once.
            let resp = self
                .http
                .post(format!("{}/channels/{channel_id}/messages", self.base))
                .bearer_auth(&self.token)
                .json(&json!({
                    "content": &rest[..end],
                    // Agent output must never ping — a reply containing
                    // @everyone/@here would otherwise mention the guild.
                    "allowed_mentions": {"parse": []}
                }))
                .send()
                .await?;
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let wait = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(1.0);
                tokio::time::sleep(std::time::Duration::from_secs_f64(wait.min(30.0))).await;
                self.http
                    .post(format!("{}/channels/{channel_id}/messages", self.base))
                    .bearer_auth(&self.token)
                    .json(&json!({
                        "content": &rest[..end],
                        "allowed_mentions": {"parse": []}
                    }))
                    .send()
                    .await?
                    .error_for_status()?;
            } else {
                resp.error_for_status()?;
            }
            rest = &rest[end..];
        }
        Ok(())
    }
}

/// Extract (channel_id, text) from a MESSAGE_CREATE dispatch.
/// Skips bots/webhooks; guild messages must mention the bot (mention is
/// stripped); DMs (no guild_id) pass through.
pub fn incoming_from_message(d: &Value, bot_id: &str) -> Option<Incoming> {
    if d["author"]["bot"].as_bool().unwrap_or(false) {
        return None;
    }
    let channel_id = d["channel_id"].as_str()?.to_string();
    let text = d["content"].as_str()?.to_string();
    if d["guild_id"].is_string() {
        // Guild channel: require a mention, then strip it.
        let mention = format!("<@{bot_id}>");
        let nick_mention = format!("<@!{bot_id}>");
        if !(text.contains(&mention) || text.contains(&nick_mention)) {
            return None;
        }
        let stripped = text.replace(&mention, "").replace(&nick_mention, "");
        let stripped = stripped
            .trim_start_matches([' ', ',', ':'])
            .trim()
            .to_string();
        if stripped.is_empty() {
            return None;
        }
        return Some(Incoming {
            chat_id: channel_id,
            sender_id: d["author"]["id"].as_str().map(String::from),
            text: stripped,
        });
    }
    Some(Incoming {
        chat_id: channel_id,
        sender_id: d["author"]["id"].as_str().map(String::from),
        text,
    })
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct GatewayConn {
    read: futures::stream::SplitStream<WsStream>,
    /// Aborted when the conn is dropped — a detached JoinHandle would
    /// keep heartbeating on the dead socket until its next tick.
    heartbeat: tokio::task::JoinHandle<()>,
    /// Dead-conn detector: heartbeat ACKs arrive every interval, so no
    /// frame at all for 2 intervals + slack means a half-open socket
    /// (NAT timeout, silent drop) — drop it and reconnect.
    read_timeout: std::time::Duration,
}

impl Drop for GatewayConn {
    fn drop(&mut self) {
        self.heartbeat.abort();
    }
}

/// Discord transport for the generic bridge.
pub struct DiscordChannel {
    api: Arc<DiscordApi>,
    bot_id: Mutex<Option<String>>,
    conn: Mutex<Option<GatewayConn>>,
    /// Last dispatched sequence number, shared with the heartbeat task.
    seq: Arc<AtomicI64>,
    /// Reconnect backoff for paths that re-IDENTIFY (op 9 invalid
    /// session, connect failure): doubles from 5s, caps at 300s, resets
    /// when the gateway accepts the identify (READY).
    backoff: Mutex<std::time::Duration>,
}

impl DiscordChannel {
    pub fn new(api: Arc<DiscordApi>) -> Self {
        Self {
            api,
            bot_id: Mutex::new(None),
            conn: Mutex::new(None),
            seq: Arc::new(AtomicI64::new(-1)),
            backoff: Mutex::new(std::time::Duration::from_secs(5)),
        }
    }

    /// Next reconnect delay for paths that re-IDENTIFY: doubles from 5s
    /// to a 300s cap. Reset once identify is accepted (READY).
    async fn next_backoff(&self) -> std::time::Duration {
        let mut b = self.backoff.lock().await;
        let delay = *b;
        *b = ((*b) * 2).min(std::time::Duration::from_secs(300));
        delay
    }

    /// READY — identify accepted; back to the initial reconnect cadence.
    async fn reset_backoff(&self) {
        *self.backoff.lock().await = std::time::Duration::from_secs(5);
    }

    async fn connect(&self) -> anyhow::Result<GatewayConn> {
        let url = self.api.gateway_url().await?;
        let (ws, _) = tokio_tungstenite::connect_async(format!("{url}/?v=10&encoding=json"))
            .await
            .context("gateway connect failed")?;
        let (write, mut read) = ws.split();
        let write = Arc::new(Mutex::new(write));

        // First frame must be Hello (op 10) with the heartbeat interval.
        let hello = read.next().await.context("gateway closed before hello")??;
        let hello: Value = serde_json::from_str(hello.to_text()?)?;
        anyhow::ensure!(hello["op"] == 10, "expected hello, got {hello}");
        // A malicious/buggy gateway could send 0 — tokio's interval panics
        // on a zero period, killing the heartbeat task.
        let interval_ms = hello["d"]["heartbeat_interval"]
            .as_u64()
            .unwrap_or(41250)
            .max(1000);

        // Identify.
        let identify = json!({
            "op": 2,
            "d": {
                "token": self.api.token,
                "intents": INTENTS,
                "properties": {"os": std::env::consts::OS, "browser": "damon", "device": "damon"},
            }
        });
        write
            .lock()
            .await
            .send(Message::Text(identify.to_string().into()))
            .await?;

        // Heartbeat loop: op 1 with the last seen seq (null before any).
        let hb_write = write.clone();
        let hb_seq = self.seq.clone();
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            tick.tick().await; // first heartbeat after one interval
            loop {
                tick.tick().await;
                let s = hb_seq.load(Ordering::Relaxed);
                let payload = if s < 0 { Value::Null } else { json!(s) };
                let msg = json!({"op": 1, "d": payload});
                if hb_write
                    .lock()
                    .await
                    .send(Message::Text(msg.to_string().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        let read_timeout =
            std::time::Duration::from_millis(interval_ms * 2) + std::time::Duration::from_secs(15);
        Ok(GatewayConn {
            read,
            heartbeat,
            read_timeout,
        })
    }
}

#[async_trait::async_trait]
impl ChannelApi for DiscordChannel {
    async fn ready(&self) -> anyhow::Result<()> {
        let id = self.api.bot_user_id().await?;
        info!(bot_id = %id, "discord bot ready");
        *self.bot_id.lock().await = Some(id);
        Ok(())
    }

    async fn recv(&self) -> anyhow::Result<Option<Incoming>> {
        loop {
            // Ensure a live connection.
            if self.conn.lock().await.is_none() {
                match self.connect().await {
                    Ok(c) => *self.conn.lock().await = Some(c),
                    Err(e) => {
                        let delay = self.next_backoff().await;
                        warn!(error = %e, delay_secs = delay.as_secs(), "discord gateway connect failed; retrying");
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                }
            }

            let mut guard = self.conn.lock().await;
            let conn = guard.as_mut().unwrap();
            match tokio::time::timeout(conn.read_timeout, conn.read.next()).await {
                Err(_) => {
                    // No frame (not even a heartbeat ACK) for 2 intervals
                    // + slack — the socket is half-open; reconnect.
                    warn!("discord gateway silent past heartbeat window; reconnecting");
                    *guard = None;
                }
                Ok(msg) => match msg {
                    Some(Ok(Message::Text(txt))) => {
                        let v: Value = match serde_json::from_str(&txt) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let Some(s) = v["s"].as_i64() {
                            self.seq.store(s, Ordering::Relaxed);
                        }
                        // op 0 dispatch
                        if v["op"] == 0 && v["t"] == "MESSAGE_CREATE" {
                            let bot_id = self.bot_id.lock().await.clone().unwrap_or_default();
                            if let Some(msg) = incoming_from_message(&v["d"], &bot_id) {
                                return Ok(Some(msg));
                            }
                        }
                        // READY: identify accepted — the backoff that
                        // throttled re-identifies served its purpose.
                        if v["op"] == 0 && v["t"] == "READY" {
                            self.reset_backoff().await;
                        }
                        // op 7 is the server asking for a normal reconnect —
                        // keep the fixed 5s. op 9 invalid session means the
                        // gateway refused the session and the next connect
                        // IDENTIFYs from scratch: exponential backoff (5s →
                        // ×2, 300s cap, reset on READY) so a gateway that
                        // keeps refusing cannot burn the identify quota
                        // (token-bucketed, ~1000/day per bot).
                        if v["op"] == 7 || v["op"] == 9 {
                            let delay = if v["op"] == 9 {
                                self.next_backoff().await
                            } else {
                                std::time::Duration::from_secs(5)
                            };
                            *guard = None;
                            drop(guard);
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                    Some(Ok(_)) => {} // ping/pong/binary — tungstenite answers pings
                    Some(Err(e)) => {
                        warn!(error = %e, "discord gateway read error; reconnecting");
                        *guard = None;
                    }
                    None => {
                        warn!("discord gateway closed; reconnecting");
                        *guard = None;
                    }
                },
            }
            drop(guard);
        }
    }

    async fn send(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        self.api.send_message(chat_id, text).await
    }

    fn flush_threshold(&self) -> usize {
        1800
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn op9_backoff_doubles_caps_and_resets_on_identify() {
        let ch = DiscordChannel::new(Arc::new(DiscordApi::new("t")));
        for want in [5u64, 10, 20, 40, 80, 160, 300, 300] {
            assert_eq!(
                ch.next_backoff().await,
                std::time::Duration::from_secs(want)
            );
        }
        // READY (identify accepted) returns to the initial cadence.
        ch.reset_backoff().await;
        assert_eq!(ch.next_backoff().await, std::time::Duration::from_secs(5));
    }
}
