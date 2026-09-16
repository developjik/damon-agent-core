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
            http: reqwest::Client::new(),
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

    /// Discord caps messages at 2000 chars.
    pub async fn send_message(&self, channel_id: &str, text: &str) -> anyhow::Result<()> {
        let text = if text.len() > 1990 { &text[..1990] } else { text };
        self.http
            .post(format!("{}/channels/{channel_id}/messages", self.base))
            .bearer_auth(&self.token)
            .json(&json!({"content": text}))
            .send()
            .await?
            .error_for_status()?;
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
        let stripped = stripped.trim_start_matches([' ', ',', ':']).trim().to_string();
        if stripped.is_empty() {
            return None;
        }
        return Some(Incoming {
            chat_id: channel_id,
            text: stripped,
        });
    }
    Some(Incoming {
        chat_id: channel_id,
        text,
    })
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct GatewayConn {
    read: futures::stream::SplitStream<WsStream>,
    /// Aborted on reconnect.
    _heartbeat: tokio::task::JoinHandle<()>,
}

/// Discord transport for the generic bridge.
pub struct DiscordChannel {
    api: Arc<DiscordApi>,
    bot_id: Mutex<Option<String>>,
    conn: Mutex<Option<GatewayConn>>,
    /// Last dispatched sequence number, shared with the heartbeat task.
    seq: Arc<AtomicI64>,
}

impl DiscordChannel {
    pub fn new(api: Arc<DiscordApi>) -> Self {
        Self {
            api,
            bot_id: Mutex::new(None),
            conn: Mutex::new(None),
            seq: Arc::new(AtomicI64::new(-1)),
        }
    }

    async fn connect(&self) -> anyhow::Result<GatewayConn> {
        let url = self.api.gateway_url().await?;
        let (ws, _) = tokio_tungstenite::connect_async(format!("{url}/?v=10&encoding=json"))
            .await
            .context("gateway connect failed")?;
        let (write, mut read) = ws.split();
        let write = Arc::new(Mutex::new(write));

        // First frame must be Hello (op 10) with the heartbeat interval.
        let hello = read
            .next()
            .await
            .context("gateway closed before hello")??;
        let hello: Value = serde_json::from_str(hello.to_text()?)?;
        anyhow::ensure!(hello["op"] == 10, "expected hello, got {hello}");
        let interval_ms = hello["d"]["heartbeat_interval"].as_u64().unwrap_or(41250);

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

        Ok(GatewayConn {
            read,
            _heartbeat: heartbeat,
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
                        warn!(error = %e, "discord gateway connect failed; retry in 5s");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }
                }
            }

            let mut guard = self.conn.lock().await;
            let conn = guard.as_mut().unwrap();
            match conn.read.next().await {
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
                    // op 7 reconnect / op 9 invalid session → drop and reconnect
                    if v["op"] == 7 || v["op"] == 9 {
                        *guard = None;
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
