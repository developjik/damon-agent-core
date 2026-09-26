//! Discord channel: Gateway WebSocket for inbound messages, REST for
//! sends. Guild messages require a @bot mention; DMs are taken as-is.
//! Reconnects RESUME the gateway session (op 6) when one survives —
//! events missed while disconnected are replayed by the gateway — and
//! fall back to a fresh IDENTIFY when the session is invalidated.

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
/// INTERACTION_CREATE (permission button presses) is deliberately NOT
/// gated by any intent bit — the gateway docs list no intent for it —
/// so nothing needs adding for component interactions.
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

    /// Download an attachment from the CDN. Attachment URLs are public
    /// (signed, expire after a few hours) — no auth header. Mime comes
    /// from the response Content-Type.
    pub async fn download_file(&self, url: &str) -> anyhow::Result<(Vec<u8>, String)> {
        let resp = self.http.get(url).send().await?.error_for_status()?;
        let mime = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or(s).trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let bytes = resp.bytes().await?;
        Ok((bytes.to_vec(), mime))
    }

    /// Send a message with one row of buttons — permission prompts.
    /// `buttons` is (label, custom_id, style) with Discord button
    /// styles (3 success, 4 danger). UNVERIFIED against the real API
    /// (no bot token on this machine) — shaped from the documented
    /// component payloads and exercised against a mock server.
    pub async fn send_button_message(
        &self,
        channel_id: &str,
        text: &str,
        buttons: &[(String, String, u8)],
    ) -> anyhow::Result<()> {
        let row: Vec<Value> = buttons
            .iter()
            .map(|(label, custom_id, style)| {
                json!({"type": 2, "style": style, "label": label, "custom_id": custom_id})
            })
            .collect();
        let resp = crate::ratelimit::send_with_rate_limit(|| {
            self.http
                .post(format!("{}/channels/{channel_id}/messages", self.base))
                .bearer_auth(&self.token)
                .json(&json!({
                    "content": text,
                    "components": [{"type": 1, "components": row}],
                    "allowed_mentions": {"parse": []}
                }))
                .send()
        })
        .await?;
        resp.error_for_status()?;
        Ok(())
    }

    /// ACK a component interaction and clear its buttons in one call:
    /// type 7 (UPDATE_MESSAGE) re-sends the original content with no
    /// components, so a resolved permission prompt can't be re-pressed.
    /// Interaction tokens live ~15 minutes and must be answered within
    /// 3 seconds — failures are best-effort from the caller's side.
    pub async fn ack_interaction(
        &self,
        interaction_id: &str,
        token: &str,
        content: &str,
    ) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!(
                "{}/interactions/{interaction_id}/{token}/callback",
                self.base
            ))
            .bearer_auth(&self.token)
            .json(&json!({
                "type": 7,
                "data": {"content": content, "components": [], "allowed_mentions": {"parse": []}}
            }))
            .send()
            .await?;
        resp.error_for_status()?;
        Ok(())
    }

    /// Upload files to a channel — one multipart request with
    /// `files[N]` parts plus `payload_json` carrying the caption.
    /// reqwest's multipart feature is off in this workspace, so the
    /// body is framed by hand. UNVERIFIED against the real API —
    /// exercised against a mock server.
    pub async fn send_files(
        &self,
        channel_id: &str,
        caption: Option<&str>,
        items: &[crate::channel::MediaOut],
    ) -> anyhow::Result<()> {
        let mut mp = crate::channel::MultipartWriter::new();
        mp.field(
            "payload_json",
            &json!({
                "content": caption.unwrap_or(""),
                "allowed_mentions": {"parse": []}
            })
            .to_string(),
        );
        for (i, item) in items.iter().enumerate() {
            mp.file(
                &format!("files[{i}]"),
                &item.filename,
                &item.mime,
                &item.data,
            );
        }
        let (ctype, body) = mp.finish();
        let resp = crate::ratelimit::send_with_rate_limit(|| {
            self.http
                .post(format!("{}/channels/{channel_id}/messages", self.base))
                .header(reqwest::header::CONTENT_TYPE, &ctype)
                .body(body.clone())
                .bearer_auth(&self.token)
                .send()
        })
        .await?;
        resp.error_for_status()?;
        Ok(())
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
            // Rate limits are handled by the shared helper: retries
            // 429s honoring Retry-After, paces the next chunk when the
            // bucket is exhausted.
            let resp = crate::ratelimit::send_with_rate_limit(|| {
                self.http
                    .post(format!("{}/channels/{channel_id}/messages", self.base))
                    .bearer_auth(&self.token)
                    .json(&json!({
                        "content": &rest[..end],
                        // Agent output must never ping — a reply containing
                        // @everyone/@here would otherwise mention the guild.
                        "allowed_mentions": {"parse": []}
                    }))
                    .send()
            })
            .await?;
            resp.error_for_status()?;
            rest = &rest[end..];
        }
        Ok(())
    }

    /// POST /channels/{id}/typing — shows "bot is typing…" for ~10s.
    /// Best-effort: a 4xx (missing access, channel gone) is ignored.
    pub async fn send_typing(&self, channel_id: &str) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!("{}/channels/{channel_id}/typing", self.base))
            .bearer_auth(&self.token)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Ok(());
        }
        Ok(())
    }
}

/// First payload after Hello: RESUME (op 6) when a gateway session
/// survives, else IDENTIFY (op 2). `seq` is the last dispatch number
/// seen before the disconnect (null when none).
fn identify_or_resume(session_id: Option<&str>, seq: i64, token: &str) -> Value {
    match session_id {
        Some(sid) => json!({
            "op": 6,
            "d": {
                "token": token,
                "session_id": sid,
                "seq": if seq < 0 { Value::Null } else { json!(seq) },
            },
        }),
        None => json!({
            "op": 2,
            "d": {
                "token": token,
                "intents": INTENTS,
                "properties": {"os": std::env::consts::OS, "browser": "damon", "device": "damon"},
            },
        }),
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
    // Attachments: Discord CDN URLs are public (signed, expire after a
    // few hours) — carry them as lazy Url attachments, fetched at
    // prompt time. No bearer: the signature in the query string is the
    // credential.
    let attachments: Vec<crate::channel::Attachment> = d["attachments"]
        .as_array()
        .map(|atts| {
            atts.iter()
                .filter_map(|a| {
                    Some(crate::channel::Attachment::Url {
                        url: a["url"].as_str()?.to_string(),
                        bearer: None,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
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
        // Empty after mention-stripping: a bare mention is ignored, but
        // a mention + attachment still comes through so the files reach
        // the prompt.
        if stripped.is_empty() && attachments.is_empty() {
            return None;
        }
        return Some(Incoming {
            chat_id: channel_id,
            // Discord threads are channels — the thread's channel_id
            // already scopes the conversation.
            thread_id: None,
            sender_id: d["author"]["id"].as_str().map(String::from),
            text: stripped,
            attachments,
        });
    }
    // A DM with neither text nor attachments is nothing to prompt on.
    if text.is_empty() && attachments.is_empty() {
        return None;
    }
    Some(Incoming {
        chat_id: channel_id,
        thread_id: None,
        sender_id: d["author"]["id"].as_str().map(String::from),
        text,
        attachments,
    })
}

/// Extract a permission button press from an INTERACTION_CREATE
/// dispatch: the synthesized Incoming carries the pressed word as text
/// and the presser's user id as sender (member.user in guilds, user in
/// DMs), so the bridge's identity check and pending lane treat it
/// exactly like a typed reply. None for non-perm components and
/// malformed payloads.
pub fn incoming_from_interaction(d: &Value) -> Option<Incoming> {
    let word = d["data"]["custom_id"].as_str()?.strip_prefix("perm:")?;
    if !matches!(word, "allow" | "deny" | "always") {
        return None;
    }
    let sender_id = d["member"]["user"]["id"]
        .as_str()
        .or_else(|| d["user"]["id"].as_str())?
        .to_string();
    Some(Incoming {
        chat_id: d["channel_id"].as_str()?.to_string(),
        // Discord threads are channels — the interaction's channel_id
        // already scopes the conversation, same as messages.
        thread_id: None,
        sender_id: Some(sender_id),
        text: word.to_string(),
        attachments: Vec::new(),
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
    /// Gateway session id from READY — kept across reconnects so they
    /// RESUME (op 6) instead of re-identifying. Cleared only when the
    /// gateway invalidates the session (op 9 `d: false`).
    session_id: Mutex<Option<String>>,
    /// Reconnect backoff for paths that re-IDENTIFY from scratch
    /// (invalidated session, connect failure): doubles from 5s, caps at
    /// 300s, resets when the gateway accepts the (re)connection (READY
    /// or RESUMED).
    backoff: Mutex<std::time::Duration>,
}

impl DiscordChannel {
    pub fn new(api: Arc<DiscordApi>) -> Self {
        Self {
            api,
            bot_id: Mutex::new(None),
            conn: Mutex::new(None),
            session_id: Mutex::new(None),
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
        let (ws, _) = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio_tungstenite::connect_async(format!("{url}/?v=10&encoding=json")),
        )
        .await
        .context("gateway connect timed out")?
        .context("gateway connect failed")?;
        let (write, mut read) = ws.split();
        let write = Arc::new(Mutex::new(write));

        // First frame must be Hello (op 10) with the heartbeat interval.
        // Bound the wait — a peer that accepts the socket but never
        // speaks would otherwise park this connect forever.
        let hello = tokio::time::timeout(std::time::Duration::from_secs(30), read.next())
            .await
            .context("gateway hello timed out")?
            .context("gateway closed before hello")??;
        let hello: Value = serde_json::from_str(hello.to_text()?)?;
        // A malicious/buggy gateway could send 0 — tokio's interval panics
        // on a zero period, killing the heartbeat task.
        let interval_ms = hello["d"]["heartbeat_interval"]
            .as_u64()
            .unwrap_or(41250)
            .max(1000);

        // RESUME (op 6) when a gateway session survives, else IDENTIFY
        // (op 2). A fresh identify restarts the dispatch numbering, so
        // the heartbeat's last-seen seq must restart too — a stale seq
        // would desync heartbeats and get the link killed.
        let session_id = self.session_id.lock().await.clone();
        let first = identify_or_resume(
            session_id.as_deref(),
            self.seq.load(Ordering::Relaxed),
            &self.api.token,
        );
        if first["op"] == 2 {
            self.seq.store(-1, Ordering::Relaxed);
        }
        write
            .lock()
            .await
            .send(Message::Text(first.to_string().into()))
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
                        // Permission button press: ACK (clearing the
                        // buttons so the resolved prompt can't be
                        // re-pressed) then synthesize the typed-reply
                        // equivalent. The ACK is best-effort — a late
                        // token only leaves stale buttons, while the
                        // press itself was real and must flow.
                        if v["op"] == 0 && v["t"] == "INTERACTION_CREATE" {
                            let d = &v["d"];
                            if let (Some(iid), Some(tok)) = (d["id"].as_str(), d["token"].as_str())
                            {
                                let content = d["message"]["content"].as_str().unwrap_or("");
                                if let Err(e) = self.api.ack_interaction(iid, tok, content).await {
                                    warn!(error = %e, "interaction ack failed");
                                }
                            }
                            if let Some(msg) = incoming_from_interaction(d) {
                                return Ok(Some(msg));
                            }
                        }
                        // READY: identify accepted — remember the
                        // gateway session so reconnects RESUME, and
                        // retire the identify backoff.
                        if v["op"] == 0 && v["t"] == "READY" {
                            if let Some(sid) = v["d"]["session_id"].as_str() {
                                *self.session_id.lock().await = Some(sid.to_string());
                            }
                            self.reset_backoff().await;
                        }
                        // RESUMED: the gateway accepted our op 6 and
                        // replayed the dispatches missed while
                        // disconnected — same reconnect cadence reset.
                        if v["op"] == 0 && v["t"] == "RESUMED" {
                            self.reset_backoff().await;
                        }
                        // op 7 asks for a reconnect; the session
                        // survives, so the next connect RESUMEs after a
                        // short fixed wait. op 9 invalidates depending
                        // on `d`: true → still resumable (short wait,
                        // RESUME again); false → session dead, drop it
                        // and IDENTIFY fresh under the exponential
                        // backoff (5s → ×2, 300s cap, reset on
                        // READY/RESUMED) so a gateway that keeps
                        // refusing cannot burn the identify quota
                        // (token-bucketed, ~1000/day per bot).
                        if v["op"] == 7 || v["op"] == 9 {
                            let (delay, drop_session) = if v["op"] == 9 {
                                if v["d"].as_bool() == Some(true) {
                                    (std::time::Duration::from_secs(3), false)
                                } else {
                                    (self.next_backoff().await, true)
                                }
                            } else {
                                (std::time::Duration::from_secs(5), false)
                            };
                            if drop_session {
                                *self.session_id.lock().await = None;
                            }
                            *guard = None;
                            drop(guard);
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }
                    Some(Ok(_)) => {
                        // Binary/ping/pong/close frames — not dispatchable.
                    }
                    Some(Err(e)) => {
                        warn!(error = %e, "discord gateway error; reconnecting");
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

    /// Permission prompts with native components. The pressed word
    /// comes back through recv's INTERACTION_CREATE handling as a
    /// synthesized Incoming — identical to a typed reply as far as
    /// the bridge is concerned.
    async fn send_permission(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        title: &str,
        actions: &[Value],
    ) -> anyhow::Result<()> {
        // Discord threads are channels — thread_id is already folded
        // into chat_id by the bridge's conversation keying.
        let _ = thread_id;
        let text = crate::channel::permission_prompt_text(title, actions);
        let buttons: Vec<(String, String, u8)> = crate::channel::permission_choices(actions)
            .into_iter()
            .map(|c| {
                // 3 success for the allow variants, 4 danger for deny.
                let style = if c.word == "deny" { 4 } else { 3 };
                (c.label, format!("perm:{}", c.word), style)
            })
            .collect();
        self.api.send_button_message(chat_id, &text, &buttons).await
    }

    async fn send_media(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        caption: Option<&str>,
        items: &[crate::channel::MediaOut],
    ) -> anyhow::Result<()> {
        let _ = thread_id;
        self.api.send_files(chat_id, caption, items).await
    }

    async fn typing(&self, chat_id: &str) -> anyhow::Result<()> {
        self.api.send_typing(chat_id).await
    }

    fn flush_threshold(&self) -> usize {
        1800
    }

    /// Resolve an attachment: Discord CDN URLs are public (the signed
    /// query string is the credential), fetched lazily at prompt time.
    async fn fetch(&self, att: &crate::channel::Attachment) -> anyhow::Result<(Vec<u8>, String)> {
        match att {
            crate::channel::Attachment::Bytes { data, mime } => Ok((data.clone(), mime.clone())),
            crate::channel::Attachment::Url { url, bearer } => {
                if bearer.is_some() {
                    anyhow::bail!("bearer-bearing urls are not supported on discord");
                }
                self.api.download_file(url).await
            }
            crate::channel::Attachment::TelegramFile { .. } => {
                anyhow::bail!("telegram file attachments are not supported on discord")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_payload_resumes_with_session_and_identifies_without() {
        let resume = identify_or_resume(Some("sess-1"), 42, "tok");
        assert_eq!(resume["op"], 6);
        assert_eq!(resume["d"]["session_id"], "sess-1");
        assert_eq!(resume["d"]["seq"], 42);
        // No dispatch seen before the disconnect: seq is null, not -1.
        let resume_blank = identify_or_resume(Some("sess-1"), -1, "tok");
        assert_eq!(resume_blank["d"]["seq"], serde_json::Value::Null);

        let identify = identify_or_resume(None, 42, "tok");
        assert_eq!(identify["op"], 2);
        assert_eq!(identify["d"]["intents"], INTENTS);
        assert!(identify["d"]["session_id"].is_null());
    }

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

    #[test]
    fn interaction_press_synthesizes_typed_reply() {
        // Guild interaction: sender under member.user.
        let d = json!({
            "id": "1", "token": "tok",
            "channel_id": "C1",
            "member": {"user": {"id": "U1"}},
            "data": {"custom_id": "perm:allow", "component_type": 2},
        });
        let msg = incoming_from_interaction(&d).unwrap();
        assert_eq!(msg.text, "allow");
        assert!(msg.thread_id.is_none());

        // DM interaction: sender under user.
        let d = json!({
            "id": "2", "token": "tok",
            "channel_id": "C2",
            "user": {"id": "U2"},
            "data": {"custom_id": "perm:deny"},
        });
        let msg = incoming_from_interaction(&d).unwrap();
        assert_eq!(msg.text, "deny");
        assert_eq!(msg.sender_id.as_deref(), Some("U2"));

        // Non-perm components and malformed presses don't synthesize.
        let menu =
            json!({"channel_id": "C1", "data": {"custom_id": "menu:x"}, "user": {"id": "U1"}});
        assert!(incoming_from_interaction(&menu).is_none());
        let bad =
            json!({"channel_id": "C1", "data": {"custom_id": "perm:reboot"}, "user": {"id": "U1"}});
        assert!(incoming_from_interaction(&bad).is_none());
        assert!(incoming_from_interaction(&json!({"data": {"custom_id": "perm:allow"}})).is_none());
    }

    /// REST surface against a mock server: permission button message,
    /// interaction ACK (type 7 clearing components), and media upload.
    #[tokio::test]
    async fn buttons_ack_and_files_hit_the_documented_shapes() {
        let sent = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(String, Value)>::new()));
        let raw = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(String, Vec<u8>)>::new()));
        let (s1, r1) = (sent.clone(), raw.clone());
        let s2 = sent.clone();
        let app = axum::Router::new()
            .route(
                "/channels/{ch}/messages",
                axum::routing::post(
                    move |axum::extract::Path(ch): axum::extract::Path<String>,
                          headers: axum::http::HeaderMap,
                          body: axum::body::Bytes| {
                        let (sent, raw) = (s1.clone(), r1.clone());
                        async move {
                            let is_json = headers
                                .get(axum::http::header::CONTENT_TYPE)
                                .and_then(|v| v.to_str().ok())
                                .is_some_and(|t| t.starts_with("application/json"));
                            if is_json {
                                let v: Value = serde_json::from_slice(&body).unwrap();
                                sent.lock().await.push((ch, v));
                            } else {
                                raw.lock().await.push((ch, body.to_vec()));
                            }
                            axum::Json(json!({"id": "m1"}))
                        }
                    },
                ),
            )
            .route(
                "/interactions/{id}/{token}/callback",
                axum::routing::post(
                    move |axum::extract::Path((id, token)): axum::extract::Path<(
                        String,
                        String,
                    )>,
                          axum::Json(v): axum::Json<Value>| {
                        let sent = s2.clone();
                        async move {
                            sent.lock().await.push((format!("{id}/{token}"), v));
                            axum::Json(json!({}))
                        }
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let api = DiscordApi::with_base("tok", &format!("http://{addr}"));

        // Button message: one action row, success/danger styles,
        // perm:<word> custom ids, no-ping allowed_mentions.
        api.send_button_message(
            "C9",
            "🔐 run command\nreply 'allow' or 'deny'",
            &[
                ("Allow".into(), "perm:allow".into(), 3),
                ("Deny".into(), "perm:deny".into(), 4),
            ],
        )
        .await
        .unwrap();
        {
            let sent = sent.lock().await;
            assert_eq!(sent[0].0, "C9");
            assert_eq!(sent[0].1["components"][0]["type"], 1);
            assert_eq!(
                sent[0].1["components"][0]["components"][0]["custom_id"],
                "perm:allow"
            );
            assert_eq!(sent[0].1["components"][0]["components"][0]["style"], 3);
            assert_eq!(sent[0].1["components"][0]["components"][1]["style"], 4);
            assert_eq!(sent[0].1["allowed_mentions"]["parse"], json!([]));
        }

        // ACK: type 7 UPDATE_MESSAGE with the original content and no
        // components — the resolved prompt loses its buttons.
        api.ack_interaction("99", "itok", "🔐 run command")
            .await
            .unwrap();
        {
            let sent = sent.lock().await;
            assert_eq!(sent[1].0, "99/itok");
            assert_eq!(sent[1].1["type"], 7);
            assert_eq!(sent[1].1["data"]["content"], "🔐 run command");
            assert_eq!(sent[1].1["data"]["components"], json!([]));
        }

        // Media upload: one multipart with payload_json + files[0].
        api.send_files(
            "C9",
            Some("chart"),
            &[crate::channel::MediaOut {
                data: vec![1, 2, 3],
                mime: "image/png".into(),
                filename: "image-0.png".into(),
            }],
        )
        .await
        .unwrap();
        {
            let raw = raw.lock().await;
            assert_eq!(raw[0].0, "C9");
            let body = String::from_utf8_lossy(&raw[0].1);
            assert!(body.contains("name=\"payload_json\""));
            assert!(body.contains("\"content\":\"chart\""));
            assert!(body.contains("name=\"files[0]\"; filename=\"image-0.png\""));
            assert!(body.contains("Content-Type: image/png"));
            assert!(body.contains("--"));
        }
    }
    #[test]
    fn incoming_message_maps_attachments() {
        // Guild: mention + files, no other text - the files are the prompt.
        let d = json!({
            "channel_id": "C1", "guild_id": "G1",
            "author": {"id": "U1"},
            "content": "<@BOT>",
            "attachments": [
                {"id": "a1", "url": "https://cdn.discordapp.com/ephemeral-attachments/C1/a1/cat.png?ex=1&is=2&hm=3"},
                {"id": "a2"},
            ],
        });
        let msg = incoming_from_message(&d, "BOT").unwrap();
        assert_eq!(msg.text, "");
        assert_eq!(msg.attachments.len(), 1);
        match &msg.attachments[0] {
            crate::channel::Attachment::Url { url, bearer } => {
                assert_eq!(
                    url,
                    "https://cdn.discordapp.com/ephemeral-attachments/C1/a1/cat.png?ex=1&is=2&hm=3"
                );
                assert!(bearer.is_none(), "CDN URLs are public - no credential");
            }
            _ => panic!("expected a Url attachment"),
        }

        // DM with files only.
        let d = json!({
            "channel_id": "C2", "author": {"id": "U2"}, "content": "",
            "attachments": [{"url": "https://cdn.discordapp.com/x.bin"}],
        });
        let msg = incoming_from_message(&d, "BOT").unwrap();
        assert_eq!(msg.attachments.len(), 1);

        // Bare mention without files is still ignored.
        let d = json!({
            "channel_id": "C3", "guild_id": "G1",
            "author": {"id": "U3"}, "content": "<@BOT>",
        });
        assert!(incoming_from_message(&d, "BOT").is_none());
    }

    /// CDN download: plain GET, no Authorization header, mime from
    /// Content-Type.
    #[tokio::test]
    async fn download_file_gets_cdn_without_auth() {
        use axum::body::Bytes;
        let saw_auth = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<Option<String>>::new()));
        let s1 = saw_auth.clone();
        let app = axum::Router::new().route(
            "/ephemeral-attachments/a1/cat.png",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let saw_auth = s1.clone();
                async move {
                    saw_auth.lock().await.push(
                        headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|a| a.to_str().ok())
                            .map(String::from),
                    );
                    (
                        [(axum::http::header::CONTENT_TYPE, "image/png")],
                        Bytes::from_static(&[7, 7, 7]),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let api = DiscordApi::with_base("bot-token", &format!("http://{addr}"));
        let (data, mime) = api
            .download_file(&format!("http://{addr}/ephemeral-attachments/a1/cat.png"))
            .await
            .unwrap();
        assert_eq!(data, vec![7, 7, 7]);
        assert_eq!(mime, "image/png");
        assert_eq!(*saw_auth.lock().await, [None], "CDN URLs carry no auth");
    }
}
