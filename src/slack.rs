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
                .send()
        };
        let resp = crate::ratelimit::send_with_rate_limit(send).await?;
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

    /// Post a message carrying Block Kit blocks — permission prompts
    /// with native buttons. `text` is the notification fallback (shown
    /// where blocks don't render); `parse: none` keeps agent-provided
    /// content from pinging the room, same as post_message.
    /// UNVERIFIED against real Slack (no workspace token here) —
    /// shaped from the documented blocks payload and exercised against
    /// a mock server.
    pub async fn post_blocks(
        &self,
        channel: &str,
        thread_ts: Option<&str>,
        text: &str,
        blocks: Vec<Value>,
    ) -> anyhow::Result<()> {
        let mut body = json!({
            "channel": channel,
            "text": text,
            "parse": "none",
            "blocks": blocks,
        });
        if let Some(ts) = thread_ts {
            body["thread_ts"] = json!(ts);
        }
        self.post("chat.postMessage", &self.bot_token, &body)
            .await?;
        Ok(())
    }

    /// chat.update a prompt message to plain text with no blocks — a
    /// resolved permission card must not stay pressable. Best-effort
    /// by contract: callers warn, they don't fail.
    pub async fn update_message(&self, channel: &str, ts: &str, text: &str) -> anyhow::Result<()> {
        self.post(
            "chat.update",
            &self.bot_token,
            &json!({"channel": channel, "ts": ts, "text": text, "blocks": []}),
        )
        .await
        .map(|_| ())
    }

    /// Download a shared file from `url_private` — Slack file URLs
    /// serve bytes only to requests bearing the bot token; an
    /// unauthenticated GET redirects to a login page instead.
    /// `bearer` overrides the bot token when the attachment carries
    /// its own credential. Mime comes from the response Content-Type.
    pub async fn download_file(
        &self,
        url: &str,
        bearer: Option<&str>,
    ) -> anyhow::Result<(Vec<u8>, String)> {
        let token = bearer.unwrap_or(&self.bot_token);
        let resp = self.http.get(url).bearer_auth(token).send().await?;
        let resp = resp.error_for_status()?;
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

    /// Upload and share media via the v2 flow:
    /// files.getUploadURLExternal → POST the bytes (octet-stream) →
    /// files.completeUploadExternal with the collected file ids.
    /// UNVERIFIED against real Slack (no workspace token on this
    /// machine) — shaped from the documented flow and exercised
    /// against a mock server.
    pub async fn upload_files(
        &self,
        channel: &str,
        thread_ts: Option<&str>,
        caption: Option<&str>,
        items: &[crate::channel::MediaOut],
    ) -> anyhow::Result<()> {
        let mut ids = Vec::new();
        for item in items {
            let resp = self
                .post(
                    "files.getUploadURLExternal",
                    &self.bot_token,
                    &json!({"filename": item.filename, "length": item.data.len()}),
                )
                .await?;
            let upload_url = resp["upload_url"]
                .as_str()
                .context("files.getUploadURLExternal returned no upload_url")?;
            let file_id = resp["file_id"]
                .as_str()
                .context("files.getUploadURLExternal returned no file_id")?
                .to_string();
            self.http
                .post(upload_url)
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(item.data.clone())
                .send()
                .await?
                .error_for_status()?;
            ids.push(json!({"id": file_id, "title": item.filename}));
        }
        // thread_ts posts the files as a reply to that parent message.
        let mut body = json!({"files": ids, "channel_id": channel});
        if let Some(ts) = thread_ts {
            body["thread_ts"] = json!(ts);
        }
        if let Some(c) = caption {
            body["initial_comment"] = json!(c);
        }
        self.post("files.completeUploadExternal", &self.bot_token, &body)
            .await?;
        Ok(())
    }
}
/// Extract (channel, text, attachments) from a Socket Mode events_api
/// message event. Skips bot/system subtypes (file_share excepted —
/// its files become prompt attachments fetched lazily at prompt time);
/// the bot's own messages are skipped; non-DM messages must mention
/// the bot (mention is stripped).
pub fn incoming_from_event(ev: &Value, bot_user_id: &str) -> Option<Incoming> {
    if ev["type"] != "message" {
        return None;
    }
    if ev.get("subtype").is_some_and(|s| s != "file_share") {
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
    // Shared files: url_private needs the bot token at fetch time
    // (bearer: None — SlackChannel::fetch supplies it), so nothing is
    // downloaded until the message actually becomes a turn.
    let attachments: Vec<crate::channel::Attachment> = ev["files"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .filter_map(|f| {
                    let url = f["url_private"]
                        .as_str()
                        .or_else(|| f["url_private_download"].as_str())?;
                    Some(crate::channel::Attachment::Url {
                        url: url.to_string(),
                        bearer: None,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
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
        // Empty after mention-stripping: a bare mention is ignored, but
        // a mention + files still comes through so the files reach the
        // prompt instead of an empty-text error.
        if stripped.is_empty() && attachments.is_empty() {
            return None;
        }
        return Some(Incoming {
            chat_id: channel,
            thread_id,
            sender_id: ev["user"].as_str().map(String::from),
            text: stripped,
            attachments,
        });
    }
    // A DM with neither text nor files is nothing to prompt on.
    if text.trim().is_empty() && attachments.is_empty() {
        return None;
    }
    Some(Incoming {
        chat_id: channel,
        thread_id,
        sender_id: ev["user"].as_str().map(String::from),
        text,
        attachments,
    })
}

/// A permission button press extracted from a Socket Mode
/// `type:"interactive"` envelope (payload `block_actions`). The
/// envelope is acked separately by recv, so this carries only what the
/// synthesized Incoming and the button-strip update need.
struct PermAction {
    chat_id: String,
    thread_id: Option<String>,
    sender_id: String,
    word: &'static str,
    /// The prompt message's ts — chat.update target for stripping the
    /// now-resolved buttons.
    ts: String,
}

fn perm_action(v: &Value) -> Option<PermAction> {
    if v["type"] != "interactive" {
        return None;
    }
    let p = &v["payload"];
    if p["type"] != "block_actions" {
        return None;
    }
    let word = p["actions"][0]["action_id"]
        .as_str()?
        .strip_prefix("perm:")?;
    if !matches!(word, "allow" | "deny" | "always") {
        return None;
    }
    Some(PermAction {
        chat_id: p["channel"]["id"].as_str()?.to_string(),
        thread_id: p["message"]["thread_ts"].as_str().map(String::from),
        sender_id: p["user"]["id"].as_str()?.to_string(),
        word: match word {
            "allow" => "allow",
            "deny" => "deny",
            _ => "always",
        },
        ts: p["message"]["ts"].as_str()?.to_string(),
    })
}

/// Escape Slack mrkdwn control characters (&, <, >) so agent-provided
/// permission titles render literally instead of opening entities or
/// link syntax.
fn escape_mrkdwn(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
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
                                // process or drop comes first. File
                                // shares resolve with their files as
                                // attachments (fetched lazily at prompt
                                // time) — they are prompts, not notices.
                                let msg = incoming_from_event(ev, &bot_id);
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
                            }
                            // Interactive payloads (block_actions for
                            // permission buttons, plus shortcuts,
                            // views, slash_commands…) arrive over this
                            // same socket — Socket Mode needs no
                            // public request URL. Permission presses
                            // synthesize the typed-reply equivalent.
                            _ => {
                                let press = perm_action(&v);
                                if let Some(eid) = v["envelope_id"].as_str() {
                                    let ack = json!({"envelope_id": eid});
                                    let _ = conn
                                        .write
                                        .lock()
                                        .await
                                        .send(Message::Text(ack.to_string().into()))
                                        .await;
                                }
                                if let Some(p) = press {
                                    // Strip the action buttons off the
                                    // resolved prompt — a still-pressable
                                    // card would let a second press send
                                    // "allow" as a literal prompt.
                                    // Best-effort: a failed update only
                                    // leaves stale buttons; the press
                                    // itself already flowed.
                                    let _ = self
                                        .api
                                        .update_message(
                                            &p.chat_id,
                                            &p.ts,
                                            &format!("permission {}ed", p.word),
                                        )
                                        .await;
                                    return Ok(Some(Incoming {
                                        chat_id: p.chat_id,
                                        thread_id: p.thread_id,
                                        sender_id: Some(p.sender_id),
                                        text: p.word.to_string(),
                                        attachments: Vec::new(),
                                    }));
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

    /// Permission prompts with native Block Kit buttons. With Socket
    /// Mode, `block_actions` presses arrive on this same WebSocket —
    /// no public request URL is involved (the old "interactive blocks
    /// need an HTTPS endpoint" rationale only holds for HTTP apps).
    /// The app still needs Interactivity enabled (with Socket Mode
    /// selected) in its Slack config. A press synthesizes the Incoming
    /// a typed reply would have produced — same word, same presser, so
    /// the bridge's pending-lane and identity checks apply unchanged.
    async fn send_permission(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        title: &str,
        actions: &[Value],
    ) -> anyhow::Result<()> {
        let elements: Vec<Value> = crate::channel::permission_choices(actions)
            .into_iter()
            .map(|c| {
                let label = match c.word {
                    "allow" => format!("✅ {}", c.label),
                    "deny" => format!("🚫 {}", c.label),
                    _ => c.label,
                };
                // Slack styles: "primary" green, "danger" red; the
                // scoped allow stays neutral.
                let mut btn = json!({
                    "type": "button",
                    "text": {"type": "plain_text", "text": label, "emoji": true},
                    "action_id": format!("perm:{}", c.word),
                });
                match c.word {
                    "allow" => btn["style"] = json!("primary"),
                    "deny" => btn["style"] = json!("danger"),
                    _ => {}
                }
                btn
            })
            .collect();
        let blocks = vec![
            json!({"type": "section", "text": {"type": "mrkdwn", "text": escape_mrkdwn(title)}}),
            json!({"type": "actions", "elements": elements}),
        ];
        self.api
            .post_blocks(
                chat_id,
                thread_id,
                &crate::channel::permission_prompt_text(title, actions),
                blocks,
            )
            .await
    }

    /// Outbound media via the Web API upload flow — no public
    /// endpoint involved, unlike interactive blocks.
    async fn send_media(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        caption: Option<&str>,
        items: &[crate::channel::MediaOut],
    ) -> anyhow::Result<()> {
        self.api
            .upload_files(chat_id, thread_id, caption, items)
            .await
    }

    /// Resolve an attachment: Slack file URLs need the bot token as a
    /// Bearer header (url_private serves bytes to authenticated
    /// requests only).
    async fn fetch(&self, att: &crate::channel::Attachment) -> anyhow::Result<(Vec<u8>, String)> {
        match att {
            crate::channel::Attachment::Bytes { data, mime } => Ok((data.clone(), mime.clone())),
            crate::channel::Attachment::Url { url, bearer } => {
                self.api.download_file(url, bearer.as_deref()).await
            }
            // Slack never hands out Bot API file_ids.
            crate::channel::Attachment::TelegramFile { .. } => {
                anyhow::bail!("telegram file attachments are not supported on slack")
            }
        }
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

    #[test]
    fn file_share_dm_becomes_a_prompt_with_url_attachments() {
        let ev = json!({
            "type": "message",
            "subtype": "file_share",
            "channel": "D1",
            "channel_type": "im",
            "user": "U1",
            "text": "analyze this",
            "files": [
                {"id": "F1", "url_private": "https://files.slack.com/files-pri/T-F1/a.png",
                 "url_private_download": "https://files.slack.com/files-pri/T-F1/download/a.png"},
                {"id": "F2"},  // no URL — skipped, not fatal
            ],
        });
        let msg = incoming_from_event(&ev, "UBOT").unwrap();
        assert_eq!(msg.text, "analyze this");
        assert_eq!(msg.attachments.len(), 1);
        match &msg.attachments[0] {
            crate::channel::Attachment::Url { url, bearer } => {
                assert_eq!(url, "https://files.slack.com/files-pri/T-F1/a.png");
                assert!(bearer.is_none());
            }
            _ => panic!("expected a Url attachment"),
        }
    }

    #[test]
    fn file_share_channel_needs_mention_and_empty_text_is_ok() {
        // No mention in a channel: dropped, like any channel message.
        let ev = json!({
            "type": "message", "subtype": "file_share",
            "channel": "C1", "channel_type": "channel", "user": "U1",
            "text": "look", "files": [{"url_private": "https://x/f.png"}],
        });
        assert!(incoming_from_event(&ev, "UBOT").is_none());
        // Mention + files, no other text: still a prompt.
        let ev = json!({
            "type": "message", "subtype": "file_share",
            "channel": "C1", "channel_type": "channel", "user": "U1",
            "text": "<@UBOT>", "files": [{"url_private": "https://x/f.png"}],
        });
        let msg = incoming_from_event(&ev, "UBOT").unwrap();
        assert_eq!(msg.text, "");
        assert_eq!(msg.attachments.len(), 1);
    }

    /// The v2 upload flow + authenticated download against mock
    /// servers: getUploadURLExternal → binary POST (octet-stream) →
    /// completeUploadExternal, and the Bearer bot token on url_private.
    #[tokio::test]
    async fn upload_and_download_hit_the_documented_shapes() {
        use axum::body::Bytes;

        // Raw upload endpoint: bound first so the API mock can point
        // getUploadURLExternal's upload_url back at it.
        let uploads = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(String, Vec<u8>)>::new()));
        let u1 = uploads.clone();
        let up_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_addr = up_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let app = axum::Router::new().route(
                "/upload/v1/ticket",
                axum::routing::post(move |headers: axum::http::HeaderMap, body: Bytes| {
                    let uploads = u1.clone();
                    async move {
                        let ctype = headers
                            .get(axum::http::header::CONTENT_TYPE)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_string();
                        uploads.lock().await.push((ctype, body.to_vec()));
                        format!("OK - {}", body.len())
                    }
                }),
            );
            axum::serve(up_listener, app).await.unwrap();
        });

        let calls = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(String, Value)>::new()));
        let bearers = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<Option<String>>::new()));
        let (c1, b1) = (calls.clone(), bearers.clone());
        let (c2v, b2) = (calls.clone(), bearers.clone());
        let upload_url = format!("http://{up_addr}/upload/v1/ticket");
        let app = axum::Router::new()
            .route(
                "/files.getUploadURLExternal",
                axum::routing::post(move |axum::Json(v): axum::Json<Value>| {
                    let (calls, upload_url) = (c1.clone(), upload_url.clone());
                    async move {
                        calls
                            .lock()
                            .await
                            .push(("files.getUploadURLExternal".into(), v));
                        axum::Json(json!({
                            "ok": true,
                            "upload_url": upload_url,
                            "file_id": "F012",
                        }))
                    }
                }),
            )
            .route(
                "/files.completeUploadExternal",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, axum::Json(v): axum::Json<Value>| {
                        let (calls, bearers) = (c2v.clone(), b1.clone());
                        async move {
                            bearers.lock().await.push(
                                headers
                                    .get(axum::http::header::AUTHORIZATION)
                                    .and_then(|a| a.to_str().ok())
                                    .map(String::from),
                            );
                            calls
                                .lock()
                                .await
                                .push(("files.completeUploadExternal".into(), v));
                            axum::Json(json!({"ok": true, "files": []}))
                        }
                    },
                ),
            )
            .route(
                "/files-pri/T-F1/a.png",
                axum::routing::get(move |headers: axum::http::HeaderMap| {
                    let bearers = b2.clone();
                    async move {
                        let auth = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|a| a.to_str().ok())
                            .map(String::from);
                        bearers.lock().await.push(auth);
                        (
                            [(axum::http::header::CONTENT_TYPE, "image/png")],
                            vec![9u8, 9, 9],
                        )
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let api = SlackApi::with_base("xapp-t", "xoxb-t", &format!("http://{addr}"));

        // Upload: two steps hit the documented shapes.
        api.upload_files(
            "C1",
            Some("1234.0001"),
            Some("chart"),
            &[crate::channel::MediaOut {
                data: vec![1, 2, 3, 4],
                mime: "image/png".into(),
                filename: "image-0.png".into(),
            }],
        )
        .await
        .unwrap();
        {
            let calls = calls.lock().await;
            assert_eq!(calls[0].0, "files.getUploadURLExternal");
            assert_eq!(calls[0].1["filename"], "image-0.png");
            assert_eq!(calls[0].1["length"], 4);
            assert_eq!(calls[1].0, "files.completeUploadExternal");
            assert_eq!(calls[1].1["channel_id"], "C1");
            assert_eq!(calls[1].1["thread_ts"], "1234.0001");
            assert_eq!(calls[1].1["initial_comment"], "chart");
            assert_eq!(calls[1].1["files"][0]["id"], "F012");
        }
        let uploads = uploads.lock().await;
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].0, "application/octet-stream");
        assert_eq!(uploads[0].1, vec![1, 2, 3, 4]);

        // Download: url_private fetch bears the bot token.
        let (data, mime) = api
            .download_file(&format!("http://{addr}/files-pri/T-F1/a.png"), None)
            .await
            .unwrap();
        assert_eq!(data, vec![9, 9, 9]);
        assert_eq!(mime, "image/png");
        assert_eq!(
            *bearers.lock().await,
            [Some("Bearer xoxb-t".into()), Some("Bearer xoxb-t".into())]
        );
    }

    #[test]
    fn perm_action_parses_block_actions_press() {
        let env = json!({
            "type": "interactive",
            "envelope_id": "e1",
            "payload": {
                "type": "block_actions",
                "actions": [{"action_id": "perm:allow", "block_id": "b", "action_ts": "1"}],
                "user": {"id": "U7"},
                "channel": {"id": "C42"},
                "message": {"ts": "1690000000.000100", "thread_ts": "1690000000.000099", "text": "…"}
            }
        });
        let p = perm_action(&env).expect("press must parse");
        assert_eq!(p.chat_id, "C42");
        assert_eq!(p.sender_id, "U7");
        assert_eq!(p.word, "allow");
        assert_eq!(p.ts, "1690000000.000100");
        assert_eq!(p.thread_id.as_deref(), Some("1690000000.000099"));
    }

    #[test]
    fn perm_action_rejects_non_permission_payloads() {
        // A menu selection: real interaction, not a perm press.
        let menu = json!({
            "type": "interactive",
            "payload": {"type": "block_actions",
                        "actions": [{"action_id": "menu:x"}],
                        "user": {"id": "U1"}, "channel": {"id": "C1"},
                        "message": {"ts": "1"}}
        });
        assert!(perm_action(&menu).is_none());
        // Unknown word — only allow/deny/always synthesize.
        let bad = json!({
            "type": "interactive",
            "payload": {"type": "block_actions",
                        "actions": [{"action_id": "perm:reboot"}],
                        "user": {"id": "U1"}, "channel": {"id": "C1"},
                        "message": {"ts": "1"}}
        });
        assert!(perm_action(&bad).is_none());
        // Other interactive flavors (view_submission, shortcuts).
        let view = json!({"type": "interactive", "payload": {"type": "view_submission"}});
        assert!(perm_action(&view).is_none());
        // Events never look like presses.
        assert!(perm_action(&json!({"type": "events_api"})).is_none());
    }
    /// Permission button message + the resolution strip against a mock
    /// server: blocks with perm:* action_ids, then chat.update with no
    /// blocks - the resolved prompt cannot be re-pressed.
    #[tokio::test]
    async fn blocks_post_and_update_hit_the_documented_shapes() {
        let calls = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(String, Value)>::new()));
        let c1 = calls.clone();
        let c2 = calls.clone();
        let app = axum::Router::new()
            .route(
                "/chat.postMessage",
                axum::routing::post(move |axum::Json(v): axum::Json<Value>| {
                    let calls = c1.clone();
                    async move {
                        calls.lock().await.push(("chat.postMessage".into(), v));
                        axum::Json(json!({"ok": true}))
                    }
                }),
            )
            .route(
                "/chat.update",
                axum::routing::post(move |axum::Json(v): axum::Json<Value>| {
                    let calls = c2.clone();
                    async move {
                        calls.lock().await.push(("chat.update".into(), v));
                        axum::Json(json!({"ok": true}))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let api = SlackApi::with_base("xapp-t", "xoxb-t", &format!("http://{addr}"));
        let actions = json!([
            {"id": "allow", "label": "Allow", "behavior": "allow"},
            {"id": "allow_session", "label": "Always allow", "behavior": "allow"},
            {"id": "deny", "label": "Deny", "behavior": "deny"},
        ]);
        let ch = SlackChannel::new(std::sync::Arc::new(api));
        {
            use crate::channel::ChannelApi;
            ch.send_permission(
                "C9",
                Some("1234.0001"),
                "Bash <rm> & files",
                actions.as_array().unwrap(),
            )
            .await
            .unwrap();
        }
        // The strip call recv makes after a press.
        ch.api
            .update_message("C9", "1234.0002", "permission allowed")
            .await
            .unwrap();

        let calls = calls.lock().await;
        let (m, body) = &calls[0];
        assert_eq!(m, "chat.postMessage");
        assert_eq!(body["channel"], "C9");
        assert_eq!(body["thread_ts"], "1234.0001");
        assert_eq!(body["parse"], "none");
        // Agent-provided title renders literally in mrkdwn.
        assert_eq!(
            body["blocks"][0]["text"]["text"],
            "Bash &lt;rm&gt; &amp; files"
        );
        let elements = body["blocks"][1]["elements"].as_array().unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0]["action_id"], "perm:allow");
        assert_eq!(elements[0]["style"], "primary");
        assert_eq!(elements[1]["action_id"], "perm:always");
        assert!(elements[1].get("style").is_none());
        assert_eq!(elements[2]["action_id"], "perm:deny");
        assert_eq!(elements[2]["style"], "danger");

        let (m, body) = &calls[1];
        assert_eq!(m, "chat.update");
        assert_eq!(body["ts"], "1234.0002");
        assert_eq!(body["blocks"], json!([]));
    }
}
