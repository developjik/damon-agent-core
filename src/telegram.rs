//! Telegram channel: long-polls the Bot API and feeds the generic
//! channel bridge. Permission requests become "reply 'allow' to approve".

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use serde_json::Value;

use crate::channel::{Bridge as ChannelBridge, ChannelApi, Incoming};
use crate::client::DamonClient;

/// Minimal Bot API surface — injectable for tests.
#[async_trait::async_trait]
pub trait TelegramApi: Send + Sync {
    /// Long-poll for updates. Returns raw update objects.
    async fn get_updates(&self, offset: i64, timeout_secs: u64) -> anyhow::Result<Vec<Value>>;
    /// Send a message; `thread_id` is a forum topic (message_thread_id);
    /// `parse_mode` requests entity parsing (HTML). `None` sends plain.
    async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        thread_id: Option<i64>,
        parse_mode: Option<&str>,
    ) -> anyhow::Result<()>;
    /// Send a message with one row of inline buttons — permission
    /// prompts. `buttons` is (label, callback_data). Default falls back
    /// to plain send_message so test doubles without buttons still
    /// exercise the text path.
    async fn send_buttons(
        &self,
        chat_id: i64,
        text: &str,
        thread_id: Option<i64>,
        buttons: &[(String, String)],
    ) -> anyhow::Result<()> {
        let _ = buttons;
        self.send_message(chat_id, text, thread_id, None).await
    }
    /// Answer a callback query — stops the client-side press spinner.
    async fn answer_callback_query(&self, _id: &str) -> anyhow::Result<()> {
        Ok(())
    }
    /// Strip a message's inline keyboard: a resolved permission prompt
    /// must not be pressable again.
    async fn edit_reply_markup(&self, _chat_id: i64, _message_id: i64) -> anyhow::Result<()> {
        Ok(())
    }
    /// Upload one media item: sendPhoto for images (multipart),
    /// sendDocument otherwise. Default errors — test doubles without
    /// uploads keep the bridge's fallback note path.
    async fn send_photo(
        &self,
        _chat_id: i64,
        _thread_id: Option<i64>,
        _caption: Option<&str>,
        _data: Vec<u8>,
        _mime: &str,
        _filename: &str,
    ) -> anyhow::Result<()> {
        anyhow::bail!("media upload not supported")
    }
    /// Show "typing…" in the chat. Best-effort — failures are ignored.
    async fn send_chat_action(&self, _chat_id: i64, _action: &str) -> anyhow::Result<()> {
        Ok(())
    }
    /// Resolve a Bot API file_id to (bytes, mime). Default errors —
    /// test doubles that never see photos don't implement it.
    async fn fetch_file(&self, _file_id: &str) -> anyhow::Result<(Vec<u8>, String)> {
        anyhow::bail!("file download not supported")
    }
}

/// Real Bot API over HTTPS. The token lives in the request URL (the Bot
/// API has no header auth), so errors are sanitized before they can be
/// logged — reqwest's Display embeds the full URL, token included.
pub struct BotApi {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl BotApi {
    pub fn new(token: &str) -> Self {
        Self {
            base: format!("https://api.telegram.org/bot{token}"),
            token: token.to_string(),
            // A stalled TCP connection must not park a send forever —
            // the bridge's run_turn would hang and brick the chat.
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Strip the bot token from an error before it reaches a log line.
    fn sanitize(&self, e: reqwest::Error) -> anyhow::Error {
        anyhow::anyhow!("{}", e.to_string().replace(&self.token, "***"))
    }
}

#[async_trait::async_trait]
impl TelegramApi for BotApi {
    async fn get_updates(&self, offset: i64, timeout_secs: u64) -> anyhow::Result<Vec<Value>> {
        let resp: Value = self
            .http
            .get(format!("{}/getUpdates", self.base))
            .query(&[("offset", offset), ("timeout", timeout_secs as i64)])
            .timeout(std::time::Duration::from_secs(timeout_secs + 10))
            .send()
            .await
            .map_err(|e| self.sanitize(e))?
            .json()
            .await
            .map_err(|e| self.sanitize(e))?;
        anyhow::ensure!(
            resp["ok"].as_bool().unwrap_or(false),
            "getUpdates failed: {}",
            resp["description"].as_str().unwrap_or("unknown")
        );
        Ok(resp["result"].as_array().cloned().unwrap_or_default())
    }

    async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        thread_id: Option<i64>,
        parse_mode: Option<&str>,
    ) -> anyhow::Result<()> {
        // Telegram caps messages at 4096 chars — split instead of
        // truncating so long agent replies aren't silently lost.
        const MAX: usize = 4000;
        let mut rest = text;
        while !rest.is_empty() {
            let end = if rest.len() > MAX {
                rest.floor_char_boundary(MAX)
            } else {
                rest.len()
            };
            let chunk = &rest[..end];
            // Two passes: formatted, then plain — a Telegram 400 on
            // entities must cost the formatting, never the message.
            let mut mode = parse_mode;
            loop {
                let mut body = serde_json::json!({"chat_id": chat_id, "text": chunk});
                if let Some(m) = mode {
                    body["parse_mode"] = serde_json::json!(m);
                }
                if let Some(t) = thread_id {
                    body["message_thread_id"] = serde_json::json!(t);
                }
                let resp = crate::ratelimit::send_with_rate_limit(|| {
                    self.http
                        .post(format!("{}/sendMessage", self.base))
                        .json(&body)
                        .send()
                })
                .await
                .map_err(|e| self.sanitize(e))?;
                let resp: Value = resp.json().await.map_err(|e| self.sanitize(e))?;
                if resp["ok"].as_bool().unwrap_or(false) {
                    break;
                }
                let desc = resp["description"].as_str().unwrap_or("unknown");
                if mode.is_some() && desc.contains("can't parse entities") {
                    mode = None;
                    continue;
                }
                anyhow::bail!("sendMessage failed: {desc}");
            }
            rest = &rest[end..];
        }
        Ok(())
    }
    async fn send_buttons(
        &self,
        chat_id: i64,
        text: &str,
        thread_id: Option<i64>,
        buttons: &[(String, String)],
    ) -> anyhow::Result<()> {
        // Permission prompts are short — no chunking, same plain-text
        // fallback as send_message when entities fail to parse.
        let row: Vec<Value> = buttons
            .iter()
            .map(|(label, data)| serde_json::json!({"text": label, "callback_data": data}))
            .collect();
        let mut mode = Some("HTML");
        loop {
            let mut body = serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "reply_markup": {"inline_keyboard": [row]},
            });
            if let Some(m) = mode {
                body["parse_mode"] = serde_json::json!(m);
            }
            if let Some(t) = thread_id {
                body["message_thread_id"] = serde_json::json!(t);
            }
            let resp = crate::ratelimit::send_with_rate_limit(|| {
                self.http
                    .post(format!("{}/sendMessage", self.base))
                    .json(&body)
                    .send()
            })
            .await
            .map_err(|e| self.sanitize(e))?;
            let resp: Value = resp.json().await.map_err(|e| self.sanitize(e))?;
            if resp["ok"].as_bool().unwrap_or(false) {
                return Ok(());
            }
            let desc = resp["description"].as_str().unwrap_or("unknown");
            if mode.is_some() && desc.contains("can't parse entities") {
                mode = None;
                continue;
            }
            anyhow::bail!("sendMessage failed: {desc}");
        }
    }
    async fn answer_callback_query(&self, id: &str) -> anyhow::Result<()> {
        // Best-effort: an answer failure must not block the synthesized
        // reply — the press itself already happened.
        let _ = self
            .http
            .post(format!("{}/answerCallbackQuery", self.base))
            .json(&serde_json::json!({"callback_query_id": id}))
            .send()
            .await;
        Ok(())
    }
    async fn edit_reply_markup(&self, chat_id: i64, message_id: i64) -> anyhow::Result<()> {
        // Omitting reply_markup clears the keyboard.
        let resp: Value = self
            .http
            .post(format!("{}/editMessageReplyMarkup", self.base))
            .json(&serde_json::json!({"chat_id": chat_id, "message_id": message_id}))
            .send()
            .await
            .map_err(|e| self.sanitize(e))?
            .json()
            .await
            .map_err(|e| self.sanitize(e))?;
        anyhow::ensure!(
            resp["ok"].as_bool().unwrap_or(false),
            "editMessageReplyMarkup failed: {}",
            resp["description"].as_str().unwrap_or("unknown")
        );
        Ok(())
    }
    async fn send_photo(
        &self,
        chat_id: i64,
        thread_id: Option<i64>,
        caption: Option<&str>,
        data: Vec<u8>,
        mime: &str,
        filename: &str,
    ) -> anyhow::Result<()> {
        // UNVERIFIED against the real Bot API (no bot token on this
        // machine) — shaped from the documented multipart sendPhoto /
        // sendDocument forms and exercised against a mock server.
        let is_image = mime.starts_with("image/");
        let (method, field) = if is_image {
            ("sendPhoto", "photo")
        } else {
            ("sendDocument", "document")
        };
        let mut mp = crate::channel::MultipartWriter::new();
        mp.field("chat_id", &chat_id.to_string());
        if let Some(t) = thread_id {
            mp.field("message_thread_id", &t.to_string());
        }
        // Captions stay plain text — no backend supplies one yet, and a
        // caption parse failure would risk the upload itself.
        if let Some(c) = caption {
            mp.field("caption", c);
        }
        mp.file(field, filename, mime, &data);
        let (ctype, body) = mp.finish();
        let resp = crate::ratelimit::send_with_rate_limit(|| {
            self.http
                .post(format!("{}/{method}", self.base))
                .header(reqwest::header::CONTENT_TYPE, &ctype)
                .body(body.clone())
                .send()
        })
        .await
        .map_err(|e| self.sanitize(e))?;
        let resp: Value = resp.json().await.map_err(|e| self.sanitize(e))?;
        anyhow::ensure!(
            resp["ok"].as_bool().unwrap_or(false),
            "{method} failed: {}",
            resp["description"].as_str().unwrap_or("unknown")
        );
        Ok(())
    }
    async fn send_chat_action(&self, chat_id: i64, action: &str) -> anyhow::Result<()> {
        let resp = self
            .http
            .post(format!("{}/sendChatAction", self.base))
            .json(&serde_json::json!({"chat_id": chat_id, "action": action}))
            .send()
            .await
            .map_err(|e| self.sanitize(e))?;
        // Best-effort: a 4xx here (e.g. chat gone) must not kill the turn.
        if !resp.status().is_success() {
            return Ok(());
        }
        Ok(())
    }
    async fn fetch_file(&self, file_id: &str) -> anyhow::Result<(Vec<u8>, String)> {
        // getFile → file_path → download from the file host. The file
        // URL embeds the token too, so errors are sanitized the same way.
        let meta: Value = self
            .http
            .get(format!("{}/getFile", self.base))
            .query(&[("file_id", file_id)])
            .send()
            .await
            .map_err(|e| self.sanitize(e))?
            .json()
            .await
            .map_err(|e| self.sanitize(e))?;
        anyhow::ensure!(
            meta["ok"].as_bool().unwrap_or(false),
            "getFile failed: {}",
            meta["description"].as_str().unwrap_or("unknown")
        );
        let path = meta["result"]["file_path"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("getFile returned no file_path"))?;
        let mime = match path.rsplit('.').next() {
            Some("png") => "image/png",
            Some("jpg") | Some("jpeg") => "image/jpeg",
            Some("gif") => "image/gif",
            Some("webp") => "image/webp",
            Some("txt") | Some("md") | Some("csv") | Some("log") => "text/plain",
            Some("json") => "application/json",
            // Unknown extensions (documents) default to a blob resource
            // — guessing image/jpeg would feed a PDF to the image path.
            _ => "application/octet-stream",
        }
        .to_string();
        let bytes = self
            .http
            .get(format!(
                "https://api.telegram.org/file/bot{}/{}",
                self.token, path
            ))
            .send()
            .await
            .map_err(|e| self.sanitize(e))?
            .bytes()
            .await
            .map_err(|e| self.sanitize(e))?;
        Ok((bytes.to_vec(), mime))
    }
}

/// One buffered update in delivery order. `msg` is what the bridge gets —
/// None for updates with nothing deliverable (edits, reactions), which
/// queue as markers so confirmation stays in update_id order and can
/// never jump past an un-handed message.
struct PendingUpdate {
    update_id: Option<i64>,
    msg: Option<Incoming>,
}

/// Telegram transport for the generic bridge. Owns the long-poll offset
/// and a buffer for the rest of each poll batch — getUpdates returns a
/// Vec, so recv must drain it one message per call or updates are lost.
pub struct TelegramChannel {
    tg: Arc<dyn TelegramApi>,
    offset: AtomicI64,
    pending: tokio::sync::Mutex<std::collections::VecDeque<PendingUpdate>>,
}

impl TelegramChannel {
    pub fn new(tg: Arc<dyn TelegramApi>) -> Self {
        Self {
            tg,
            offset: AtomicI64::new(0),
            pending: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Pop the next buffered entry, confirming each update's offset as
    /// it is handed toward the bridge. Bridge::handle_message cannot
    /// fail (it logs its own errors), so handout is the commit point:
    /// a crash before it leaves the tail unconfirmed and Telegram
    /// redelivers — at-least-once, with only the single in-flight
    /// message as the loss window.
    async fn take_confirmed(&self) -> Option<Incoming> {
        let mut pending = self.pending.lock().await;
        while let Some(next) = pending.pop_front() {
            if let Some(id) = next.update_id {
                self.offset.fetch_max(id + 1, Ordering::Relaxed);
            }

            if let Some(msg) = next.msg {
                return Some(msg);
            }
        }
        None
    }
}

#[async_trait::async_trait]
impl ChannelApi for TelegramChannel {
    async fn recv(&self) -> anyhow::Result<Option<Incoming>> {
        if let Some(msg) = self.take_confirmed().await {
            return Ok(Some(msg));
        }
        let updates = self
            .tg
            .get_updates(self.offset.load(Ordering::Relaxed), 30)
            .await?;
        let mut batch = Vec::new();
        // Updates arrive in ascending update_id order, and a batch is
        // only polled when pending is empty — every earlier message was
        // already handed out.
        for u in updates {
            let id = u["update_id"].as_i64();
            // Idempotent by update_id: a redelivered update at or below
            // the confirmed offset was already handed to the bridge.
            if id.is_some_and(|i| i < self.offset.load(Ordering::Relaxed)) {
                continue;
            }
            let msg = if u["callback_query"].is_object() {
                self.handle_callback(&u).await
            } else {
                incoming_from_update(&u)
            };
            // Media we can't turn into a prompt (voice, video, sticker…)
            // gets a one-line notice instead of silence — the update is
            // still confirmed normally so it isn't redelivered.
            if let Some((chat_id, thread_id)) = unsupported_message(&u) {
                let _ = self
                    .tg
                    .send_message(chat_id, "unsupported message type", thread_id, None)
                    .await;
            }
            // A malformed entry has no id to confirm — its message, if
            // any, is delivered once and Telegram is never told it was
            // seen.
            if msg.is_some() || id.is_some() {
                batch.push(PendingUpdate { update_id: id, msg });
            }
        }
        self.pending.lock().await.extend(batch);
        Ok(self.take_confirmed().await)
    }

    async fn send(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        self.send_in_thread(chat_id, None, text).await
    }

    async fn send_in_thread(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        text: &str,
    ) -> anyhow::Result<()> {
        let id: i64 = chat_id
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid telegram chat_id: {chat_id}"))?;
        let tid: Option<i64> = thread_id.and_then(|t| t.parse().ok());
        // Markdown is native to the model's output; Telegram wants
        // HTML — convert here so every outbound text (replies,
        // prompts, notices) renders rich on one code path.
        let html = crate::channel::to_telegram_html(text);
        self.tg.send_message(id, &html, tid, Some("HTML")).await
    }

    /// Permission prompts with native buttons. The pressed word comes
    /// back through recv's callback handling as a synthesized Incoming
    /// — identical to a typed reply as far as the bridge is concerned.
    async fn send_permission(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        title: &str,
        actions: &[Value],
    ) -> anyhow::Result<()> {
        let id: i64 = chat_id
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid telegram chat_id: {chat_id}"))?;
        let tid: Option<i64> = thread_id.and_then(|t| t.parse().ok());
        let text = crate::channel::to_telegram_html(&crate::channel::permission_prompt_text(
            title, actions,
        ));
        let buttons: Vec<(String, String)> = crate::channel::permission_choices(actions)
            .into_iter()
            .map(|c| {
                // allow/deny get their affordance emoji; the scoped
                // allow keeps its own label ("Always allow" etc.).
                let label = match c.word {
                    "allow" => format!("✅ {}", c.label),
                    "deny" => format!("🚫 {}", c.label),
                    _ => c.label,
                };
                (label, format!("perm:{}", c.word))
            })
            .collect();
        self.tg.send_buttons(id, &text, tid, &buttons).await
    }

    async fn send_media(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        caption: Option<&str>,
        items: &[crate::channel::MediaOut],
    ) -> anyhow::Result<()> {
        let id: i64 = chat_id
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid telegram chat_id: {chat_id}"))?;
        let tid: Option<i64> = thread_id.and_then(|t| t.parse().ok());
        // One message per item; the caption rides the first only —
        // Telegram albums caption once, single photos do the same.
        for (i, item) in items.iter().enumerate() {
            self.tg
                .send_photo(
                    id,
                    tid,
                    if i == 0 { caption } else { None },
                    item.data.clone(),
                    &item.mime,
                    &item.filename,
                )
                .await?;
        }
        Ok(())
    }

    async fn typing(&self, chat_id: &str) -> anyhow::Result<()> {
        let id: i64 = chat_id
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid telegram chat_id: {chat_id}"))?;
        self.tg.send_chat_action(id, "typing").await
    }

    async fn fetch(&self, att: &crate::channel::Attachment) -> anyhow::Result<(Vec<u8>, String)> {
        match att {
            crate::channel::Attachment::TelegramFile { file_id } => {
                self.tg.fetch_file(file_id).await
            }
            crate::channel::Attachment::Bytes { data, mime } => Ok((data.clone(), mime.clone())),
            // Telegram attachments resolve through file_ids — a raw
            // URL attachment would have needed credentials this
            // channel never received.
            crate::channel::Attachment::Url { .. } => {
                anyhow::bail!("url attachments are not supported on telegram")
            }
        }
    }
}

impl TelegramChannel {
    /// Handle a callback_query update: answer the query, strip the
    /// keyboard (a resolved prompt must not be re-pressable), and for
    /// a permission button synthesize the Incoming a typed reply
    /// would have produced — same word, same presser identity, same
    /// chat coordinates — so the bridge's pending-lane and identity
    /// checks apply unchanged.
    async fn handle_callback(&self, u: &Value) -> Option<Incoming> {
        let cq = &u["callback_query"];
        if let Some(qid) = cq["id"].as_str() {
            let _ = self.tg.answer_callback_query(qid).await;
        }
        let press = perm_callback(u)?;
        let _ = self
            .tg
            .edit_reply_markup(press.chat_id, press.message_id)
            .await;
        Some(Incoming {
            chat_id: press.chat_id.to_string(),
            thread_id: press.thread_id.map(|t| t.to_string()),
            sender_id: Some(press.sender_id),
            text: press.word,
            attachments: Vec::new(),
        })
    }
}

/// A permission button press extracted from a callback_query update.
/// The query is answered separately (handle_callback reads the query
/// id off the raw update), so this carries only what the synthesized
/// Incoming needs.
struct PermCallback {
    chat_id: i64,
    thread_id: Option<i64>,
    message_id: i64,
    /// The pressed reply word ("allow"/"deny"/"always").
    word: String,
    /// The presser's user id — the permission identity check binds
    /// approvals to whoever triggered the tool call.
    sender_id: String,
}

/// Extract a permission button press. None for non-perm callbacks
/// (still answered by the caller) and malformed payloads.
fn perm_callback(u: &Value) -> Option<PermCallback> {
    let cq = &u["callback_query"];
    let word = cq["data"].as_str()?.strip_prefix("perm:")?;
    if !matches!(word, "allow" | "deny" | "always") {
        return None;
    }
    Some(PermCallback {
        chat_id: cq["message"]["chat"]["id"].as_i64()?,
        thread_id: cq["message"]["message_thread_id"].as_i64(),
        message_id: cq["message"]["message_id"].as_i64()?,
        word: word.to_string(),
        sender_id: cq["from"]["id"].to_string(),
    })
}

/// Extract (chat_id, text) from a Telegram update. Non-message updates
/// (edits, reactions, service events) return None — and so do bot-authored
/// messages: a third-party bot in an allowlisted group could otherwise
/// trigger a prompt AND approve it (its sender_id satisfies the
/// requester binding), with no human in the loop.
pub fn incoming_from_update(u: &Value) -> Option<Incoming> {
    if u["message"]["from"]["is_bot"].as_bool() == Some(true) {
        return None;
    }
    let chat_id = u["message"]["chat"]["id"].as_i64()?;
    let sender_id = u["message"]["from"]["id"].as_i64().map(|id| id.to_string());
    // Forum topics: message_thread_id scopes the conversation to its
    // own session lane.
    let thread_id = u["message"]["message_thread_id"]
        .as_i64()
        .map(|t| t.to_string());
    if let Some(text) = u["message"]["text"].as_str() {
        return Some(Incoming {
            chat_id: chat_id.to_string(),
            thread_id,
            sender_id,
            text: text.to_string(),
            attachments: Vec::new(),
        });
    }
    // Photo message: take the largest size (last in the array), caption
    // becomes the prompt text. The file_id is resolved lazily at prompt
    // time — a message that never becomes a turn never downloads.
    if let Some(photos) = u["message"]["photo"].as_array()
        && let Some(file_id) = photos.last().and_then(|p| p["file_id"].as_str())
    {
        return Some(Incoming {
            chat_id: chat_id.to_string(),
            thread_id,
            sender_id,
            text: u["message"]["caption"].as_str().unwrap_or("").to_string(),
            attachments: vec![crate::channel::Attachment::TelegramFile {
                file_id: file_id.to_string(),
            }],
        });
    }
    // Document message: caption becomes the prompt text, the file_id is
    // resolved lazily at prompt time like a photo.
    if let Some(file_id) = u["message"]["document"]["file_id"].as_str() {
        return Some(Incoming {
            chat_id: chat_id.to_string(),
            thread_id,
            sender_id,
            text: u["message"]["caption"].as_str().unwrap_or("").to_string(),
            attachments: vec![crate::channel::Attachment::TelegramFile {
                file_id: file_id.to_string(),
            }],
        });
    }
    None
}

/// Chat coordinates of a message whose payload we can't deliver —
/// voice, video, sticker and friends. Returns None for non-message
/// updates and for payloads incoming_from_update already handles.
fn unsupported_message(u: &Value) -> Option<(i64, Option<i64>)> {
    let msg = &u["message"];
    if !msg.is_object() {
        return None;
    }
    for key in [
        "voice",
        "video",
        "video_note",
        "audio",
        "sticker",
        "animation",
    ] {
        if msg[key].is_object() {
            return Some((
                msg["chat"]["id"].as_i64()?,
                msg["message_thread_id"].as_i64(),
            ));
        }
    }
    None
}

/// Telegram bridge: thin wrapper over the generic channel bridge.
pub struct Bridge {
    inner: Arc<ChannelBridge>,
}

impl Bridge {
    pub fn new(tg: Arc<dyn TelegramApi>, client: DamonClient) -> Arc<Self> {
        let inner = ChannelBridge::new(Arc::new(TelegramChannel::new(tg)), client);
        Arc::new(Self { inner })
    }

    /// Restrict the bridge to these sender/chat ids — see
    /// `channel::Bridge::set_allowed`.
    pub fn set_allowed(&self, ids: impl IntoIterator<Item = String>) {
        self.inner.set_allowed(ids);
    }
    /// Main loop: the generic bridge drives TelegramChannel::recv — one
    /// poll loop, no second copy to drift.
    pub async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        self.inner.run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as StdMutex;
    use serde_json::json;

    /// Queue of poll responses; records the offset each poll requested —
    /// the commit point is observable as the offset the NEXT poll asks for.
    /// Recorded sends capture (text, parse_mode) for the HTML path.
    struct MockTg {
        batches: StdMutex<Vec<Vec<Value>>>,
        requested: StdMutex<Vec<i64>>,
        sent: StdMutex<Vec<(String, Option<String>)>>,
        buttons: StdMutex<Vec<Vec<(String, String)>>>,
        answered: StdMutex<Vec<String>>,
        stripped: StdMutex<Vec<(i64, i64)>>,
    }

    impl MockTg {
        fn with_batches(batches: Vec<Vec<Value>>) -> Arc<Self> {
            Arc::new(Self {
                batches: StdMutex::new(batches),
                requested: StdMutex::new(vec![]),
                sent: StdMutex::new(vec![]),
                buttons: StdMutex::new(vec![]),
                answered: StdMutex::new(vec![]),
                stripped: StdMutex::new(vec![]),
            })
        }
    }

    #[async_trait::async_trait]
    impl TelegramApi for MockTg {
        async fn get_updates(&self, offset: i64, _t: u64) -> anyhow::Result<Vec<Value>> {
            self.requested.lock().push(offset);
            let mut batches = self.batches.lock();
            if batches.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(batches.remove(0))
            }
        }
        async fn send_message(
            &self,
            _chat_id: i64,
            text: &str,
            _thread_id: Option<i64>,
            parse_mode: Option<&str>,
        ) -> anyhow::Result<()> {
            self.sent
                .lock()
                .push((text.to_string(), parse_mode.map(String::from)));
            Ok(())
        }
        async fn send_buttons(
            &self,
            _chat_id: i64,
            text: &str,
            _thread_id: Option<i64>,
            buttons: &[(String, String)],
        ) -> anyhow::Result<()> {
            self.sent
                .lock()
                .push((text.to_string(), Some("HTML".into())));
            self.buttons.lock().push(buttons.to_vec());
            Ok(())
        }
        async fn answer_callback_query(&self, id: &str) -> anyhow::Result<()> {
            self.answered.lock().push(id.to_string());
            Ok(())
        }
        async fn edit_reply_markup(&self, chat_id: i64, message_id: i64) -> anyhow::Result<()> {
            self.stripped.lock().push((chat_id, message_id));
            Ok(())
        }
    }

    fn msg_update(id: i64, text: &str) -> Value {
        json!({"update_id": id, "message": {"chat": {"id": 42}, "text": text}})
    }

    #[tokio::test]
    async fn offset_advances_only_as_messages_are_handed_out() {
        // Batch: a message, a non-message update behind it, another
        // message. The committed offset may never run ahead of the
        // oldest un-handed message — a crash mid-batch must leave the
        // tail unconfirmed so Telegram redelivers it.
        let tg = MockTg::with_batches(vec![vec![
            msg_update(100, "hi"),
            json!({"update_id": 101, "edited_message": {"chat": {"id": 42}}}),
            msg_update(102, "yo"),
        ]]);
        let ch = TelegramChannel::new(tg.clone());
        let requested = || tg.requested.lock().clone();

        assert_eq!(ch.recv().await.unwrap().unwrap().text, "hi");
        // Only 100 confirmed by its handout — 101/102 stay unconfirmed.
        assert_eq!(ch.offset.load(Ordering::Relaxed), 101);
        // The edit queues as a marker; handing "yo" confirms through it.
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "yo");
        assert_eq!(ch.offset.load(Ordering::Relaxed), 103);
        assert_eq!(requested(), [0]); // no poll between handouts
        // The next poll resumes after the whole confirmed batch.
        assert!(ch.recv().await.unwrap().is_none());
        assert_eq!(requested(), [0, 103]);
    }

    #[tokio::test]
    async fn updates_below_confirmed_offset_are_deduped() {
        let tg = MockTg::with_batches(vec![
            vec![msg_update(300, "a")],
            // A resend of already-confirmed updates ahead of a new one.
            vec![msg_update(300, "a"), msg_update(301, "b")],
        ]);
        let ch = TelegramChannel::new(tg);
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "a");
        // 300 is below the confirmed offset (301) — only "b" survives.
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "b");
    }

    #[tokio::test]
    async fn update_without_id_delivers_once() {
        let tg = MockTg::with_batches(vec![vec![
            json!({"message": {"chat": {"id": 42}, "text": "no id"}}),
            msg_update(400, "after"),
        ]]);
        let ch = TelegramChannel::new(tg);
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "no id");
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "after");
        assert_eq!(ch.offset.load(Ordering::Relaxed), 401);
    }

    fn callback_update(data: &str) -> Value {
        json!({
            "update_id": 500,
            "callback_query": {
                "id": "cq1",
                "from": {"id": 7, "is_bot": false},
                "message": {
                    "message_id": 99,
                    "chat": {"id": 42},
                    "text": "🔐 run command",
                },
                "data": data,
            },
        })
    }

    #[tokio::test]
    async fn permission_button_press_synthesizes_a_typed_reply() {
        let tg = MockTg::with_batches(vec![vec![callback_update("perm:allow")]]);
        let ch = TelegramChannel::new(tg.clone());
        let msg = ch.recv().await.unwrap().unwrap();
        // The synthesized Incoming is byte-for-byte what a typed
        // "allow" from user 7 in chat 42 looks like — the bridge's
        // identity check and pending lane need no special case.
        assert_eq!(msg.text, "allow");
        assert_eq!(msg.sender_id.as_deref(), Some("7"));
        assert_eq!(msg.chat_id, "42");
        assert!(msg.thread_id.is_none());
        // Query answered and keyboard stripped so the resolved prompt
        // can't be re-pressed.
        assert_eq!(*tg.answered.lock(), ["cq1".to_string()]);
        assert_eq!(*tg.stripped.lock(), [(42, 99)]);
        // The press confirmed its update like any message.
        assert_eq!(ch.offset.load(Ordering::Relaxed), 501);
    }

    #[tokio::test]
    async fn non_permission_callbacks_are_answered_but_dropped() {
        let tg = MockTg::with_batches(vec![vec![callback_update("menu:open")]]);
        let ch = TelegramChannel::new(tg.clone());
        assert!(ch.recv().await.unwrap().is_none());
        // Still answered (spinner) and still confirmed — a dropped
        // callback must not reappear on the next poll forever.
        assert_eq!(*tg.answered.lock(), ["cq1".to_string()]);
        assert!(tg.stripped.lock().is_empty());
        assert_eq!(ch.offset.load(Ordering::Relaxed), 501);
    }

    #[tokio::test]
    async fn forum_topic_press_carries_thread_id() {
        let mut u = callback_update("perm:deny");
        u["callback_query"]["message"]["message_thread_id"] = json!(12);
        let tg = MockTg::with_batches(vec![vec![u]]);
        let ch = TelegramChannel::new(tg);
        let msg = ch.recv().await.unwrap().unwrap();
        assert_eq!(msg.text, "deny");
        assert_eq!(msg.thread_id.as_deref(), Some("12"));
    }

    #[tokio::test]
    async fn outbound_text_is_converted_to_telegram_html() {
        let tg = MockTg::with_batches(vec![]);
        let ch = TelegramChannel::new(tg.clone());
        ch.send_in_thread("42", None, "**hi** `<x>`\n```\ncode\n```")
            .await
            .unwrap();
        let sent = tg.sent.lock();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].0,
            // The closing fence emits a trailing newline to separate
            // any following text — here there is none, but the \n is
            // unconditional.
            "<b>hi</b> <code>&lt;x&gt;</code>\n<pre><code>code\n</code></pre>\n"
        );
    }

    #[tokio::test]
    async fn permission_prompt_renders_buttons_with_hint_text() {
        let tg = MockTg::with_batches(vec![]);
        let ch = TelegramChannel::new(tg.clone());
        let actions = serde_json::json!([
            {"id": "allow", "label": "Allow", "behavior": "allow"},
            {"id": "allow_session", "label": "Always allow", "behavior": "allow"},
            {"id": "deny", "label": "Deny", "behavior": "deny"},
        ])
        .as_array()
        .unwrap()
        .clone();
        ch.send_permission("42", None, "run command", &actions)
            .await
            .unwrap();
        let sent = tg.sent.lock();
        assert_eq!(sent.len(), 1);
        // Same 🔐 title + hint the plain-text channels show.
        assert!(
            sent[0]
                .0
                .starts_with("🔐 run command\nreply 'allow', 'always', or 'deny'")
        );
        // One row: allow (✅), scoped allow (own label), deny (🚫) —
        // callback_data is the reply word the press synthesizes.
        let buttons = tg.buttons.lock();
        assert_eq!(
            buttons[0],
            [
                ("✅ Allow".to_string(), "perm:allow".to_string()),
                ("Always allow".to_string(), "perm:always".to_string()),
                ("🚫 Deny".to_string(), "perm:deny".to_string()),
            ]
        );
    }
}
