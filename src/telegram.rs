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
    async fn send_message(&self, chat_id: i64, text: &str) -> anyhow::Result<()>;
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

    async fn send_message(&self, chat_id: i64, text: &str) -> anyhow::Result<()> {
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
            let resp = self
                .http
                .post(format!("{}/sendMessage", self.base))
                .json(&serde_json::json!({"chat_id": chat_id, "text": &rest[..end]}))
                .send()
                .await
                .map_err(|e| self.sanitize(e))?;
            // A 429 mid-split must not lose the remaining chunks —
            // honor parameters.retry_after and retry the failed chunk once.
            let resp: Value = if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let wait = resp
                    .json::<Value>()
                    .await
                    .ok()
                    .and_then(|v| v["parameters"]["retry_after"].as_f64())
                    .unwrap_or(1.0);
                tokio::time::sleep(std::time::Duration::from_secs_f64(wait.min(30.0))).await;
                self.http
                    .post(format!("{}/sendMessage", self.base))
                    .json(&serde_json::json!({"chat_id": chat_id, "text": &rest[..end]}))
                    .send()
                    .await
                    .map_err(|e| self.sanitize(e))?
                    .json()
                    .await
                    .map_err(|e| self.sanitize(e))?
            } else {
                resp.json().await.map_err(|e| self.sanitize(e))?
            };
            anyhow::ensure!(
                resp["ok"].as_bool().unwrap_or(false),
                "sendMessage failed: {}",
                resp["description"].as_str().unwrap_or("unknown")
            );
            rest = &rest[end..];
        }
        Ok(())
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
            let msg = incoming_from_update(&u);
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
        let id: i64 = chat_id
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid telegram chat_id: {chat_id}"))?;
        self.tg.send_message(id, text).await
    }
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
    let text = u["message"]["text"].as_str()?;
    Some(Incoming {
        chat_id: chat_id.to_string(),
        sender_id: u["message"]["from"]["id"].as_i64().map(|id| id.to_string()),
        text: text.to_string(),
    })
}

/// Telegram bridge: thin wrapper over the generic channel bridge that
/// keeps the update-driven API used by tests and embedders.
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

    /// Exposed for tests and embedders that drive updates manually.
    pub fn client(&self) -> &DamonClient {
        self.inner.client()
    }

    /// Single consumer of client.events(); fans out to per-session channels.
    pub async fn spawn_event_router(self: &Arc<Self>) {
        self.inner.spawn_event_router().await;
    }

    /// Handle one raw Telegram update.
    pub async fn handle_update(self: &Arc<Self>, u: Value) {
        if let Some(msg) = incoming_from_update(&u) {
            self.inner
                .handle_message(msg.chat_id, msg.sender_id, msg.text)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as StdMutex;
    use serde_json::json;

    /// Queue of poll responses; records the offset each poll requested —
    /// the commit point is observable as the offset the NEXT poll asks for.
    struct MockTg {
        batches: StdMutex<Vec<Vec<Value>>>,
        requested: StdMutex<Vec<i64>>,
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
        async fn send_message(&self, _chat_id: i64, _text: &str) -> anyhow::Result<()> {
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
        let tg = Arc::new(MockTg {
            batches: StdMutex::new(vec![vec![
                msg_update(100, "hi"),
                json!({"update_id": 101, "edited_message": {"chat": {"id": 42}}}),
                msg_update(102, "yo"),
            ]]),
            requested: StdMutex::new(vec![]),
        });
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
        let tg = Arc::new(MockTg {
            batches: StdMutex::new(vec![
                vec![msg_update(300, "a")],
                // A resend of already-confirmed updates ahead of a new one.
                vec![msg_update(300, "a"), msg_update(301, "b")],
            ]),
            requested: StdMutex::new(vec![]),
        });
        let ch = TelegramChannel::new(tg);
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "a");
        // 300 is below the confirmed offset (301) — only "b" survives.
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "b");
    }

    #[tokio::test]
    async fn update_without_id_delivers_once() {
        let tg = Arc::new(MockTg {
            batches: StdMutex::new(vec![vec![
                json!({"message": {"chat": {"id": 42}, "text": "no id"}}),
                msg_update(400, "after"),
            ]]),
            requested: StdMutex::new(vec![]),
        });
        let ch = TelegramChannel::new(tg);
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "no id");
        assert_eq!(ch.recv().await.unwrap().unwrap().text, "after");
        assert_eq!(ch.offset.load(Ordering::Relaxed), 401);
    }
}
