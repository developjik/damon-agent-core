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

/// Real Bot API over HTTPS.
pub struct BotApi {
    base: String,
    http: reqwest::Client,
}

impl BotApi {
    pub fn new(token: &str) -> Self {
        Self {
            base: format!("https://api.telegram.org/bot{token}"),
            http: reqwest::Client::new(),
        }
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
            .await?
            .json()
            .await?;
        Ok(resp["result"].as_array().cloned().unwrap_or_default())
    }

    async fn send_message(&self, chat_id: i64, text: &str) -> anyhow::Result<()> {
        // Telegram caps messages at 4096 chars.
        let text = if text.len() > 4000 { &text[..4000] } else { text };
        self.http
            .post(format!("{}/sendMessage", self.base))
            .json(&serde_json::json!({"chat_id": chat_id, "text": text}))
            .send()
            .await?;
        Ok(())
    }
}

/// Telegram transport for the generic bridge. Owns the long-poll offset.
pub struct TelegramChannel {
    tg: Arc<dyn TelegramApi>,
    offset: AtomicI64,
}

impl TelegramChannel {
    pub fn new(tg: Arc<dyn TelegramApi>) -> Self {
        Self {
            tg,
            offset: AtomicI64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ChannelApi for TelegramChannel {
    async fn recv(&self) -> anyhow::Result<Option<Incoming>> {
        let updates = self.tg.get_updates(self.offset.load(Ordering::Relaxed), 30).await?;
        for u in updates {
            self.offset
                .fetch_max(u["update_id"].as_i64().unwrap_or(0) + 1, Ordering::Relaxed);
            if let Some(msg) = incoming_from_update(&u) {
                return Ok(Some(msg));
            }
        }
        Ok(None)
    }

    async fn send(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        let id: i64 = chat_id.parse().unwrap_or(0);
        self.tg.send_message(id, text).await
    }
}

/// Extract (chat_id, text) from a Telegram update. Non-message updates
/// (edits, reactions, service events) return None.
pub fn incoming_from_update(u: &Value) -> Option<Incoming> {
    let chat_id = u["message"]["chat"]["id"].as_i64()?;
    let text = u["message"]["text"].as_str()?;
    Some(Incoming {
        chat_id: chat_id.to_string(),
        text: text.to_string(),
    })
}

/// Telegram bridge: thin wrapper over the generic channel bridge that
/// keeps the update-driven API used by tests and embedders.
pub struct Bridge {
    tg: Arc<dyn TelegramApi>,
    inner: Arc<ChannelBridge>,
}

impl Bridge {
    pub fn new(tg: Arc<dyn TelegramApi>, client: DamonClient) -> Arc<Self> {
        let inner = ChannelBridge::new(
            Arc::new(TelegramChannel::new(tg.clone())),
            client,
        );
        Arc::new(Self { tg, inner })
    }

    /// Main loop: poll Telegram, dispatch messages.
    pub async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        self.inner.client().initialize().await?;
        self.inner.spawn_event_router().await;
        let mut offset = 0i64;
        loop {
            let updates = match self.tg.get_updates(offset, 30).await {
                Ok(u) => u,
                Err(e) => {
                    tracing::warn!(error = %e, "getUpdates failed; retrying in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };
            for u in updates {
                offset = offset.max(u["update_id"].as_i64().unwrap_or(0) + 1);
                self.handle_update(u).await;
            }
        }
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
            self.inner.handle_message(msg.chat_id, msg.text).await;
        }
    }
}
