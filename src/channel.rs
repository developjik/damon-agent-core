//! Generic chat-channel bridge: maps channel chats to damon sessions.
//! A channel adapter supplies inbound messages and outbound delivery;
//! this bridge owns session mapping, event demux, permission replies
//! ("allow"/"deny"), and the per-turn streaming loop.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::bail;
use serde_json::json;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::client::{ClientEvent, DamonClient};

/// One inbound message from a channel chat.
pub struct Incoming {
    /// Channel-specific chat/conversation id, normalized to String.
    pub chat_id: String,
    /// Sender identity, when the channel exposes one — used to authorize
    /// "allow"/"deny" permission replies in group chats.
    pub sender_id: Option<String>,
    /// Message text with any bot mention already stripped.
    pub text: String,
}

/// A chat channel's transport surface — injectable for tests.
/// Implementations own their receive loop internals (polling offsets,
/// gateway reconnects, socket-mode acks) inside `recv`.
#[async_trait::async_trait]
pub trait ChannelApi: Send + Sync {
    /// One-time setup before the receive loop (e.g. resolve bot user id).
    async fn ready(&self) -> anyhow::Result<()> {
        Ok(())
    }
    /// Wait for the next inbound message. Implementations should retry
    /// transient transport failures internally; an Err here is logged
    /// and retried by the bridge after a short delay.
    async fn recv(&self) -> anyhow::Result<Option<Incoming>>;
    /// Deliver text to a chat. Implementations truncate to the
    /// channel's message cap.
    async fn send(&self, chat_id: &str, text: &str) -> anyhow::Result<()>;
    /// Buffered chunks are flushed early once they exceed this size,
    /// so `send` stays under the channel cap.
    fn flush_threshold(&self) -> usize {
        3500
    }
}

/// Routes daemon events to the chat that owns the session.
struct Demux {
    /// session_id -> channel to that chat's turn handler
    sessions: HashMap<String, mpsc::Sender<ClientEvent>>,
    /// chat_id -> (requester sender_id, answer channel). The sender id
    /// binds the reply to whoever triggered the tool call.
    pending_permissions: HashMap<String, (Option<String>, oneshot::Sender<bool>)>,
}

pub struct Bridge {
    ch: Arc<dyn ChannelApi>,
    client: DamonClient,
    /// chat_id -> session_id
    chat_sessions: Mutex<HashMap<String, String>>,
    /// chat_ids with a turn in flight — serializes turns per chat.
    active_turns: Mutex<std::collections::HashSet<String>>,
    demux: Mutex<Demux>,
}

impl Bridge {
    pub fn new(ch: Arc<dyn ChannelApi>, client: DamonClient) -> Arc<Self> {
        Arc::new(Self {
            ch,
            client,
            chat_sessions: Mutex::new(HashMap::new()),
            active_turns: Mutex::new(std::collections::HashSet::new()),
            demux: Mutex::new(Demux {
                sessions: HashMap::new(),
                pending_permissions: HashMap::new(),
            }),
        })
    }

    /// Main loop: channel setup, then dispatch inbound messages forever.
    pub async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        self.ch.ready().await?;
        self.client.initialize().await?;
        self.spawn_event_router().await;
        loop {
            match self.ch.recv().await {
                Ok(Some(msg)) => {
                    self.handle_message(msg.chat_id, msg.sender_id, msg.text)
                        .await
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(error = %e, "channel recv failed; retrying in 5s");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Exposed for tests and embedders that drive messages manually.
    pub fn client(&self) -> &DamonClient {
        &self.client
    }

    /// Single consumer of client.events(); fans out to per-session channels.
    /// PromptDone arrives through the same stream, ordered after that
    /// session's notifications, so trailing chunks are never lost.
    pub async fn spawn_event_router(self: &Arc<Self>) {
        let mut events = self.client.events().await;
        let me = self.clone();
        tokio::spawn(async move {
            while let Some(ev) = events.recv().await {
                let session_id = match &ev {
                    ClientEvent::Update(p) => p["sessionId"].as_str().map(String::from),
                    ClientEvent::Request { params, .. } => {
                        params["sessionId"].as_str().map(String::from)
                    }
                    ClientEvent::PromptDone { session_id, .. } => Some(session_id.clone()),
                };
                if let Some(sid) = session_id {
                    let tx = me.demux.lock().await.sessions.get(&sid).cloned();
                    if let Some(tx) = tx {
                        let _ = tx.send(ev).await;
                    }
                }
            }
        });
    }

    /// Handle one inbound message: permission reply, or a new prompt turn.
    pub async fn handle_message(
        self: &Arc<Self>,
        chat_id: String,
        sender_id: Option<String>,
        text: String,
    ) {
        // "allow"/"deny" answers a pending permission request — but only
        // from the sender who triggered it. In group chats anyone else
        // typing "allow" must not approve a tool run.
        let lower = text.trim().to_lowercase();
        if lower == "allow" || lower == "deny" {
            let pending = self
                .demux
                .lock()
                .await
                .pending_permissions
                .get(&chat_id)
                .map(|(req, _)| req.clone());
            let authorized = match (&pending, &sender_id) {
                (Some(Some(req)), Some(sender)) => req == sender,
                // Unknown requester or unknown sender: allow (DM-style
                // channels that don't expose identity).
                _ => true,
            };
            if pending.is_some() {
                if authorized {
                    let (_, tx) = self
                        .demux
                        .lock()
                        .await
                        .pending_permissions
                        .remove(&chat_id)
                        .unwrap();
                    let _ = tx.send(lower == "allow");
                    let _ = self
                        .ch
                        .send(
                            &chat_id,
                            if lower == "allow" {
                                "✅ allowed"
                            } else {
                                "🚫 denied"
                            },
                        )
                        .await;
                }
                // Pending exists but the replier isn't the requester —
                // swallow the reply; it must not become a prompt.
                return;
            }
        }

        // One live turn per chat — a second prompt would overwrite the
        // demux entry and starve the first turn's events.
        {
            let mut active = self.active_turns.lock().await;
            if !active.insert(chat_id.clone()) {
                let _ = self
                    .ch
                    .send(&chat_id, "a prompt is already running in this chat")
                    .await;
                return;
            }
        }

        let session_id = match self.session_for(&chat_id).await {
            Ok(s) => s,
            Err(e) => {
                self.active_turns.lock().await.remove(&chat_id);
                let _ = self.ch.send(&chat_id, &format!("error: {e:#}")).await;
                return;
            }
        };
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = me.run_turn(&chat_id, &session_id, sender_id, &text).await {
                let _ = me.ch.send(&chat_id, &format!("error: {e:#}")).await;
            }
            me.active_turns.lock().await.remove(&chat_id);
        });
    }

    /// Get or create the damon session for this chat. A creation failure
    /// propagates — caching a fabricated id would brick the chat forever.
    async fn session_for(&self, chat_id: &str) -> anyhow::Result<String> {
        let mut map = self.chat_sessions.lock().await;
        if let Some(s) = map.get(chat_id) {
            return Ok(s.clone());
        }
        let sid = self.client.new_session("", None).await?;
        map.insert(chat_id.to_string(), sid.clone());
        Ok(sid)
    }

    /// One prompt turn for a chat: forward events to the channel until done.
    async fn run_turn(
        self: &Arc<Self>,
        chat_id: &str,
        session_id: &str,
        sender_id: Option<String>,
        text: &str,
    ) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(64);
        self.demux
            .lock()
            .await
            .sessions
            .insert(session_id.to_string(), tx);

        // Send the prompt; the turn result arrives as PromptDone on `rx`.
        let prompt_send = {
            let client = self.client.clone();
            let sid = session_id.to_string();
            let text = text.to_string();
            tokio::spawn(async move { client.prompt(&sid, &text, None).await })
        };

        let mut buf = String::new();
        // Fail fast if the send itself errors; otherwise wait for PromptDone.
        match prompt_send.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.end_turn(chat_id, session_id).await;
                return Err(e);
            }
            Err(e) => {
                self.end_turn(chat_id, session_id).await;
                return Err(e.into());
            }
        }
        loop {
            match rx.recv().await {
                Some(ClientEvent::PromptDone { result, .. }) => {
                    self.end_turn(chat_id, session_id).await;
                    self.flush(chat_id, &mut buf).await;
                    match result {
                        Ok(v) => {
                            let stop = v["stopReason"].as_str().unwrap_or("?");
                            if stop != "end_turn" {
                                let _ = self.ch.send(chat_id, &format!("[{stop}]")).await;
                            }
                        }
                        Err(e) => bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
                    }
                    return Ok(());
                }
                Some(ClientEvent::Update(p)) => {
                    let u = &p["update"];
                    if u["sessionUpdate"] == "agent_message_chunk" {
                        if let Some(t) = u["content"]["text"].as_str() {
                            buf.push_str(t);
                            if buf.len() > self.ch.flush_threshold() {
                                self.flush(chat_id, &mut buf).await;
                            }
                        }
                    } else if u["sessionUpdate"] == "tool_call_update" {
                        let status = u["status"].as_str().unwrap_or("");
                        let _ = self.ch.send(chat_id, &format!("🔧 tool {}", status)).await;
                    }
                }
                Some(ClientEvent::Request { id, method, params }) => {
                    if method == "session/request_permission" {
                        let title = params["toolCall"]["title"]
                            .as_str()
                            .unwrap_or("tool")
                            .to_string();
                        let (ptx, prx) = oneshot::channel();
                        self.demux
                            .lock()
                            .await
                            .pending_permissions
                            .insert(chat_id.to_string(), (sender_id.clone(), ptx));
                        let _ = self
                            .ch
                            .send(chat_id, &format!("🔐 {title}\nreply 'allow' or 'deny'"))
                            .await;
                        let client = self.client.clone();
                        tokio::spawn(async move {
                            let allow = prx.await.unwrap_or(false);
                            let _ = client
                                        .respond(
                                            id,
                                            json!({
                                                "outcome": {
                                                    "outcome": "selected",
                                                    "optionId": if allow { "allow-once" } else { "reject-once" }
                                                }
                                            }),
                                        )
                                        .await;
                        });
                    }
                }
                None => {
                    self.end_turn(chat_id, session_id).await;
                    anyhow::bail!("event channel closed")
                }
            }
        }
    }

    /// Turn teardown: drop the demux session and any unanswered
    /// permission request so a stale "allow" can't resolve a dead ask.
    async fn end_turn(&self, chat_id: &str, session_id: &str) {
        let mut demux = self.demux.lock().await;
        demux.sessions.remove(session_id);
        demux.pending_permissions.remove(chat_id);
    }

    async fn flush(&self, chat_id: &str, buf: &mut String) {
        if !buf.is_empty() {
            let _ = self.ch.send(chat_id, buf).await;
            buf.clear();
        }
    }
}
