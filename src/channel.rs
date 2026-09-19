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
    sessions: HashMap<String, mpsc::UnboundedSender<ClientEvent>>,
    /// chat_id -> (requester sender_id, answer channel). The sender id
    /// binds the reply to whoever triggered the tool call.
    pending_permissions: HashMap<String, (Option<String>, oneshot::Sender<bool>)>,
}
/// Cached chat→session mapping entry. `last_used` drives eviction:
/// entries idle for over 24h are dropped when session_for misses, so
/// the map stays bounded on long-lived bots serving many chats. Only
/// the map entry is dropped — daemon-side session retention (the
/// daemon's own sweep) is unchanged.
struct ChatSession {
    session_id: String,
    last_used: std::time::Instant,
}

/// Idle age at which a chat→session mapping entry is evicted.
const SESSION_IDLE_EVICTION: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

pub struct Bridge {
    ch: Arc<dyn ChannelApi>,
    client: DamonClient,
    /// chat_id -> session_id
    chat_sessions: Mutex<HashMap<String, ChatSession>>,
    /// chat_ids with a turn in flight — serializes turns per chat.
    active_turns: Mutex<std::collections::HashSet<String>>,
    /// Sender/chat ids allowed to drive the agent. `None` = open (the
    /// channel binaries refuse to start without one — a public bot with
    /// no allowlist is unauthenticated agent access).
    allowed: parking_lot::RwLock<Option<std::collections::HashSet<String>>>,
    demux: Mutex<Demux>,
    /// events() is single-consumer — a second router would get a dead
    /// receiver and exit silently, hanging every turn. Spawn at most one.
    router_started: std::sync::atomic::AtomicBool,
}

impl Bridge {
    pub fn new(ch: Arc<dyn ChannelApi>, client: DamonClient) -> Arc<Self> {
        Arc::new(Self {
            ch,
            client,
            chat_sessions: Mutex::new(HashMap::new()),
            active_turns: Mutex::new(std::collections::HashSet::new()),
            allowed: parking_lot::RwLock::new(None),
            demux: Mutex::new(Demux {
                sessions: HashMap::new(),
                pending_permissions: HashMap::new(),
            }),
            router_started: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Restrict the bridge to these sender/chat ids. Checked before any
    /// session or prompt work — an unlisted sender is dropped silently.
    pub fn set_allowed(&self, ids: impl IntoIterator<Item = String>) {
        *self.allowed.write() = Some(ids.into_iter().collect());
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
                Ok(None) => {
                    // Same backoff as Err — a channel polling empty in
                    // a tight loop must not spin the CPU.
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
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
    /// session's Updates; per-chat channels are unbounded (see below).
    pub async fn spawn_event_router(self: &Arc<Self>) {
        // events() is single-consumer: a second call gets a dead
        // receiver whose router exits instantly and hangs every turn.
        // An embedder that already spawned one must not start another.
        if self
            .router_started
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
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
                        // Per-chat channels are unbounded: a slow chat
                        // (rate-limited send, hung HTTP) can no longer
                        // stall this router and freeze every session's
                        // events. No slot cap is needed — memory stays
                        // finite because a demux entry lives only while
                        // that chat's turn is in flight, and a turn's
                        // Update stream is finite (bounded by the model
                        // reply, ending at PromptDone), so run_turn
                        // drains the queue.
                        // Send never blocks; failure only means the
                        // turn handler already dropped its receiver
                        // (turn ended) — those events are stale.
                        let _ = tx.send(ev);
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
        // Allowlist gate: an unlisted sender/chat must not reach the
        // agent at all — no session, no prompt, no permission reply.
        if let Some(allowed) = &*self.allowed.read() {
            let sender_ok = sender_id.as_deref().is_some_and(|s| allowed.contains(s));
            if !sender_ok && !allowed.contains(&chat_id) {
                return;
            }
        }
        // "allow"/"deny" answers a pending permission request — but only
        // from the sender who triggered it. In group chats anyone else
        // typing "allow" must not approve a tool run.
        let lower = text.trim().to_lowercase();
        if lower == "allow" || lower == "deny" {
            // Check + consume under ONE lock — splitting them let end_turn
            // remove the entry between the two and panic on unwrap.
            let mut demux = self.demux.lock().await;
            let authorized = match demux.pending_permissions.get(&chat_id) {
                None => false,
                Some((req, _)) => match (req, &sender_id) {
                    (Some(req), Some(sender)) => req == sender,
                    // An unidentified REQUESTER may only be answered by
                    // an unidentified sender (DM-style channels without
                    // identity). A named replier must never approve an
                    // anonymous requester's prompt — in a group chat
                    // that would let any member authorize the tool run.
                    (None, None) => true,
                    (None, Some(_)) | (Some(_), None) => false,
                },
            };
            if !demux.pending_permissions.contains_key(&chat_id) {
                // No pending request — fall through to prompt handling.
            } else if !authorized {
                // Pending exists but the replier isn't the requester —
                // swallow the reply; it must not become a prompt.
                return;
            } else {
                if let Some((_, tx)) = demux.pending_permissions.remove(&chat_id) {
                    let _ = tx.send(lower == "allow");
                }
                drop(demux);
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
                return;
            }
            drop(demux);
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
            // RAII cleanup: a panic inside run_turn must still free the
            // active_turns entry or this chat bricks until restart.
            struct TurnGuard<'a> {
                bridge: &'a Arc<Bridge>,
                chat_id: String,
            }
            impl Drop for TurnGuard<'_> {
                fn drop(&mut self) {
                    let bridge = self.bridge.clone();
                    let chat_id = std::mem::take(&mut self.chat_id);
                    tokio::spawn(async move {
                        bridge.active_turns.lock().await.remove(&chat_id);
                    });
                }
            }
            let _guard = TurnGuard {
                bridge: &me,
                chat_id: chat_id.clone(),
            };
            if let Err(e) = me.run_turn(&chat_id, &session_id, sender_id, &text).await {
                let _ = me.ch.send(&chat_id, &format!("error: {e:#}")).await;
            }
        });
    }

    /// Get or create the damon session for this chat. A creation failure
    /// propagates — caching a fabricated id would brick the chat forever.
    async fn session_for(&self, chat_id: &str) -> anyhow::Result<String> {
        // Fast path: an existing entry is checked, refreshed, and
        // returned with the lock held — no await under the lock.
        {
            let mut map = self.chat_sessions.lock().await;
            if let Some(e) = map.get_mut(chat_id) {
                e.last_used = std::time::Instant::now();
                return Ok(e.session_id.clone());
            }
            // Evict entries idle for over 24h so the map can't grow
            // without bound. Only the map entry goes — the daemon
            // session itself stays under the daemon's own retention
            // sweep (policy unchanged).
            map.retain(|_, e| e.last_used.elapsed() < SESSION_IDLE_EVICTION);
        }
        // Slow path: create outside the lock (new_session must not
        // hold it), then re-acquire to insert.
        let sid = self.client.new_session("", None).await?;
        let mut map = self.chat_sessions.lock().await;
        match map.get(chat_id) {
            // Double-create: another task won the race while we
            // awaited. Prefer the existing entry; delete our orphaned
            // daemon session so its history doesn't leak.
            Some(existing) => {
                let existing = existing.session_id.clone();
                drop(map);
                if let Err(e) = self.client.delete_session(&sid).await {
                    // Leaving the orphan is safe — the daemon's
                    // retention sweep collects it.
                    warn!(error = %e, session = %sid, "orphan session delete failed");
                }
                Ok(existing)
            }
            None => {
                map.insert(
                    chat_id.to_string(),
                    ChatSession {
                        session_id: sid.clone(),
                        last_used: std::time::Instant::now(),
                    },
                );
                Ok(sid)
            }
        }
    }

    /// One prompt turn for a chat: forward events to the channel until done.
    async fn run_turn(
        self: &Arc<Self>,
        chat_id: &str,
        session_id: &str,
        sender_id: Option<String>,
        text: &str,
    ) -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut session_id = session_id.to_string();
        self.demux
            .lock()
            .await
            .sessions
            .insert(session_id.clone(), tx.clone());

        // Send the prompt; the turn result arrives as PromptDone on `rx`.
        // A stale cached session (deleted via session/delete or the
        // retention sweep) reports "session not found" as a PromptDone
        // error — prompt() itself only fails on send. Drop the mapping
        // and retry once on a fresh session instead of bricking the
        // chat until restart.
        let mut buf = String::new();
        let mut retried = false;
        'turn: loop {
            {
                let client = self.client.clone();
                let sid = session_id.clone();
                let text = text.to_string();
                if let Err(e) = tokio::spawn(async move { client.prompt(&sid, &text, None).await })
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|r| r)
                {
                    self.end_turn(chat_id, &session_id).await;
                    return Err(e);
                }
            }
            loop {
                match rx.recv().await {
                    Some(ClientEvent::PromptDone { result, .. }) => {
                        // Stale session: remap and retry once on a fresh
                        // session before giving up.
                        if let Err(e) = &result
                            && !retried
                            && e["message"]
                                .as_str()
                                .unwrap_or("")
                                .contains("session not found")
                        {
                            retried = true;
                            self.chat_sessions.lock().await.remove(chat_id);
                            self.demux.lock().await.sessions.remove(&session_id);
                            match self.session_for(chat_id).await {
                                Ok(fresh) => {
                                    session_id = fresh;
                                    self.demux
                                        .lock()
                                        .await
                                        .sessions
                                        .insert(session_id.clone(), tx.clone());
                                    continue 'turn;
                                }
                                Err(e) => {
                                    self.end_turn(chat_id, &session_id).await;
                                    return Err(e);
                                }
                            }
                        }
                        self.end_turn(chat_id, &session_id).await;
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
                            let me = self.clone();
                            let cid = chat_id.to_string();
                            tokio::spawn(async move {
                                // Bound the wait by the daemon's default
                                // permission timeout — a late "allow"
                                // after the daemon already denied the
                                // call must not print ✅ for a dead ask.
                                let timeout = crate::runtime::PERMISSION_TIMEOUT;
                                let allow = match tokio::time::timeout(timeout, prx).await {
                                    Ok(a) => a.unwrap_or(false),
                                    Err(_) => {
                                        me.demux.lock().await.pending_permissions.remove(&cid);
                                        let _ = me
                                            .ch
                                            .send(&cid, "⏱ permission request timed out")
                                            .await;
                                        false
                                    }
                                };
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
                        self.end_turn(chat_id, &session_id).await;
                        anyhow::bail!("event channel closed")
                    }
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
