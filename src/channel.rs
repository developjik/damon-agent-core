//! Generic chat-channel bridge: maps channel chats to damon sessions.
//! A channel adapter supplies inbound messages and outbound delivery;
//! this bridge owns session mapping, per-conversation settings
//! (!cwd/!agent), bounded prompt queueing, event demux, permission
//! prompts (native buttons where the adapter has them, text replies
//! everywhere), and the per-turn streaming loop.

use anyhow::bail;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::client::{ClientEvent, DamonClient};

/// One inbound message from a channel chat.
pub struct Incoming {
    /// Channel-specific chat/conversation id, normalized to String.
    pub chat_id: String,
    /// Thread/topic id within the chat, when the channel supports
    /// threading (Slack thread_ts, Telegram message_thread_id). Each
    /// thread gets its own damon session.
    pub thread_id: Option<String>,
    /// Sender identity, when the channel exposes one — used to authorize
    /// "allow"/"deny" permission replies in group chats.
    pub sender_id: Option<String>,
    /// Message text with any bot mention already stripped.
    pub text: String,
    /// Attachments referenced by the message — fetched lazily by the
    /// channel at prompt time so a dropped turn never downloads them.
    pub attachments: Vec<Attachment>,
}

/// An attachment on an inbound message. `TelegramFile` is a Bot API
/// file_id resolved via getFile + download; `Bytes` is already-fetched
/// content for channels that inline media; `Url` is a URL the channel
/// must fetch itself — Slack file shares hand out `url_private`, which
/// only serves bytes to a request bearing the bot token (`bearer`;
/// None means the fetching adapter supplies its own credentials).
pub enum Attachment {
    TelegramFile { file_id: String },
    Bytes { data: Vec<u8>, mime: String },
    Url { url: String, bearer: Option<String> },
}

/// One outbound media item: decoded bytes plus the metadata channel
/// upload APIs need to name and type the file.
pub struct MediaOut {
    pub data: Vec<u8>,
    pub mime: String,
    pub filename: String,
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
    /// Deliver text into a thread. Default falls back to `send` —
    /// channels without threading still deliver the message.
    async fn send_in_thread(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        text: &str,
    ) -> anyhow::Result<()> {
        let _ = thread_id;
        self.send(chat_id, text).await
    }
    /// Show a typing/working indicator. Default no-op — channels
    /// without the concept (Slack bots) skip it.
    async fn typing(&self, _chat_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
    /// Buffered chunks are flushed early once they exceed this size,
    /// so `send` stays under the channel cap.
    fn flush_threshold(&self) -> usize {
        3500
    }
    /// Deliver a permission prompt. `actions` is the agent's offered
    /// list ({behavior, id, label}); adapters with native buttons
    /// render them, and button presses must flow back through the
    /// bridge's text path as a synthesized "allow"/"deny"/"always"
    /// reply from the presser — the identity check and single
    /// pending-lane semantics then apply unchanged. The default sends
    /// the plain-text "🔐 title\nhint" message every channel supports.
    async fn send_permission(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        title: &str,
        actions: &[Value],
    ) -> anyhow::Result<()> {
        self.send_in_thread(chat_id, thread_id, &permission_prompt_text(title, actions))
            .await
    }
    /// Deliver media (decoded images/files) to a chat. The default
    /// degrades to a text note — a channel without uploads must still
    /// say something, or a dropped image looks like a lost reply.
    async fn send_media(
        &self,
        chat_id: &str,
        thread_id: Option<&str>,
        caption: Option<&str>,
        items: &[MediaOut],
    ) -> anyhow::Result<()> {
        let mut note = format!("[{} media attachment(s)]", items.len());
        if let Some(c) = caption {
            note = format!("{c}\n{note}");
        }
        self.send_in_thread(chat_id, thread_id, &note).await
    }
    /// Resolve an attachment to bytes. Channels that never produce
    /// attachments keep the default (error) implementation.
    async fn fetch(&self, _att: &Attachment) -> anyhow::Result<(Vec<u8>, String)> {
        anyhow::bail!("this channel does not support attachments")
    }
}

/// Routes daemon events to the chat that owns the session.
struct Demux {
    /// session_id -> channel to that chat's turn handler
    sessions: HashMap<String, mpsc::UnboundedSender<ClientEvent>>,
    /// conv_id -> (requester sender_id, answer channel). The sender id
    /// binds the reply to whoever triggered the tool call. The answer
    /// is the reply word ("allow", "always", "deny") the waiter maps
    /// onto the request's offered actions.
    pending_permissions: HashMap<String, (Option<String>, oneshot::Sender<String>)>,
    /// conv_id -> a permission ask on a WATCHED session (no local turn
    /// owns it). The chat answers it with allow/deny/always exactly
    /// like a live turn's prompt; the reply goes straight to
    /// `permission.respond` instead of a waiting turn waiter.
    watched_asks: HashMap<String, WatchedAsk>,
}

/// One pending watched-session permission ask — see `Demux`.
#[derive(Clone)]
struct WatchedAsk {
    session_id: String,
    request_id: String,
    actions: Vec<Value>,
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

/// Reserved channel_state conv id holding the global watch registry.
/// Real conv ids are numeric/alphanumeric channel ids, never this.
const WATCH_STATE_CONV: &str = "__bridge_watches";
/// Watches per conversation and in total — a runaway registry must not
/// grow the event fanout without bound.
const MAX_WATCHES_PER_CONV: usize = 8;
const MAX_WATCHES_TOTAL: usize = 64;

/// One watch subscription: this conversation follows a session that
/// may be driven by ANY surface (web UI, another chat, CLI) and gets
/// pinged when it finishes a turn or asks for permission. `chat_id`/
/// `thread_id` carry the delivery address so the router never has to
/// parse the conv key apart.
#[derive(Clone)]
struct Watch {
    session_id: String,
    /// Cached display title ("" → the short id is shown).
    title: String,
    backend: String,
    chat_id: String,
    thread_id: Option<String>,
}

/// Per-conversation cap on prompts waiting behind a running turn.
/// Past this the sender is told to retry — an unbounded queue would
/// let one slow turn accumulate unbounded memory (each prompt holds
/// attachment metadata) and dump a wall of stale turns when it ends.
const MAX_QUEUED_PROMPTS: usize = 3;

/// Per-conversation chat settings (!cwd / !agent). In-memory by
/// design: it pairs with `chat_sessions`, which is also in-memory, so
/// a daemon or bridge restart resets conversations to the configured
/// defaults — the sessions themselves survive daemon-side either way.
#[derive(Default, Clone, PartialEq)]
struct ChatPrefs {
    /// Backend for the conversation's NEXT session (None = the
    /// daemon's default_backend). Fixed per session at create time.
    backend: Option<String>,
    /// Working directory for the conversation's NEXT session (None =
    /// the bridge process's cwd). Fixed per session at create time.
    cwd: Option<std::path::PathBuf>,
}

/// One prompt waiting behind a running turn, fully owned — the drain
/// path re-spawns it through the same turn pipeline without borrowing
/// the receive loop's frame.
struct QueuedPrompt {
    conv_id: String,
    chat_id: String,
    thread_id: Option<String>,
    sender_id: Option<String>,
    text: String,
    attachments: Vec<Attachment>,
}

/// Per-conversation turn lane: `live` serializes turns, `queue` holds
/// prompts that arrived mid-turn. `live` is set under the turns lock
/// by whoever starts a turn and cleared exactly once by that turn's
/// TurnGuard; the drain (pop next + mark live again) happens in the
/// same critical section, so a finished turn and an arriving prompt
/// can neither lose nor double-drain the queue.
#[derive(Default)]
struct TurnLane {
    live: bool,
    queue: std::collections::VecDeque<QueuedPrompt>,
}

pub struct Bridge {
    ch: Arc<dyn ChannelApi>,
    client: DamonClient,
    /// chat_id -> session_id
    chat_sessions: Mutex<HashMap<String, ChatSession>>,
    /// conv_id -> turn lane — one live turn per conversation plus its
    /// bounded queue of waiting prompts.
    turns: Mutex<HashMap<String, TurnLane>>,
    /// conv_id -> !cwd/!agent settings consulted at session create.
    chat_prefs: Mutex<HashMap<String, ChatPrefs>>,
    /// conv_id -> watched sessions (!watch / !resume auto-watch).
    /// Cross-surface pickup: when a watched session finishes a turn or
    /// asks for permission — on any surface — the chat is pinged.
    watches: Mutex<HashMap<String, Vec<Watch>>>,
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
            turns: Mutex::new(HashMap::new()),
            chat_prefs: Mutex::new(HashMap::new()),
            watches: Mutex::new(HashMap::new()),
            allowed: parking_lot::RwLock::new(None),
            demux: Mutex::new(Demux {
                sessions: HashMap::new(),
                pending_permissions: HashMap::new(),
                watched_asks: HashMap::new(),
            }),
            router_started: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

impl Bridge {
    /// Persisted conversation state (daemon-side `channel_state`
    /// rows): the `!cwd`/`!agent` prefs and the chat→session mapping
    /// survive bridge restarts. Persistence failures only warn — the
    /// in-memory view stays authoritative for this run.
    async fn state_get(&self, conv_id: &str, key: &str) -> Option<String> {
        match self
            .client
            .request("channel.get_state", json!({"convId": conv_id, "key": key}))
            .await
        {
            Ok(v) => v["value"].as_str().map(String::from),
            Err(e) => {
                warn!(error = %e, conv = conv_id, key, "channel state load failed");
                None
            }
        }
    }

    async fn state_set(&self, conv_id: &str, key: &str, value: &str) {
        if let Err(e) = self
            .client
            .request(
                "channel.set_state",
                json!({"convId": conv_id, "key": key, "value": value}),
            )
            .await
        {
            warn!(error = %e, conv = conv_id, key, "channel state save failed");
        }
    }

    async fn state_del(&self, conv_id: &str, key: &str) {
        if let Err(e) = self
            .client
            .request(
                "channel.delete_state",
                json!({"convId": conv_id, "key": key}),
            )
            .await
        {
            warn!(error = %e, conv = conv_id, key, "channel state delete failed");
        }
    }

    fn prefs_json(p: &ChatPrefs) -> String {
        json!({
            "backend": p.backend,
            "cwd": p.cwd.as_ref().map(|c| c.to_string_lossy()),
        })
        .to_string()
    }

    fn prefs_parse(raw: &str) -> ChatPrefs {
        let v: serde_json::Value = serde_json::from_str(raw).unwrap_or_default();
        ChatPrefs {
            backend: v["backend"].as_str().map(String::from),
            cwd: v["cwd"].as_str().map(std::path::PathBuf::from),
        }
    }

    // --- Cross-surface watch registry ----------------------------------
    // Watches live in ONE daemon-side channel_state row (the reserved
    // WATCH_STATE_CONV conv) so a bridge restart restores them wholesale
    // — per-conversation rows would need a conv enumeration RPC that
    // does not exist.

    fn watches_json(map: &HashMap<String, Vec<Watch>>) -> String {
        json!(
            map.iter()
                .map(|(conv, ws)| {
                    (
                        conv.clone(),
                        ws.iter()
                            .map(|w| {
                                json!({"session": w.session_id, "title": w.title,
                                   "backend": w.backend, "chat": w.chat_id,
                                   "thread": w.thread_id})
                            })
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<HashMap<_, _>>()
        )
        .to_string()
    }

    fn watches_parse(raw: &str) -> HashMap<String, Vec<Watch>> {
        serde_json::from_str::<HashMap<String, Vec<serde_json::Value>>>(raw)
            .unwrap_or_default()
            .into_iter()
            .map(|(conv, ws)| {
                (
                    conv,
                    ws.iter()
                        .filter_map(|w| {
                            Some(Watch {
                                session_id: w["session"].as_str()?.to_string(),
                                title: w["title"].as_str().unwrap_or("").to_string(),
                                backend: w["backend"].as_str().unwrap_or("").to_string(),
                                chat_id: w["chat"].as_str().unwrap_or("").to_string(),
                                thread_id: w["thread"].as_str().map(String::from),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, ws)| !ws.is_empty())
            .collect()
    }

    async fn persist_watches(&self) {
        let map = self.watches.lock().await.clone();
        self.state_set(WATCH_STATE_CONV, "registry", &Self::watches_json(&map))
            .await;
    }

    /// Load the watch registry from daemon state and re-subscribe every
    /// watched session on this connection. Called once at bridge start;
    /// after each reconnect [`Self::resubscribe_watches`] re-issues the
    /// daemon-side subscriptions from the in-memory registry.
    pub async fn restore_watches(&self) {
        if let Some(raw) = self.state_get(WATCH_STATE_CONV, "registry").await {
            let map = Self::watches_parse(&raw);
            if !map.is_empty() {
                *self.watches.lock().await = map;
            }
        }
        self.resubscribe_watches().await;
    }

    /// Re-issue `session.watch` for every watched session on the
    /// current connection — daemon-side subscriptions are per-connection
    /// and vanish with it (reconnect, bridge restart). A session that
    /// is no longer live warns and is skipped; its watch entry stays in
    /// case the session comes back and is watched again.
    async fn resubscribe_watches(&self) {
        let sids: std::collections::HashSet<String> = {
            let map = self.watches.lock().await;
            map.values()
                .flatten()
                .map(|w| w.session_id.clone())
                .collect()
        };
        for sid in sids {
            if let Err(e) = self.client.watch_session(&sid).await {
                warn!(error = %e, session = %sid, "watch resubscribe failed");
            }
        }
    }
    /// Restrict the bridge to these sender/chat ids. Checked before any
    /// session or prompt work — an unlisted sender is dropped silently.
    pub fn set_allowed(&self, ids: impl IntoIterator<Item = String>) {
        *self.allowed.write() = Some(ids.into_iter().collect());
    }

    /// Main loop: channel setup, then dispatch inbound messages forever.
    pub async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        self.ch.ready().await?;
        // `hello` negotiates the protocol; the connect-time hello push
        // already fed `client.permission_timeout()`.
        self.client.hello().await?;
        // Restore cross-surface watches before the router starts so no
        // notification falls into an unpopulated registry.
        self.restore_watches().await;
        self.spawn_event_router().await;
        loop {
            match self.ch.recv().await {
                Ok(Some(msg)) => {
                    self.handle_message(
                        msg.chat_id,
                        msg.thread_id,
                        msg.sender_id,
                        msg.text,
                        msg.attachments,
                    )
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

    /// Single consumer of client.events(); fans out session.event
    /// pushes to per-session channels. Per-chat channels are unbounded
    /// (see below).
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
                // A reconnect replaced the connection — daemon-side watch
                // subscriptions died with it. Re-issue them from the
                // in-memory registry so cross-surface pings keep flowing.
                if matches!(ev, ClientEvent::Connected) {
                    me.resubscribe_watches().await;
                    continue;
                }
                // session.event pushes and TurnDone carry a session id —
                // connection-state mirrors route nowhere.
                let session_id = match &ev {
                    ClientEvent::Event { session_id, .. }
                    | ClientEvent::TurnDone { session_id, .. } => Some(session_id.clone()),
                    _ => None,
                };
                if let Some(sid) = session_id {
                    let tx = me.demux.lock().await.sessions.get(&sid).cloned();
                    if let Some(tx) = tx {
                        // Per-chat channels are unbounded: a slow chat
                        // (rate-limited send, hung HTTP) can no longer
                        // stall this router and freeze every session's
                        // Send never blocks; failure only means the
                        // turn handler already dropped its receiver
                        // (turn ended) — those events are stale.
                        let _ = tx.send(ev);
                    } else if let ClientEvent::Event {
                        event,
                        replay: false,
                        ..
                    } = &ev
                    {
                        // No local turn owns this session — its
                        // milestones may still matter to a conversation
                        // watching it from another surface.
                        me.notify_watchers(&sid, event).await;
                    }
                }
            }
        });
    }
    /// Handle one inbound message: permission reply, `!` command, or a
    /// new prompt turn. `attachments` are fetched lazily inside the turn.
    /// `thread_id` scopes the conversation: each thread gets its own
    /// session, turn slot, and permission lane.
    pub async fn handle_message(
        self: &Arc<Self>,
        chat_id: String,
        thread_id: Option<String>,
        sender_id: Option<String>,
        text: String,
        attachments: Vec<Attachment>,
    ) {
        // Conversation key: chat + thread. A Slack thread or Telegram
        // topic is its own session lane; unthreaded messages share the
        // bare chat_id as before.
        let conv_id = match &thread_id {
            Some(t) => format!("{chat_id}#{t}"),
            None => chat_id.clone(),
        };
        // Allowlist gate: an unlisted sender/chat must not reach the
        // agent at all — no session, no prompt, no permission reply.
        if let Some(allowed) = &*self.allowed.read() {
            let sender_ok = sender_id.as_deref().is_some_and(|s| allowed.contains(s));
            if !sender_ok && !allowed.contains(&chat_id) {
                return;
            }
        }
        // "allow"/"deny"/"always" answers a pending permission request —
        // but only from the sender who triggered it. In group chats
        // anyone else typing "allow" must not approve a tool run.
        let lower = text.trim().to_lowercase();
        if lower == "allow" || lower == "deny" || lower == "always" {
            // Check + consume under ONE lock — splitting them let end_turn
            // remove the entry between the two and panic on unwrap.
            let mut demux = self.demux.lock().await;
            let authorized = match demux.pending_permissions.get(&conv_id) {
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
            if !demux.pending_permissions.contains_key(&conv_id) {
                // No live-turn ask for this conversation — but a WATCHED
                // session's ask can be pending. Any allowlisted member
                // may answer it (no requester identity is bound; the
                // allowlist gate already filtered the sender). The reply
                // goes straight to permission.respond — no turn waiter
                // exists on this side.
                if let Some(ask) = demux.watched_asks.get(&conv_id).cloned() {
                    demux.watched_asks.remove(&conv_id);
                    drop(demux);
                    let response = permission_response_for(&lower, &ask.actions);
                    match self
                        .client
                        .respond_to_permission(&ask.session_id, &ask.request_id, response)
                        .await
                    {
                        Ok(()) => {
                            self.deliver(
                                &chat_id,
                                thread_id.as_deref(),
                                match lower.as_str() {
                                    "allow" | "always" => "✅ allowed",
                                    _ => "🚫 denied",
                                },
                            )
                            .await;
                        }
                        Err(e) => {
                            // The daemon's own permission timeout (or
                            // another surface) resolved it first.
                            self.deliver(
                                &chat_id,
                                thread_id.as_deref(),
                                &format!("⏱ permission already resolved ({e:#})"),
                            )
                            .await;
                        }
                    }
                    return;
                }
                // No pending request — fall through to prompt handling.
            } else if !authorized {
                // Pending exists but the replier isn't the requester —
                // swallow the reply; it must not become a prompt.
                return;
            } else {
                if let Some((_, tx)) = demux.pending_permissions.remove(&conv_id) {
                    // The waiter maps the reply word onto the request's
                    // offered actions (allow/deny/always-scoped allow).
                    let _ = tx.send(lower.clone());
                }
                drop(demux);
                self.deliver(
                    &chat_id,
                    thread_id.as_deref(),
                    match lower.as_str() {
                        "allow" => "✅ allowed",
                        "always" => "✅ always allowed",
                        _ => "🚫 denied",
                    },
                )
                .await;
                return;
            }
            drop(demux);
        }

        // `!` commands are chat-local control messages — they never
        // reach the model and never occupy a turn slot.
        if let Some(cmd) = text.trim().strip_prefix('!') {
            self.handle_command(&conv_id, &chat_id, thread_id.as_deref(), cmd.trim())
                .await;
            return;
        }

        // One live turn per conversation — a second prompt would
        // overwrite the demux entry and starve the first turn's
        // events. Instead of rejecting it, queue it (bounded) behind
        // the running turn; the drain hands the lane over when the
        // turn ends.
        let prompt = QueuedPrompt {
            conv_id: conv_id.clone(),
            chat_id: chat_id.clone(),
            thread_id: thread_id.clone(),
            sender_id,
            text,
            attachments,
        };
        enum LaneAction {
            Start(QueuedPrompt),
            Queued(usize),
            Full,
        }
        let action = {
            let mut lanes = self.turns.lock().await;
            let lane = lanes.entry(conv_id.clone()).or_default();
            if !lane.live {
                lane.live = true;
                LaneAction::Start(prompt)
            } else if lane.queue.len() < MAX_QUEUED_PROMPTS {
                lane.queue.push_back(prompt);
                LaneAction::Queued(lane.queue.len())
            } else {
                LaneAction::Full
            }
        };
        // Replies happen outside the lock — holding it across a
        // channel send would serialize every conversation on one mutex.
        match action {
            LaneAction::Start(p) => self.start_turn(p),
            LaneAction::Queued(n) => {
                self.deliver(
                    &chat_id,
                    thread_id.as_deref(),
                    &format!("queued — position {n}"),
                )
                .await;
            }
            LaneAction::Full => {
                self.deliver(
                    &chat_id,
                    thread_id.as_deref(),
                    "queue full — try again after the running turn",
                )
                .await;
            }
        }
    }

    /// Spawn the turn pipeline for an owned prompt (fresh or drained
    /// from a lane's queue). The caller has already marked the lane
    /// live under the turns lock; this task owns marking it idle again
    /// via its guard, whose Drop fires exactly once whether run_turn
    /// returns Ok, returns Err, or the task panics.
    fn start_turn(self: &Arc<Self>, p: QueuedPrompt) {
        let me = self.clone();
        tokio::spawn(async move {
            // RAII cleanup: a panic inside the turn must still free
            // the lane and drain its queue, or this chat bricks until
            // restart.
            struct TurnGuard<'a> {
                bridge: &'a Arc<Bridge>,
                conv_id: String,
            }
            impl Drop for TurnGuard<'_> {
                fn drop(&mut self) {
                    let bridge = self.bridge.clone();
                    let conv_id = std::mem::take(&mut self.conv_id);
                    tokio::spawn(async move {
                        bridge.turn_finished(&conv_id).await;
                    });
                }
            }
            let QueuedPrompt {
                conv_id,
                chat_id,
                thread_id,
                sender_id,
                text,
                attachments,
            } = p;
            // Guard before session_for: a creation failure must also
            // drain the queue, or prompts wait on a lane nobody holds.
            let _guard = TurnGuard {
                bridge: &me,
                conv_id: conv_id.clone(),
            };
            let session_id = match me.session_for(&conv_id).await {
                Ok(s) => s,
                Err(e) => {
                    me.deliver(&chat_id, thread_id.as_deref(), &format!("error: {e:#}"))
                        .await;
                    return;
                }
            };
            if let Err(e) = me
                .run_turn(TurnCtx {
                    chat_id: &chat_id,
                    thread_id: thread_id.as_deref(),
                    conv_id: &conv_id,
                    session_id: &session_id,
                    sender_id,
                    text: &text,
                    attachments,
                })
                .await
            {
                me.deliver(&chat_id, thread_id.as_deref(), &format!("error: {e:#}"))
                    .await;
            }
        });
    }

    /// A turn ended (completed, errored, or panicked): free the lane
    /// and hand it to the next queued prompt, if any. Pop-and-relive
    /// happens under one lock — a prompt arriving between "turn done"
    /// and "next starts" observes a live lane and queues, so a
    /// handoff can never be lost or doubled.
    async fn turn_finished(self: &Arc<Self>, conv_id: &str) {
        let next = {
            let mut lanes = self.turns.lock().await;
            let next = lanes
                .get_mut(conv_id)
                .and_then(|lane| lane.queue.pop_front());
            if next.is_some() {
                lanes.get_mut(conv_id).unwrap().live = true;
                next
            } else {
                // Nothing waiting: drop the lane itself — an absent
                // entry means "not live", keeping the map bounded by
                // conversations with pending work.
                lanes.remove(conv_id);
                None
            }
        };
        if let Some(p) = next {
            self.start_turn(p);
        }
    }
    /// `!`-prefixed chat commands. Unknown commands get a hint, not a
    /// prompt — a typo'd command must not silently reach the model.
    async fn handle_command(
        self: &Arc<Self>,
        conv_id: &str,
        chat_id: &str,
        thread_id: Option<&str>,
        cmd: &str,
    ) {
        let (verb, arg) = match cmd.split_once(char::is_whitespace) {
            Some((v, a)) => (v, a.trim()),
            None => (cmd, ""),
        };
        let reply = match verb {
            "new" => {
                self.chat_sessions.lock().await.remove(conv_id);
                self.state_del(conv_id, "session").await;
                Some("session reset — next message starts a fresh session".to_string())
            }
            "delete" => {
                self.state_del(conv_id, "session").await;
                let sid = self
                    .chat_sessions
                    .lock()
                    .await
                    .remove(conv_id)
                    .map(|e| e.session_id);
                match sid {
                    Some(sid) => match self.client.delete_session(&sid).await {
                        Ok(()) => {
                            // A watched session is gone — its pings would
                            // never fire again; drop the stale entry.
                            self.watch_remove(conv_id, &sid).await;
                            Some(format!("deleted session {sid}"))
                        }
                        Err(e) => Some(format!("delete failed: {e:#}")),
                    },
                    None => Some("no session to delete".to_string()),
                }
            }
            "cancel" => {
                // Clear the queue FIRST — cancel must also drop
                // prompts waiting behind the turn, or they would run
                // right after the cancellation the user just asked
                // for. The live flag stays: the running turn owns it
                // until its guard drains.
                let cleared = {
                    let mut lanes = self.turns.lock().await;
                    lanes
                        .get_mut(conv_id)
                        .map(|lane| std::mem::take(&mut lane.queue).len())
                        .unwrap_or(0)
                };
                let sid = self
                    .chat_sessions
                    .lock()
                    .await
                    .get(conv_id)
                    .map(|e| e.session_id.clone());
                let base = match sid {
                    Some(sid) => match self.client.turn_cancel(&sid).await {
                        Ok(()) => "cancel requested".to_string(),
                        Err(e) => format!("cancel failed: {e:#}"),
                    },
                    None => "no session to cancel".to_string(),
                };
                Some(if cleared > 0 {
                    format!("{base} — dropped {cleared} queued prompt(s)")
                } else {
                    base
                })
            }
            // Working directory for this conversation's next session.
            // Absolute + must exist: a typo'd or relative path would
            // otherwise surface only later, as a session created in a
            // surprising place.
            "cwd" => {
                let pref = self
                    .chat_prefs
                    .lock()
                    .await
                    .get(conv_id)
                    .cloned()
                    .unwrap_or_default();
                if arg.is_empty() {
                    let eff = pref
                        .cwd
                        .map(|p| p.to_string_lossy().into_owned())
                        .or_else(|| {
                            std::env::current_dir()
                                .ok()
                                .map(|p| p.to_string_lossy().into_owned())
                        })
                        .unwrap_or_else(|| ".".to_string());
                    Some(format!("cwd: {eff} — applies to the next new session"))
                } else {
                    let path = std::path::PathBuf::from(arg);
                    if !path.is_absolute() {
                        Some(format!("cwd must be an absolute path: {arg}"))
                    } else if !path.is_dir() {
                        Some(format!("not a directory: {}", path.display()))
                    } else {
                        let prefs = {
                            let mut map = self.chat_prefs.lock().await;
                            let e = map.entry(conv_id.to_string()).or_default();
                            e.cwd = Some(path);
                            e.clone()
                        };
                        self.state_set(conv_id, "prefs", &Self::prefs_json(&prefs))
                            .await;
                        Some(
                            "cwd set — it applies to the next new session (!new starts one now)"
                                .to_string(),
                        )
                    }
                }
            }
            // Backend picker: backend is fixed per session at create
            // time, so setting it also drops the session mapping —
            // same reset semantics as !new.
            "agent" => match self.client.request("backend.list", json!({})).await {
                Err(e) => Some(format!("error: {e:#}")),
                Ok(v) => {
                    let backends: Vec<(String, bool)> = v["backends"]
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .map(|b| {
                                    (
                                        b["id"].as_str().unwrap_or("?").to_string(),
                                        b["available"].as_bool().unwrap_or(false),
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    let names = || {
                        let ids: Vec<&str> = backends.iter().map(|(id, _)| id.as_str()).collect();
                        if ids.is_empty() {
                            "(none)".to_string()
                        } else {
                            ids.join(", ")
                        }
                    };
                    if arg.is_empty() {
                        let pref = self
                            .chat_prefs
                            .lock()
                            .await
                            .get(conv_id)
                            .cloned()
                            .unwrap_or_default();
                        let eff = pref.backend.unwrap_or_else(|| {
                            // No pref: whatever the daemon creates by
                            // default — surface it without a second RPC.
                            "daemon default".to_string()
                        });
                        let mut shown: Vec<String> = backends
                            .iter()
                            .map(|(id, av)| {
                                if *av {
                                    id.clone()
                                } else {
                                    format!("{id} (unavailable)")
                                }
                            })
                            .collect();
                        if shown.is_empty() {
                            shown.push("(none)".to_string());
                        }
                        Some(format!("backends: {}\neffective: {eff}", shown.join(", ")))
                    } else {
                        match backends.iter().find(|(id, _)| id == arg) {
                            None => {
                                Some(format!("unknown backend '{arg}' — available: {}", names()))
                            }
                            Some((_, false)) => Some(format!("backend '{arg}' is not available")),
                            Some((id, true)) => {
                                let prefs = {
                                    let mut map = self.chat_prefs.lock().await;
                                    let e = map.entry(conv_id.to_string()).or_default();
                                    e.backend = Some(id.clone());
                                    e.clone()
                                };
                                self.state_set(conv_id, "prefs", &Self::prefs_json(&prefs))
                                    .await;
                                self.state_del(conv_id, "session").await;
                                self.chat_sessions.lock().await.remove(conv_id);
                                Some(format!(
                                    "backend set to {id} — the next message starts a \
                                     fresh session there"
                                ))
                            }
                        }
                    }
                }
            },
            "usage" => {
                let sid = self
                    .chat_sessions
                    .lock()
                    .await
                    .get(conv_id)
                    .map(|e| e.session_id.clone());
                match self.client.usage(sid.as_deref()).await {
                    Ok(v) => {
                        let fmt = |used: u64, size: u64, cost: f64, turns: u64| {
                            let ctx = if size > 0 {
                                format!("{used}/{size} ctx")
                            } else {
                                format!("{used} ctx")
                            };
                            let cost_s = if cost > 0.0 {
                                format!(" / ${cost:.4}")
                            } else {
                                String::new()
                            };
                            format!(
                                "{ctx}{cost_s} / {turns} {}",
                                if turns == 1 { "turn" } else { "turns" }
                            )
                        };
                        // No sessionId: the daemon returns a per-model
                        // rollup — rows are [model, used, size, cost, turns].
                        if let Some(rows) = v["sessions"].as_array() {
                            let mut lines = vec!["usage:".to_string()];
                            for r in rows {
                                lines.push(format!(
                                    "  {} — {}",
                                    r[0].as_str().unwrap_or("?"),
                                    fmt(
                                        r[1].as_u64().unwrap_or(0),
                                        r[2].as_u64().unwrap_or(0),
                                        r[3].as_f64().unwrap_or(0.0),
                                        r[4].as_u64().unwrap_or(0),
                                    ),
                                ));
                            }
                            Some(lines.join("\n"))
                        } else {
                            Some(format!(
                                "this session: {}",
                                fmt(
                                    v["contextUsed"].as_u64().unwrap_or(0),
                                    v["contextSize"].as_u64().unwrap_or(0),
                                    v["costUsd"].as_f64().unwrap_or(0.0),
                                    v["turns"].as_u64().unwrap_or(0),
                                )
                            ))
                        }
                    }
                    Err(e) => Some(format!("error: {e:#}")),
                }
            }
            "fork" => {
                let sid = self
                    .chat_sessions
                    .lock()
                    .await
                    .get(conv_id)
                    .map(|e| e.session_id.clone());
                match sid {
                    Some(sid) => match self.client.fork_session(&sid, None).await {
                        Ok(new_id) => {
                            self.chat_sessions.lock().await.insert(
                                conv_id.to_string(),
                                ChatSession {
                                    session_id: new_id.clone(),
                                    last_used: std::time::Instant::now(),
                                },
                            );
                            self.state_set(conv_id, "session", &new_id).await;
                            Some(format!("forked → {new_id} — next message continues there"))
                        }
                        Err(e) => Some(format!("fork failed: {e:#}")),
                    },
                    None => Some("no session to fork — send a message first".to_string()),
                }
            }
            // Recent sessions across every surface — the pickup point
            // for continuing desk work from the phone.
            "sessions" => match self.client.list_sessions(Some(8), None).await {
                Err(e) => Some(format!("error: {e:#}")),
                Ok(rows) if rows.is_empty() => {
                    Some("no sessions yet — send a message first".to_string())
                }
                Ok(rows) => {
                    let current = self
                        .chat_sessions
                        .lock()
                        .await
                        .get(conv_id)
                        .map(|e| e.session_id.clone());
                    let mut lines = vec!["sessions:".to_string()];
                    for (id, created, backend, title) in &rows {
                        let mark = if current.as_deref() == Some(id.as_str()) {
                            "▶"
                        } else {
                            "·"
                        };
                        let label = if title.is_empty() {
                            "(untitled)"
                        } else {
                            title
                        };
                        lines.push(format!(
                            "{mark} {label} — {} · {} · {}",
                            if backend.is_empty() { "?" } else { backend },
                            created.get(..16).unwrap_or(created),
                            Self::short_id(id),
                        ));
                    }
                    lines.push("→ !resume <id prefix or title> continues one here".to_string());
                    Some(lines.join("\n"))
                }
            },
            // Adopt an existing session (started on any surface) into
            // this conversation: bring it live, bind the chat mapping,
            // and auto-watch it so completions and permission asks ping
            // this chat even when the turn runs elsewhere.
            "resume" => {
                if arg.is_empty() {
                    let cur = self
                        .chat_sessions
                        .lock()
                        .await
                        .get(conv_id)
                        .map(|e| e.session_id.clone());
                    Some(match cur {
                        Some(sid) => format!(
                            "current: {} — usage: !resume <id prefix from !sessions>",
                            Self::short_id(&sid)
                        ),
                        None => "usage: !resume <id prefix or title from !sessions>".to_string(),
                    })
                } else {
                    match self.resolve_session(arg).await {
                        Err(msg) => Some(msg),
                        Ok((sid, title, backend)) => {
                            let already = self
                                .chat_sessions
                                .lock()
                                .await
                                .get(conv_id)
                                .map(|e| e.session_id.clone())
                                .is_some_and(|c| c == sid);
                            if already {
                                Some("already attached to this session".to_string())
                            } else if let Err(e) = self.client.resume_session(&sid).await {
                                Some(format!("resume failed: {e:#}"))
                            } else {
                                self.chat_sessions.lock().await.insert(
                                    conv_id.to_string(),
                                    ChatSession {
                                        session_id: sid.clone(),
                                        last_used: std::time::Instant::now(),
                                    },
                                );
                                self.state_set(conv_id, "session", &sid).await;
                                let note = self
                                    .watch_add(conv_id, &sid, &title, &backend, chat_id, thread_id)
                                    .await;
                                Some(format!(
                                    "attached: {} [{backend}] — next message continues \
                                     there.{note}",
                                    Self::watch_label(&sid, &title)
                                ))
                            }
                        }
                    }
                }
            }
            // Follow a live session without adopting it — completion
            // and permission pings land here; prompts stay wherever
            // they started.
            "watch" => {
                if arg.is_empty() {
                    let listed = Self::watches_list(&*self.watches.lock().await, conv_id);
                    Some(listed)
                } else {
                    match self.resolve_session(arg).await {
                        Err(msg) => Some(msg),
                        Ok((sid, title, backend)) => {
                            let note = self
                                .watch_add(conv_id, &sid, &title, &backend, chat_id, thread_id)
                                .await;
                            let label = Self::watch_label(&sid, &title);
                            if note.is_empty() {
                                Some(format!(
                                    "watching {label} — I'll ping this chat when it finishes \
                                     a turn or asks for permission"
                                ))
                            } else {
                                Some(format!("not watching {label}:{note}"))
                            }
                        }
                    }
                }
            }
            "unwatch" => {
                if arg.is_empty() {
                    let listed = Self::watches_list(&*self.watches.lock().await, conv_id);
                    Some(format!("{listed}\n→ !unwatch <prefix|all>"))
                } else if arg == "all" {
                    let removed = self
                        .watches
                        .lock()
                        .await
                        .remove(conv_id)
                        .map_or(0, |v| v.len());
                    if removed > 0 {
                        self.persist_watches().await;
                    }
                    Some(format!("stopped following {removed} session(s)"))
                } else {
                    let candidates: Vec<(String, String)> = {
                        let map = self.watches.lock().await;
                        map.get(conv_id)
                            .map(|ws| {
                                ws.iter()
                                    .filter(|w| {
                                        w.session_id.starts_with(arg)
                                            || (!w.title.is_empty()
                                                && w.title
                                                    .to_lowercase()
                                                    .contains(&arg.to_lowercase()))
                                    })
                                    .map(|w| (w.session_id.clone(), w.title.clone()))
                                    .collect()
                            })
                            .unwrap_or_default()
                    };
                    match candidates.len() {
                        0 => Some(format!("not following a session matching '{arg}'")),
                        1 => {
                            let (sid, title) = candidates.into_iter().next().unwrap();
                            self.watch_remove(conv_id, &sid).await;
                            let _ = self.client.unwatch_session(&sid).await;
                            Some(format!(
                                "stopped following {}",
                                Self::watch_label(&sid, &title)
                            ))
                        }
                        _ => Some(format!(
                            "ambiguous — be more specific: {}",
                            candidates
                                .iter()
                                .map(|(sid, title)| Self::watch_label(sid, title))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )),
                    }
                }
            }
            "help" => Some(
                "commands: !new (reset session) · !agent [name] (show/set backend) · \
                 !cwd [path] (show/set working dir) · !fork (branch session) · \
                 !delete (remove session) · !cancel (stop the running turn and \
                 clear its queue) · !usage (token totals) · \
                 !sessions (recent sessions) · !resume <id|title> (continue one \
                 here, notifications on) · !watch/!unwatch <id|title> (follow a \
                 session's finishes and permission asks) · \
                 allow/deny/always (answer a permission prompt)"
                    .to_string(),
            ),
            _ => Some(format!("unknown command '!{verb}' — try !help")),
        };
        if let Some(text) = reply {
            self.deliver(chat_id, thread_id, &text).await;
        }
    }

    /// Get or create the damon session for this chat. A creation failure
    /// propagates — caching a fabricated id would brick the chat forever.
    async fn session_for(&self, chat_id: &str) -> anyhow::Result<String> {
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
        // A bridge restart (or the 24h cache eviction) loses only the
        // in-memory map — the daemon-side channel_state row remembers
        // the conversation's session. Reattach before creating: a stale
        // id self-heals through the not-live retry path.
        if let Some(sid) = self.state_get(chat_id, "session").await {
            let mut map = self.chat_sessions.lock().await;
            map.retain(|_, e| e.last_used.elapsed() < SESSION_IDLE_EVICTION);
            map.insert(
                chat_id.to_string(),
                ChatSession {
                    session_id: sid,
                    last_used: std::time::Instant::now(),
                },
            );
            // Re-run the fast path with the restored entry.
            if let Some(e) = map.get_mut(chat_id) {
                e.last_used = std::time::Instant::now();
                return Ok(e.session_id.clone());
            }
        }
        // Slow path: create outside the lock (create_session must not
        // hold it), then re-acquire to insert. Conversation prefs (!cwd /
        // !agent) pick the backend and cwd — both are fixed at create
        // time, which is why setting them resets the session mapping.
        // Cache first, then the persisted copy — this run's !cwd/!agent
        // always wins over what a previous run stored.
        let pref = {
            let cached = self
                .chat_prefs
                .lock()
                .await
                .get(chat_id)
                .cloned()
                .unwrap_or_default();
            match self.state_get(chat_id, "prefs").await {
                Some(raw) if cached == ChatPrefs::default() => Self::prefs_parse(&raw),
                _ => cached,
            }
        };
        let cwd = pref
            .cwd
            .clone()
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));
        let sid = self
            .client
            .create_session(pref.backend.as_deref(), &cwd.to_string_lossy(), None)
            .await?;
        self.state_set(chat_id, "session", &sid).await;
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

    /// One prompt turn for a conversation: forward events to the
    /// channel until done. `conv_id` keys the demux/permission maps;
    /// `chat_id`/`thread_id` address the channel.
    async fn run_turn(self: &Arc<Self>, ctx: TurnCtx<'_>) -> anyhow::Result<()> {
        let TurnCtx {
            chat_id,
            thread_id,
            conv_id,
            session_id,
            sender_id,
            text,
            attachments,
        } = ctx;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut session_id = session_id.to_string();
        self.demux
            .lock()
            .await
            .sessions
            .insert(session_id.clone(), tx.clone());

        // Send the prompt; the turn result arrives as TurnDone on `rx`,
        // ordered after that session's session.event pushes.
        // A stale cached session (deleted via session.delete or the
        // retention sweep) reports "session not live" as a TurnDone
        // error — turn_start() itself only fails on send. Drop the
        // mapping and retry once on a fresh session instead of bricking
        // the chat until restart.
        // Resolve attachments once per turn — a fetched photo is reused
        // if the stale-session retry re-prompts. Fetch failures degrade
        // to a text note rather than killing the turn: the user still
        // gets an answer about the caption.
        let mut blocks: Vec<Value> = Vec::new();
        if !text.is_empty() {
            blocks.push(json!({"type": "text", "text": text}));
        }
        for att in &attachments {
            match self.ch.fetch(att).await {
                Ok((data, mime)) => {
                    use base64::Engine;
                    if mime.starts_with("image/") {
                        blocks.push(json!({
                            "type": "image",
                            "data": base64::engine::general_purpose::STANDARD.encode(data),
                            "mime": mime,
                        }));
                    } else if mime.starts_with("text/") || mime == "application/json" {
                        // Text-ish payloads go inline as a text block —
                        // the agent reads them like a pasted file.
                        let text = String::from_utf8_lossy(&data);
                        blocks.push(json!({"type": "text", "text": text}));
                    } else {
                        // v2 prompts carry text and images only — a
                        // binary attachment degrades to a note.
                        blocks.push(json!({
                            "type": "text",
                            "text": format!("[attachment {mime} not sent — binary files are unsupported]")
                        }));
                    }
                }
                Err(e) => {
                    warn!(error = %e, "attachment fetch failed");
                    blocks.push(json!({
                        "type": "text",
                        "text": "[attachment could not be downloaded]"
                    }));
                }
            }
        }
        if blocks.is_empty() {
            blocks.push(json!({"type": "text", "text": ""}));
        }
        let mut buf = String::new();
        let mut retried = false;
        // Typing indicator: fire once now, then every 4s while the turn
        // runs — channels without the concept no-op it.
        let _ = self.ch.typing(chat_id).await;
        let typing_tick = {
            let ch = self.ch.clone();
            let cid = chat_id.to_string();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                    let _ = ch.typing(&cid).await;
                }
            })
        };
        'turn: loop {
            {
                let client = self.client.clone();
                let sid = session_id.clone();
                let blocks = blocks.clone();
                // No per-prompt model override — the session's own
                // model always applies (turn_start_blocks takes none).
                if let Err(e) =
                    tokio::spawn(async move { client.turn_start_blocks(&sid, blocks).await })
                        .await
                        .map_err(anyhow::Error::from)
                        .and_then(|r| r)
                {
                    typing_tick.abort();
                    self.end_turn(conv_id, &session_id).await;
                    return Err(e);
                }
            }
            loop {
                match rx.recv().await {
                    Some(ClientEvent::TurnDone { result, .. }) => {
                        // Stale session: remap and retry once on a fresh
                        // session before giving up.
                        if let Err(e) = &result
                            && !retried
                            && (e["message"]
                                .as_str()
                                .unwrap_or("")
                                .contains("session not live")
                                || e["message"]
                                    .as_str()
                                    .unwrap_or("")
                                    .contains("session not found"))
                        {
                            retried = true;
                            self.chat_sessions.lock().await.remove(conv_id);
                            self.state_del(conv_id, "session").await;
                            self.demux.lock().await.sessions.remove(&session_id);
                            match self.session_for(conv_id).await {
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
                                    typing_tick.abort();
                                    self.end_turn(conv_id, &session_id).await;
                                    return Err(e);
                                }
                            }
                        }
                        typing_tick.abort();
                        self.end_turn(conv_id, &session_id).await;
                        self.flush(chat_id, thread_id, &mut buf).await;
                        match result {
                            Ok(v) => {
                                let stop = v["stopReason"].as_str().unwrap_or("?");
                                if stop != "completed" {
                                    self.deliver(chat_id, thread_id, &format!("[{stop}]")).await;
                                }
                            }
                            Err(e) => bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
                        }
                        return Ok(());
                    }
                    Some(ClientEvent::Event {
                        event,
                        replay: false,
                        ..
                    }) => {
                        match event["type"].as_str() {
                            // Timeline items carry their own `kind` tag.
                            Some("timeline") => match event["kind"].as_str() {
                                Some("assistant_message") => {
                                    if let Some(t) = event["text"].as_str() {
                                        buf.push_str(t);
                                        if buf.len() > self.ch.flush_threshold() {
                                            self.flush(chat_id, thread_id, &mut buf).await;
                                        }
                                    }
                                    // Optional outbound images on the
                                    // event: no backend emits them yet,
                                    // but the plumbing is complete so
                                    // one that does renders without
                                    // bridge changes. Media follows its
                                    // text — flush first so the order
                                    // the user sees matches the model's.
                                    let media = media_from_event(&event);
                                    if !media.is_empty() {
                                        self.flush(chat_id, thread_id, &mut buf).await;
                                        if let Err(e) = self
                                            .ch
                                            .send_media(chat_id, thread_id, None, &media)
                                            .await
                                        {
                                            warn!(error = %e, chat = %chat_id, "media send failed");
                                            self.deliver(
                                                chat_id,
                                                thread_id,
                                                &format!(
                                                    "[{} media attachment(s) could not be sent]",
                                                    media.len()
                                                ),
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Some("tool_call") => {
                                    let name = event["name"].as_str().unwrap_or("tool");
                                    let status = event["status"].as_str().unwrap_or("");
                                    self.deliver(
                                        chat_id,
                                        thread_id,
                                        &format!("🔧 {name} {status}"),
                                    )
                                    .await;
                                }
                                _ => {}
                            },
                            Some("subagent") => {
                                // Raw backend frame — summarize from the
                                // common fields, fall back to its type.
                                let f = &event["event"];
                                let name = f["name"]
                                    .as_str()
                                    .or_else(|| f["agent"].as_str())
                                    .or_else(|| f["description"].as_str())
                                    .unwrap_or("subagent");
                                let status = f["status"]
                                    .as_str()
                                    .or_else(|| f["state"].as_str())
                                    .or_else(|| f["type"].as_str())
                                    .unwrap_or("");
                                self.deliver(chat_id, thread_id, &format!("🤖 {name} {status}"))
                                    .await;
                            }
                            Some("permission_requested") => {
                                let request_id = event["id"].as_str().unwrap_or_default();
                                if request_id.is_empty() {
                                    continue;
                                }
                                let title = event["title"]
                                    .as_str()
                                    .or_else(|| event["name"].as_str())
                                    .unwrap_or("tool")
                                    .to_string();
                                // The agent's own offered actions —
                                // "always" is only meaningful when an
                                // allow action is session/always-scoped.
                                let actions: Vec<Value> =
                                    event["actions"].as_array().cloned().unwrap_or_default();
                                let (ptx, prx) = oneshot::channel();
                                self.demux
                                    .lock()
                                    .await
                                    .pending_permissions
                                    .insert(conv_id.to_string(), (sender_id.clone(), ptx));
                                // One call for every channel: adapters
                                // with native buttons render them and
                                // route presses back as synthesized
                                // text replies; the rest get the hint
                                // text. Failures are logged like any
                                // deliver — the turn still waits on the
                                // daemon's own permission timeout.
                                if let Err(e) = self
                                    .ch
                                    .send_permission(chat_id, thread_id, &title, &actions)
                                    .await
                                {
                                    warn!(error = %e, chat = %chat_id, "permission prompt send failed");
                                }
                                let client = self.client.clone();
                                let me = self.clone();
                                let cid = conv_id.to_string();
                                let chat = chat_id.to_string();
                                let tid = thread_id.map(String::from);
                                let sid = session_id.clone();
                                let rid = request_id.to_string();
                                tokio::spawn(async move {
                                    // Bound the wait by the daemon's own
                                    // permission timeout (from the hello
                                    // push, 300s fallback) — a late
                                    // "allow" after the daemon already
                                    // denied the call must not print ✅
                                    // for a dead ask.
                                    let timeout = client.permission_timeout();
                                    let reply = match tokio::time::timeout(timeout, prx).await {
                                        Ok(r) => r.unwrap_or_else(|_| "deny".to_string()),
                                        Err(_) => {
                                            me.demux.lock().await.pending_permissions.remove(&cid);
                                            me.deliver(
                                                &chat,
                                                tid.as_deref(),
                                                "⏱ permission request timed out",
                                            )
                                            .await;
                                            "deny".to_string()
                                        }
                                    };
                                    // Map the reply word onto an offered
                                    // action — shared with the
                                    // watched-ask replier.
                                    let response = permission_response_for(&reply, &actions);
                                    let _ =
                                        client.respond_to_permission(&sid, &rid, response).await;
                                });
                            }
                            // A permission resolved elsewhere (another
                            // client, or the daemon's own timeout) —
                            // drop the pending lane so a later "allow"
                            // isn't swallowed or answered into a dead ask.
                            Some("permission_resolved") => {
                                self.demux.lock().await.pending_permissions.remove(conv_id);
                            }
                            _ => {}
                        }
                    }
                    // Replayed catch-up history: this bridge already
                    // delivered those turns live when they happened —
                    // printing them again on reconnect would duplicate
                    // the whole conversation.
                    Some(ClientEvent::Event { replay: true, .. }) => {}
                    // Connection-state mirrors don't affect an in-flight
                    // turn — the reconnect supervisor parks requests.
                    Some(ClientEvent::Connected | ClientEvent::Disconnected) => {}
                    None => {
                        typing_tick.abort();
                        self.end_turn(conv_id, &session_id).await;
                        anyhow::bail!("event channel closed")
                    }
                }
            }
        }
    }

    /// Turn teardown: drop the demux session and any unanswered
    /// permission request so a stale "allow" can't resolve a dead ask.
    async fn end_turn(&self, conv_id: &str, session_id: &str) {
        let mut demux = self.demux.lock().await;
        demux.sessions.remove(session_id);
        demux.pending_permissions.remove(conv_id);
    }

    /// Short display form of a session id (first 8 chars).
    fn short_id(id: &str) -> &str {
        id.get(..8).unwrap_or(id)
    }

    /// Display label for a session: its title when known, else the
    /// short id.
    fn watch_label(session_id: &str, title: &str) -> String {
        if title.is_empty() {
            format!("session {}", Self::short_id(session_id))
        } else {
            format!("\"{title}\"")
        }
    }

    fn watches_list(map: &HashMap<String, Vec<Watch>>, conv_id: &str) -> String {
        match map.get(conv_id) {
            Some(ws) if !ws.is_empty() => {
                let lines: Vec<String> = ws
                    .iter()
                    .map(|w| {
                        format!(
                            "· {} [{}]",
                            Self::watch_label(&w.session_id, &w.title),
                            if w.backend.is_empty() {
                                "?"
                            } else {
                                &w.backend
                            }
                        )
                    })
                    .collect();
                format!("watching:\n{}", lines.join("\n"))
            }
            _ => "not watching anything — !watch <id prefix>".to_string(),
        }
    }

    /// Resolve `arg` to a session: exact id, unique id prefix, or
    /// unique case-insensitive title substring — the friendly forms a
    /// phone keyboard can produce. Returns (id, title, backend); the
    /// error is the user-facing message.
    async fn resolve_session(&self, arg: &str) -> Result<(String, String, String), String> {
        let rows = self
            .client
            .list_sessions(Some(100), None)
            .await
            .map_err(|e| format!("session list failed: {e:#}"))?;
        if rows.is_empty() {
            return Err("no sessions yet — send a message first".to_string());
        }
        let one = |r: &(String, String, String, String)| (r.0.clone(), r.3.clone(), r.2.clone());
        let ambiguous = |cands: Vec<&(String, String, String, String)>| {
            format!(
                "ambiguous — be more specific: {}",
                cands
                    .iter()
                    .map(|(id, _, _, t)| Self::watch_label(id, t))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let by_prefix: Vec<_> = rows.iter().filter(|(id, ..)| id.starts_with(arg)).collect();
        let arg_lc = arg.to_lowercase();
        let by_title: Vec<_> = rows
            .iter()
            .filter(|(_, _, _, t)| !t.is_empty() && t.to_lowercase().contains(&arg_lc))
            .collect();
        match (by_prefix.len(), by_title.len()) {
            (1, _) => Ok(one(by_prefix[0])),
            (0, 1) => Ok(one(by_title[0])),
            (0, 0) => Err(format!("no session matches '{arg}'")),
            _ => Err(if by_prefix.len() > 1 {
                ambiguous(by_prefix)
            } else {
                ambiguous(by_title)
            }),
        }
    }

    /// Register a watch for this conversation (used by !watch and
    /// auto-watched by !resume) and subscribe the bridge connection
    /// daemon-side. Returns a note to append to the command reply —
    /// empty when the subscription landed.
    async fn watch_add(
        self: &Arc<Self>,
        conv_id: &str,
        session_id: &str,
        title: &str,
        backend: &str,
        chat_id: &str,
        thread_id: Option<&str>,
    ) -> String {
        if let Err(e) = self.client.watch_session(session_id).await {
            return format!(" {e:#}");
        }
        {
            let mut map = self.watches.lock().await;
            let list = map.entry(conv_id.to_string()).or_default();
            match list.iter_mut().find(|w| w.session_id == session_id) {
                Some(w) => {
                    // Re-watching refreshes the delivery address and
                    // cached metadata in place.
                    w.title = title.to_string();
                    w.backend = backend.to_string();
                    w.chat_id = chat_id.to_string();
                    w.thread_id = thread_id.map(String::from);
                }
                None => {
                    if list.len() >= MAX_WATCHES_PER_CONV {
                        list.remove(0);
                    }
                    list.push(Watch {
                        session_id: session_id.to_string(),
                        title: title.to_string(),
                        backend: backend.to_string(),
                        chat_id: chat_id.to_string(),
                        thread_id: thread_id.map(String::from),
                    });
                }
            }
            let mut total: usize = map.values().map(Vec::len).sum();
            while total > MAX_WATCHES_TOTAL {
                let Some(oldest) = map.values_mut().find(|v| !v.is_empty()) else {
                    break;
                };
                oldest.remove(0);
                total -= 1;
            }
            map.retain(|_, v| !v.is_empty());
        }
        self.persist_watches().await;
        String::new()
    }

    async fn watch_remove(&self, conv_id: &str, session_id: &str) {
        {
            let mut map = self.watches.lock().await;
            if let Some(list) = map.get_mut(conv_id) {
                list.retain(|w| w.session_id != session_id);
                if list.is_empty() {
                    map.remove(conv_id);
                }
            }
        }
        self.persist_watches().await;
    }

    /// Fan a watched session's cross-surface milestones out to the
    /// conversations watching it. Only called when NO local turn owns
    /// the session (the demux route handled that case), so a reply this
    /// bridge is already streaming never doubles as a notification.
    async fn notify_watchers(self: &Arc<Self>, session_id: &str, event: &Value) {
        let watchers: Vec<(String, Watch)> = {
            let map = self.watches.lock().await;
            map.iter()
                .flat_map(|(conv, ws)| ws.iter().map(move |w| (conv.clone(), w.clone())))
                .filter(|(_, w)| w.session_id == session_id)
                .collect()
        };
        if watchers.is_empty() {
            return;
        }
        match event["type"].as_str().unwrap_or("") {
            "turn_completed" | "turn_failed" | "turn_canceled" => {
                for (_, w) in &watchers {
                    let label = Self::watch_label(&w.session_id, &w.title);
                    let body = match event["type"].as_str().unwrap_or("") {
                        "turn_failed" => format!(
                            "⚠️ {label} turn failed: {}",
                            event["error"].as_str().unwrap_or("unknown")
                        ),
                        "turn_canceled" => format!("⏹ {label} turn canceled"),
                        _ => format!("🔔 {label} finished a turn"),
                    };
                    self.deliver(&w.chat_id, w.thread_id.as_deref(), &body)
                        .await;
                }
                // The turn is over — any outstanding watched asks for
                // this session are dead (answered elsewhere or hit the
                // daemon's permission timeout).
                self.demux
                    .lock()
                    .await
                    .watched_asks
                    .retain(|_, a| a.session_id != session_id);
            }
            "permission_requested" => {
                let request_id = event["id"].as_str().unwrap_or_default();
                if request_id.is_empty() {
                    return;
                }
                let name = event["title"]
                    .as_str()
                    .or_else(|| event["name"].as_str())
                    .unwrap_or("tool");
                let actions = event["actions"].as_array().cloned().unwrap_or_default();
                let ask_text = permission_prompt_text(name, &actions);
                let texts: Vec<(String, Option<String>, String)> = {
                    let mut demux = self.demux.lock().await;
                    watchers
                        .iter()
                        .map(|(conv, w)| {
                            // One answerable ask per conversation — a
                            // stale one is dead and simply replaced.
                            demux.watched_asks.insert(
                                conv.clone(),
                                WatchedAsk {
                                    session_id: session_id.to_string(),
                                    request_id: request_id.to_string(),
                                    actions: actions.clone(),
                                },
                            );
                            (
                                w.chat_id.clone(),
                                w.thread_id.clone(),
                                format!(
                                    "👀 {} —\n{ask_text}",
                                    Self::watch_label(&w.session_id, &w.title)
                                ),
                            )
                        })
                        .collect()
                };
                for (chat_id, thread_id, text) in texts {
                    self.deliver(&chat_id, thread_id.as_deref(), &text).await;
                }
            }
            // Answered elsewhere (another surface, or the daemon's
            // timeout) — clear the ask lane so a late "allow" here
            // cannot fire into a dead request.
            "permission_resolved" => {
                self.demux
                    .lock()
                    .await
                    .watched_asks
                    .retain(|_, a| a.session_id != session_id);
            }
            _ => {}
        }
    }

    /// Deliver text to the chat, logging failures instead of silently
    /// dropping them — a lost final reply or permission prompt must at
    /// least leave a trace. Adapters already retry transport errors and
    /// 429s internally; nothing actionable remains here but the log.
    async fn deliver(&self, chat_id: &str, thread_id: Option<&str>, text: &str) {
        if let Err(e) = self.ch.send_in_thread(chat_id, thread_id, text).await {
            warn!(error = %e, chat = %chat_id, "channel send failed");
        }
    }

    /// Flush the accumulated reply, split into fence-safe chunks so a
    /// long code block never arrives as an unterminated fragment —
    /// the channel renders each chunk as standalone markdown.
    async fn flush(&self, chat_id: &str, thread_id: Option<&str>, buf: &mut String) {
        if !buf.is_empty() {
            for chunk in chunk_fence_safe(buf, self.ch.flush_threshold()) {
                self.deliver(chat_id, thread_id, &chunk).await;
            }
            buf.clear();
        }
    }
}

/// Everything one prompt turn needs: where to deliver output
/// (`chat_id`/`thread_id`), which maps key the conversation
/// (`conv_id`), and what to send (`text`/`attachments`).
struct TurnCtx<'a> {
    chat_id: &'a str,
    thread_id: Option<&'a str>,
    conv_id: &'a str,
    session_id: &'a str,
    sender_id: Option<String>,
    text: &'a str,
    attachments: Vec<Attachment>,
}

// --- Shared permission-prompt rendering -------------------------------------

/// The action's behavior is "allow".
fn action_is_allow(a: &Value) -> bool {
    a["behavior"].as_str() == Some("allow")
}

/// The action is a session/always-scoped allow — the daemon marks
/// those only through the action id/label, so "always" is offered
/// only when one is present.
fn action_is_scoped(a: &Value) -> bool {
    let id = a["id"].as_str().unwrap_or("").to_lowercase();
    let label = a["label"].as_str().unwrap_or("").to_lowercase();
    id.contains("always")
        || id.contains("session")
        || label.contains("always")
        || label.contains("session")
}

/// One rendered permission choice: `word` is the reply that flows
/// back through the text path (typed or synthesized from a button),
/// `label` its display name from the source action when it has one.
pub(crate) struct PermissionChoice {
    pub word: &'static str,
    pub label: String,
}

/// Collapse the agent's offered `actions` into the choices the chat
/// UIs show: allow, deny, and — only when a session/always-scoped
/// allow is offered — always. The single source for hint text,
/// native buttons, and the reply-word mapping, so they can never
/// disagree. Allow/deny stay present even without a matching action:
/// the responder falls back to an action_id-less response exactly
/// like a typed reply does.
pub(crate) fn permission_choices(actions: &[Value]) -> Vec<PermissionChoice> {
    let allow = actions
        .iter()
        .find(|a| action_is_allow(a) && !action_is_scoped(a))
        .or_else(|| actions.iter().find(|a| action_is_allow(a)));
    let always = actions
        .iter()
        .find(|a| action_is_allow(a) && action_is_scoped(a));
    let deny = actions
        .iter()
        .find(|a| a["behavior"].as_str() == Some("deny"));
    let mut out = vec![PermissionChoice {
        word: "allow",
        label: allow
            .and_then(|a| a["label"].as_str())
            .unwrap_or("Allow")
            .to_string(),
    }];
    if let Some(a) = always {
        out.push(PermissionChoice {
            word: "always",
            label: a["label"].as_str().unwrap_or("Always allow").to_string(),
        });
    }
    out.push(PermissionChoice {
        word: "deny",
        label: deny
            .and_then(|a| a["label"].as_str())
            .unwrap_or("Deny")
            .to_string(),
    });
    out
}

/// Plain-text permission prompt body: the title plus the reply hint
/// for channels without native buttons (and the doc text under them).
pub(crate) fn permission_prompt_text(title: &str, actions: &[Value]) -> String {
    let has_always = actions
        .iter()
        .any(|a| action_is_allow(a) && action_is_scoped(a));
    let hint = if has_always {
        "reply 'allow', 'always', or 'deny'"
    } else {
        "reply 'allow' or 'deny'"
    };
    format!("🔐 {title}\n{hint}")
}

/// Map a permission reply word onto the request's offered actions —
/// "always" takes the session/always-scoped allow (falling back to a
/// plain allow when the agent offered none), "deny" takes the offered
/// deny action. Shared by the live-turn waiter and the watched-ask
/// replier so they can never drift apart.
fn permission_response_for(reply: &str, actions: &[Value]) -> Value {
    if reply == "deny" {
        let action_id = actions
            .iter()
            .find(|a| a["behavior"].as_str() == Some("deny"))
            .and_then(|a| a["id"].as_str())
            .map(String::from);
        json!({
            "behavior": "deny",
            "action_id": action_id,
            "message": "denied by user",
            "interrupt": false,
        })
    } else {
        let action_id = actions
            .iter()
            .find(|a| action_is_allow(a) && (reply != "always" || action_is_scoped(a)))
            .or_else(|| actions.iter().find(|a| action_is_allow(a)))
            .and_then(|a| a["id"].as_str())
            .map(String::from);
        json!({
            "behavior": "allow",
            "action_id": action_id,
        })
    }
}

/// Decode the optional `images` array a timeline assistant_message
/// event may carry: [{data: base64, mime}]. Undecodable entries are
/// skipped — one bad image must not take the whole reply down.
fn media_from_event(event: &Value) -> Vec<MediaOut> {
    use base64::Engine;
    event["images"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .enumerate()
                .filter_map(|(i, img)| {
                    let data = img["data"]
                        .as_str()
                        .and_then(|d| base64::engine::general_purpose::STANDARD.decode(d).ok())?;
                    let mime = img["mime"]
                        .as_str()
                        .unwrap_or("application/octet-stream")
                        .to_string();
                    let ext = mime.rsplit('/').next().unwrap_or("bin").to_string();
                    Some(MediaOut {
                        data,
                        mime,
                        filename: format!("image-{i}.{ext}"),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

// --- Fence-safe chunking and Telegram HTML -----------------------------------

/// A ``` fence line: returns its info string ("rust", or "" for a
/// bare fence). Indentation is ignored — this is a chunker, not a
/// full CommonMark parser.
fn fence_line_info(line: &str) -> Option<&str> {
    let t = line.trim_start_matches(' ');
    t.strip_prefix("```").map(|info| info.trim())
}

/// Split `text` into chunks of at most `max` bytes without breaking a
/// code fence open: the cut lands on the last line boundary that
/// leaves fences balanced. When a single fenced block alone exceeds
/// `max`, it is hard-split at the byte limit and the fence re-opened
/// at the head of the next chunk, so both sides still render as code.
pub(crate) fn chunk_fence_safe(text: &str, max: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut remaining = text.to_string();
    while !remaining.is_empty() {
        if remaining.len() <= max {
            chunks.push(std::mem::take(&mut remaining));
            break;
        }
        let limit = remaining.floor_char_boundary(max);
        let window = &remaining[..limit];
        // Walk complete lines, tracking fence state; remember the last
        // boundary with fences balanced.
        let mut cut = None;
        let mut in_fence = false;
        let mut open_info = String::new();
        let mut pos = 0;
        for line in window.split_inclusive('\n') {
            if let Some(info) = fence_line_info(line) {
                in_fence = !in_fence;
                open_info = if in_fence {
                    info.to_string()
                } else {
                    String::new()
                };
            }
            pos += line.len();
            if !in_fence {
                cut = Some(pos);
            }
        }
        match cut {
            Some(c) => {
                chunks.push(window[..c].to_string());
                remaining = remaining[c..].to_string();
            }
            None => {
                // Only inside an oversized fence: hard-split and
                // re-open the fence on the next chunk.
                chunks.push(window.to_string());
                remaining = format!("```{open_info}\n{}", &remaining[limit..]);
            }
        }
    }
    chunks
}

/// Escape Telegram-HTML specials in text content.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Inline markdown → Telegram HTML for one non-fence line: `code` and
/// **bold** spans (the nearer opener wins; `code` inside **bold**
/// still converts), everything else escaped. Unclosed markers stay
/// literal — a mid-chunk split must not swallow half the line.
fn telegram_inline(line: &str, out: &mut String) {
    let mut rest = line;
    if let Some(r) = rest.strip_prefix("- ") {
        out.push_str("• ");
        rest = r;
    }
    while !rest.is_empty() {
        let code = rest.find('`');
        let bold = rest.find("**");
        // Nearest opener wins; a tie cannot happen ('`' vs '**').
        let take_code = match (code, bold) {
            (None, None) => {
                out.push_str(&escape_html(rest));
                return;
            }
            (Some(_), None) => true,
            (Some(i), Some(j)) => i < j,
            (None, Some(_)) => false,
        };
        if take_code {
            let i = code.unwrap();
            out.push_str(&escape_html(&rest[..i]));
            let after = &rest[i + 1..];
            match after.find('`') {
                Some(j) => {
                    out.push_str("<code>");
                    out.push_str(&escape_html(&after[..j]));
                    out.push_str("</code>");
                    rest = &after[j + 1..];
                }
                None => {
                    // Unclosed marker stays literal.
                    out.push('`');
                    out.push_str(&escape_html(after));
                    return;
                }
            }
        } else {
            let j = bold.unwrap();
            out.push_str(&escape_html(&rest[..j]));
            let after = &rest[j + 2..];
            match after.find("**") {
                Some(k) => {
                    out.push_str("<b>");
                    telegram_inline(&after[..k], out);
                    out.push_str("</b>");
                    rest = &after[k + 2..];
                }
                None => {
                    // Unclosed marker stays literal.
                    out.push_str("**");
                    out.push_str(&escape_html(after));
                    return;
                }
            }
        }
    }
}

/// Markdown-ish reply → Telegram HTML: escape &, <, >; ```lang fences
/// → `<pre><code class="language-lang">`; `code`; **bold**; "- "
/// bullets → "• ". An unclosed fence closes at the end so a chunk
/// hard-split inside a code block still renders.
pub(crate) fn to_telegram_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_fence = false;
    for line in text.split_inclusive('\n') {
        if let Some(info) = fence_line_info(line) {
            if in_fence {
                out.push_str("</code></pre>\n");
                in_fence = false;
            } else if info.is_empty() {
                out.push_str("<pre><code>");
                in_fence = true;
            } else {
                out.push_str(&format!("<pre><code class=\"language-{info}\">"));
                in_fence = true;
            }
        } else if in_fence {
            out.push_str(&escape_html(line));
        } else {
            telegram_inline(line, &mut out);
        }
    }
    if in_fence {
        out.push_str("</code></pre>");
    }
    out
}

// --- Minimal multipart writer -----------------------------------------------

/// Hand-rolled multipart/form-data body writer — reqwest's `multipart`
/// feature is off in this workspace, and enabling it is a Cargo.toml
/// change two upload paths don't justify. Format per RFC 7578.
pub(crate) struct MultipartWriter {
    boundary: String,
    body: Vec<u8>,
}

impl Default for MultipartWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl MultipartWriter {
    pub fn new() -> Self {
        Self {
            boundary: format!("damon-{}", uuid::Uuid::new_v4()),
            body: Vec::new(),
        }
    }

    fn part_headers(&mut self, name: &str, filename: Option<&str>, mime: Option<&str>) {
        self.body
            .extend_from_slice(format!("--{}\r\n", self.boundary).as_bytes());
        match filename {
            Some(f) => self.body.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"; filename=\"{f}\"\r\n")
                    .as_bytes(),
            ),
            None => self.body.extend_from_slice(
                format!("Content-Disposition: form-data; name=\"{name}\"\r\n").as_bytes(),
            ),
        }
        if let Some(m) = mime {
            self.body
                .extend_from_slice(format!("Content-Type: {m}\r\n").as_bytes());
        }
        self.body.extend_from_slice(b"\r\n");
    }

    /// One text field.
    pub fn field(&mut self, name: &str, value: &str) {
        self.part_headers(name, None, None);
        self.body.extend_from_slice(value.as_bytes());
        self.body.extend_from_slice(b"\r\n");
    }

    /// One file part — raw bytes with an explicit content type.
    pub fn file(&mut self, name: &str, filename: &str, mime: &str, data: &[u8]) {
        self.part_headers(name, Some(filename), Some(mime));
        self.body.extend_from_slice(data);
        self.body.extend_from_slice(b"\r\n");
    }

    /// Finish: returns the Content-Type header value and the body.
    pub fn finish(mut self) -> (String, Vec<u8>) {
        self.body
            .extend_from_slice(format!("--{}--\r\n", self.boundary).as_bytes());
        (
            format!("multipart/form-data; boundary={}", self.boundary),
            self.body,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking_without_fences_splits_at_line_boundaries() {
        let text = "line one\nline two\nline three\nline four\n";
        let chunks = chunk_fence_safe(text, 20);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert!(c.len() <= 20, "oversized chunk: {c:?}");
        }
        assert_eq!(chunks.concat(), text);
    }

    /// Fence-marker count of a chunk (```-prefixed lines).
    fn fences_of(c: &str) -> usize {
        c.lines().filter(|l| fence_line_info(l).is_some()).count()
    }

    /// Reassemble the original text from chunks: a chunk following one
    /// that ended mid-fence starts with the re-opened "```info\n"
    /// marker, which the chunker injected and must be stripped again.
    fn reassemble(chunks: &[String]) -> String {
        let mut joined = String::new();
        for (i, c) in chunks.iter().enumerate() {
            if i > 0 && fences_of(&chunks[i - 1]) % 2 == 1 {
                joined.push_str(c.split_once('\n').unwrap().1);
            } else {
                joined.push_str(c);
            }
        }
        joined
    }

    #[test]
    fn chunking_never_cuts_a_fence_open() {
        // A fence block spanning the threshold: no chunk boundary may
        // land between the fences and claim balance — a chunk that
        // ends mid-fence must be followed by a re-opened fence, and
        // stripping the re-open markers reconstructs the text exactly.
        let fenced = format!(
            "before\n```rust\n{}\n```\nafter\n",
            "let x = 1;\n".repeat(8)
        );
        let chunks = chunk_fence_safe(&fenced, 40);
        assert!(chunks.len() >= 2);
        for (i, c) in chunks.iter().enumerate() {
            assert!(c.len() <= 40, "oversized chunk: {c:?}");
            if fences_of(c) % 2 == 1 {
                assert!(
                    i + 1 < chunks.len(),
                    "final chunk ends inside a fence: {c:?}"
                );
                assert!(
                    chunks[i + 1].starts_with("```"),
                    "fence not re-opened: {:?}",
                    chunks[i + 1]
                );
            }
        }
        assert_eq!(reassemble(&chunks), fenced);
    }

    #[test]
    fn oversized_fence_hard_splits_and_reopens() {
        // One fenced block alone over the limit: hard-split at the
        // byte cap, fence re-opened on every continuation chunk.
        let fenced = format!("```rust\n{}\n```", "let x = 1;\n".repeat(30));
        let chunks = chunk_fence_safe(&fenced, 60);
        assert!(chunks.len() >= 2);
        assert!(chunks[0].starts_with("```rust\n"));
        for (i, c) in chunks.iter().enumerate() {
            assert!(c.len() <= 60, "oversized chunk: {c:?}");
            if fences_of(c) % 2 == 1 {
                assert!(
                    i + 1 < chunks.len(),
                    "final chunk ends inside a fence: {c:?}"
                );
                assert!(
                    chunks[i + 1].starts_with("```rust\n"),
                    "fence not re-opened: {:?}",
                    chunks[i + 1]
                );
            }
        }
        assert!(chunks.last().unwrap().ends_with("```"));
        assert_eq!(reassemble(&chunks), fenced);
    }

    #[test]
    fn telegram_html_converts_markdown() {
        let md = "**bold** and `code <here>`\n- item\n```rust\nfn main() {}\n```\n";
        let html = to_telegram_html(md);
        assert_eq!(
            html,
            "<b>bold</b> and <code>code &lt;here&gt;</code>\n• item\n\
             <pre><code class=\"language-rust\">fn main() {}\n</code></pre>\n"
        );
    }

    #[test]
    fn telegram_html_escapes_and_closes_unclosed_fence() {
        assert_eq!(to_telegram_html("a < b & c"), "a &lt; b &amp; c");
        // Unclosed fence (a chunk hard-split mid-block): still closes.
        assert_eq!(
            to_telegram_html("```\nlet x = 1;"),
            "<pre><code>let x = 1;</code></pre>"
        );
        // Unclosed inline markers stay literal.
        assert_eq!(to_telegram_html("a ** b"), "a ** b");
    }

    #[test]
    fn permission_choices_collapse_actions() {
        let actions = json!([
            {"id": "allow", "label": "Allow", "behavior": "allow"},
            {"id": "allow_session", "label": "Always allow", "behavior": "allow"},
            {"id": "deny", "label": "Deny", "behavior": "deny"},
        ])
        .as_array()
        .unwrap()
        .clone();
        let words: Vec<&str> = permission_choices(&actions)
            .iter()
            .map(|c| c.word)
            .collect();
        assert_eq!(words, ["allow", "always", "deny"]);
        // No scoped allow → no "always", matching the hint text.
        let plain = json!([
            {"id": "allow", "behavior": "allow"},
            {"id": "deny", "behavior": "deny"},
        ])
        .as_array()
        .unwrap()
        .clone();
        let words: Vec<&str> = permission_choices(&plain).iter().map(|c| c.word).collect();
        assert_eq!(words, ["allow", "deny"]);
        assert_eq!(
            permission_prompt_text("run cmd", &plain),
            "🔐 run cmd\nreply 'allow' or 'deny'"
        );
        assert_eq!(
            permission_prompt_text("run cmd", &actions),
            "🔐 run cmd\nreply 'allow', 'always', or 'deny'"
        );
    }

    #[test]
    fn media_from_event_decodes_and_skips_bad_entries() {
        use base64::Engine;
        let png = base64::engine::general_purpose::STANDARD.encode([1, 2, 3]);
        let ev = json!({
            "type": "timeline",
            "kind": "assistant_message",
            "text": "chart:",
            "images": [
                {"data": png, "mime": "image/png"},
                {"data": "!!!not base64!!!", "mime": "image/png"},
            ],
        });
        let media = media_from_event(&ev);
        assert_eq!(media.len(), 1);
        assert_eq!(media[0].data, vec![1, 2, 3]);
        assert_eq!(media[0].mime, "image/png");
        assert_eq!(media[0].filename, "image-0.png");
        // Absent images → empty.
        assert!(media_from_event(&json!({"text": "hi"})).is_empty());
    }

    #[test]
    fn multipart_writer_frames_parts() {
        let mut mp = MultipartWriter::new();
        mp.field("chat_id", "42");
        mp.file("photo", "a.png", "image/png", &[0x89, 0x50]);
        let (ctype, body) = mp.finish();
        assert!(ctype.starts_with("multipart/form-data; boundary=damon-"));
        let text = String::from_utf8_lossy(&body);
        let boundary = ctype.split('=').nth(1).unwrap();
        assert!(text.contains(&format!("--{boundary}\r\n")));
        assert!(text.contains("Content-Disposition: form-data; name=\"chat_id\"\r\n\r\n42\r\n"));
        assert!(
            text.contains("Content-Disposition: form-data; name=\"photo\"; filename=\"a.png\"\r\n")
        );
        assert!(text.contains("Content-Type: image/png\r\n"));
        assert!(text.ends_with(&format!("--{boundary}--\r\n")));
    }
}
