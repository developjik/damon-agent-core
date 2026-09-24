//! Generic chat-channel bridge: maps channel chats to damon sessions.
//! A channel adapter supplies inbound messages and outbound delivery;
//! this bridge owns session mapping, event demux, permission replies
//! ("allow"/"deny"), and the per-turn streaming loop.

use anyhow::bail;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;

use crate::client::{ClientEvent, DamonClient};

/// One inbound message from a channel chat.
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
/// content for channels that inline media.
pub enum Attachment {
    TelegramFile { file_id: String },
    Bytes { data: Vec<u8>, mime: String },
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
        // `hello` negotiates the protocol; the connect-time hello push
        // already fed `client.permission_timeout()`.
        self.client.hello().await?;
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
                let _ = self
                    .ch
                    .send_in_thread(
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
        // overwrite the demux entry and starve the first turn's events.
        {
            let mut active = self.active_turns.lock().await;
            if !active.insert(conv_id.clone()) {
                let _ = self
                    .ch
                    .send_in_thread(
                        &chat_id,
                        thread_id.as_deref(),
                        "a prompt is already running in this chat",
                    )
                    .await;
                return;
            }
        }

        let session_id = match self.session_for(&conv_id).await {
            Ok(s) => s,
            Err(e) => {
                self.active_turns.lock().await.remove(&conv_id);
                let _ = self
                    .ch
                    .send_in_thread(&chat_id, thread_id.as_deref(), &format!("error: {e:#}"))
                    .await;
                return;
            }
        };
        let me = self.clone();
        tokio::spawn(async move {
            // RAII cleanup: a panic inside run_turn must still free the
            // active_turns entry or this chat bricks until restart.
            struct TurnGuard<'a> {
                bridge: &'a Arc<Bridge>,
                conv_id: String,
            }
            impl Drop for TurnGuard<'_> {
                fn drop(&mut self) {
                    let bridge = self.bridge.clone();
                    let conv_id = std::mem::take(&mut self.conv_id);
                    tokio::spawn(async move {
                        bridge.active_turns.lock().await.remove(&conv_id);
                    });
                }
            }
            let _guard = TurnGuard {
                bridge: &me,
                conv_id: conv_id.clone(),
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
                let _ = me
                    .ch
                    .send_in_thread(&chat_id, thread_id.as_deref(), &format!("error: {e:#}"))
                    .await;
            }
        });
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
        let (verb, _arg) = match cmd.split_once(char::is_whitespace) {
            Some((v, a)) => (v, a.trim()),
            None => (cmd, ""),
        };
        let reply = match verb {
            "new" => {
                self.chat_sessions.lock().await.remove(conv_id);
                Some("session reset — next message starts a fresh session".to_string())
            }
            "delete" => {
                let sid = self
                    .chat_sessions
                    .lock()
                    .await
                    .remove(conv_id)
                    .map(|e| e.session_id);
                match sid {
                    Some(sid) => match self.client.delete_session(&sid).await {
                        Ok(()) => Some(format!("deleted session {sid}")),
                        Err(e) => Some(format!("delete failed: {e:#}")),
                    },
                    None => Some("no session to delete".to_string()),
                }
            }
            "agent" => Some(
                "backends are fixed per session — use !new to start one on the \
                 default backend (set `default_backend` in config)"
                    .to_string(),
            ),
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
                            Some(format!("forked → {new_id} — next message continues there"))
                        }
                        Err(e) => Some(format!("fork failed: {e:#}")),
                    },
                    None => Some("no session to fork — send a message first".to_string()),
                }
            }
            "help" => Some(
                "commands: !new (reset session) · !fork (branch session) · \
                 !delete (remove session) · !usage (token totals) · \
                 allow/deny/always (answer a permission prompt)"
                    .to_string(),
            ),
            _ => Some(format!("unknown command '!{verb}' — try !help")),
        };
        if let Some(text) = reply {
            let _ = self.ch.send_in_thread(chat_id, thread_id, &text).await;
        }
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
        // Slow path: create outside the lock (create_session must not
        // hold it), then re-acquire to insert.
        let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
        let sid = self
            .client
            .create_session(None, &cwd.to_string_lossy(), None)
            .await?;
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
                                    let _ = self
                                        .ch
                                        .send_in_thread(chat_id, thread_id, &format!("[{stop}]"))
                                        .await;
                                }
                            }
                            Err(e) => bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
                        }
                        return Ok(());
                    }
                    Some(ClientEvent::Event { event, .. }) => {
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
                                }
                                Some("tool_call") => {
                                    let name = event["name"].as_str().unwrap_or("tool");
                                    let status = event["status"].as_str().unwrap_or("");
                                    let _ = self
                                        .ch
                                        .send_in_thread(
                                            chat_id,
                                            thread_id,
                                            &format!("🔧 {name} {status}"),
                                        )
                                        .await;
                                }
                                Some("error") => {
                                    let msg = event["message"].as_str().unwrap_or("unknown error");
                                    let _ = self
                                        .ch
                                        .send_in_thread(chat_id, thread_id, &format!("⚠️ {msg}"))
                                        .await;
                                }
                                _ => {}
                            },
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
                                let is_allow = |a: &Value| a["behavior"].as_str() == Some("allow");
                                let is_scoped = |a: &Value| {
                                    let id = a["id"].as_str().unwrap_or("").to_lowercase();
                                    let label = a["label"].as_str().unwrap_or("").to_lowercase();
                                    id.contains("always")
                                        || id.contains("session")
                                        || label.contains("always")
                                        || label.contains("session")
                                };
                                let has_always =
                                    actions.iter().any(|a| is_allow(a) && is_scoped(a));
                                let (ptx, prx) = oneshot::channel();
                                self.demux
                                    .lock()
                                    .await
                                    .pending_permissions
                                    .insert(conv_id.to_string(), (sender_id.clone(), ptx));
                                let hint = if has_always {
                                    "reply 'allow', 'always', or 'deny'"
                                } else {
                                    "reply 'allow' or 'deny'"
                                };
                                let _ = self
                                    .ch
                                    .send_in_thread(
                                        chat_id,
                                        thread_id,
                                        &format!("🔐 {title}\n{hint}"),
                                    )
                                    .await;
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
                                            let _ = me
                                                .ch
                                                .send_in_thread(
                                                    &chat,
                                                    tid.as_deref(),
                                                    "⏱ permission request timed out",
                                                )
                                                .await;
                                            "deny".to_string()
                                        }
                                    };
                                    // Map the reply word onto an offered
                                    // action: "always" wants the
                                    // session/always-scoped allow, and
                                    // falls back to a plain allow when
                                    // the agent offered none.
                                    let response = if reply == "deny" {
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
                                            .find(|a| {
                                                is_allow(a) && (reply != "always" || is_scoped(a))
                                            })
                                            .or_else(|| actions.iter().find(|a| is_allow(a)))
                                            .and_then(|a| a["id"].as_str())
                                            .map(String::from);
                                        json!({
                                            "behavior": "allow",
                                            "action_id": action_id,
                                        })
                                    };
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

    async fn flush(&self, chat_id: &str, thread_id: Option<&str>, buf: &mut String) {
        if !buf.is_empty() {
            let _ = self.ch.send_in_thread(chat_id, thread_id, buf).await;
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
