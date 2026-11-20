//! SessionManager — maps Damon session ids to live backend sessions.
//!
//! One Damon session owns one `AgentSession` (which owns its process).
//! The manager routes prompts, permission answers, and interrupts, and
//! reaps idle sessions so unused agent processes don't pile up —
//! reattach happens through the persistence handle on next use.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::Mutex;

use crate::backend::types::*;
use crate::backend::{AgentClient, AgentSession};

/// A turn's terminal event — everything the journal must not carry past
/// this point is persisted by `rpc::run_turn` instead.
fn is_turn_boundary(ev: &StreamEvent) -> bool {
    matches!(
        &ev.kind,
        StreamEventKind::TurnCompleted { .. }
            | StreamEventKind::TurnFailed { .. }
            | StreamEventKind::TurnCanceled { .. }
    )
}

/// One session event tagged with the cursor the fan-out assigned it.
///
/// The cursor is session-scoped and strictly increasing, assigned once
/// per event whether or not the journal keeps it. That single numbering
/// is what makes delivery honest: a client holding seq 41 and then
/// receiving seq 47 can *see* that 42–46 are missing instead of
/// rendering a holed stream as the whole answer.
#[derive(Clone)]
pub struct SeqEvent {
    pub seq: u64,
    pub event: StreamEvent,
}

/// Live fan-out capacity. Each subscriber has its own receiver; a lagging
/// one loses only its own events (`RecvError::Lagged`), and it knows how
/// many because the cursor jumps.
const FEED_CAP: usize = 512;

/// Bounded buffer of the CURRENT turn's events for replay to
/// subscribers that attach mid-turn (`rpc::Conn::subscribe`). Completed
/// turns are not kept — their artifacts are persisted by
/// `rpc::run_turn` and replayed from the store — so the journal holds
/// exactly the events a late subscriber cannot get anywhere else.
///
/// It is also the session's event fan-out: every event gets its cursor
/// here and is re-broadcast cursor-tagged, so replay and live frames
/// carry the same numbering.
pub struct EventJournal {
    inner: parking_lot::Mutex<JournalState>,
    /// Cursor of the newest event fanned out (0 = none yet).
    seq: std::sync::atomic::AtomicU64,
    /// Cursor-tagged live fan-out — one receiver per subscribing
    /// connection.
    feed: tokio::sync::broadcast::Sender<SeqEvent>,
}

struct JournalState {
    events: VecDeque<SeqEvent>,
    /// The buffer overflowed mid-turn: the replayable prefix is now a
    /// garbled partial (deltas missing in the middle), so it is not
    /// replayed at all — the same shedding a lagging live broadcast
    /// receiver suffers.
    shed: bool,
}

/// Journal capacity in events. Delta-heavy long turns can exceed it;
/// overflow sheds the replay (see `JournalState::shed`).
const JOURNAL_CAP: usize = 2048;

impl EventJournal {
    fn new() -> Self {
        let (feed, _) = tokio::sync::broadcast::channel(FEED_CAP);
        Self {
            inner: parking_lot::Mutex::new(JournalState {
                events: VecDeque::new(),
                shed: false,
            }),
            seq: std::sync::atomic::AtomicU64::new(0),
            feed,
        }
    }

    /// Cursor of the newest event handed to this session's fan-out (0 =
    /// the session has produced nothing). A frame carrying `seq: N` tells
    /// a client that every event up to N reached the connection — unless
    /// a `session.gap` frame says how many were skipped.
    pub fn cursor(&self) -> u64 {
        self.seq.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Whether the journal shed the current turn's replayable prefix —
    /// it overflowed, or the daemon itself fell behind the backend. A
    /// subscriber must resync from the store when this is true.
    pub fn is_shed(&self) -> bool {
        self.inner.lock().shed
    }

    /// Subscribe to the cursor-tagged live fan-out.
    pub(crate) fn subscribe(&self) -> tokio::sync::broadcast::Receiver<SeqEvent> {
        self.feed.subscribe()
    }

    fn record(&self, ev: StreamEvent) {
        let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        {
            let mut g = self.inner.lock();
            if is_turn_boundary(&ev) {
                // Turn over: from here the turn lives in the store. Keeping
                // it would double-replay once run_turn persists it.
                g.events.clear();
                g.shed = false;
            } else {
                if g.events.len() >= JOURNAL_CAP {
                    g.events.pop_front();
                    g.shed = true;
                }
                g.events.push_back(SeqEvent {
                    seq,
                    event: ev.clone(),
                });
            }
        }
        // Live fan-out last, and after the journal holds it: a subscriber
        // that reads the journal then subscribes to the feed cannot see
        // this event twice, only (in a race) miss it — which its cursor
        // makes visible. No receivers is not an error: every connection
        // owns its own receiver and reports its own gaps.
        let _ = self.feed.send(SeqEvent { seq, event: ev });
    }

    /// The in-flight turn's replayable prefix, oldest first, or `None`
    /// when the journal shed it (overflow, or the daemon itself fell
    /// behind the backend): replaying a partial turn would render a garbled
    /// answer as the whole one, so the caller points the client at the
    /// store instead (`session.snapshot_required`).
    pub(crate) fn replayable(&self) -> Option<Vec<SeqEvent>> {
        let g = self.inner.lock();
        if g.shed {
            None
        } else {
            Some(g.events.iter().cloned().collect())
        }
    }
}

/// Resident subscriber that journals a session's events for
/// late-subscriber replay. Exits when the backend event channel closes
/// (session closed or reaped) — reaped-and-resumed sessions start a
/// fresh journal; the store covers everything before that.
fn spawn_journal(session: &Arc<dyn AgentSession>) -> Arc<EventJournal> {
    let journal = Arc::new(EventJournal::new());
    let j = journal.clone();
    let mut rx = session.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => j.record(ev),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // The daemon itself fell behind the backend: events are
                    // gone before they ever got a cursor, so the journal is
                    // unreplayable from here on. Live consumers still see the
                    // cursor jump rather than a silent hole.
                    let mut g = j.inner.lock();
                    g.events.clear();
                    g.shed = true;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
    journal
}

/// A live Damon session: the backend session plus its routing metadata.
pub struct ManagedSession {
    /// Damon-facing session id.
    pub id: String,
    /// The backend that owns this session.
    pub provider: ProviderId,
    /// The live backend session object.
    pub session: Arc<dyn AgentSession>,
    /// Persistence handle (native session id) as of session
    /// create/resume; turns re-read the live session's
    /// `persistence_handle()` before persisting, which adopts native
    /// ids the agent reports mid-conversation.
    pub handle: Option<PersistenceHandle>,
    /// A turn is in flight — the idle sweep must not reap this session.
    pub busy: std::sync::atomic::AtomicBool,
    /// This session ever ran a turn (this process incarnation). The
    /// fresh-session grace below applies only until then — a session
    /// that has been used measures real disuse via its idle counter.
    pub ever_used: std::sync::atomic::AtomicBool,
    /// Creation time, for the fresh-session grace.
    pub created_at: Instant,
    /// Current-turn event journal for late-subscriber replay.
    pub journal: Arc<EventJournal>,
    /// Persona bound at create/resume time (bot id + composed block).
    /// The block is prepended to every turn like the memory block;
    /// the binding is fixed for the session's life — persona file
    /// edits apply to the NEXT session, never mid-conversation.
    pub persona: Option<crate::bots::Persona>,
}

impl ManagedSession {
    /// Subscribe to this session's cursor-tagged live fan-out — the same
    /// numbering the replay frames carry, so a connection can detect a
    /// hole instead of rendering one.
    pub fn subscribe_seq(&self) -> tokio::sync::broadcast::Receiver<SeqEvent> {
        self.journal.subscribe()
    }

    /// Cursor of the newest event fanned out for this session (0 = none).
    pub fn cursor(&self) -> u64 {
        self.journal.cursor()
    }
}

/// A session younger than this that has NEVER run a turn is not
/// reaped, whatever its idle counter says — the client's
/// create→turn.start gap must survive the sweep even when
/// `agent_idle_secs` is tiny. Used sessions reap on idle alone.
const FRESH_SESSION_GRACE: Duration = Duration::from_secs(60);

/// Registry of backend clients keyed by provider id.
pub struct SessionManager {
    clients: parking_lot::RwLock<HashMap<ProviderId, Arc<dyn AgentClient>>>,
    sessions: Mutex<HashMap<String, Arc<ManagedSession>>>,
    /// Default provider when session.create omits `backend`.
    default_provider: parking_lot::RwLock<Option<String>>,
    /// Cap on live sessions (config `max_sessions`); hot-reloaded.
    max_sessions: parking_lot::RwLock<Option<usize>>,
}

impl SessionManager {
    /// Build from daemon config: resolve every catalog backend against
    /// `[backends]` overrides, keep the detected ones, and wrap each in
    /// its client. Undetected backends are absent from the map — a
    /// session.create naming one fails with "not available".
    pub fn from_config(cfg: &crate::config::Config) -> Self {
        let resolved = crate::backend::registry::resolve_backends(&cfg.backends);
        let mut clients = HashMap::new();
        for r in resolved {
            if !r.detected {
                continue;
            }
            if let Some(client) = crate::backend::registry::client_for(&r) {
                clients.insert(r.id.clone(), client);
            }
        }
        let mgr = Self::new(clients, cfg.default_backend.clone());
        *mgr.max_sessions.write() = cfg.max_sessions;
        mgr
    }

    /// Replace the registered backend clients (tests isolate the
    /// scanner from the machine's real CLIs this way).
    pub fn set_clients(&self, clients: HashMap<ProviderId, Arc<dyn AgentClient>>) {
        *self.clients.write() = clients;
    }

    pub fn new(
        clients: HashMap<ProviderId, Arc<dyn AgentClient>>,
        default_provider: Option<String>,
    ) -> Self {
        Self {
            clients: parking_lot::RwLock::new(clients),
            sessions: Mutex::new(HashMap::new()),
            default_provider: parking_lot::RwLock::new(default_provider),
            max_sessions: parking_lot::RwLock::new(None),
        }
    }
    /// Register an extra backend client — tests inject in-process mocks
    /// this way instead of spawning a real agent CLI.
    #[doc(hidden)]
    pub fn insert_client(&self, id: ProviderId, client: Arc<dyn AgentClient>) {
        self.clients.write().insert(id, client);
    }

    /// Rebuild the client map after a config reload. Live sessions keep
    /// their existing backend objects; only new creates/resumes see the
    /// refreshed clients.
    pub fn refresh(&self, cfg: &crate::config::Config) {
        let resolved = crate::backend::registry::resolve_backends(&cfg.backends);
        let mut clients = HashMap::new();
        for r in resolved {
            if !r.detected {
                continue;
            }
            if let Some(client) = crate::backend::registry::client_for(&r) {
                clients.insert(r.id.clone(), client);
            }
        }
        *self.clients.write() = clients;
        *self.default_provider.write() = cfg.default_backend.clone();
        *self.max_sessions.write() = cfg.max_sessions;
    }

    /// All registered (provider id, client) pairs — for backend.list.
    pub fn clients(&self) -> Vec<(ProviderId, Arc<dyn AgentClient>)> {
        self.clients
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Session ids with a turn in flight — retention/idle sweeps skip them.
    pub async fn busy_ids(&self) -> std::collections::HashSet<String> {
        self.sessions
            .lock()
            .await
            .iter()
            .filter(|(_, m)| m.busy.load(std::sync::atomic::Ordering::Relaxed))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Provider ids with a detected CLI.
    pub fn available_backends(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.clients.read().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Resolve which backend a new session uses: explicit `backend`
    /// param, the configured default, or the first available.
    fn resolve_provider(&self, requested: Option<&str>) -> Result<ProviderId> {
        let clients = self.clients.read();
        if let Some(id) = requested {
            if clients.contains_key(id) {
                return Ok(id.to_string());
            }
            return Err(crate::rpc::RpcError::error(
                crate::rpc::error_code::BACKEND_UNAVAILABLE,
                format!("backend `{id}` is not available"),
            ));
        }
        if let Some(d) = self.default_provider.read().as_ref()
            && clients.contains_key(d)
        {
            return Ok(d.clone());
        }
        clients.keys().next().cloned().ok_or_else(|| {
            crate::rpc::RpcError::error(
                crate::rpc::error_code::BACKEND_UNAVAILABLE,
                "no backends available — install claude, codex, or omp",
            )
        })
    }
    /// Create a session on the resolved backend. Fails when the live
    /// session count is at `max_sessions` — the check runs before the
    /// backend process spawns and again at insert, so a creation race
    /// can only ever close its own orphan.
    pub async fn create(
        &self,
        backend: Option<&str>,
        config: SessionConfig,
        persona: Option<crate::bots::Persona>,
    ) -> Result<Arc<ManagedSession>> {
        let cap = *self.max_sessions.read();
        if let Some(cap) = cap
            && self.sessions.lock().await.len() >= cap
        {
            return Err(crate::rpc::RpcError::error(
                crate::rpc::error_code::SESSION_LIMIT,
                format!(
                    "session limit reached ({cap} live) — close, delete, or let idle sessions reap first"
                ),
            ));
        }
        let provider = self.resolve_provider(backend)?;
        let client = self.clients.read()[&provider].clone();
        let session = client.create_session(config).await?;
        let journal = spawn_journal(&session);
        let id = uuid::Uuid::new_v4().to_string();
        let managed = Arc::new(ManagedSession {
            id: id.clone(),
            provider,
            ever_used: std::sync::atomic::AtomicBool::new(false),
            handle: session.persistence_handle(),
            session: session.clone(),
            busy: std::sync::atomic::AtomicBool::new(false),
            created_at: Instant::now(),
            journal,
            persona,
        });
        let mut sessions = self.sessions.lock().await;
        let cap = *self.max_sessions.read();
        if let Some(cap) = cap
            && !sessions.contains_key(&id)
            && sessions.len() >= cap
        {
            drop(sessions);
            let _ = session.close().await;
            return Err(crate::rpc::RpcError::error(
                crate::rpc::error_code::SESSION_LIMIT,
                format!("session limit reached ({cap} live)"),
            ));
        }
        sessions.insert(id.clone(), managed.clone());
        Ok(managed)
    }

    /// Resume a persisted session on its recorded backend. Subject to
    /// the same `max_sessions` cap as create — a resume is a live
    /// session too.
    pub async fn resume(
        &self,
        session_id: &str,
        handle: &PersistenceHandle,
        config: SessionConfig,
        persona: Option<crate::bots::Persona>,
    ) -> Result<Arc<ManagedSession>> {
        let client = self
            .clients
            .read()
            .get(&handle.provider)
            .cloned()
            .ok_or_else(|| {
                crate::rpc::RpcError::error(
                    crate::rpc::error_code::BACKEND_UNAVAILABLE,
                    format!("backend `{}` not available", handle.provider),
                )
            })?;
        let cap = *self.max_sessions.read();
        let already_live = self.sessions.lock().await.contains_key(session_id);
        if let Some(cap) = cap
            && !already_live
            && self.sessions.lock().await.len() >= cap
        {
            return Err(crate::rpc::RpcError::error(
                crate::rpc::error_code::SESSION_LIMIT,
                format!(
                    "session limit reached ({cap} live) — close, delete, or let idle sessions reap first"
                ),
            ));
        }
        let session = client.resume_session(handle, config).await?;
        let journal = spawn_journal(&session);
        let managed = Arc::new(ManagedSession {
            id: session_id.to_string(),
            provider: handle.provider.clone(),
            handle: Some(handle.clone()),
            session,
            ever_used: std::sync::atomic::AtomicBool::new(false),
            busy: std::sync::atomic::AtomicBool::new(false),
            created_at: Instant::now(),
            journal,
            persona,
        });
        self.sessions
            .lock()
            .await
            .insert(session_id.to_string(), managed.clone());
        Ok(managed)
    }

    /// Look up a live session.
    pub async fn get(&self, session_id: &str) -> Option<Arc<ManagedSession>> {
        self.sessions.lock().await.get(session_id).cloned()
    }

    /// Number of live sessions (busy or idle) — the honest
    /// `damon_live_sessions` gauge.
    pub async fn live_count(&self) -> usize {
        self.sessions.lock().await.len()
    }

    /// One live session's introspection row — the `session.status`
    /// RPC. `idle_secs` comes from the backend session (its own
    /// idle clock, the same one the reaper uses).
    pub async fn live_status(&self) -> Vec<(String, ProviderId, bool, u64)> {
        let mut rows: Vec<(String, ProviderId, bool, u64)> = self
            .sessions
            .lock()
            .await
            .values()
            .map(|m| {
                (
                    m.id.clone(),
                    m.provider.clone(),
                    m.busy.load(std::sync::atomic::Ordering::Relaxed),
                    m.session.idle_secs(),
                )
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    /// Remove a session from the live map (does not close it).
    pub async fn detach(&self, session_id: &str) -> Option<Arc<ManagedSession>> {
        self.sessions.lock().await.remove(session_id)
    }

    /// Close and remove a session.
    pub async fn close(&self, session_id: &str) -> Result<()> {
        if let Some(m) = self.sessions.lock().await.remove(session_id) {
            m.session.close().await?;
        }
        Ok(())
    }

    /// Kill sessions idle longer than `max_idle`, skipping any with a
    /// live turn or still inside the fresh-session grace window (the
    /// client's create→turn.start gap must survive, whatever
    /// `agent_idle_secs` says). Returns how long until the next sweep
    /// is worthwhile.
    pub async fn reap_idle(&self, max_idle: Duration) -> Duration {
        let mut nearest = max_idle;
        let mut to_close: Vec<String> = vec![];
        {
            let sessions = self.sessions.lock().await;
            for (id, m) in sessions.iter() {
                if m.busy.load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                let fresh = !m.ever_used.load(std::sync::atomic::Ordering::Relaxed)
                    && m.created_at.elapsed() < FRESH_SESSION_GRACE;
                if fresh {
                    // Created/resumed but never prompted: the client's
                    // create→turn.start gap must survive, whatever
                    // `agent_idle_secs` says.
                    nearest = nearest.min(FRESH_SESSION_GRACE - m.created_at.elapsed());
                    continue;
                }
                let idle = Duration::from_secs(m.session.idle_secs());
                if idle >= max_idle {
                    to_close.push(id.clone());
                } else {
                    nearest = nearest.min(max_idle - idle);
                }
            }
        }
        for id in to_close {
            let _ = self.close(&id).await;
        }
        nearest.max(Duration::from_secs(1))
    }

    /// Close every session — daemon shutdown.
    pub async fn shutdown(&self) {
        let sessions: Vec<Arc<ManagedSession>> =
            self.sessions.lock().await.values().cloned().collect();
        for m in sessions {
            let _ = m.session.close().await;
        }
        self.sessions.lock().await.clear();
        let clients: Vec<Arc<dyn AgentClient>> = self.clients.read().values().cloned().collect();
        for client in clients {
            let _ = client.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assistant(text: &str) -> StreamEvent {
        StreamEvent::new(StreamEventKind::Timeline(TimelineItem::AssistantMessage {
            text: text.to_string(),
        }))
    }

    #[test]
    fn journal_keeps_only_the_inflight_turn() {
        let j = EventJournal::new();
        j.record(assistant("turn-1 text"));
        j.record(StreamEvent::new(StreamEventKind::TurnCompleted {
            usage: None,
        }));
        // The completed turn lives in the store — replaying it here
        // would duplicate every artifact.
        assert!(j.replayable().unwrap_or_default().is_empty());

        j.record(assistant("turn-2 delta"));
        let snap = j.replayable().unwrap_or_default();
        assert_eq!(snap.len(), 1);
        assert!(matches!(
            &snap[0].event.kind,
            StreamEventKind::Timeline(TimelineItem::AssistantMessage { text }) if text == "turn-2 delta"
        ));
    }

    #[test]
    fn journal_overflow_sheds_rather_than_replay_partial() {
        let j = EventJournal::new();
        for i in 0..=JOURNAL_CAP {
            j.record(assistant(&i.to_string()));
        }
        // A prefix with a hole in the middle replays garbled text —
        // shed entirely instead.
        assert!(j.replayable().is_none());
        // The cursor keeps counting, so a client can still see where
        // the hole is (and reload from the store around it).
        assert_eq!(j.cursor(), JOURNAL_CAP as u64 + 1);
    }

    #[test]
    fn cursor_counts_every_event_and_turn_boundaries_do_not_reset_it() {
        let j = EventJournal::new();
        assert_eq!(j.cursor(), 0);
        j.record(assistant("a"));
        j.record(StreamEvent::new(StreamEventKind::TurnCompleted {
            usage: None,
        }));
        j.record(assistant("b"));
        assert_eq!(j.cursor(), 3);
        // Frames carry the cursor the event was assigned.
        let snap = j.replayable().unwrap_or_default();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].seq, 3);
    }
}
