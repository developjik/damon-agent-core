//! SessionManager — maps Damon session ids to live backend sessions.
//!
//! One Damon session owns one `AgentSession` (which owns its process).
//! The manager routes prompts, permission answers, and interrupts, and
//! reaps idle sessions so unused agent processes don't pile up —
//! reattach happens through the persistence handle on next use.
//!
//! `#16`: the mutable picture of a session (phase, open turn, parked ask,
//! timestamps, rollups, handle) lives in [`crate::state`], a pure data
//! module with no process, task, or file access. This file is the runtime
//! around it: it owns the processes and the event journals, feeds
//! transitions into the ledger, and writes the ledger out as the
//! `sessions.state.json` checkpoint so a killed daemon can come back with
//! a coherent picture instead of an empty one.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Mutex;

use crate::backend::types::*;
use crate::backend::{AgentClient, AgentSession};
use crate::state::{
    AskKind, PendingAsk, Phase, RECOVERED_TTL_MS, SessionLedger, SessionRecord, TurnClaim,
    TurnOutcome,
};

/// Wall-clock milliseconds. Session state is stamped in wall time so it
/// survives a restart in the checkpoint (a monotonic clock is meaningful
/// only inside one process).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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

/// The pure session ledger plus the checkpoint file it is written to.
///
/// The lock is short-lived by construction: every transition takes it,
/// mutates the record, releases it, and only then serializes + writes.
/// A session only transitions at turn/ask boundaries, so the write cost
/// (a few kilobytes, tmp file + rename) is paid a handful of times per
/// turn — and paying it inline keeps the checkpoint exactly in step with
/// the last applied transition, which is what makes the crash test
/// deterministic.
struct SessionState {
    ledger: parking_lot::Mutex<SessionLedger>,
    checkpoint: Option<PathBuf>,
}

impl SessionState {
    fn new(checkpoint: Option<PathBuf>) -> Self {
        Self {
            ledger: parking_lot::Mutex::new(SessionLedger::new()),
            checkpoint,
        }
    }

    /// Serialize the ledger and replace the checkpoint atomically. A
    /// failed write is logged, never fatal: the ledger is the truth in
    /// this incarnation, the file is only the next one's evidence.
    fn save(&self) {
        let Some(path) = &self.checkpoint else {
            return;
        };
        let now = now_ms();
        let bytes = {
            let mut g = self.ledger.lock();
            g.stamp(now);
            match g.to_bytes() {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "session checkpoint not serialized");
                    return;
                }
            }
        };
        if let Err(e) = write_checkpoint(path, &bytes) {
            tracing::warn!(path = %path.display(), error = %e, "session checkpoint not written");
        }
    }

    fn get(&self, id: &str) -> Option<SessionRecord> {
        self.ledger.lock().get(id).cloned()
    }

    fn active_ids(&self) -> std::collections::HashSet<String> {
        self.ledger.lock().active_ids().into_iter().collect()
    }

    fn counts(&self) -> crate::state::Counts {
        self.ledger.lock().counts()
    }

    fn recovered(&self) -> Vec<SessionRecord> {
        self.ledger.lock().recovered()
    }

    fn open(
        &self,
        id: &str,
        provider: &str,
        native_handle: Option<String>,
        persona: Option<String>,
    ) {
        self.ledger
            .lock()
            .open(id, provider, native_handle, persona, now_ms());
        self.save();
    }

    /// Adopt the native session id the backend reports for a session
    /// created before the process had one.
    fn note_handle(&self, id: &str, native_handle: Option<String>) {
        let Some(native_handle) = native_handle else {
            return;
        };
        if self.ledger.lock().set_native_handle(id, native_handle) {
            self.save();
        }
    }

    fn remove(&self, id: &str) -> Option<SessionRecord> {
        let removed = self.ledger.lock().remove(id);
        if removed.is_some() {
            self.save();
        }
        removed
    }

    /// Claim the session for a turn. `false` means another turn (or a
    /// parked ask) already holds it.
    fn begin_turn(&self, id: &str) -> bool {
        let claim = self.ledger.lock().begin_turn(id, now_ms());
        if claim == TurnClaim::Claimed {
            self.save();
        }
        claim == TurnClaim::Claimed
    }

    fn note_turn_started(&self, id: &str, turn_id: &str) {
        if self.ledger.lock().note_turn_started(id, turn_id, now_ms()) {
            self.save();
        }
    }

    fn end_turn(&self, id: &str, turn_id: Option<&str>, outcome: TurnOutcome) -> bool {
        let settled = self.ledger.lock().end_turn(id, turn_id, outcome, now_ms());
        if settled {
            self.save();
        }
        settled
    }

    fn ask_opened(&self, id: &str, ask: PendingAsk) {
        if self.ledger.lock().ask_opened(id, ask, now_ms()) {
            self.save();
        }
    }

    fn ask_settled(&self, id: &str, request_id: &str) {
        if self.ledger.lock().ask_settled(id, request_id, now_ms()) {
            self.save();
        }
    }

    /// Fold the events the runtime already forwards into the ledger, so
    /// the parked-ask half of the state is derived from the same stream
    /// every surface sees (an ask parks the turn on the user, its
    /// resolution resumes it). Non-state events cost one match and no
    /// lock.
    ///
    /// Turn ENDS are deliberately not taken from here: the boundary event
    /// reaches this tap a moment before `drive_turn` has finished
    /// persisting the turn, and freeing the claim that early would let a
    /// second `turn.start` run beside the first one's writes. Only the
    /// collector (or its drop-guard) releases the claim.
    fn observe(&self, id: &str, ev: &StreamEvent) {
        match &ev.kind {
            StreamEventKind::TurnStarted => {
                // The id is filled in by `note_turn_started` once the
                // backend reports it; an unclaimed turn (a resumed
                // session driving itself) is adopted here.
                let _ = self.ledger.lock().note_turn_started(id, "", now_ms());
            }
            StreamEventKind::PermissionRequested(req) => {
                let ask = PendingAsk {
                    request_id: req.id.clone(),
                    kind: match req.kind {
                        PermissionKind::Tool => AskKind::Permission,
                        PermissionKind::Question => AskKind::Question,
                        PermissionKind::Plan => AskKind::Plan,
                    },
                    label: req.title.clone().or_else(|| Some(req.name.clone())),
                    asked_at_ms: now_ms(),
                };
                self.ask_opened(id, ask);
            }
            StreamEventKind::PermissionResolved { request_id } => {
                self.ask_settled(id, request_id);
            }
            _ => {}
        }
    }
}

/// Replace the checkpoint atomically: a reader (the next boot) either
/// sees the previous document or the new one, never a half-written mix.
/// The file lists live session ids and native handles, so it gets the
/// same private permissions as the store.
///
/// No `fsync`: the failure this document guards against is the daemon
/// process dying, after which the page cache still holds the write. A
/// machine losing power loses the last transitions too — the turn ledger
/// in SQLite is the durable record for that case, and the checkpoint is
/// only ever a live picture.
fn write_checkpoint(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        crate::store::tighten_permissions(parent, 0o700);
    }
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "sessions.state.json".to_string());
    let tmp = path.with_file_name(format!("{name}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
    }
    #[cfg(unix)]
    crate::store::tighten_permissions(&tmp, 0o600);
    std::fs::rename(&tmp, path)
}

/// Resident subscriber that journals a session's events for
/// late-subscriber replay and folds the same events into the session
/// ledger. Exits when the backend event channel closes (session closed or
/// reaped) — reaped-and-resumed sessions start a fresh journal; the store
/// covers everything before that.
fn spawn_journal(
    state: Arc<SessionState>,
    session_id: String,
    session: &Arc<dyn AgentSession>,
) -> Arc<EventJournal> {
    let journal = Arc::new(EventJournal::new());
    let j = journal.clone();
    let mut rx = session.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    state.observe(&session_id, &ev);
                    j.record(ev);
                }
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
///
/// Everything mutable about the session (phase, open turn, parked ask,
/// timestamps, rollups, handle) is in the ledger — this struct holds only
/// what a process and a subscription need.
pub struct ManagedSession {
    /// Damon-facing session id.
    pub id: String,
    /// The backend that owns this session.
    pub provider: ProviderId,
    /// The live backend session object.
    pub session: Arc<dyn AgentSession>,
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

/// One row of `session.status` — the live process plus the ledger's view
/// of what it is doing.
#[derive(Debug, Clone)]
pub struct LiveStatus {
    pub id: String,
    pub provider: ProviderId,
    pub phase: Phase,
    pub idle_secs: u64,
    /// The open turn's id, when the backend has reported it.
    pub turn_id: Option<String>,
    pub pending_ask: Option<PendingAsk>,
}

/// What a boot restore found in the checkpoint: the sessions the previous
/// incarnation left mid-turn or parked on an answer, already converged to
/// `Idle` in the live ledger.
#[derive(Debug, Clone)]
pub struct RestoreReport {
    pub path: PathBuf,
    pub recovered: Vec<SessionRecord>,
    pub pruned: usize,
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
    /// The pure state (#16) — phases, turns, asks, handles, rollups.
    state: Arc<SessionState>,
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
        Self::from_config_with_checkpoint(cfg, None)
    }

    /// As [`SessionManager::from_config`], with the checkpoint document
    /// the live session ledger is written to (and restored from).
    pub fn from_config_with_checkpoint(
        cfg: &crate::config::Config,
        checkpoint: Option<PathBuf>,
    ) -> Self {
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
        let mgr = Self::new_with_checkpoint(clients, cfg.default_backend.clone(), checkpoint);
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
        Self::new_with_checkpoint(clients, default_provider, None)
    }

    /// A manager whose ledger is checkpointed to `checkpoint` (the
    /// daemon passes the store's neighbour file; tests that do not care
    /// pass `None` and keep everything in memory).
    pub fn new_with_checkpoint(
        clients: HashMap<ProviderId, Arc<dyn AgentClient>>,
        default_provider: Option<String>,
        checkpoint: Option<PathBuf>,
    ) -> Self {
        Self {
            clients: parking_lot::RwLock::new(clients),
            sessions: Mutex::new(HashMap::new()),
            state: Arc::new(SessionState::new(checkpoint)),
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

    /// Session ids with a turn in flight or an ask parked on the user —
    /// retention/idle sweeps skip them.
    pub async fn busy_ids(&self) -> std::collections::HashSet<String> {
        self.state.active_ids()
    }

    /// Provider ids with a detected CLI.
    pub fn available_backends(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.clients.read().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// One session's pure record, as of the last applied transition.
    pub fn record(&self, session_id: &str) -> Option<SessionRecord> {
        self.state.get(session_id)
    }

    /// The session's persisted-backend handle (native id), read from the
    /// ledger so a resumed session adopts ids the agent reports mid-turn.
    pub fn handle_of(&self, session_id: &str) -> Option<PersistenceHandle> {
        let record = self.state.get(session_id)?;
        let native = record.native_handle?;
        Some(PersistenceHandle {
            provider: record.provider,
            native_handle: native,
            metadata: serde_json::Value::Null,
        })
    }

    /// Sessions the previous incarnations left mid-turn or waiting for an
    /// answer, converged to idle (newest first). These are tombstones:
    /// resuming the session clears its mark.
    pub fn recovered_sessions(&self) -> Vec<SessionRecord> {
        self.state.recovered()
    }

    /// Ledger tallies — `/metrics` and `session.status`.
    pub fn session_counts(&self) -> crate::state::Counts {
        self.state.counts()
    }

    /// Claim the session for a turn. `false` means the session already
    /// has one in flight (or parked on an answer) — the dispatcher turns
    /// that into `-32006`.
    pub fn begin_turn(&self, session_id: &str) -> bool {
        self.state.begin_turn(session_id)
    }

    /// Record the backend's turn id once the prompt is accepted.
    pub fn note_turn_started(&self, session_id: &str, turn_id: &str) {
        self.state.note_turn_started(session_id, turn_id);
    }

    /// Release the turn claim and settle its rollup. Idempotent: the
    /// collector and its drop-guard both call it, only the first one
    /// lands.
    pub fn end_turn(&self, session_id: &str, turn_id: Option<&str>, outcome: TurnOutcome) {
        self.state.end_turn(session_id, turn_id, outcome);
    }

    /// Load the checkpoint written by a previous incarnation and converge
    /// it: anything that was `busy` or `awaiting_input` becomes `idle`
    /// with a `recovered` mark (its process died with the old daemon).
    /// Also prunes tombstones past their TTL and rewrites the file, so a
    /// second crash cannot report the same interruption twice.
    pub fn restore(&self) -> Option<RestoreReport> {
        let path = self.state.checkpoint.clone()?;
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "session checkpoint unreadable — starting empty");
                return None;
            }
        };
        let mut ledger = match SessionLedger::from_bytes(&bytes) {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "session checkpoint ignored");
                return None;
            }
        };
        let now = now_ms();
        let recovered = ledger.recover_previous_incarnation(now);
        let pruned = ledger.prune_recovered(now, RECOVERED_TTL_MS);
        *self.state.ledger.lock() = ledger;
        self.state.save();
        Some(RestoreReport {
            path,
            recovered,
            pruned,
        })
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
        // The id and its ledger row exist before the process does: an
        // event the journal sees in the first microsecond must land in a
        // record, not create a phantom one.
        let id = uuid::Uuid::new_v4().to_string();
        self.state.open(
            &id,
            &provider,
            None,
            persona.as_ref().map(|p| p.bot.clone()),
        );
        let session = match client.create_session(config).await {
            Ok(s) => s,
            Err(e) => {
                self.state.remove(&id);
                return Err(e);
            }
        };
        self.state
            .note_handle(&id, session.persistence_handle().map(|h| h.native_handle));
        let journal = spawn_journal(self.state.clone(), id.clone(), &session);
        let managed = Arc::new(ManagedSession {
            id: id.clone(),
            provider,
            session: session.clone(),
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
            self.state.remove(&id);
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
        // Re-opening the id clears any recovered tombstone: the session
        // is live again, so nothing is "left behind" any more.
        self.state.open(
            session_id,
            &handle.provider,
            Some(handle.native_handle.clone()),
            persona.as_ref().map(|p| p.bot.clone()),
        );
        let journal = spawn_journal(self.state.clone(), session_id.to_string(), &session);
        let managed = Arc::new(ManagedSession {
            id: session_id.to_string(),
            provider: handle.provider.clone(),
            session,
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
    /// idle clock, the same one the reaper uses); the phase, open turn,
    /// and parked ask come from the ledger.
    pub async fn live_status(&self) -> Vec<LiveStatus> {
        let mut rows: Vec<LiveStatus> = self
            .sessions
            .lock()
            .await
            .values()
            .map(|m| {
                let record = self.state.get(&m.id);
                LiveStatus {
                    id: m.id.clone(),
                    provider: m.provider.clone(),
                    phase: record.as_ref().map(|r| r.phase).unwrap_or(Phase::Idle),
                    idle_secs: m.session.idle_secs(),
                    turn_id: record
                        .as_ref()
                        .and_then(|r| r.open_turn.as_ref())
                        .map(|t| t.turn_id.clone())
                        .filter(|t| !t.is_empty()),
                    pending_ask: record.and_then(|r| r.pending_ask),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        rows
    }

    /// Remove a session from the live map (does not close it).
    pub async fn detach(&self, session_id: &str) -> Option<Arc<ManagedSession>> {
        let removed = self.sessions.lock().await.remove(session_id);
        if removed.is_some() {
            self.state.remove(session_id);
        }
        removed
    }

    /// Close and remove a session.
    pub async fn close(&self, session_id: &str) -> Result<()> {
        let removed = self.sessions.lock().await.remove(session_id);
        self.state.remove(session_id);
        if let Some(m) = removed {
            m.session.close().await?;
        }
        Ok(())
    }

    /// Kill sessions idle longer than `max_idle`, skipping any with a
    /// live turn, a parked ask, or still inside the fresh-session grace
    /// window (the client's create→turn.start gap must survive, whatever
    /// `agent_idle_secs` says). Returns how long until the next sweep
    /// is worthwhile.
    pub async fn reap_idle(&self, max_idle: Duration) -> Duration {
        let mut nearest = max_idle;
        let mut to_close: Vec<String> = vec![];
        let now = now_ms();
        {
            let sessions = self.sessions.lock().await;
            for (id, m) in sessions.iter() {
                let record = self.state.get(id);
                let phase = record.as_ref().map(|r| r.phase).unwrap_or(Phase::Idle);
                if phase.is_active() {
                    // A turn in flight or an ask parked on the user: the
                    // backend is not idle in the sense this sweep means.
                    continue;
                }
                let created = record.as_ref().map(|r| r.created_at_ms).unwrap_or(now);
                let ever_used = record.as_ref().is_some_and(|r| r.ever_used);
                let age = Duration::from_millis(now.saturating_sub(created));
                if !ever_used && age < FRESH_SESSION_GRACE {
                    // Created/resumed but never prompted: the client's
                    // create→turn.start gap must survive, whatever
                    // `agent_idle_secs` says.
                    nearest = nearest.min(FRESH_SESSION_GRACE - age);
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

    /// Close every session — daemon shutdown. The checkpoint is cleared
    /// first: an orderly shutdown leaves nothing "left behind", so the
    /// next boot must not report this incarnation's sessions as
    /// recovered.
    pub async fn shutdown(&self) {
        let sessions: Vec<Arc<ManagedSession>> =
            self.sessions.lock().await.values().cloned().collect();
        for m in sessions {
            let _ = m.session.close().await;
        }
        self.sessions.lock().await.clear();
        {
            let mut g = self.state.ledger.lock();
            *g = SessionLedger::new();
        }
        self.state.save();
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

    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-session-state-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The checkpoint is the previous incarnation's evidence: rewrite it
    /// as a daemon that died mid-turn would have left it, then restore.
    #[test]
    fn restore_converges_a_checkpoint_left_mid_turn() {
        let dir = tempdir();
        let path = dir.join("sessions.state.json");
        let mut ledger = SessionLedger::new();
        ledger.open("live-busy", "claude", None, None, now_ms());
        ledger.open("live-waiting", "codex", None, None, now_ms());
        ledger.open("live-idle", "claude", None, None, now_ms());
        ledger.begin_turn("live-busy", now_ms());
        ledger.ask_opened(
            "live-waiting",
            PendingAsk {
                request_id: "req-1".into(),
                kind: AskKind::Permission,
                label: Some("Bash".into()),
                asked_at_ms: now_ms(),
            },
            now_ms(),
        );
        std::fs::write(&path, ledger.to_bytes().unwrap()).unwrap();

        let mgr = SessionManager::new_with_checkpoint(HashMap::new(), None, Some(path.clone()));
        assert_eq!(mgr.session_counts().active(), 0, "nothing live yet");
        let report = mgr.restore().expect("checkpoint present");
        assert_eq!(report.recovered.len(), 2);
        assert_eq!(mgr.session_counts().active(), 0, "no ghost is live");
        assert_eq!(mgr.recovered_sessions().len(), 2);
        assert_eq!(mgr.record("live-busy").unwrap().phase, Phase::Idle);
        assert_eq!(
            mgr.record("live-waiting").unwrap().phase,
            Phase::Idle,
            "an ask left parked by a dead daemon is not still waiting"
        );
        assert!(mgr.record("live-idle").unwrap().recovered.is_none());

        // The converged document was written back: a second boot (a
        // second crash before any work) reports nothing twice.
        let mgr2 = SessionManager::new_with_checkpoint(HashMap::new(), None, Some(path));
        let report2 = mgr2.restore().expect("checkpoint present");
        assert!(report2.recovered.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_corrupt_checkpoint_is_ignored_not_fatal() {
        let dir = tempdir();
        let path = dir.join("sessions.state.json");
        std::fs::write(&path, b"{ not a checkpoint").unwrap();
        let mgr = SessionManager::new_with_checkpoint(HashMap::new(), None, Some(path));
        assert!(mgr.restore().is_none());
        assert_eq!(mgr.session_counts().sessions, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_turn_claim_through_the_manager_is_exclusive_and_settles_once() {
        let mgr = SessionManager::new(HashMap::new(), None);
        mgr.state.open("s1", "claude", Some("native".into()), None);
        assert!(mgr.begin_turn("s1"));
        assert!(!mgr.begin_turn("s1"));
        mgr.note_turn_started("s1", "turn-1");
        mgr.end_turn("s1", Some("turn-1"), TurnOutcome::Completed);
        let record = mgr.record("s1").unwrap();
        assert_eq!(record.phase, Phase::Idle);
        assert_eq!(record.turns_started, 1);
        assert_eq!(record.turns_settled, 1);
        assert_eq!(
            mgr.handle_of("s1").map(|h| h.native_handle),
            Some("native".to_string())
        );
    }

    #[tokio::test]
    async fn observing_a_permission_ask_parks_and_a_boundary_frees_the_session() {
        let mgr = SessionManager::new(HashMap::new(), None);
        mgr.state.open("s1", "mock", None, None);
        assert!(mgr.begin_turn("s1"));
        mgr.state.observe(
            "s1",
            &StreamEvent::new(StreamEventKind::PermissionRequested(PermissionRequest {
                id: "req-1".into(),
                kind: PermissionKind::Tool,
                name: "Bash".into(),
                title: None,
                input: None,
                detail: None,
                actions: Vec::new(),
                suggestions: Vec::new(),
                questions: Vec::new(),
            })),
        );
        let record = mgr.record("s1").unwrap();
        assert_eq!(record.phase, Phase::AwaitingInput);
        assert_eq!(
            record.pending_ask.as_ref().unwrap().label.as_deref(),
            Some("Bash")
        );
        assert_eq!(
            mgr.busy_ids().await.len(),
            1,
            "parked asks are not reapable"
        );

        mgr.state.observe(
            "s1",
            &StreamEvent::new(StreamEventKind::PermissionResolved {
                request_id: "req-1".into(),
            }),
        );
        assert_eq!(mgr.record("s1").unwrap().phase, Phase::Busy);

        // The turn boundary itself does NOT free the claim (the collector
        // still has rows to persist) — its settlement does.
        mgr.state.observe(
            "s1",
            &StreamEvent::new(StreamEventKind::TurnCompleted { usage: None }),
        );
        assert_eq!(mgr.record("s1").unwrap().phase, Phase::Busy);
        mgr.end_turn("s1", None, TurnOutcome::Completed);
        let record = mgr.record("s1").unwrap();
        assert_eq!(record.phase, Phase::Idle);
        assert_eq!(record.turns_settled, 1);
    }
}
