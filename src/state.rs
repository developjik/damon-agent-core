//! Pure, serializable session state — the live picture as plain data.
//!
//! `#16`: the session layer used to keep its state inside the runtime
//! objects (`ManagedSession`'s busy flag, handle, timestamps, plus a
//! `HashMap` of them behind a task mutex). That made every state question
//! ("is this session free?", "which session is waiting for an answer?")
//! unanswerable without the process, and untestable without spawning one.
//!
//! This module holds the state instead: a [`SessionLedger`] of
//! [`SessionRecord`]s, with transitions as methods that take plain data in
//! and leave new state out. Nothing here touches a process, a file, a
//! socket, or a runtime handle — `SessionManager` (the runtime, in
//! `session.rs`) owns those and applies the transitions. The boundary is
//! enforced by `tests/state_boundary.rs`, which reads this file's own
//! source and fails the build gate if a runtime import appears — the
//! module literally cannot reach process or task types without breaking
//! the guard.
//!
//! The same record is the daemon's checkpoint document
//! (`sessions.state.json` next to the store, version [`SNAPSHOT_VERSION`]).
//! A daemon killed mid-turn leaves that file holding a `busy` or
//! `awaiting_input` phase; the next boot converges those to `idle` with a
//! [`Recovered`] mark instead of reporting a ghost session as live
//! (the same rule the turn ledger keeps for turns and the mission runner
//! for runs).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Version of the checkpoint document. A file written by a newer daemon
/// is refused as a whole (never half-read), which is why this is checked
/// before anything else.
pub const SNAPSHOT_VERSION: u32 = 1;

/// How long a session that was mid-turn (or waiting for an answer) when
/// the previous daemon died stays listed as recovered. Long enough for a
/// client to reopen and see what happened, short enough that the
/// checkpoint does not turn into a history file.
pub const RECOVERED_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Where a session stands. `Idle` is the only state the idle sweep may
/// reap; `Busy` has a turn running; `AwaitingInput` has the turn parked
/// on a permission/question answer (also not reapable — the user still
/// owes the agent a decision).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Idle,
    Busy,
    AwaitingInput,
}

impl Phase {
    /// The wire spelling used by `session.status`.
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Busy => "busy",
            Phase::AwaitingInput => "awaiting_input",
        }
    }

    /// A session with a turn in flight or parked on the user is not
    /// reapable and not removable by the retention sweep.
    pub fn is_active(self) -> bool {
        !matches!(self, Phase::Idle)
    }
}

/// What kind of answer the agent is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AskKind {
    Permission,
    Question,
    Plan,
}

impl AskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AskKind::Permission => "permission",
            AskKind::Question => "question",
            AskKind::Plan => "plan",
        }
    }
}

/// One ask parked on the user. The answer itself never lands here (its
/// content is the client's business); the ledger only records that the
/// turn is blocked and on which request, so a crash-restored picture can
/// name it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingAsk {
    pub request_id: String,
    pub kind: AskKind,
    /// Human-facing label — the tool name ("Bash") or the ask title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub asked_at_ms: u64,
}

/// The turn a session is running. `turn_id` is empty until the backend
/// hands one over (`turn.start` claims the session before the process
/// reports its id).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenTurn {
    pub turn_id: String,
    pub started_at_ms: u64,
}

/// How a turn ended. `Interrupted` is only ever set by boot recovery —
/// no live path can observe a turn of its own incarnation ending that way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnOutcome {
    Completed,
    Canceled,
    Failed,
    Interrupted,
}

impl TurnOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            TurnOutcome::Completed => "completed",
            TurnOutcome::Canceled => "canceled",
            TurnOutcome::Failed => "failed",
            TurnOutcome::Interrupted => "interrupted",
        }
    }
}

/// Boot-recovery evidence: this session was not idle when the previous
/// daemon stopped, and has since been converged to `Idle`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Recovered {
    pub at_ms: u64,
    pub from_phase: Phase,
    /// The turn that was running (or parked on an ask) when the daemon
    /// died, when the backend had reported its id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
}

/// One session, as plain data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRecord {
    pub id: String,
    /// Backend id (`claude`, `codex`, …). Empty only for a placeholder
    /// inserted because a turn arrived for an id the runtime had not
    /// registered yet.
    #[serde(default)]
    pub provider: String,
    /// Native backend session id, when the backend has one — the handle a
    /// resume needs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_handle: Option<String>,
    /// Bound bot id (`session.create` persona), when one was given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persona: Option<String>,
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_turn: Option<OpenTurn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_ask: Option<PendingAsk>,
    pub created_at_ms: u64,
    pub last_active_ms: u64,
    /// Rollups: turns claimed, turns that reached a boundary, turns a
    /// dead daemon left open, asks raised.
    #[serde(default)]
    pub turns_started: u64,
    #[serde(default)]
    pub turns_settled: u64,
    #[serde(default)]
    pub turns_interrupted: u64,
    #[serde(default)]
    pub asks_asked: u64,
    /// Whether a turn ever ran on this session (this incarnation or a
    /// recorded one) — the fresh-session reap grace ends here.
    #[serde(default)]
    pub ever_used: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovered: Option<Recovered>,
}

impl SessionRecord {
    /// Whether the idle sweep must leave this session alone.
    pub fn is_active(&self) -> bool {
        self.phase.is_active()
    }
}

/// The result of claiming a session for a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnClaim {
    Claimed,
    /// Another turn is already in flight (or parked on the user).
    AlreadyActive,
}

/// Ledger-wide tallies — the numbers `/metrics` and `session.status`
/// expose without walking the map twice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub sessions: usize,
    pub busy: usize,
    pub awaiting_input: usize,
}

impl Counts {
    /// Anything the idle sweep must not touch.
    pub fn active(&self) -> usize {
        self.busy + self.awaiting_input
    }
}

/// The pure session state: every live session's record, plus the
/// checkpoint stamp.
///
/// Transitions are `&mut self` methods that take plain values (ids,
/// timestamps, outcomes) and return plain values (claims, hits). No
/// method waits, writes, or spawns — the runtime does all of that around
/// them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionLedger {
    pub version: u32,
    /// Unix-ms stamp of the last transition, stamped by the runtime right
    /// before it writes the document.
    #[serde(default)]
    pub saved_at_ms: u64,
    sessions: BTreeMap<String, SessionRecord>,
}

impl Default for SessionLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionLedger {
    pub fn new() -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            saved_at_ms: 0,
            sessions: BTreeMap::new(),
        }
    }

    /// Read a checkpoint document. A version this build does not speak is
    /// refused as a whole — a half-understood checkpoint would restore a
    /// wrong picture, which is worse than an empty one.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let parsed: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("unreadable checkpoint: {e}"))?;
        if parsed.version > SNAPSHOT_VERSION {
            return Err(format!(
                "checkpoint version {} is newer than this daemon ({SNAPSHOT_VERSION})",
                parsed.version
            ));
        }
        Ok(parsed)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec_pretty(self).map_err(|e| format!("unserializable checkpoint: {e}"))
    }

    pub fn stamp(&mut self, now_ms: u64) {
        self.saved_at_ms = now_ms;
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    pub fn get(&self, id: &str) -> Option<&SessionRecord> {
        self.sessions.get(id)
    }

    pub fn records(&self) -> impl Iterator<Item = &SessionRecord> {
        self.sessions.values()
    }

    pub fn phase(&self, id: &str) -> Option<Phase> {
        self.sessions.get(id).map(|r| r.phase)
    }

    pub fn is_active(&self, id: &str) -> bool {
        self.sessions.get(id).is_some_and(|r| r.is_active())
    }

    /// Ids with a turn in flight or an ask parked on the user — exactly
    /// the set the idle/retention sweeps must skip.
    pub fn active_ids(&self) -> BTreeSet<String> {
        self.sessions
            .values()
            .filter(|r| r.is_active())
            .map(|r| r.id.clone())
            .collect()
    }

    pub fn counts(&self) -> Counts {
        let mut counts = Counts {
            sessions: self.sessions.len(),
            ..Counts::default()
        };
        for r in self.sessions.values() {
            match r.phase {
                Phase::Busy => counts.busy += 1,
                Phase::AwaitingInput => counts.awaiting_input += 1,
                Phase::Idle => {}
            }
        }
        counts
    }

    /// Sessions the previous daemon left mid-turn or waiting for an
    /// answer, newest first. These are tombstones — restarting or
    /// resuming the session clears the mark.
    pub fn recovered(&self) -> Vec<SessionRecord> {
        let mut rows: Vec<SessionRecord> = self
            .sessions
            .values()
            .filter(|r| r.recovered.is_some())
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            let at = |r: &SessionRecord| r.recovered.as_ref().map(|m| m.at_ms);
            at(b).cmp(&at(a))
        });
        rows
    }

    /// Register a session (create or resume). An existing record is
    /// replaced: a resume re-opens the same id with a fresh handle, and a
    /// recovered tombstone loses its mark because the session is live
    /// again.
    pub fn open(
        &mut self,
        id: &str,
        provider: &str,
        native_handle: Option<String>,
        persona: Option<String>,
        now_ms: u64,
    ) {
        self.sessions.insert(
            id.to_string(),
            SessionRecord {
                id: id.to_string(),
                provider: provider.to_string(),
                native_handle,
                persona,
                phase: Phase::Idle,
                open_turn: None,
                pending_ask: None,
                created_at_ms: now_ms,
                last_active_ms: now_ms,
                turns_started: 0,
                turns_settled: 0,
                turns_interrupted: 0,
                asks_asked: 0,
                ever_used: false,
                recovered: None,
            },
        );
    }

    /// Claim the session for a turn. The claim is the single-exclusion
    /// point for concurrent `turn.start`s (it replaces the old atomic
    /// flag): only one caller can move `Idle` → `Busy`.
    ///
    /// A session id the runtime has not registered yet gets a placeholder
    /// record rather than a refusal — bookkeeping must never be the
    /// reason a turn that is actually running has no row.
    pub fn begin_turn(&mut self, id: &str, now_ms: u64) -> TurnClaim {
        let record = self
            .sessions
            .entry(id.to_string())
            .or_insert_with(|| SessionRecord {
                id: id.to_string(),
                provider: String::new(),
                native_handle: None,
                persona: None,
                phase: Phase::Idle,
                open_turn: None,
                pending_ask: None,
                created_at_ms: now_ms,
                last_active_ms: now_ms,
                turns_started: 0,
                turns_settled: 0,
                turns_interrupted: 0,
                asks_asked: 0,
                ever_used: false,
                recovered: None,
            });
        if record.phase.is_active() {
            return TurnClaim::AlreadyActive;
        }
        record.phase = Phase::Busy;
        record.open_turn = Some(OpenTurn {
            turn_id: String::new(),
            started_at_ms: now_ms,
        });
        record.turns_started += 1;
        record.ever_used = true;
        record.last_active_ms = now_ms;
        TurnClaim::Claimed
    }

    /// Attach the backend's turn id to the turn already claimed. Backends
    /// report it after the prompt is accepted, so `turn.start` claims a
    /// session whose open turn has no id yet.
    pub fn note_turn_started(&mut self, id: &str, turn_id: &str, now_ms: u64) -> bool {
        let Some(record) = self.sessions.get_mut(id) else {
            return false;
        };
        // A backend may also report a turn nobody claimed (a resumed
        // session driving itself): adopt it instead of dropping it.
        if record.phase == Phase::Idle {
            record.phase = Phase::Busy;
            record.turns_started += 1;
            record.ever_used = true;
        }
        let open = record.open_turn.get_or_insert(OpenTurn {
            turn_id: String::new(),
            started_at_ms: now_ms,
        });
        if open.turn_id.is_empty() {
            open.turn_id = turn_id.to_string();
        }
        record.last_active_ms = now_ms;
        true
    }

    /// Settle the open turn. Idempotent: a settlement the runtime already
    /// applied (the collector first, its drop-guard second) is a no-op,
    /// so a turn is never counted twice. A `turn_id` that does not match
    /// the open turn means a different turn is running — leave it alone.
    pub fn end_turn(
        &mut self,
        id: &str,
        turn_id: Option<&str>,
        outcome: TurnOutcome,
        now_ms: u64,
    ) -> bool {
        let Some(record) = self.sessions.get_mut(id) else {
            return false;
        };
        if let Some(open) = &record.open_turn
            && let Some(given) = turn_id
            && !open.turn_id.is_empty()
            && open.turn_id != given
        {
            return false;
        }
        if !record.phase.is_active() {
            // Already settled (or a guard racing the event stream).
            record.open_turn = None;
            record.pending_ask = None;
            return false;
        }
        record.phase = Phase::Idle;
        record.open_turn = None;
        record.pending_ask = None;
        record.last_active_ms = now_ms;
        match outcome {
            TurnOutcome::Interrupted => record.turns_interrupted += 1,
            _ => record.turns_settled += 1,
        }
        true
    }

    /// The agent parked its turn on a permission/question answer.
    pub fn ask_opened(&mut self, id: &str, ask: PendingAsk, now_ms: u64) -> bool {
        let Some(record) = self.sessions.get_mut(id) else {
            return false;
        };
        record.phase = Phase::AwaitingInput;
        record.pending_ask = Some(ask);
        record.asks_asked += 1;
        record.last_active_ms = now_ms;
        true
    }

    /// The ask was answered, withdrawn, or died with its turn. A request
    /// id that is not the parked one is ignored — the one-shot answer
    /// belongs to whoever claimed it first.
    pub fn ask_settled(&mut self, id: &str, request_id: &str, now_ms: u64) -> bool {
        let Some(record) = self.sessions.get_mut(id) else {
            return false;
        };
        let matches = record
            .pending_ask
            .as_ref()
            .is_some_and(|a| a.request_id == request_id);
        if !matches {
            return false;
        }
        record.pending_ask = None;
        // The turn is still running unless it also ended.
        record.phase = if record.open_turn.is_some() {
            Phase::Busy
        } else {
            Phase::Idle
        };
        record.last_active_ms = now_ms;
        true
    }

    /// Adopt the native session id a backend reports after the session
    /// (or its first turn) exists. A session that has none keeps `None`.
    pub fn set_native_handle(&mut self, id: &str, native_handle: String) -> bool {
        let Some(record) = self.sessions.get_mut(id) else {
            return false;
        };
        if record.native_handle.as_deref() == Some(native_handle.as_str()) {
            return false;
        }
        record.native_handle = Some(native_handle);
        true
    }

    /// Note usage without a phase change (a turn's events flowing).
    pub fn touch(&mut self, id: &str, now_ms: u64) {
        if let Some(record) = self.sessions.get_mut(id) {
            record.last_active_ms = now_ms;
        }
    }

    /// Drop a session's record (closed, reaped, deleted, or detached).
    pub fn remove(&mut self, id: &str) -> Option<SessionRecord> {
        self.sessions.remove(id)
    }

    /// Boot convergence: everything the previous daemon left `Busy` or
    /// `AwaitingInput` belongs to a process that no longer exists (this
    /// incarnation has not started a turn yet). Converge it to `Idle`
    /// with a [`Recovered`] mark — never report a ghost as live. Returns
    /// the rows that were converged.
    pub fn recover_previous_incarnation(&mut self, now_ms: u64) -> Vec<SessionRecord> {
        let mut recovered = Vec::new();
        for record in self.sessions.values_mut() {
            if !record.phase.is_active() {
                continue;
            }
            let from_phase = record.phase;
            let turn_id = record
                .open_turn
                .as_ref()
                .map(|t| t.turn_id.clone())
                .filter(|t| !t.is_empty());
            if record.open_turn.is_some() {
                record.turns_interrupted += 1;
            }
            record.phase = Phase::Idle;
            record.open_turn = None;
            record.pending_ask = None;
            record.recovered = Some(Recovered {
                at_ms: now_ms,
                from_phase,
                turn_id,
            });
            record.last_active_ms = now_ms;
            recovered.push(record.clone());
        }
        recovered.sort_by(|a, b| a.id.cmp(&b.id));
        recovered
    }

    /// Drop recovered tombstones older than `ttl_ms`. Returns how many
    /// records went away.
    pub fn prune_recovered(&mut self, now_ms: u64, ttl_ms: u64) -> usize {
        let before = self.sessions.len();
        self.sessions.retain(|_, r| {
            r.recovered
                .as_ref()
                .is_none_or(|rec| now_ms.saturating_sub(rec.at_ms) < ttl_ms)
        });
        before - self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: u64 = 1_700_000_000_000;

    fn ledger_with(id: &str) -> SessionLedger {
        let mut l = SessionLedger::new();
        l.open(id, "claude", Some("native-1".into()), None, T0);
        l
    }

    fn ask(request_id: &str) -> PendingAsk {
        PendingAsk {
            request_id: request_id.to_string(),
            kind: AskKind::Permission,
            label: Some("Bash".into()),
            asked_at_ms: T0 + 10,
        }
    }

    #[test]
    fn a_new_session_is_idle_and_carries_its_handle() {
        let l = ledger_with("s1");
        let r = l.get("s1").expect("record");
        assert_eq!(r.phase, Phase::Idle);
        assert_eq!(r.provider, "claude");
        assert_eq!(r.native_handle.as_deref(), Some("native-1"));
        assert_eq!(l.counts().sessions, 1);
        assert_eq!(l.counts().active(), 0);
    }

    #[test]
    fn one_claim_at_a_time_and_the_second_is_rejected() {
        let mut l = ledger_with("s1");
        assert_eq!(l.begin_turn("s1", T0 + 1), TurnClaim::Claimed);
        assert_eq!(l.begin_turn("s1", T0 + 2), TurnClaim::AlreadyActive);
        assert_eq!(l.get("s1").unwrap().turns_started, 1);
        assert!(l.is_active("s1"));
    }

    #[test]
    fn the_backend_turn_id_is_attached_after_the_claim() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        assert!(l.note_turn_started("s1", "turn-9", T0 + 2));
        assert_eq!(
            l.get("s1").unwrap().open_turn.as_ref().unwrap().turn_id,
            "turn-9"
        );
        assert_eq!(l.get("s1").unwrap().turns_started, 1);
    }

    #[test]
    fn a_backend_reported_turn_nobody_claimed_is_adopted() {
        let mut l = ledger_with("s1");
        assert!(l.note_turn_started("s1", "turn-1", T0 + 1));
        assert_eq!(l.get("s1").unwrap().phase, Phase::Busy);
        assert_eq!(l.get("s1").unwrap().turns_started, 1);
    }

    #[test]
    fn settling_a_turn_frees_the_session_and_counts_once() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        l.note_turn_started("s1", "turn-1", T0 + 2);
        assert!(l.end_turn("s1", Some("turn-1"), TurnOutcome::Completed, T0 + 3));
        let r = l.get("s1").unwrap();
        assert_eq!(r.phase, Phase::Idle);
        assert!(r.open_turn.is_none());
        assert_eq!(r.turns_settled, 1);
        // The second boundary (drop-guard after the event stream) is a no-op.
        assert!(!l.end_turn("s1", Some("turn-1"), TurnOutcome::Failed, T0 + 4));
        assert_eq!(l.get("s1").unwrap().turns_settled, 1);
    }

    #[test]
    fn a_boundary_for_another_turn_is_ignored() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        l.note_turn_started("s1", "turn-1", T0 + 2);
        assert!(!l.end_turn("s1", Some("turn-2"), TurnOutcome::Completed, T0 + 3));
        assert_eq!(l.get("s1").unwrap().phase, Phase::Busy);
    }

    #[test]
    fn an_ask_parks_the_turn_and_its_answer_resumes_it() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        l.note_turn_started("s1", "turn-1", T0 + 2);
        assert!(l.ask_opened("s1", ask("req-1"), T0 + 3));
        let r = l.get("s1").unwrap();
        assert_eq!(r.phase, Phase::AwaitingInput);
        assert_eq!(r.pending_ask.as_ref().unwrap().request_id, "req-1");
        assert_eq!(l.counts().awaiting_input, 1);
        // Waiting for the user is not reapable.
        assert!(l.is_active("s1"));

        // A different request id does not clear the parked one.
        assert!(!l.ask_settled("s1", "req-2", T0 + 4));
        assert_eq!(l.get("s1").unwrap().phase, Phase::AwaitingInput);

        assert!(l.ask_settled("s1", "req-1", T0 + 5));
        let r = l.get("s1").unwrap();
        assert_eq!(r.phase, Phase::Busy, "the turn is still in flight");
        assert!(r.pending_ask.is_none());
    }

    #[test]
    fn an_answer_with_no_turn_left_finishes_the_session() {
        let mut l = ledger_with("s1");
        l.ask_opened("s1", ask("req-1"), T0 + 1);
        assert!(l.ask_settled("s1", "req-1", T0 + 2));
        assert_eq!(l.get("s1").unwrap().phase, Phase::Idle);
    }

    #[test]
    fn a_turn_boundary_withdraws_a_parked_ask() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        l.ask_opened("s1", ask("req-1"), T0 + 2);
        l.end_turn("s1", None, TurnOutcome::Canceled, T0 + 3);
        let r = l.get("s1").unwrap();
        assert_eq!(r.phase, Phase::Idle);
        assert!(r.pending_ask.is_none());
        assert_eq!(r.turns_settled, 1);
    }

    #[test]
    fn boot_convergence_turns_a_ghost_into_a_recovered_row() {
        let mut l = ledger_with("busy");
        l.open("waiting", "codex", None, None, T0);
        l.open("idle", "claude", None, None, T0);
        l.begin_turn("busy", T0 + 1);
        l.note_turn_started("busy", "turn-1", T0 + 2);
        l.ask_opened("waiting", ask("req-1"), T0 + 3);

        let recovered = l.recover_previous_incarnation(T0 + 1000);
        assert_eq!(recovered.len(), 2, "only the non-idle sessions");
        assert_eq!(l.counts().active(), 0, "no ghost is reported as live");
        for id in ["busy", "waiting"] {
            let r = l.get(id).unwrap();
            assert_eq!(r.phase, Phase::Idle);
            assert!(r.open_turn.is_none());
            assert!(r.pending_ask.is_none());
            assert!(r.recovered.is_some());
        }
        assert_eq!(
            l.get("busy")
                .unwrap()
                .recovered
                .as_ref()
                .unwrap()
                .from_phase,
            Phase::Busy
        );
        assert_eq!(l.get("busy").unwrap().turns_interrupted, 1);
        assert_eq!(l.get("waiting").unwrap().turns_interrupted, 0);
        assert!(l.get("idle").unwrap().recovered.is_none());
        // Newest first, and the rows carry the phase they were in.
        assert_eq!(l.recovered().len(), 2);
    }

    #[test]
    fn resuming_a_recovered_session_clears_its_mark() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        l.recover_previous_incarnation(T0 + 2);
        assert!(l.get("s1").unwrap().recovered.is_some());
        l.open("s1", "claude", Some("native-2".into()), None, T0 + 3);
        assert!(l.get("s1").unwrap().recovered.is_none());
        assert_eq!(
            l.get("s1").unwrap().native_handle.as_deref(),
            Some("native-2")
        );
    }

    #[test]
    fn recovered_tombstones_age_out() {
        let mut l = ledger_with("s1");
        l.begin_turn("s1", T0 + 1);
        l.recover_previous_incarnation(T0 + 2);
        assert_eq!(
            l.prune_recovered(T0 + 2 + RECOVERED_TTL_MS, RECOVERED_TTL_MS),
            1
        );
        assert!(l.get("s1").is_none());
    }

    #[test]
    fn a_closed_session_leaves_no_record() {
        let mut l = ledger_with("s1");
        assert!(l.remove("s1").is_some());
        assert!(l.is_empty());
        assert!(l.remove("s1").is_none());
    }

    #[test]
    fn an_unregistered_id_still_gets_a_record_so_the_turn_is_tracked() {
        let mut l = SessionLedger::new();
        assert_eq!(l.begin_turn("orphan", T0), TurnClaim::Claimed);
        let r = l.get("orphan").unwrap();
        assert_eq!(r.provider, "");
        assert_eq!(r.phase, Phase::Busy);
    }

    #[test]
    fn the_checkpoint_round_trips_through_its_wire_form() {
        let mut l = ledger_with("s1");
        l.open("s2", "codex", None, Some("helper".into()), T0);
        l.begin_turn("s1", T0 + 1);
        l.note_turn_started("s1", "turn-1", T0 + 2);
        l.ask_opened("s1", ask("req-1"), T0 + 3);
        l.stamp(T0 + 4);

        let bytes = l.to_bytes().expect("serialize");
        let back = SessionLedger::from_bytes(&bytes).expect("deserialize");
        assert_eq!(back.version, SNAPSHOT_VERSION);
        assert_eq!(back.saved_at_ms, T0 + 4);
        assert_eq!(back.len(), 2);
        assert_eq!(back.get("s1").unwrap().phase, Phase::AwaitingInput);
        assert_eq!(
            back.get("s1").unwrap().pending_ask.as_ref().unwrap().kind,
            AskKind::Permission
        );
        assert_eq!(back.get("s2").unwrap().persona.as_deref(), Some("helper"));
        assert_eq!(back.counts().awaiting_input, 1);
    }

    #[test]
    fn a_newer_checkpoint_version_is_refused_whole() {
        let mut l = ledger_with("s1");
        l.version = SNAPSHOT_VERSION + 1;
        let bytes = l.to_bytes().unwrap();
        let err = SessionLedger::from_bytes(&bytes).expect_err("must refuse");
        assert!(err.contains("newer"), "{err}");
        assert!(SessionLedger::from_bytes(b"not json").is_err());
    }
}
