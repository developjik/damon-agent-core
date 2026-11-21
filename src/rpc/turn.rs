//! `turn.*` — starting, steering and cancelling a turn, and the Keyed
//! retry contract those two requests carry (see [`super::params`] for
//! the typed-params machinery this family follows).
//!
//! `turn.start` is the one method here that interleaves its params with
//! *state*: the session is looked up and the idempotency key claimed
//! before the prompt is parsed, so a dead session answers
//! `SESSION_NOT_LIVE` and a Keyed retry replays its first answer without
//! the prompt ever being looked at. That wire order is preserved — the
//! two fields it needs early are read through the same field table
//! (`peek_str`/`peek_str_opt`), and `Start::parse` still checks all six.
//!
//! Wire behavior is unchanged on purpose — codes, messages, defaults and
//! the silently-ignored optional params included.

use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::api::AppState;
use crate::backend::types::PromptInput;
use crate::session::ManagedSession;

use super::params::{Field, Params};
use super::{BusyGuard, Conn, RpcError, error_code, run_turn};

// ─────────────────────────────────────────────────────────────────────
// Requests
// ─────────────────────────────────────────────────────────────────────

/// `turn.start` — one prompt, blocking or detached.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Start {
    /// Checked here like every other field, but the handler resolves the
    /// live session through `peek_str` *before* parsing — that is the
    /// pre-refactor read order — so this copy is never read again.
    session_id: String,
    /// `string|array`: text, or an array of text/image blocks. Kept as
    /// the raw JSON because *deserializing it is the check* — the legacy
    /// handler answered `prompt required` for anything that did not fit
    /// ([`prompt_input`]), and an absent prompt fails the field table
    /// with the same message.
    prompt: Option<Value>,
    timeout_secs: Option<u64>,
    detach: Option<bool>,
    idempotency_key: Option<String>,
    memory_owner: Option<String>,
}

impl Params for Start {
    const METHOD: &'static str = "turn.start";
    const FIELDS: &'static [Field] = &[
        Field::str("sessionId"),
        Field::json("prompt"),
        Field::uint_opt("timeoutSecs"),
        Field::bool_opt("detach"),
        Field::str_opt("idempotencyKey"),
        Field::str_opt("memoryOwner"),
    ];
}

impl Start {
    /// The prompt as a backend takes it — see [`prompt_input`].
    pub(super) fn prompt(&self) -> Result<PromptInput> {
        prompt_input(self.prompt.as_ref())
    }

    /// How long the turn may run, when the request capped it.
    pub(super) fn timeout(&self) -> Option<std::time::Duration> {
        self.timeout_secs.map(std::time::Duration::from_secs)
    }

    /// The bot ledger whose ACTIVE entries join the injected memory
    /// block, when `[memory]` is enabled.
    pub(super) fn memory_owner(&self) -> Option<&str> {
        self.memory_owner.as_deref()
    }

    /// A `turn.start` for a turn the daemon starts itself (a mission's
    /// agent node): the prompt and nothing else — the wire request it
    /// stands for is exactly `{"prompt": …}`.
    pub(super) fn from_prompt(prompt: PromptInput) -> Self {
        Self {
            prompt: Some(json!(prompt)),
            ..Default::default()
        }
    }
}

/// `turn.steer` — mid-turn input for a backend that supports it.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Steer {
    /// See [`Start::session_id`]: `peek_str` reads it before parsing.
    session_id: String,
    /// See [`Start::prompt`].
    prompt: Option<Value>,
    expected_turn: Option<String>,
    idempotency_key: Option<String>,
}

impl Params for Steer {
    const METHOD: &'static str = "turn.steer";
    const FIELDS: &'static [Field] = &[
        Field::str("sessionId"),
        Field::json("prompt"),
        Field::str_opt("expectedTurn"),
        Field::str_opt("idempotencyKey"),
    ];
}

/// `turn.cancel` — interrupt whatever runs, or nothing.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub(super) struct Cancel {
    session_id: String,
}

impl Params for Cancel {
    const METHOD: &'static str = "turn.cancel";
    const FIELDS: &'static [Field] = &[Field::str("sessionId")];
}

/// The prompt as a backend takes it: text, or an array of blocks.
/// Anything else — including a present-but-null `prompt`, which the
/// field table lets through as "the key is there" — answers the
/// historical `prompt required`.
pub(super) fn prompt_input(raw: Option<&Value>) -> Result<PromptInput> {
    serde_json::from_value(raw.cloned().unwrap_or(Value::Null))
        .map_err(|_| RpcError::error(error_code::INVALID_PARAMS, "prompt required"))
}

// ─────────────────────────────────────────────────────────────────────
// The Keyed retry contract
// ─────────────────────────────────────────────────────────────────────

/// The idempotency-cache key for a Keyed request, when the caller
/// supplied one. Scoped per session and method, so a client can mint one
/// key per turn (a counter or a uuid) without colliding across sessions.
fn idem_key(session_id: &str, method: &str, key: Option<&str>) -> Option<String> {
    key.map(str::trim)
        .filter(|k| !k.is_empty())
        .map(|k| format!("{session_id}:{method}:{k}"))
}

/// Mark a replayed answer so the client can tell a duplicate from a fresh
/// result: the turn it names already ran, and re-sending the prompt did
/// not start a second one.
fn duplicate_frame(mut v: Value) -> Value {
    if let Some(obj) = v.as_object_mut() {
        obj.insert("duplicate".to_string(), Value::Bool(true));
    }
    v
}

/// What claiming a `turn.start` key produced.
enum KeyClaim {
    /// No key, or the claim is ours: run the turn.
    Owned,
    /// An earlier attempt settled, or is still running: answer with this
    /// frame instead of starting a second turn.
    Answered(Value),
}

/// Claim `turn.start`'s idempotency key, if it has one. `Fresh` is the
/// only outcome that does work — the other two replay the first attempt.
async fn claim_key(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    ms: &Arc<ManagedSession>,
    key: &Option<String>,
) -> Result<KeyClaim> {
    let Some(key) = key else {
        return Ok(KeyClaim::Owned);
    };
    match state.idempotency.begin(key) {
        crate::idempotency::Begin::Fresh => Ok(KeyClaim::Owned),
        crate::idempotency::Begin::Done(v) => Ok(KeyClaim::Answered(duplicate_frame(v))),
        crate::idempotency::Begin::InFlight => {
            // The first attempt owns the turn and this request cannot
            // await it (`turn.start` resolves when the TURN ends), so
            // attach the retrying connection to the same stream: it sees
            // the turn it would otherwise have started a second time.
            conn.subscribe(state, ms).await;
            Ok(KeyClaim::Answered(json!({
                "sessionId": ms.id,
                "duplicate": true,
                "inProgress": true,
            })))
        }
    }
}

/// Release a claim that never settled — the client may retry it.
fn release_key(state: &AppState, key: &Option<String>) {
    if let Some(key) = key {
        state.idempotency.forget(key);
    }
}

// ─────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────

pub(super) async fn start(
    state: &Arc<AppState>,
    conn: &Arc<Conn>,
    params: &Value,
) -> Result<Value> {
    // The wire order the untyped handler had is kept: the session is
    // resolved, and a Keyed retry replayed, before the rest of the
    // request is validated — a dead session answers `SESSION_NOT_LIVE`
    // even for a request that also lacks a prompt.
    let id = Start::peek_str(params, "sessionId")?;
    let ms = state.sessions.get(id).await.ok_or_else(|| {
        RpcError::error(
            error_code::SESSION_NOT_LIVE,
            "session not live — call session.resume first",
        )
    })?;
    // Keyed retry: a client that lost the response to an earlier attempt
    // gets that attempt's answer instead of a second turn (which would
    // spend tokens and re-run tool side effects). Without the key nothing
    // changes — the pre-0.9 request shape stays byte for byte.
    let idem_key = idem_key(
        id,
        Start::METHOD,
        Start::peek_str_opt(params, "idempotencyKey"),
    );
    match claim_key(state, conn, &ms, &idem_key).await? {
        KeyClaim::Answered(frame) => return Ok(frame),
        KeyClaim::Owned => {}
    }
    // Claim the session exclusively: a second concurrent start would run
    // two collectors on the same broadcast and double-persist every
    // event. The session ledger's phase is the one-and-only claim (#16
    // replaced the old atomic flag).
    if !state.sessions.begin_turn(&ms.id) {
        release_key(state, &idem_key);
        return Err(RpcError::error(
            error_code::TURN_IN_PROGRESS,
            "a turn is already running on this session",
        ));
    }
    // The claim is held from here on; the guard releases it on every
    // exit path, the params error below included.
    let busy = BusyGuard::new(state.sessions.clone(), &ms.id);
    let req = match Start::parse(params) {
        Ok(req) => req,
        Err(e) => {
            release_key(state, &idem_key);
            return Err(e);
        }
    };
    // Register disconnect-cancel ownership immediately, before any
    // further await: a client that hangs up right after `turn.start`
    // must not leak an uncancellable turn. The closed-flag check covers
    // a disconnect whose sweep already ran (it found an empty map);
    // detach skips ownership entirely — its turn outlives the connection
    // by design.
    let detach = req.detach.unwrap_or(false);
    let cancel = if detach {
        None
    } else {
        let c = CancellationToken::new();
        let mut owned = conn.owned_turns.lock().await;
        owned.insert(ms.id.clone(), c.clone());
        if conn.closed.load(std::sync::atomic::Ordering::SeqCst) {
            c.cancel();
        }
        drop(owned);
        Some(c)
    };
    conn.subscribe(state, &ms).await;
    let out = run_turn(state, conn, &ms, &req, detach, cancel, busy).await;
    match &out {
        // Settled: the retry window holds this answer.
        Ok(v) => {
            if let Some(key) = &idem_key {
                state.idempotency.finish(key, v.clone());
            }
        }
        // Failed or canceled before it settled: release the key so the
        // client can ask again (a failed turn did not complete —
        // replaying its error would hide the retry).
        Err(_) => release_key(state, &idem_key),
    }
    out
}

pub(super) async fn steer(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    // Same wire order as `turn.start`: the live session is resolved
    // before the prompt is parsed.
    let id = Steer::peek_str(params, "sessionId")?;
    let ms = state
        .sessions
        .get(id)
        .await
        .ok_or_else(|| RpcError::error(error_code::SESSION_NOT_LIVE, "session not live"))?;
    let req = Steer::parse(params)?;
    let prompt = prompt_input(req.prompt.as_ref())?;
    let expected = req.expected_turn.as_deref().unwrap_or_default();
    // Same Keyed contract as turn.start: a steer delivered twice would
    // hand the agent the same words twice.
    let idem_key = idem_key(id, Steer::METHOD, req.idempotency_key.as_deref());
    if let Some(key) = &idem_key {
        match state.idempotency.begin(key) {
            crate::idempotency::Begin::Done(v) => return Ok(duplicate_frame(v)),
            crate::idempotency::Begin::InFlight => {
                return Err(RpcError::error(
                    error_code::TURN_IN_PROGRESS,
                    "a steer with this idempotencyKey is still being delivered",
                ));
            }
            crate::idempotency::Begin::Fresh => {}
        }
    }
    match ms.session.steer(prompt, expected).await {
        Ok(res) => {
            let out = json!({"result": res});
            if let Some(key) = &idem_key {
                state.idempotency.finish(key, out.clone());
            }
            Ok(out)
        }
        Err(e) => {
            release_key(state, &idem_key);
            Err(e)
        }
    }
}

pub(super) async fn cancel(state: &Arc<AppState>, params: &Value) -> Result<Value> {
    let req = Cancel::parse(params)?;
    if let Some(ms) = state.sessions.get(&req.session_id).await {
        ms.session.interrupt().await?;
    }
    Ok(json!({"cancelled": true}))
}

// ─────────────────────────────────────────────────────────────────────
// The family contract, for the drift tests
// ─────────────────────────────────────────────────────────────────────

/// Every method in this module with the fields its struct reads. The
/// drift tests walk this table against `rpc_methods()`; a new turn
/// method adds its row here (and the tests fail until it does).
#[cfg(test)]
pub(super) const FAMILY: &[(&str, &[Field])] = &[
    (Start::METHOD, Start::FIELDS),
    (Steer::METHOD, Steer::FIELDS),
    (Cancel::METHOD, Cancel::FIELDS),
];

/// Parse one family method's params through its typed struct — the seam
/// the drift tests drive.
#[cfg(test)]
pub(super) fn parse_family(method: &str, params: &Value) -> Result<()> {
    match method {
        Start::METHOD => Start::parse(params).map(drop),
        Steer::METHOD => Steer::parse(params).map(drop),
        Cancel::METHOD => Cancel::parse(params).map(drop),
        other => panic!("{other} is in FAMILY but has no typed parse"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::params::drift;
    use super::{FAMILY, Params, Start, parse_family, prompt_input};
    use crate::backend::types::PromptInput;

    /// The catalog's `turn.*` entries and the structs in this module are
    /// one contract, checked both ways (see `params::drift`).
    #[test]
    fn catalog_declares_exactly_the_fields_the_structs_read() {
        drift::catalog_matches(FAMILY, &["turn."]);
    }

    /// The table is what parse enforces — every required field removed or
    /// mistyped fails with its declared message, every optional field
    /// tolerates junk.
    #[test]
    fn the_declared_fields_are_the_fields_that_are_checked() {
        drift::fields_are_checked(FAMILY, parse_family);
    }

    /// The documented shapes parse: one representative request per
    /// method, spelled exactly as `docs/protocol-v2.md` spells it.
    #[test]
    fn documented_requests_parse() {
        for (method, params) in [
            ("turn.start", json!({"sessionId": "s", "prompt": "hi"})),
            (
                "turn.start",
                json!({"sessionId": "s", "prompt": [{"type": "text", "text": "hi"}],
                       "timeoutSecs": 30, "detach": true,
                       "idempotencyKey": "k", "memoryOwner": "bot"}),
            ),
            (
                "turn.steer",
                json!({"sessionId": "s", "prompt": "and then",
                       "expectedTurn": "t", "idempotencyKey": "k"}),
            ),
            ("turn.cancel", json!({"sessionId": "s"})),
        ] {
            parse_family(method, &params).unwrap_or_else(|e| panic!("{method} {params}: {e}"));
        }
    }

    /// `prompt` is text or a block array, and nothing else: an absent key
    /// fails the field table, a present-but-unusable value fails the
    /// deserializer, and both answer the one historical message.
    #[test]
    fn prompt_is_text_or_blocks_and_nothing_else() {
        let err = Start::parse(&json!({"sessionId": "s"})).unwrap_err();
        assert_eq!("prompt required", format!("{err}"));

        let read = |raw: Value| {
            let req = Start::parse(&json!({"sessionId": "s", "prompt": raw})).unwrap();
            prompt_input(req.prompt.as_ref())
        };
        assert!(matches!(read(json!("hi")).unwrap(), PromptInput::Text(t) if t == "hi"));
        assert!(
            matches!(read(json!([{"type": "text", "text": "hi"}])).unwrap(),
                     PromptInput::Blocks(b) if b.len() == 1)
        );
        for bad in [json!(null), json!(7), json!([{"nope": true}])] {
            let err = read(bad.clone()).unwrap_err();
            assert_eq!("prompt required", format!("{err}"), "for {bad}");
        }
    }
}
