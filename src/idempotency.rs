//! Idempotency keys for the Keyed requests (`turn.start` / `turn.steer`).
//!
//! A response can be lost after the daemon already acted on the request —
//! a phone loses signal mid-turn, a relay hop dies after the prompt
//! reached the backend, a browser tab reloads. ACP v2 classifies requests
//! four ways (Idempotent / **Keyed** / Duplicating / Not retried) for
//! exactly this reason: a turn is the Keyed kind, so re-running it does
//! real work twice (tokens spent, tool side effects applied) unless the
//! client supplies a key and the daemon answers the repeat from the first
//! attempt.
//!
//! Scope, on purpose: one daemon process. The cache lives in memory with
//! a TTL, so a daemon restart forgets the keys — the turn ledger in the
//! store (`turns`) is what keeps that case honest: a turn cut off by the
//! restart is converged to `interrupted` and marked in the transcript
//! instead of vanishing.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::Value;

/// How long a settled key answers retries — long enough to cover a
/// reconnect after a tunnel drop, short enough that an accidentally
/// reused key (one per TURN, not one per session) cannot pin a stale
/// answer onto a later request.
pub const DEFAULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Upper bound on live keys. A day of heavy use stays far below this; a
/// runaway key space evicts its oldest entry instead of growing without
/// bound.
const MAX_ENTRIES: usize = 4_096;

/// What one key knows.
#[derive(Clone, Debug)]
enum Entry {
    /// The first attempt is still running — a repeat must not start a
    /// second one.
    InFlight { started: Instant },
    /// The first attempt settled: this is its response, replayable.
    Done { at: Instant, result: Value },
}

impl Entry {
    fn at(&self) -> Instant {
        match self {
            Entry::InFlight { started } => *started,
            Entry::Done { at, .. } => *at,
        }
    }
}

/// What [`IdempotencyCache::begin`] says the caller should do.
#[derive(Debug)]
pub enum Begin {
    /// No live entry: this caller owns the key and must run the work.
    /// The claim is already recorded, so a concurrent repeat sees
    /// `InFlight` rather than a second `Fresh`.
    Fresh,
    /// The first attempt is still running — do not run a second one.
    InFlight,
    /// The first attempt settled: replay this response instead of
    /// re-running.
    Done(Value),
}

/// TTL-bounded map of `sessionId:method:key` → attempt. Cheap enough to
/// hold behind one mutex: entries are tiny and only touch the map on a
/// request that carries a key.
pub struct IdempotencyCache {
    ttl: Duration,
    inner: parking_lot::Mutex<HashMap<String, Entry>>,
}

impl IdempotencyCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Claim `key`. A `Fresh` answer IS the claim — the caller must
    /// finish it with [`finish`](Self::finish) or release it with
    /// [`forget`](Self::forget), or the key stays `InFlight` until it
    /// expires and every retry is refused.
    pub fn begin(&self, key: &str) -> Begin {
        let mut g = self.inner.lock();
        sweep(&mut g, self.ttl);
        match g.get(key) {
            Some(Entry::Done { result, .. }) => Begin::Done(result.clone()),
            Some(Entry::InFlight { .. }) => Begin::InFlight,
            None => {
                if g.len() >= MAX_ENTRIES {
                    // The map is a retry window, not a ledger: the oldest
                    // attempt is the least likely to be retried.
                    let oldest = g.iter().min_by_key(|(_, e)| e.at()).map(|(k, _)| k.clone());
                    if let Some(oldest) = oldest {
                        g.remove(&oldest);
                    }
                }
                g.insert(
                    key.to_string(),
                    Entry::InFlight {
                        started: Instant::now(),
                    },
                );
                Begin::Fresh
            }
        }
    }

    /// Settle the claim with the response the first attempt produced —
    /// every later request carrying this key replays it.
    pub fn finish(&self, key: &str, result: Value) {
        self.inner.lock().insert(
            key.to_string(),
            Entry::Done {
                at: Instant::now(),
                result,
            },
        );
    }

    /// Release the claim without an answer: the attempt failed or never
    /// ran, so the client must be able to ask again. (A failed turn did
    /// not complete — replaying its error as the result of a retry
    /// would hide the retry from the client entirely.)
    pub fn forget(&self, key: &str) {
        self.inner.lock().remove(key);
    }

    /// Live entries (expired ones are swept by the next `begin`).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Drop entries older than `ttl`. `Duration::ZERO` disables expiry (the
/// tests use it to keep a key alive across a whole assertion).
fn sweep(map: &mut HashMap<String, Entry>, ttl: Duration) {
    if ttl.is_zero() {
        return;
    }
    let now = Instant::now();
    map.retain(|_, e| now.duration_since(e.at()) < ttl);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fresh_cache() -> IdempotencyCache {
        IdempotencyCache::new(DEFAULT_TTL)
    }

    #[test]
    fn first_claim_is_fresh_and_a_repeat_is_in_flight() {
        let c = fresh_cache();
        assert!(matches!(c.begin("k"), Begin::Fresh));
        assert!(matches!(c.begin("k"), Begin::InFlight));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn a_settled_key_replays_its_answer() {
        let c = fresh_cache();
        c.begin("k");
        c.finish("k", json!({"turnId": "t1", "stopReason": "completed"}));
        match c.begin("k") {
            Begin::Done(v) => assert_eq!(v["turnId"], "t1"),
            other => panic!("expected the settled answer, got {other:?}"),
        }
    }

    #[test]
    fn forgot_keys_start_over() {
        let c = fresh_cache();
        c.begin("k");
        c.forget("k");
        assert!(c.is_empty());
        assert!(matches!(c.begin("k"), Begin::Fresh));
    }

    #[test]
    fn keys_are_independent() {
        let c = fresh_cache();
        c.begin("a");
        c.finish("b", json!({"turnId": "tb"}));
        assert!(matches!(c.begin("a"), Begin::InFlight));
        match c.begin("b") {
            Begin::Done(v) => assert_eq!(v["turnId"], "tb"),
            other => panic!("expected the settled answer, got {other:?}"),
        }
    }

    #[test]
    fn expired_keys_start_over() {
        let c = IdempotencyCache::new(Duration::from_millis(1));
        c.begin("k");
        std::thread::sleep(Duration::from_millis(20));
        assert!(matches!(c.begin("k"), Begin::Fresh));
    }

    #[test]
    fn overflow_evicts_the_oldest_entry() {
        let c = IdempotencyCache::new(Duration::ZERO);
        for i in 0..MAX_ENTRIES {
            assert!(matches!(c.begin(&format!("k{i}")), Begin::Fresh));
        }
        assert_eq!(c.len(), MAX_ENTRIES);
        // One more claim still succeeds — the cache is a bounded retry
        // window, never a hard failure.
        assert!(matches!(c.begin("overflow"), Begin::Fresh));
        assert_eq!(c.len(), MAX_ENTRIES);
    }
}
