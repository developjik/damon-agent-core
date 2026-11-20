//! `#16` boundary guard: the pure session state must not be able to
//! reach the runtime.
//!
//! `src/state.rs` is compiled into the same crate as the daemon, so the
//! compiler alone cannot stop someone from importing a process type
//! there. This test is the gate that does: it reads the module's own
//! source (baked in at compile time, so it travels with the build) and
//! fails when a runtime import, a file/process call, or an awaitable
//! function appears.
//!
//! Keeping the state module free of those is what makes every transition
//! testable without spawning an agent CLI, and what makes the module's
//! snapshot a plain document rather than a view of live objects.

/// The module under guard, as the compiler saw it.
const SOURCE: &str = include_str!("../src/state.rs");

/// Tokens that would mean the module reached the runtime. Each one is a
/// concrete construct, not a word, so ordinary prose in doc comments
/// stays allowed.
const FORBIDDEN: &[(&str, &str)] = &[
    ("crate::", "reaching another daemon module"),
    ("tokio", "a task or async runtime handle"),
    ("async fn", "an awaitable transition"),
    ("std::fs", "file IO"),
    ("std::process", "a process handle"),
    ("std::net", "a socket"),
    ("rusqlite", "the store"),
    ("parking_lot", "a lock the runtime shares"),
    ("dyn AgentSession", "a backend session"),
    ("Arc<dyn", "a runtime trait object"),
];

/// Everything the module is allowed to depend on.
const ALLOWED_USE_ROOTS: &[&str] = &["use serde", "use std::collections", "use std::fmt"];

/// The production half of the module — its inline test module may import
/// `super::*` (it is the same file, not a dependency).
fn production() -> &'static str {
    SOURCE.split("#[cfg(test)]").next().unwrap_or(SOURCE)
}

#[test]
fn pure_state_cannot_reach_the_runtime() {
    let source = production();
    for (token, why) in FORBIDDEN {
        assert!(
            !source.contains(token),
            "src/state.rs must stay pure — found `{token}` ({why}). \
             The session runtime lives in src/session.rs; feed it plain values instead."
        );
    }
}

#[test]
fn pure_state_imports_nothing_from_the_daemon() {
    for line in production().lines() {
        let line = line.trim();
        if !line.starts_with("use ") {
            continue;
        }
        assert!(
            ALLOWED_USE_ROOTS.iter().any(|root| line.starts_with(root)),
            "src/state.rs may only import serde and std collections, found: `{line}`"
        );
    }
}

/// The guard would be worthless if the file it reads were not the state
/// module — keep the two in step.
#[test]
fn the_guard_reads_the_real_state_module() {
    assert!(
        SOURCE.contains("pub struct SessionLedger"),
        "the guard is reading a file that is not src/state.rs"
    );
    assert!(
        SOURCE.contains("#[derive(Debug, Clone, Serialize, Deserialize)]"),
        "state types stay serializable — that is what makes the checkpoint possible"
    );
}
