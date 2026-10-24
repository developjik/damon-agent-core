//! Agent backends — Damon drives each coding agent over its own native
//! protocol (Claude stream-json, Codex app-server, OMP rpc-mode), not a
//! shared standard. Each backend translates its wire format into the
//! normalized model in [`types`]; the daemon only ever sees that model.
//!
//! Two levels, borrowed from Paseo:
//! - [`AgentClient`] — the brand: detection, catalog, session create/resume/import
//! - [`AgentSession`] — one live conversation: turns, steering, permissions

pub mod registry;
pub mod transport;
pub mod types;

pub mod claude;
pub mod codex;
pub mod droid;
pub mod omp;
pub mod opencode;
pub mod opencode_server;
pub mod qwen;
pub mod streamjson;
pub mod zcode;

pub use types::*;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Native-history discovery helpers (shared by the file-reading backends)
// ---------------------------------------------------------------------------

/// Bounded head peek — up to `max_lines` complete lines from the head of
/// a file, read in 16 KiB chunks and capped at `max_bytes` total (1 MiB
/// upstream default). Codex-family transcripts embed their full
/// base_instructions system prompt in the first `session_meta` line, so
/// line 1 alone can run to tens of KB: a fixed small read truncates
/// mid-JSON and a fixed big read pulls GBs across a large library. The
/// chunked walk stops as soon as `max_lines` complete lines arrived, so
/// a typical file costs tens of KB. A tail truncated by the cap is
/// dropped (a partial line never parses).
pub fn peek_head_lines(path: &Path, max_lines: usize, max_bytes: usize) -> Vec<String> {
    use std::io::Read;

    const CHUNK_BYTES: usize = 16 * 1024;
    let Ok(mut file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let mut buf = Vec::new();
    let mut newline_count = 0usize;
    let mut eof = false;
    while newline_count < max_lines && buf.len() < max_bytes {
        let mut chunk = [0u8; CHUNK_BYTES];
        match file.read(&mut chunk) {
            Ok(0) => {
                eof = true;
                break;
            }
            Ok(n) => {
                newline_count += chunk[..n].iter().filter(|&&b| b == b'\n').count();
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => return Vec::new(),
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines: Vec<&str> = head.split('\n').collect();
    if !eof {
        // The cap can leave the tail segment mid-line; a partial line
        // never parses, so drop it. At EOF the tail is complete.
        lines.pop();
    }
    lines
        .into_iter()
        .take(max_lines)
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect()
}

/// Noise prefixes a derived title must never start with — injected
/// context the agent pasted before the user's actual words (upstream's
/// NOISE_TITLE_WHERE list). A title matching one of these is dropped so
/// the scan can re-derive from the next candidate message instead of
/// naming a session after a pasted environment dump.
pub fn is_noise_title(title: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "<file ",
        "[Image #",
        "<user_info>",
        "<user_query>",
        "# AGENTS.md instructions",
        "<environment_context>",
        "<agents-instructions>",
        "<skill>",
        "<recommended_plugins>",
        "<command-message>",
        "<command-name>",
        "<INSTRUCTIONS>",
    ];
    PREFIXES.iter().any(|p| title.starts_with(p))
}

/// Title hygiene for text derived from transcript bodies: strip
/// `<file …>…</file>` envelopes (an unterminated one loses just its
/// opening tag) and `[Image #N, WxH]` / `[Image #N: path]` placeholders
/// wherever they appear, then collapse whitespace. User text that
/// merely mentions these markers survives; a body that is ONLY noise
/// collapses to empty (callers treat that as "no title here").
pub fn strip_title_noise(text: &str) -> String {
    // <file …>…</file> envelopes.
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("<file ") {
        out.push_str(&rest[..start]);
        let after = &rest[start..];
        if let Some(end) = after.find("</file>") {
            rest = &after[end + "</file>".len()..];
        } else if let Some(gt) = after.find('>') {
            rest = &after[gt + 1..];
        } else {
            rest = "";
        }
    }
    out.push_str(rest);
    // [Image #N, WxH] / [Image #N: path] placeholders. A `[Image #`
    // run whose bracket body is neither dimensions nor a path payload
    // is user text and stays.
    let mut cleaned = String::with_capacity(out.len());
    let mut rest = out.as_str();
    const MARK: &str = "[Image #";
    while let Some(start) = rest.find(MARK) {
        let after = &rest[start + MARK.len()..];
        let placeholder = after.find(']').is_some_and(|e| {
            let body = &after[..e];
            let digits = body.len() - body.trim_start_matches(|c: char| c.is_ascii_digit()).len();
            if digits == 0 {
                return false;
            }
            let tail = &body[digits..];
            tail.chars()
                .all(|c| c.is_ascii_digit() || matches!(c, ',' | ' ' | 'x'))
                || tail.strip_prefix(": ").is_some_and(|p| !p.is_empty())
        });
        if placeholder {
            cleaned.push_str(&rest[..start]);
            let end = start + MARK.len() + after.find(']').unwrap() + 1;
            rest = &rest[end..];
        } else {
            cleaned.push_str(&rest[..start + MARK.len()]);
            rest = after;
        }
    }
    cleaned.push_str(rest);
    cleaned.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Stat signature for the incremental rescan — (size bytes, mtime ms).
/// Unchanged signature → the scan skips the file entirely.
pub fn stat_signature(path: &Path) -> Option<(i64, i64)> {
    let meta = std::fs::metadata(path).ok()?;
    let mtime_ms = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis() as i64;
    Some((meta.len() as i64, mtime_ms))
}

/// The brand-level handle: detection on this machine, model/mode
/// catalog, and session lifecycle entry points.
#[async_trait]
pub trait AgentClient: Send + Sync {
    fn provider(&self) -> &str;
    fn capabilities(&self) -> &Capabilities;

    /// Whether the backend's CLI is installed and usable right now.
    async fn is_available(&self) -> bool;

    /// Discover models and modes together. Implementations may use one
    /// upstream probe or static lists; callers never probe separately.
    async fn fetch_catalog(&self, cwd: Option<&Path>) -> Result<ProviderCatalog>;

    /// Start a fresh session. The returned object owns its process.
    async fn create_session(&self, config: SessionConfig) -> Result<Arc<dyn AgentSession>>;

    /// Reattach to the provider's durable session.
    async fn resume_session(
        &self,
        handle: &PersistenceHandle,
        config: SessionConfig,
    ) -> Result<Arc<dyn AgentSession>>;

    /// Native sessions created outside the daemon (e.g. a `claude` run
    /// in a terminal) that can be imported into Damon's store.
    async fn list_importable_sessions(&self, _cwd: &Path) -> Result<Vec<ImportableSession>> {
        Ok(vec![])
    }

    /// Every discoverable native session across ALL workspaces — the
    /// history scanner's source (`session.rescan`). Default: the
    /// per-cwd listing at the daemon's working directory; backends
    /// whose discovery enumerates global roots override this (and must
    /// honor their env overrides — CLAUDE_CONFIG_DIR and friends).
    async fn list_all_importable(&self) -> Result<Vec<ImportableSession>> {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        self.list_importable_sessions(&cwd).await
    }

    /// Release provider-owned resources (cached processes, sockets).
    /// Idempotent; called on daemon shutdown.
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

/// One live conversation. The process is an implementation detail —
/// backends may spawn per session (Claude, OMP) or multiplex (Codex
/// app-server can host many threads in one process).
#[async_trait]
pub trait AgentSession: Send + Sync {
    fn capabilities(&self) -> &Capabilities;

    /// The normalized event stream. Lagging receivers see gaps, never
    /// block the backend.
    fn subscribe(&self) -> broadcast::Receiver<StreamEvent>;

    /// Begin a prompt turn; returns the turn id events correlate to.
    async fn start_turn(&self, prompt: PromptInput) -> Result<String>;

    /// Deliver a mid-turn steering message. Default: unsupported.
    async fn steer(&self, _prompt: PromptInput, _expected_turn: &str) -> Result<SteerResult> {
        Ok(SteerResult::Unavailable)
    }

    /// Seconds since the session's process last saw traffic — the idle
    /// sweep uses it to reap unused sessions.
    fn idle_secs(&self) -> u64 {
        0
    }

    /// Abort the in-flight turn only — the session stays usable.
    async fn interrupt(&self) -> Result<()>;

    /// Release live runtime resources without deleting the durable
    /// native session. Resume via the persistence handle afterwards.
    async fn close(&self) -> Result<()>;

    /// Answer a pending permission ask.
    async fn respond_to_permission(
        &self,
        request_id: &str,
        response: PermissionResponse,
    ) -> Result<()>;

    /// The resume token for this session, once the backend has one.
    fn persistence_handle(&self) -> Option<PersistenceHandle>;

    /// Switch permission/behavior mode. Default: unsupported — the
    /// typed code lets clients hide the affordance instead of surfacing
    /// the error.
    async fn set_mode(&self, _mode: &str) -> Result<()> {
        Err(crate::rpc::RpcError::error(
            crate::rpc::error_code::NOT_SUPPORTED,
            "mode switching not supported",
        ))
    }

    /// Switch model. Default: unsupported (see `set_mode`).
    async fn set_model(&self, _model: &str) -> Result<()> {
        Err(crate::rpc::RpcError::error(
            crate::rpc::error_code::NOT_SUPPORTED,
            "model switching not supported",
        ))
    }
}
