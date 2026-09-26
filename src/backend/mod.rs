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

pub mod amp;
pub mod claude;
pub mod codex;
pub mod cursor;
pub mod gemini;
pub mod kimi;
pub mod omp;
pub mod qwen;
pub mod streamjson;

pub use types::*;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

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
