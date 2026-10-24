pub mod api;
pub mod backend;
pub mod bots;
pub mod channel;
pub mod cli_lifecycle;
pub mod client;
pub mod config;
pub mod discord;
pub mod discovery;
pub mod file_index;
pub mod history_scan;
pub mod logs;
pub mod mcp_inventory;
pub mod memory;
pub mod mission;
pub mod prompts;
pub mod ratelimit;
pub mod relay;
pub mod rpc;
pub mod service;
pub mod session;
pub mod skills_hub;
pub mod slack;
pub mod slash_catalog;
pub mod store;
pub mod telegram;
pub mod ui;
pub mod worktree;

/// Serialization for tests that touch process-global env vars (the
/// MCP inventory tests point CLAUDE_CONFIG_DIR/CODEX_HOME/… at
/// fixture dirs; parallel tests reading the same vars would race).
/// Returns the LOCKED guard — hold it for the whole test body.
#[doc(hidden)]
pub fn tests_env_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    LOCK.lock()
}
