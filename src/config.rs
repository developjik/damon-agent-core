use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use notify::Watcher;
use serde::Deserialize;
use tracing::{info, warn};

/// Daemon configuration. Hot-reloaded: changes to the config file are picked
/// up without restart, except `bind` which requires a restart.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    /// Optional bearer token for /ws and /metrics. Required on
    /// non-loopback binds; `env:VAR` / `keychain:` / `!cmd` refs only.
    pub auth_token: Option<String>,
    /// SQLite store location; default ~/.local/share/damon (or platform
    /// equivalent).
    pub data_dir: Option<PathBuf>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    /// Per-backend launch overrides: `[backends.claude] command = "…"`
    /// for a custom adapter, or an absolute path to a locally installed
    /// agent CLI.
    #[serde(default)]
    pub backends: BTreeMap<String, AgentConfig>,
    /// Default backend id for session.create without an explicit backend.
    pub default_backend: Option<String>,
    #[serde(rename = "relay")]
    pub relay: Option<RelayConfig>,
    /// Deny tool calls if the permission prompt goes unanswered this long.
    pub permission_timeout_secs: Option<u64>,
    /// Delete sessions older than this; unset = keep forever.
    pub session_retention_days: Option<u64>,
    /// Idle ACP subprocesses are killed after this many seconds of no
    /// use; the session reattaches via the resume chain on the next
    /// prompt. 0 disables the idle sweep. Default 1800.
    #[serde(default = "default_agent_idle_secs")]
    pub agent_idle_secs: u64,
    /// Cap on live in-memory backend sessions; unset = unlimited.
    /// `session.create`/`resume` past the cap fail until sessions are
    /// closed, deleted, or reaped by the idle sweep. Hot-reloaded.
    pub max_sessions: Option<usize>,
    /// Directory allowlist for session working directories. Empty
    /// (default) = unrestricted — the zero-config promise. When set,
    /// `session.create`, both `session.resume` paths, and
    /// `session.import` reject cwds outside these roots
    /// (component-wise prefix after best-effort canonicalization, so
    /// `/a/bc` cannot sneak under `/a/b`). Meant for remote/TLS
    /// deployments, where the shared token would otherwise let an
    /// agent process run anywhere on the machine. Hot-reloaded.
    #[serde(default)]
    pub allowed_dirs: Vec<PathBuf>,
}

/// `[agents.<id>]` — explicit launch line for one agent. Every field
/// optional; unset fields fall back to the catalog default.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

/// `[relay]` — outbound tunnel to a public relay.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelayConfig {
    /// ws://host:port of the relay server.
    pub url: String,
    /// Name clients use to reach this daemon through the relay.
    pub name: String,
    /// Registration secret when the relay sets DAMON_RELAY_SECRET.
    /// Supports env:/keychain:/!cmd secret refs.
    pub secret: Option<String>,
}

/// A secret that lives outside the config file.
#[derive(Clone, Debug)]
pub enum SecretRef {
    /// Plain environment variable (`env:VAR`).
    Env(String),
    /// OS keychain entry (`keychain:service/account`).
    Keychain { service: String, account: String },
    /// Shell command whose stdout is the secret (`!cmd…`).
    Command(String),
}

impl SecretRef {
    /// Parse a `env:`/`keychain:`/`!` reference. `Err` carries the parse
    /// failure; a bare literal is NOT accepted here for api keys anymore
    /// — callers decide policy.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        if let Some(var) = raw.strip_prefix("env:") {
            if var.is_empty() || var.contains(':') {
                anyhow::bail!("invalid env reference {raw:?}");
            }
            return Ok(SecretRef::Env(var.to_string()));
        }
        if let Some(rest) = raw.strip_prefix("keychain:") {
            let Some((service, account)) = rest.split_once('/') else {
                anyhow::bail!("keychain ref must be keychain:service/account, got {raw:?}");
            };
            if service.is_empty() || account.is_empty() {
                anyhow::bail!("keychain ref must be keychain:service/account, got {raw:?}");
            }
            return Ok(SecretRef::Keychain {
                service: service.to_string(),
                account: account.to_string(),
            });
        }
        if let Some(cmd) = raw.strip_prefix('!') {
            if cmd.trim().is_empty() {
                anyhow::bail!("empty ! command");
            }
            return Ok(SecretRef::Command(cmd.to_string()));
        }
        anyhow::bail!("expected env:, keychain:, or ! prefix")
    }

    pub fn resolve(&self) -> anyhow::Result<String> {
        match self {
            SecretRef::Env(var) => std::env::var(var).with_context(|| format!("${var} not set")),
            SecretRef::Keychain { service, account } => keyring::Entry::new(service, account)
                .context("keychain backend unavailable")
                .and_then(|e| e.get_password().context("keychain read failed")),
            SecretRef::Command(cmd) => resolve_command(cmd),
        }
    }

    /// Display form for logs — never the resolved value.
    pub fn describe(&self) -> String {
        match self {
            SecretRef::Env(v) => format!("env:{v}"),
            SecretRef::Keychain { service, account } => format!("keychain:{service}/{account}"),
            SecretRef::Command(_) => "!command".to_string(),
        }
    }
}

/// Run a shell command and return trimmed stdout. 10s timeout; empty or
/// failing commands are errors (resolution must fail closed).
fn resolve_command(cmd: &str) -> anyhow::Result<String> {
    use std::io::Read;
    use std::process::Stdio;
    let mut child = std::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdout(Stdio::piped())
        // stderr was never surfaced by the old .output() either — null
        // keeps a noisy command from deadlocking on a full pipe.
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("cannot run `{cmd}`"))?;
    // Drain stdout on a helper thread: a command that fills the pipe
    // buffer would otherwise deadlock against the wait loop below.
    let mut stdout = child.stdout.take().expect("stdout piped");
    let drained = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    // Poll instead of wait_with_output so a hung command can be killed —
    // the documented 10s bound must hold even when the child stalls.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = drained.join();
                anyhow::bail!("`{cmd}` timed out after 10s");
            }
            Err(e) => {
                let _ = child.kill();
                let _ = drained.join();
                return Err(e).with_context(|| format!("cannot wait on `{cmd}`"));
            }
        }
    };
    let out = drained.join().unwrap_or_default();
    if !status.success() {
        anyhow::bail!("`{cmd}` exited with {status}");
    }
    let s = String::from_utf8_lossy(&out).trim().to_string();
    if s.is_empty() {
        anyhow::bail!("`{cmd}` produced an empty secret");
    }
    Ok(s)
}

/// Constant-time byte equality for token/proof comparisons — a remote
/// endpoint must not get a timing oracle on the secret.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

impl Config {
    /// Load and parse the config file. `.env` files load first (config
    /// dir, then cwd) so `env:` refs resolve.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Self::load_inner(path, true)
    }

    /// `load` without the `.env` pass — for the hot-reload path, where
    /// mutating process env is unsound (other threads may be reading it)
    /// and pointless: dotenvy never overrides already-set vars anyway.
    pub fn load_no_env(path: &Path) -> anyhow::Result<Self> {
        Self::load_inner(path, false)
    }

    fn load_inner(path: &Path, dotenv: bool) -> anyhow::Result<Self> {
        if dotenv {
            // dotenvy: first hit wins per var, miroring the old precedence.
            if let Some(dir) = path.parent() {
                // SAFETY: daemon startup — no other threads reference the env
                // yet (single-threaded config load before the runtime spawns).
                unsafe {
                    let _ = dotenvy::from_path_iter(dir.join(".env")).map(|mut it| {
                        while let Some(Ok(item)) = it.next() {
                            if std::env::var_os(&item.0).is_none() {
                                std::env::set_var(&item.0, item.1);
                            }
                        }
                    });
                }
            }
            let _ = dotenvy::dotenv();
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("invalid config {}", path.display()))
    }
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:9470".parse().unwrap()
}

fn default_agent_idle_secs() -> u64 {
    1800
}

/// Default config path: platform config dir, e.g. ~/.config/damon/config.toml
pub fn default_config_path() -> PathBuf {
    directories::ProjectDirs::from("dev", "damon", "damon")
        .map(|d| d.config_dir().join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("damon-config.toml"))
}

/// Default data dir for the SQLite store, e.g. ~/.local/share/damon
pub fn default_data_dir() -> PathBuf {
    directories::ProjectDirs::from("dev", "damon", "damon")
        .map(|d| d.data_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from(".damon"))
}

/// Write a starter config if none exists. Returns the path used.
pub fn ensure_config(path: &Path) -> anyhow::Result<PathBuf> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .write(true)
            // create_new keeps check-and-create atomic: exists()+write
            // would clobber a config another damond installed meanwhile.
            .create_new(true)
            // The file may hold a literal auth_token later — create it
            // private instead of writing-then-chmodding.
            .mode(0o600)
            .open(path)
        {
            Ok(mut f) => {
                f.write_all(STARTER_CONFIG.as_bytes())?;
                info!(path = %path.display(), "wrote starter config");
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                warn_if_loose(path);
            }
            Err(e) => return Err(e.into()),
        }
    }
    #[cfg(not(unix))]
    if !path.exists() {
        std::fs::write(path, STARTER_CONFIG)?;
        info!(path = %path.display(), "wrote starter config");
    }
    Ok(path.to_path_buf())
}

/// Warn when an existing config is group/world-accessible — it may hold
/// literal secrets. Warn-only: chmod-ing the user's file unprompted
/// could surprise their tooling.
#[cfg(unix)]
fn warn_if_loose(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let loose = std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o077 != 0)
        .unwrap_or(false);
    if loose {
        warn!(
            path = %path.display(),
            "config file is group/world-accessible; consider chmod 600"
        );
    }
}

const STARTER_CONFIG: &str = r#"# Damon agent core configuration.
#
# Damon drives coding agents through their native CLIs: Claude Code,
# Codex CLI, and Oh My Pi — each backend brings its own login, models,
# and tools. If the CLIs are installed and logged in, nothing here is
# required.

# Top-level keys must precede every [table] — TOML would otherwise
# attach them to the last table above.
# bind = "127.0.0.1:9470"            # default; changing requires restart
# auth_token — a literal token or an env ref like "env:DAMON_TOKEN";
#   required for non-loopback binds
# max_sessions = 16               # cap on live backend sessions; unset = unlimited
# default_backend = "claude"       # when session.create omits `backend`
# permission_timeout_secs = 300    # deny agent permission asks after this
# session_retention_days = 30      # delete sessions older than this

# Launch overrides:
# [backends.claude]
# command = "claude"                   # default launch line for claude
# args = ["-p", "--output-format", "stream-json", "--input-format", "stream-json", "--verbose"]
# [backends.claude.env]
# ANTHROPIC_MODEL = "claude-sonnet-4-5"
"#;

/// Shared, hot-reloadable config. Sync lock: guards are held only for
/// reads/writes of the struct, never across .await.
pub type SharedConfig = Arc<parking_lot::RwLock<Config>>;

/// Watch the config file and reload on change. Returns a receiver that
/// fires after each successful reload; None if the watcher could not be
/// installed.
pub fn watch(path: PathBuf, shared: SharedConfig) -> Option<tokio::sync::mpsc::Receiver<()>> {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
    let (reload_tx, reload_rx) = tokio::sync::mpsc::channel(1);

    let mut watcher = match notify::recommended_watcher(move |res| {
        let _ = event_tx.blocking_send(res);
    }) {
        Ok(w) => w,
        Err(e) => {
            warn!(error = %e, "config watcher unavailable; hot reload disabled");
            return None;
        }
    };

    // Watch the ORIGINAL path's parent dir — a symlinked config reports
    // events under the symlink's dir, not the target's. Match events
    // against both the given path and its canonical form (macOS reports
    // /tmp as /private/tmp).
    let dir = path.parent().unwrap_or(Path::new(".")).to_path_buf();
    let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
    if let Err(e) = watcher.watch(&dir, notify::RecursiveMode::NonRecursive) {
        warn!(error = %e, dir = %dir.display(), "cannot watch config dir");
        return None;
    }

    tokio::spawn(async move {
        let _watcher = watcher; // keep alive for the task's lifetime
        while let Some(res) = event_rx.recv().await {
            match res {
                Ok(event) if event.paths.iter().any(|p| p == &path || p == &canonical) => {
                    // Debounce: editors often write via rename bursts.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    // load_no_env: mutating process env mid-runtime is
                    // unsound — the .env pass only belongs at boot.
                    match Config::load_no_env(&path) {
                        Ok(new) => {
                            {
                                let mut cfg = shared.write();
                                if new.bind != cfg.bind {
                                    warn!("bind change requires restart; keeping {}", cfg.bind);
                                }
                                *cfg = new;
                            }
                            info!("config reloaded");
                            // Coalesce, don't block: a busy consumer would
                            // stall this task, fill event_tx, and make
                            // notify drop later events — losing reloads.
                            let _ = reload_tx.try_send(());
                        }
                        Err(e) => warn!(error = %e, "config reload failed; keeping previous"),
                    }
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "config watch error"),
            }
        }
    });
    Some(reload_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_and_agent_overrides() {
        let cfg: Config = toml::from_str("").unwrap();
        assert_eq!(cfg.bind.to_string(), "127.0.0.1:9470");

        let cfg: Config = toml::from_str(
            r#"
            default_backend = "codex"
            [backends.claude]
            command = "/opt/claude"
            args = ["--flag"]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.default_backend.as_deref(), Some("codex"));
        assert_eq!(
            cfg.backends["claude"].command.as_deref(),
            Some("/opt/claude")
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(toml::from_str::<Config>(r#"providers = {}"#).is_err());
    }

    #[test]
    fn secret_refs_parse_and_describe() {
        assert!(matches!(SecretRef::parse("env:X"), Ok(SecretRef::Env(_))));
        assert!(SecretRef::parse("literal").is_err());
        assert!(SecretRef::parse("env:").is_err());
        assert!(SecretRef::parse("keychain:s").is_err());
        assert_eq!(SecretRef::parse("!echo hi").unwrap().describe(), "!command");
    }

    #[test]
    fn constant_time_eq_behaves() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
