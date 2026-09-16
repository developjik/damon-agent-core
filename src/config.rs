use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;
use tracing::{info, warn};
use notify::Watcher;

/// Daemon configuration. Hot-reloaded: changes to the config file are picked
/// up without restart, except `bind` which requires a restart.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Listen address. Default 127.0.0.1:9470.
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
    /// Bearer token required on /v1/* and /ws when set. May be a literal or a
    /// `env:`/`keychain:` reference.
    pub auth_token: Option<String>,
    /// Directory for the SQLite store. Default: platform data dir.
    pub data_dir: Option<PathBuf>,
    /// TLS cert/key for wss:// remote access. Both or neither.
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// MCP tool servers (stdio).
    #[serde(default)]
    pub mcp_servers: HashMap<String, McpServerConfig>,
    /// LLM providers keyed by name. `default` is used when no routing exists.
    /// BTreeMap so iteration order (and the default fallback) is deterministic.
    #[serde(default)]
    pub providers: std::collections::BTreeMap<String, ProviderConfig>,
    /// Per-model metadata overrides, keyed by model id or glob.
    /// `[models."claude-*"]` context_window = 200000 etc.
    #[serde(default)]
    pub models: std::collections::BTreeMap<String, ModelMeta>,
    /// Remote relay: dial out to a public `damon-relay` so clients can reach
    /// this daemon without an inbound port.
    pub relay: Option<RelayConfig>,
}

/// `[relay]` — outbound tunnel to a public relay.
#[derive(Clone, Debug, Deserialize)]
pub struct RelayConfig {
    /// ws://host:port of the relay server.
    pub url: String,
    /// Name clients use to reach this daemon through the relay.
    pub name: String,
    /// Registration secret when the relay sets DAMON_RELAY_SECRET.
    /// Supports env:/keychain:/!cmd secret refs.
    pub secret: Option<String>,
}

/// Per-model metadata. User config (`[models."<id-or-glob>"]`) overrides
/// the built-in hints; unknown models get None → no context-aware features.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ModelMeta {
    /// Total context window in tokens.
    pub context_window: Option<u64>,
    /// Max output tokens the model can emit.
    pub max_output_tokens: Option<u64>,
    /// USD per million input tokens (for usage reporting).
    pub input_cost: Option<f64>,
    /// USD per million output tokens.
    pub output_cost: Option<f64>,
}

/// Built-in context-window hints for common model families. Deliberately
/// small — the [models] table is the override surface, not a catalog.
fn builtin_context_window(model: &str) -> Option<u64> {
    Some(match model {
        m if m.starts_with("claude-") => 200_000,
        m if m.starts_with("gpt-4o") || m.starts_with("gpt-4-turbo") => 128_000,
        m if m.starts_with("gpt-4.1") || m.starts_with("gpt-5") => 400_000,
        m if m.starts_with('o')
            && m[1..].chars().next().is_some_and(|c| c.is_ascii_digit()) =>
        {
            200_000
        }
        m if m.starts_with("gemini-") => 1_000_000,
        m if m.starts_with("llama")
            || m.starts_with("qwen")
            || m.starts_with("mistral")
            || m.starts_with("deepseek") =>
        {
            128_000
        }
        _ => return None,
    })
}

fn default_bind() -> SocketAddr {
    "127.0.0.1:9470".parse().unwrap()
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct McpServerConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Skip the permission round-trip for tools from this server.
    #[serde(default)]
    pub auto_approve: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProviderConfig {
    /// Wire API: "openai-completions" (default), "openai-responses",
    /// "anthropic-messages", or "gemini".
    #[serde(default = "default_provider_api")]
    pub api: String,
    /// Base URL. Defaults per api when omitted.
    pub base_url: Option<String>,
    /// Secret reference: `env:VAR_NAME` or `keychain:service/account`.
    /// Bare literals are rejected — secrets never live in config files.
    pub api_key: Option<String>,
    /// Model globs routed to this provider, e.g. ["claude-*"].
    #[serde(default)]
    pub models: Vec<String>,
    /// Model the agent runtime uses with this provider.
    pub default_model: Option<String>,
    /// Extra headers sent on every upstream request. Values may be
    /// `env:`/`keychain:`/`!cmd` secret references or literals.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Model discovery: "openai-models-list" (GET {base}/models) or
    /// "ollama" (GET {base}/api/tags).
    pub discovery: Option<String>,
    /// Fallback model on context-overflow errors: "model-id" (same provider)
    /// or "provider/model-id". Retried once before surfacing the error.
    pub context_promotion_target: Option<String>,
    /// Endpoint quirk flags — see ProviderCompat.
    #[serde(default)]
    pub compat: ProviderCompat,
}

fn default_provider_api() -> String {
    "openai-completions".to_string()
}

/// Per-provider endpoint quirks, applied when shaping OpenAI requests.
/// All default to "leave the request alone" — set only what the endpoint needs.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ProviderCompat {
    /// Send `store: false` on every request (non-standard endpoints that
    /// reject or ignore the field get it explicitly).
    #[serde(default)]
    pub supports_store: bool,
    /// Rewrite `system` role messages to `developer` (reasoning models).
    #[serde(default)]
    pub supports_developer_role: bool,
    /// Preserve separate leading system/developer messages. `false` coalesces
    /// them into one (strict-template / local hosts).
    #[serde(default = "default_true")]
    pub supports_multiple_system_messages: bool,
    /// Token-limit field name: "max_tokens" (default) or
    /// "max_completion_tokens" (o-series / newer endpoints).
    pub max_tokens_field: Option<String>,
    /// Tool-result messages must carry a `name` field (Mistral).
    #[serde(default)]
    pub requires_tool_result_name: bool,
    /// Normalize tool-call ids to exactly 9 alphanumeric chars (Mistral).
    #[serde(default)]
    pub requires_mistral_tool_ids: bool,
    /// Send `stream_options.include_usage` on streaming requests.
    #[serde(default = "default_true")]
    pub supports_usage_in_streaming: bool,
    /// Extra top-level fields merged into every request body.
    #[serde(default)]
    pub extra_body: HashMap<String, serde_json::Value>,
    /// Render tools as a text prompt and parse <tool_call> blocks from the
    /// response — for local models without a native tool API.
    #[serde(default)]
    pub inband_tools: bool,
}

fn default_true() -> bool {
    true
}

/// A secret that lives outside the config file.
#[derive(Clone, Debug)]
pub enum SecretRef {
    Env(String),
    Keychain { service: String, account: String },
    /// `!command` — resolved from the command's stdout (10s timeout).
    Command(String),
}

impl SecretRef {
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        if let Some(var) = raw.strip_prefix("env:") {
            if var.is_empty() {
                bail!("empty env var name in secret reference");
            }
            return Ok(Self::Env(var.to_string()));
        }
        if let Some(rest) = raw.strip_prefix("keychain:") {
            let (service, account) = rest
                .split_once('/')
                .context("keychain reference must be keychain:<service>/<account>")?;
            if service.is_empty() || account.is_empty() {
                bail!("keychain reference must be keychain:<service>/<account>");
            }
            return Ok(Self::Keychain {
                service: service.to_string(),
                account: account.to_string(),
            });
        }
        if let Some(cmd) = raw.strip_prefix('!') {
            if cmd.trim().is_empty() {
                bail!("empty command in secret reference");
            }
            return Ok(Self::Command(cmd.to_string()));
        }
        bail!("secret must be a reference, not a literal — use env:VAR, keychain:service/account, or !command")
    }

    pub fn resolve(&self) -> anyhow::Result<String> {
        match self {
            Self::Env(var) => {
                std::env::var(var).with_context(|| format!("env var {var} is not set"))
            }
            Self::Keychain { service, account } => keyring::Entry::new(service, account)
                .context("keychain backend unavailable")?
                .get_password()
                .with_context(|| format!("keychain entry {service}/{account} not found")),
            Self::Command(cmd) => resolve_command(cmd),
        }
    }
}

/// Run a shell command and return trimmed stdout. 10s timeout; empty or
/// failing commands are errors (the provider build then fails cleanly).
fn resolve_command(cmd: &str) -> anyhow::Result<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::Duration;
    use wait_timeout::ChildExt;

    let mut child = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("cannot spawn secret command: {cmd}"))?;
    // Drain stdout on a reader thread — a child that writes more than the
    // OS pipe buffer blocks on write and would otherwise always time out.
    let mut stdout = child.stdout.take();
    let (out_tx, out_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut out = String::new();
        if let Some(s) = stdout.as_mut() {
            let _ = s.read_to_string(&mut out);
        }
        let _ = out_tx.send(out);
    });
    let status = match child.wait_timeout(Duration::from_secs(10))? {
        Some(s) => s,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            bail!("secret command timed out after 10s: {cmd}");
        }
    };
    // A backgrounded grandchild can hold the pipe open forever — bound
    // the read instead of joining unconditionally.
    let out = out_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_default();
    if !status.success() {
        bail!("secret command failed ({status}): {cmd}");
    }
    let out = out.trim().to_string();
    if out.is_empty() {
        bail!("secret command produced no output: {cmd}");
    }
    Ok(out)
}
/// Constant-time byte equality for token/proof comparisons — a remote
/// endpoint must not get a timing oracle on the secret.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        // .env files: config dir first (daemon-specific), then cwd.
        // dotenvy never overrides already-set vars; earlier files win.
        if let Some(dir) = path.parent() {
            let _ = dotenvy::from_path(dir.join(".env"));
        }
        let _ = dotenvy::from_path(Path::new(".env"));
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config {}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("invalid TOML in {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> anyhow::Result<()> {
        for (name, p) in &self.providers {
            match p.api.as_str() {
                "openai-completions" | "openai-responses" | "anthropic-messages" | "gemini" => {}
                other => bail!("provider {name}: unknown api '{other}'"),
            }
            if let Some(key) = &p.api_key {
                SecretRef::parse(key)
                    .with_context(|| format!("provider {name}: invalid api_key"))?;
            }
        }
        Ok(())
    }

    /// Route a model name to a provider: explicit `provider/model` prefix,
    /// then config globs, then default.
    pub fn route_model<'a>(&'a self, model: &'a str) -> Option<(&'a str, &'a ProviderConfig, String)> {
        self.route_model_strict(model).or_else(|| {
            self.default_provider()
                .map(|(n, p)| (n, p, model.to_string()))
        })
    }

    /// Route without the default fallback — only explicit prefix or glob.
    /// Used to distinguish "matched" from "fell through to default" so
    /// discovered models can claim the request first.
    pub fn route_model_strict<'a>(
        &'a self,
        model: &'a str,
    ) -> Option<(&'a str, &'a ProviderConfig, String)> {
        if let Some((name, upstream)) = model.split_once('/') {
            if let Some(p) = self.providers.get(name) {
                return Some((name, p, upstream.to_string()));
            }
        }
        for (name, p) in &self.providers {
            if p.models.iter().any(|g| glob_match(g, model)) {
                return Some((name, p, model.to_string()));
            }
        }
        None
    }

    pub fn default_provider(&self) -> Option<(&str, &ProviderConfig)> {
        self.providers
            .get("default")
            .map(|p| ("default", p))
            .or_else(|| self.providers.iter().next().map(|(n, p)| (n.as_str(), p)))
    }


    /// Metadata for a model: user [models] entry (exact then glob) wins,
    /// then the built-in context-window hint.
    pub fn model_meta(&self, model: &str) -> ModelMeta {
        if let Some(m) = self.models.get(model) {
            return m.clone();
        }
        for (pat, m) in &self.models {
            if glob_match(pat, model) {
                return m.clone();
            }
        }
        ModelMeta {
            context_window: builtin_context_window(model),
            ..Default::default()
        }
    }

    /// Split a `model:level` suffix. Returns (model, level) where level is
    /// one of "low" | "medium" | "high", or None if absent/unrecognized.
    pub fn split_thinking_level(model: &str) -> (&str, Option<&str>) {
        match model.rsplit_once(':') {
            Some((m, level @ ("low" | "medium" | "high"))) => (m, Some(level)),
            _ => (model, None),
        }
    }
}

/// Minimal glob: `*` matches any suffix/infix, `?` one char.
pub fn glob_match(pattern: &str, s: &str) -> bool {
    glob_rec(pattern.as_bytes(), s.as_bytes())
}

fn glob_rec(p: &[u8], s: &[u8]) -> bool {
    match (p.first(), s.first()) {
        (None, None) => true,
        (Some(b'*'), _) => (0..=s.len()).any(|i| glob_rec(&p[1..], &s[i..])),
        (Some(b'?'), Some(_)) => glob_rec(&p[1..], &s[1..]),
        (Some(a), Some(b)) => a == b && glob_rec(&p[1..], &s[1..]),
        _ => false,
    }
}


/// Default config path: platform config dir, e.g. ~/.config/damon/config.toml
pub fn default_config_path() -> PathBuf {
    directories::ProjectDirs::from("dev", "damon", "damon")
        .map(|d| d.config_dir().join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("damon.toml"))
}

/// Default data dir for the SQLite store, e.g. ~/.local/share/damon
pub fn default_data_dir() -> PathBuf {
    directories::ProjectDirs::from("dev", "damon", "damon")
        .map(|d| d.data_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Write a starter config if none exists. Returns the path used.
pub fn ensure_config(path: &Path) -> anyhow::Result<PathBuf> {
    if path.exists() {
        return Ok(path.to_path_buf());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, STARTER_CONFIG)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    info!(path = %path.display(), "wrote starter config");
    Ok(path.to_path_buf())
}

const STARTER_CONFIG: &str = r#"# Damon agent core configuration
# bind = "127.0.0.1:9470"        # default; changing requires restart
# auth_token = "env:DAMON_TOKEN" # optional; literal or env:/keychain: ref

# [providers.default]
# base_url = "https://api.openai.com/v1"
# api_key  = "env:OPENAI_API_KEY"   # or "keychain:damon/openai"
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
                Ok(event)
                    if event
                        .paths
                        .iter()
                        .any(|p| p == &path || p == &canonical) =>
                {
                    // Debounce: editors often write via rename bursts.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    match Config::load(&path) {
                        Ok(new) => {
                            {
                                let mut cfg = shared.write();
                                if new.bind != cfg.bind {
                                    warn!("bind change requires restart; keeping {}", cfg.bind);
                                }
                                *cfg = new;
                            }
                            info!("config reloaded");
                            let _ = reload_tx.send(()).await;
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
