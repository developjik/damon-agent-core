//! Backend registry — the supported agents, their detection on
//! this machine, and launch-line resolution against config overrides.
//!
//! Unlike the old ACP catalog there is no generic "any binary speaks the
//! protocol" fallback: each backend implements one native protocol, so
//! the registry is a fixed set, not an open catalog. Config can still
//! override the launch line per backend (`[backends.X]`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::backend::{AgentClient, claude, codex, droid, omp, opencode, qwen, zcode};
use crate::config::AgentConfig;
use crate::sandbox::{NetworkPolicy, SandboxDecision, SandboxRequest, SandboxSpec, SandboxSupport};

/// One backend entry: how to detect it and how to launch a session.
pub struct BackendSpec {
    /// Damon-facing id, also the routing name in session.create.
    pub id: &'static str,
    pub title: &'static str,
    /// Binary whose presence on PATH means "installed".
    pub detect: &'static str,
    /// Launch line for the native-protocol process.
    pub command: &'static str,
    pub args: &'static [&'static str],
    /// Catalog-default env (`[backends.X].env` wins per-key).
    pub env: &'static [(&'static str, &'static str)],
    /// How the agent authenticates — surfaced by doctor and errors.
    pub auth_hint: &'static str,
    /// npm package to install/update through (`npm i -g`), when the CLI
    /// ships that way. `None` → `manual_update` (or Unknown).
    pub npm_package: Option<&'static str>,
    /// Copy-paste update command for CLIs npm doesn't distribute.
    /// Empty string + `npm_package: None` = no known update path.
    pub manual_update: &'static str,
    /// How the backend launches — the thing that decides whether a sandbox
    /// driver has a per-session child to wrap. Serve-mode backends are
    /// `unsupported`, never silently `none` (see `sandbox::decide`).
    pub sandbox_support: SandboxSupport,
    /// Egress this backend needs when the user leaves
    /// `sandbox_network = "backend"`. Every spawn-mode agent CLI talks to
    /// its model API, so denying it here would leave a CLI that cannot
    /// think; one-off command confinement opts into `deny` instead.
    pub sandbox_network: NetworkPolicy,
}

pub const BACKENDS: &[BackendSpec] = &[
    BackendSpec {
        id: "claude",
        title: "Claude Code (Claude Pro/Max)",
        detect: "claude",
        command: "claude",
        args: &[
            "-p",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--verbose",
        ],
        env: &[],
        auth_hint: "log in with `claude` once; the subscription follows",
        npm_package: Some("@anthropic-ai/claude-code"),
        manual_update: "",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "codex",
        title: "Codex CLI (ChatGPT Plus/Pro)",
        detect: "codex",
        command: "codex",
        // --enable default_mode_request_user_input turns on the model's
        // mid-turn question channel (item/tool/requestUserInput) that
        // Damon relays as a Question card; verified live against
        // codex-cli 0.157.1 (the server confirms via a `warning`
        // notification listing the enabled feature).
        args: &["app-server", "--enable", "default_mode_request_user_input"],
        env: &[],
        auth_hint: "log in with `codex` once; the subscription follows",
        npm_package: Some("@openai/codex"),
        manual_update: "",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "omp",
        title: "Oh My Pi",
        detect: "omp",
        command: "omp",
        args: &["--mode", "rpc"],
        env: &[],
        auth_hint: "configure `omp` once; its provider auth follows",
        npm_package: Some("@oh-my-pi/pi-coding-agent"),
        manual_update: "",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "pi",
        title: "Pi (pi.dev)",
        detect: "pi",
        command: "pi",
        // Same rpc wire as omp (its fork parent): ready/negotiate
        // handshake, message_update deltas, extension dialogs. pi's own
        // tools run under its trust model — no approval switches — so
        // the catalog advertises no mode switches for it.
        args: &["--mode", "rpc"],
        env: &[],
        auth_hint: "configure `pi` with a provider API key; auth follows the CLI",
        npm_package: Some("@earendil-works/pi-coding-agent"),
        manual_update: "",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "qwen",
        title: "Qwen Code (Alibaba)",
        detect: "qwen",
        command: "qwen",
        // Bidirectional stream-json with a Claude-shaped control plane;
        // permission asks and interrupts relay (verified live against
        // 0.24.6). Auth stays the CLI's own (qwen-oauth or API key in
        // its settings) — Damon passes nothing.
        args: &[
            "-p",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--include-partial-messages",
        ],
        env: &[],
        auth_hint: "log in with `qwen` (qwen-oauth) or configure an API key in its settings; auth follows the CLI",
        npm_package: Some("@qwen-code/qwen-code"),
        manual_update: "",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "droid",
        title: "Droid (Factory)",
        detect: "droid",
        command: "droid",
        // JSON-RPC over stdio: permission asks arrive as
        // droid.request_permission the client must answer (verified
        // live against 0.228.0). Custom models and auth stay in the
        // CLI's own ~/.factory settings.
        args: &[
            "exec",
            "--input-format",
            "stream-jsonrpc",
            "-o",
            "stream-jsonrpc",
        ],
        env: &[],
        auth_hint: "log in with `droid` or set FACTORY_API_KEY; custom models live in ~/.factory/settings.json",
        npm_package: None,
        manual_update: "curl -fsSL https://app.factory.ai/cli | sh",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "opencode",
        title: "OpenCode (sst)",
        detect: "opencode",
        command: "opencode",
        // HTTP+SSE: Damon adopts a running `opencode serve` or spawns
        // its own; permission asks and questions round-trip over HTTP
        // (verified live against 1.18.32).
        args: &[],
        env: &[],
        auth_hint: "configure providers in opencode itself; Damon adopts or spawns `opencode serve`",
        npm_package: Some("opencode-ai"),
        manual_update: "",
        // HTTP+SSE against a shared `opencode serve`: no per-session child
        // to confine, so a sandbox request is refused rather than ignored.
        sandbox_support: SandboxSupport::Serve,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "mimo",
        title: "MiMo Code (Xiaomi)",
        detect: "mimo",
        command: "mimo",
        // OpenCode fork speaking the same serve surface (verified
        // against its inherited HTTP API).
        args: &[],
        env: &[],
        auth_hint: "configure providers in mimo itself; Damon adopts or spawns `mimo serve`",
        npm_package: None,
        manual_update: "curl -fsSL https://mimo.xiaomi.com/install | bash",
        // opencode's serve surface, and the same sandbox answer: unsupported.
        sandbox_support: SandboxSupport::Serve,
        sandbox_network: NetworkPolicy::Allow,
    },
    BackendSpec {
        id: "zcode",
        title: "ZCode (Z.ai)",
        detect: "zcode",
        command: "zcode",
        // "ZCode Protocol" v1 over stdio: {id, method, params} frames
        // (no jsonrpc field), permission asks as interaction/
        // requestPermission server requests (envelope + session create
        // verified live against 3.14.3; the model turn needs a GLM
        // login).
        args: &["app-server", "--stdio"],
        env: &[],
        auth_hint: "log in with `zcode` (GLM Coding Plan); auth follows the CLI",
        npm_package: None,
        manual_update: "",
        sandbox_support: SandboxSupport::Spawn,
        sandbox_network: NetworkPolicy::Allow,
    },
];

/// A backend entry resolved against config overrides.
#[derive(Clone, PartialEq)]
pub struct ResolvedBackend {
    pub id: String,
    pub title: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub auth_hint: String,
    /// Whether the detect binary was found on PATH at resolve time.
    pub detected: bool,
    /// The sandbox declaration for this backend: its own `[backends.X]
    /// sandbox`, else the top-level default.
    pub sandbox: SandboxRequest,
    /// Spawn vs serve — whether a driver has a child to wrap at all.
    pub sandbox_support: SandboxSupport,
    /// Egress inside the sandbox, with `backend` already resolved to the
    /// catalog's declaration.
    pub sandbox_network: NetworkPolicy,
}

/// Resolve catalog + config overrides into launch lines. Shared by boot
/// and hot-reload so a reloaded config produces exactly the same backend
/// set a restart would.
pub fn resolve_backends(
    overrides: &BTreeMap<String, AgentConfig>,
    default_sandbox: SandboxRequest,
    default_network: NetworkPolicy,
) -> Vec<ResolvedBackend> {
    BACKENDS
        .iter()
        .map(|spec| {
            let o = overrides.get(spec.id);
            ResolvedBackend {
                id: spec.id.to_string(),
                title: spec.title.to_string(),
                command: o
                    .and_then(|c| c.command.clone())
                    .unwrap_or_else(|| spec.command.to_string()),
                args: o
                    .and_then(|c| c.args.clone())
                    .unwrap_or_else(|| spec.args.iter().map(|s| s.to_string()).collect()),
                env: {
                    let mut env: Vec<(String, String)> = spec
                        .env
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    if let Some(extra) = o.map(|c| &c.env) {
                        for (k, v) in extra {
                            if let Some(e) = env.iter_mut().find(|(ek, _)| *ek == *k) {
                                e.1 = v.clone();
                            } else {
                                env.push((k.clone(), v.clone()));
                            }
                        }
                    }
                    env
                },
                auth_hint: spec.auth_hint.to_string(),
                detected: which(spec.detect).is_some(),
                sandbox: o.and_then(|c| c.sandbox).unwrap_or(default_sandbox),
                sandbox_support: spec.sandbox_support,
                sandbox_network: match default_network {
                    NetworkPolicy::Backend => spec.sandbox_network,
                    explicit => explicit,
                },
            }
        })
        .collect()
}

/// [`resolve_backends`] against a loaded config — the sandbox defaults ride
/// with the launch lines, so every surface (boot, hot reload, `doctor`, the
/// backend.* RPCs) resolves a backend exactly the same way.
pub fn resolve_from_config(cfg: &crate::config::Config) -> Vec<ResolvedBackend> {
    resolve_backends(&cfg.backends, cfg.sandbox, cfg.sandbox_network)
}

impl ResolvedBackend {
    /// The isolation decision for one session on this backend.
    ///
    /// `Err` is the fail-loudly case: the user (per session, per backend, or
    /// globally) asked for a driver this backend or host cannot honor. The
    /// message names the cause and the choices — this is the spawn point's
    /// chance to refuse before the agent CLI ever starts.
    pub fn sandbox_decision(
        &self,
        session: &crate::backend::types::SessionConfig,
    ) -> anyhow::Result<SandboxDecision> {
        let spec = SandboxSpec {
            backend: &self.id,
            support: self.sandbox_support,
            request: session.sandbox.unwrap_or(self.sandbox),
            network: self.sandbox_network,
            cwd: &session.cwd,
            extra_writable: &session.granted_roots,
            launch_env: &self.env,
        };
        crate::sandbox::decide(&spec)
    }

    /// Spawn this backend's stdio child through the sandbox driver.
    ///
    /// Every spawn-mode backend's launch goes through here instead of
    /// `NdjsonTransport::spawn` directly, so "the agent CLI runs under the
    /// resolved policy" is one call site per backend rather than one more
    /// thing each backend has to remember.
    pub async fn spawn_ndjson(
        &self,
        args: &[String],
        env: &std::collections::HashMap<String, String>,
        session: &crate::backend::types::SessionConfig,
    ) -> anyhow::Result<Arc<crate::backend::transport::NdjsonTransport>> {
        let decision = self.sandbox_decision(session)?;
        crate::backend::transport::NdjsonTransport::spawn(
            &self.command,
            args,
            env,
            &session.cwd,
            Some(&decision),
        )
        .await
    }
}

/// Build the client for a resolved backend. Returns None for an id the
/// registry doesn't know — overrides can change launch lines, not add
/// protocols.
pub fn client_for(resolved: &ResolvedBackend) -> Option<Arc<dyn AgentClient>> {
    match resolved.id.as_str() {
        "claude" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            claude::ClaudeDialect::dialect(),
        ))),
        "codex" => Some(Arc::new(codex::CodexClient::new(resolved.clone()))),
        "droid" => Some(Arc::new(droid::DroidClient::new(resolved.clone()))),
        "opencode" => Some(Arc::new(opencode::OpencodeClient::new(
            resolved, "opencode",
        ))),
        "mimo" => Some(Arc::new(opencode::OpencodeClient::new(resolved, "mimo"))),
        "zcode" => Some(Arc::new(zcode::ZcodeClient::new(resolved.clone()))),
        "omp" => Some(Arc::new(omp::OmpClient::new(resolved.clone()))),
        "pi" => Some(Arc::new(omp::OmpClient::with_profile(
            resolved.clone(),
            &omp::PI,
        ))),
        "qwen" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            qwen::QwenDialect::dialect(),
        ))),
        _ => None,
    }
}

/// PATH lookup without shelling out to `which`.
pub(crate) fn which(bin: &str) -> Option<PathBuf> {
    #[cfg(windows)]
    const EXTS: &[&str] = &[".exe", ".cmd", ".bat", ""];
    #[cfg(not(windows))]
    const EXTS: &[&str] = &[""];
    for dir in search_dirs() {
        for ext in EXTS {
            let candidate = dir.join(format!("{bin}{ext}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Directories searched on every `which` call, in order: the process
/// env's PATH first, then — on Windows — the CURRENT registry PATHs
/// (machine, then user). A resident daemon keeps the PATH snapshot of
/// whatever launched it; a CLI installed while the daemon runs is
/// invisible to that snapshot but present in the registry, so re-
/// reading it makes freshly installed CLIs detectable without a
/// daemon restart (adapted from upstream desktop-cc-gui's registry
/// PATH re-read).
fn search_dirs() -> Vec<PathBuf> {
    let dirs: Vec<PathBuf> = std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).collect())
        .unwrap_or_default();
    // Block-tail, not `return`: with the `#[cfg(not(windows))]` arm
    // compiled out, a `return dirs` here is the function tail and
    // windows clippy (-D warnings) flags needless_return.
    #[cfg(windows)]
    let dirs = {
        let mut dirs = dirs;
        dirs.extend(registry_path_dirs());
        dirs
    };
    dirs
}

/// The machine and user PATH values straight from the registry
/// (adapted from upstream desktop-cc-gui). `RegGetValueW` expands
/// `REG_EXPAND_SZ` values (`%SystemRoot%\...`) into plain text, so no
/// manual expansion pass is needed. Read failures contribute nothing
/// — the env PATH above still stands on its own.
#[cfg(windows)]
fn registry_path_dirs() -> Vec<PathBuf> {
    use windows_sys::Win32::System::Registry::{
        HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE, RRF_RT_REG_SZ, RegGetValueW,
    };

    fn utf16(s: &str) -> Vec<u16> {
        s.encode_utf16().chain([0]).collect()
    }

    fn read(root: windows_sys::Win32::System::Registry::HKEY, subkey: &str) -> Vec<PathBuf> {
        let mut buf = [0u16; 32 * 1024];
        let mut len = (buf.len() * 2) as u32;
        let ret = unsafe {
            RegGetValueW(
                root,
                utf16(subkey).as_ptr(),
                utf16("Path").as_ptr(),
                RRF_RT_REG_SZ,
                std::ptr::null_mut(),
                buf.as_mut_ptr().cast(),
                &mut len,
            )
        };
        if ret != 0 {
            return Vec::new();
        }
        // `len` is bytes copied, including the NUL when present.
        let chars = &buf[..(len as usize / 2).min(buf.len())];
        let end = chars.iter().position(|&c| c == 0).unwrap_or(chars.len());
        std::env::split_paths(&String::from_utf16_lossy(&chars[..end])).collect()
    }

    let mut out = read(
        HKEY_LOCAL_MACHINE,
        r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
    );
    out.extend(read(HKEY_CURRENT_USER, "Environment"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_dirs_lead_with_the_env_path() {
        // The env PATH is always the FIRST search source — the
        // registry PATHs (Windows) only extend the list behind it.
        let dirs = search_dirs();
        let env: Vec<PathBuf> = std::env::var_os("PATH")
            .map(|p| std::env::split_paths(&p).collect())
            .unwrap_or_default();
        let n = env.len().min(dirs.len());
        assert!(
            !dirs.is_empty(),
            "a PATH-less environment cannot resolve CLIs"
        );
        assert_eq!(&dirs[..n], &env[..n]);
    }

    #[test]
    fn catalog_lists_supported_backends() {
        assert_eq!(BACKENDS.len(), 9);
        let ids: Vec<_> = BACKENDS.iter().map(|b| b.id).collect();
        assert_eq!(
            ids,
            [
                "claude", "codex", "omp", "pi", "qwen", "droid", "opencode", "mimo", "zcode"
            ]
        );
    }

    /// Only the serve-mode backends have no spawn point to confine; every
    /// stdio backend does, and every agent CLI declares that it needs its
    /// model API (network allow).
    #[test]
    fn sandbox_support_follows_the_process_model() {
        for spec in BACKENDS {
            let want = match spec.id {
                "opencode" | "mimo" => SandboxSupport::Serve,
                _ => SandboxSupport::Spawn,
            };
            assert_eq!(spec.sandbox_support, want, "{}", spec.id);
            assert_eq!(spec.sandbox_network, NetworkPolicy::Allow, "{}", spec.id);
        }
    }

    /// The global `sandbox_network` override reaches every backend, while
    /// the default (`backend`) keeps each one's own declaration.
    #[test]
    fn network_override_wins_over_the_catalog() {
        let resolved =
            resolve_backends(&BTreeMap::new(), SandboxRequest::Auto, NetworkPolicy::Deny);
        assert!(
            resolved
                .iter()
                .all(|b| b.sandbox_network == NetworkPolicy::Deny)
        );
    }

    /// A per-backend `sandbox` beats the top-level default at resolve time,
    /// which is what the spawn point later sees.
    #[test]
    fn backend_sandbox_override_beats_the_default() {
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "codex".to_string(),
            AgentConfig {
                sandbox: Some(SandboxRequest::None),
                ..Default::default()
            },
        );
        let resolved = resolve_backends(&overrides, SandboxRequest::Bwrap, NetworkPolicy::Backend);
        let codex = resolved.iter().find(|b| b.id == "codex").unwrap();
        assert_eq!(codex.sandbox, SandboxRequest::None);
        let claude = resolved.iter().find(|b| b.id == "claude").unwrap();
        assert_eq!(claude.sandbox, SandboxRequest::Bwrap);
    }

    #[test]
    fn overrides_replace_launch_line() {
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "claude".to_string(),
            AgentConfig {
                command: Some("/opt/claude-custom".to_string()),
                args: Some(vec!["--flag".to_string()]),
                env: std::collections::HashMap::from([("K".to_string(), "V".to_string())]),
                sandbox: None,
            },
        );
        let resolved = resolve_backends(&overrides, SandboxRequest::Auto, NetworkPolicy::Backend);
        let claude = resolved.iter().find(|b| b.id == "claude").unwrap();
        assert_eq!(claude.command, "/opt/claude-custom");
        assert_eq!(claude.args, vec!["--flag"]);
        assert!(claude.env.contains(&("K".to_string(), "V".to_string())));
        // Untouched backends keep catalog defaults.
        let codex = resolved.iter().find(|b| b.id == "codex").unwrap();
        assert_eq!(codex.command, "codex");
    }
}
