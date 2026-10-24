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
}

/// Resolve catalog + config overrides into launch lines. Shared by boot
/// and hot-reload so a reloaded config produces exactly the same backend
/// set a restart would.
pub fn resolve_backends(overrides: &BTreeMap<String, AgentConfig>) -> Vec<ResolvedBackend> {
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
            }
        })
        .collect()
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
    #[cfg(windows)]
    {
        let mut dirs = dirs;
        dirs.extend(registry_path_dirs());
        return dirs;
    }
    #[cfg(not(windows))]
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

    #[test]
    fn overrides_replace_launch_line() {
        let mut overrides = BTreeMap::new();
        overrides.insert(
            "claude".to_string(),
            AgentConfig {
                command: Some("/opt/claude-custom".to_string()),
                args: Some(vec!["--flag".to_string()]),
                env: std::collections::HashMap::from([("K".to_string(), "V".to_string())]),
            },
        );
        let resolved = resolve_backends(&overrides);
        let claude = resolved.iter().find(|b| b.id == "claude").unwrap();
        assert_eq!(claude.command, "/opt/claude-custom");
        assert_eq!(claude.args, vec!["--flag"]);
        assert!(claude.env.contains(&("K".to_string(), "V".to_string())));
        // Untouched backends keep catalog defaults.
        let codex = resolved.iter().find(|b| b.id == "codex").unwrap();
        assert_eq!(codex.command, "codex");
    }
}
