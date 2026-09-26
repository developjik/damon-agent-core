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

use crate::backend::{AgentClient, amp, claude, codex, cursor, gemini, kimi, omp, qwen};
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
    },
    BackendSpec {
        id: "codex",
        title: "Codex CLI (ChatGPT Plus/Pro)",
        detect: "codex",
        command: "codex",
        args: &["app-server"],
        env: &[],
        auth_hint: "log in with `codex` once; the subscription follows",
    },
    BackendSpec {
        id: "omp",
        title: "Oh My Pi",
        detect: "omp",
        command: "omp",
        args: &["--mode", "rpc"],
        env: &[],
        auth_hint: "configure `omp` once; its provider auth follows",
    },
    BackendSpec {
        id: "cursor",
        title: "Cursor Agent (Cursor subscription)",
        detect: "cursor-agent",
        command: "cursor-agent",
        args: &["-p", "--output-format", "stream-json", "--trust"],
        env: &[],
        auth_hint: "log in with `cursor-agent login` or set CURSOR_API_KEY",
    },
    BackendSpec {
        id: "amp",
        title: "Amp (Sourcegraph)",
        detect: "amp",
        command: "amp",
        // Launch args come from the dialect — `threads continue` is a
        // subcommand that must precede the exec flags.
        args: &[],
        env: &[],
        auth_hint: "log in with `amp` once; the login follows",
    },
    BackendSpec {
        id: "kimi",
        title: "Kimi Code (Moonshot)",
        detect: "kimi",
        command: "kimi",
        args: &["-p", "--output-format", "stream-json"],
        env: &[],
        auth_hint: "log in with `kimi login` once; the token follows",
    },
    BackendSpec {
        id: "qwen",
        title: "Qwen Code (Alibaba)",
        detect: "qwen",
        command: "qwen",
        args: &["-p", "--output-format", "stream-json"],
        env: &[],
        auth_hint: "log in with `qwen` once; the OAuth flow follows",
    },
    BackendSpec {
        id: "gemini",
        title: "Gemini CLI (Google)",
        detect: "gemini",
        command: "gemini",
        // One-shot headless runs; the dialect appends --prompt <text>,
        // --approval-mode, and resume/model flags per turn. --skip-trust
        // keeps the folder-trust prompt from blocking a non-TTY spawn.
        args: &["--output-format", "stream-json", "--skip-trust"],
        env: &[],
        auth_hint: "log in with `gemini` once or set GEMINI_API_KEY; the quota follows",
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
        "cursor" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            cursor::CursorDialect::dialect(),
        ))),
        "amp" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            amp::AmpDialect::dialect(),
        ))),
        "kimi" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            kimi::KimiDialect::dialect(),
        ))),
        "qwen" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            qwen::QwenDialect::dialect(),
        ))),
        "gemini" => Some(Arc::new(crate::backend::streamjson::StreamJsonClient::new(
            resolved.clone(),
            gemini::GeminiDialect::dialect(),
        ))),
        "codex" => Some(Arc::new(codex::CodexClient::new(resolved.clone()))),
        "omp" => Some(Arc::new(omp::OmpClient::new(resolved.clone()))),
        _ => None,
    }
}

/// PATH lookup without shelling out to `which`.
pub(crate) fn which(bin: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    #[cfg(windows)]
    const EXTS: &[&str] = &[".exe", ".cmd", ".bat", ""];
    #[cfg(not(windows))]
    const EXTS: &[&str] = &[""];
    for dir in std::env::split_paths(&path) {
        for ext in EXTS {
            let candidate = dir.join(format!("{bin}{ext}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_has_eight_backends() {
        assert_eq!(BACKENDS.len(), 8);
        let ids: Vec<_> = BACKENDS.iter().map(|b| b.id).collect();
        assert_eq!(
            ids,
            [
                "claude", "codex", "omp", "cursor", "amp", "kimi", "qwen", "gemini"
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
