//! Where skills live per agent CLI. The target table is the single
//! source of truth for two consumers: the hub's sync/classify
//! engine, and the composer's `/` picker (`slash_catalog`), so a
//! skill the hub installs appears in the picker with no extra
//! wiring. Every root is read from the daemon's own environment and
//! home (never from client input) — the same trust boundary as the
//! `~/.claude/projects` transcript discovery.

use std::path::{Path, PathBuf};

/// A sync target: one agent CLI's skill directory family.
pub struct Target {
    pub id: &'static str,
    pub label: &'static str,
    /// Hidden targets still classify and sync but are not offered as
    /// checkboxes in the UI — `agents` is the cross-agent convention
    /// dir, not an engine damon drives.
    pub visible: bool,
    /// Resolve the engine home: env overrides win over the `~/`
    /// default, `~` expanded.
    pub home: fn() -> Option<PathBuf>,
}

/// `$CLAUDE_CONFIG_DIR` when set non-empty, else `~/.claude`.
pub fn claude_home() -> Option<PathBuf> {
    env_dir("CLAUDE_CONFIG_DIR").or_else(|| home().map(|h| h.join(".claude")))
}

/// `$CODEX_HOME` when set non-empty, else `~/.codex`.
pub fn codex_home() -> Option<PathBuf> {
    env_dir("CODEX_HOME").or_else(|| home().map(|h| h.join(".codex")))
}

/// `$PI_CODING_AGENT_DIR` when set non-empty, else `~/.pi/agent`.
pub fn pi_home() -> Option<PathBuf> {
    env_dir("PI_CODING_AGENT_DIR").or_else(|| home().map(|h| h.join(".pi").join("agent")))
}

/// `$OMP_CODING_AGENT_DIR` / `$PI_CODING_AGENT_DIR` (OMP shares the
/// pi convention), else `~/.omp/agent`.
pub fn omp_home() -> Option<PathBuf> {
    env_dir("OMP_CODING_AGENT_DIR")
        .or_else(|| env_dir("PI_CODING_AGENT_DIR"))
        .or_else(|| home().map(|h| h.join(".omp").join("agent")))
}

/// `$XDG_CONFIG_HOME/opencode` when set, else `~/.config/opencode`.
pub fn opencode_home() -> Option<PathBuf> {
    env_dir("XDG_CONFIG_HOME")
        .map(|x| x.join("opencode"))
        .or_else(|| home().map(|h| h.join(".config").join("opencode")))
}

/// `~/.agents` — the cross-agent convention dir. Not an engine;
/// always present as a hidden sync target when it exists.
pub fn agents_home() -> Option<PathBuf> {
    home().map(|h| h.join(".agents"))
}

/// The target table, in shadow-priority order (matters for the
/// picker roots; the hub itself treats it as a set). A `static` so
/// every consumer shares one address — `target()` lookups and
/// iterations must agree on identity.
pub static TARGETS: &[Target] = &[
    Target {
        id: "claude",
        label: "Claude Code",
        visible: true,
        home: claude_home,
    },
    Target {
        id: "codex",
        label: "Codex CLI",
        visible: true,
        home: codex_home,
    },
    Target {
        id: "pi",
        label: "Pi",
        visible: true,
        home: pi_home,
    },
    Target {
        id: "omp",
        label: "Oh My Pi",
        visible: true,
        home: omp_home,
    },
    Target {
        id: "opencode",
        label: "OpenCode",
        visible: true,
        home: opencode_home,
    },
    Target {
        id: "agents",
        label: "~/.agents (cross-agent)",
        visible: false,
        home: agents_home,
    },
];

pub fn target(id: &str) -> Option<&'static Target> {
    TARGETS.iter().find(|t| t.id == id)
}

/// The target's skills directory (`<home>/skills` for every engine
/// in the table).
pub fn target_skill_dir(t: &Target) -> Option<PathBuf> {
    (t.home)().map(|h| h.join("skills"))
}

/// A target is offered when its skills dir exists or its engine home
/// does — syncing must never *create* an engine home for a CLI the
/// user has not installed (a stray `~/.claude` would confuse the
/// CLI's own first-run setup). Existing copies are still classified
/// and removable when unavailable.
pub fn target_available(t: &Target) -> bool {
    let Some(home) = (t.home)() else { return false };
    home.join("skills").is_dir() || home.is_dir()
}

/// Skills roots whose engine home already exists, deduped — the
/// "never create engine homes" list.
pub fn installed_engine_skill_roots() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    for t in TARGETS {
        let Some(dir) = target_skill_dir(t) else {
            continue;
        };
        if (t.home)().is_some_and(|h| h.is_dir()) && !out.contains(&dir) {
            out.push(dir);
        }
    }
    out
}

/// One read-only source the hub lists but refuses to import from or
/// delete: Codex ships system skills one level inside its home, and
/// plugin caches drop `skills/` directories as they install.
pub struct ReadonlyRoot {
    pub kind: &'static str, // "system" | "plugin"
    pub label: String,
    pub dir: PathBuf,
}

/// Enumerate the read-only skill sources that exist right now.
pub fn readonly_sources() -> Vec<ReadonlyRoot> {
    let mut out = Vec::new();
    if let Some(h) = codex_home() {
        let sys = h.join("skills").join(".system");
        if sys.is_dir() {
            out.push(ReadonlyRoot {
                kind: "system",
                label: "codex:.system".into(),
                dir: sys,
            });
        }
        for dir in codex_plugin_skills_dirs(&h) {
            let rel = dir
                .strip_prefix(&h)
                .map(|r| r.to_string_lossy().into_owned())
                .unwrap_or_else(|_| dir.display().to_string());
            out.push(ReadonlyRoot {
                kind: "plugin",
                label: format!("codex:{rel}"),
                dir,
            });
        }
    }
    out
}

/// Codex plugin cache `skills/` directories (`plugins/cache/**`,
/// bounded depth). Handles both `cache/<plugin>/<ver>/skills` and
/// `cache/<market>/<plugin>/<ver>/skills` shapes.
fn codex_plugin_skills_dirs(codex_home: &Path) -> Vec<PathBuf> {
    const MAX_DEPTH: usize = 8;
    let mut out = Vec::new();
    fn walk(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
        if depth > MAX_DEPTH {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let Ok(ft) = e.file_type() else { continue };
            let path = e.path();
            if !ft.is_dir() || ft.is_symlink() {
                continue;
            }
            if e.file_name().eq_ignore_ascii_case("skills") {
                out.push(path.clone());
            }
            walk(&path, depth + 1, out);
        }
    }
    walk(&codex_home.join("plugins").join("cache"), 1, &mut out);
    out
}

/// Global skill roots for the composer's `/` picker, in shadow
/// order. Codex's `.system` subtree is listed ahead of its plain
/// skills dir so system names win ties, matching the CLI's own
/// precedence. Consumed by `slash_catalog`.
pub fn picker_skill_roots() -> Vec<(PathBuf, &'static str)> {
    let mut roots = Vec::new();
    for t in TARGETS {
        let Some(dir) = target_skill_dir(t) else {
            continue;
        };
        if t.id == "codex" {
            roots.push((dir.join(".system"), "global"));
        }
        roots.push((dir, "global"));
    }
    roots
}

pub(crate) fn home() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// An env-var path override: non-empty, with a leading `~` expanded
/// against the home dir.
pub(crate) fn env_dir(var: &str) -> Option<PathBuf> {
    let raw = std::env::var(var).ok()?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return home().map(|h| h.join(rest));
    }
    if raw == "~" {
        return home();
    }
    Some(PathBuf::from(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_table_ids_are_unique_and_match_lookup() {
        let mut seen = std::collections::HashSet::new();
        for t in TARGETS {
            assert!(seen.insert(t.id), "duplicate id {}", t.id);
            assert!(target(t.id).is_some_and(|found| found.id == t.id));
        }
        assert!(target("nope").is_none());
    }

    #[test]
    fn picker_roots_put_codex_system_ahead_of_plain_skills() {
        let roots = picker_skill_roots();
        let paths: Vec<&str> = roots.iter().map(|(p, _)| p.to_str().unwrap()).collect();
        let sys = paths
            .iter()
            .position(|p| p.ends_with(".system"))
            .expect(".system root present");
        let plain = paths
            .iter()
            .position(|p| p.contains("codex") && p.ends_with("skills"))
            .expect("plain codex skills root present");
        assert!(sys < plain, ".system must shadow the plain skills dir");
        // Every root carries the global source tag.
        assert!(roots.iter().all(|(_, s)| *s == "global"));
    }
}
