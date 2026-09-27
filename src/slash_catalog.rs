//! Slash-command catalog for the composer's `/` picker.
//!
//! Discovers the command/skill files the installed agent CLIs already
//! understand — the agents themselves expand `/name` at turn time; this
//! module only makes the catalog visible to clients. Workspace roots
//! (relative to the picker root) shadow same-named global entries, and
//! global roots are read from the daemon's own environment/home (never
//! from client input), the same trust boundary as claude's
//! `~/.claude/projects` transcript discovery.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// One pickable entry. `name` is slash-less; directory segments join
/// with `:` (`.claude/commands/aimax/plan.md` → `aimax:plan`), matching
/// what the CLIs themselves accept.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlashEntry {
    pub name: String,
    pub description: Option<String>,
    pub argument_hint: Option<String>,
    /// "workspace" (picker-root-relative) or "global" (CLI homes).
    pub source: &'static str,
    /// "command" or "skill".
    pub kind: &'static str,
}

/// Upper bound on returned entries — a pathological commands dir must
/// not produce an unbounded response.
const MAX_ENTRIES: usize = 500;

/// `$CLAUDE_CONFIG_DIR` when set non-empty (`~` expanded), else
/// `~/.claude` — the env override Claude Code itself honors.
fn claude_home() -> Option<PathBuf> {
    env_dir("CLAUDE_CONFIG_DIR").or_else(|| home().map(|h| h.join(".claude")))
}

/// `$CODEX_HOME` when set non-empty, else `~/.codex`.
fn codex_home() -> Option<PathBuf> {
    env_dir("CODEX_HOME").or_else(|| home().map(|h| h.join(".codex")))
}

fn home() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// An env-var path override: non-empty, with a leading `~` expanded
/// against the home dir.
fn env_dir(var: &str) -> Option<PathBuf> {
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

/// Build the catalog for one picker root (the session cwd).
pub fn catalog(cwd: &Path) -> Vec<SlashEntry> {
    let mut commands = Vec::new();
    let mut skills = Vec::new();

    // Commands: recursive `.md` files, workspace first so its names win.
    if let Some(root) = dir_exists(&cwd.join(".claude").join("commands")) {
        scan_commands(&root, "workspace", &mut commands);
    }
    if let Some(root) = claude_home()
        .map(|h| h.join("commands"))
        .and_then(|p| dir_exists(&p))
    {
        scan_commands(&root, "global", &mut commands);
    }

    // Skills: one directory per child that holds a SKILL.md — not
    // recursive.
    for (root, source) in skill_roots(cwd) {
        if let Some(root) = dir_exists(&root) {
            scan_skills(&root, source, &mut skills);
        }
    }

    let mut commands = dedup_and_sort(commands);
    let mut skills = dedup_and_sort(skills);
    commands.truncate(MAX_ENTRIES / 2);
    skills.truncate(MAX_ENTRIES / 2);
    commands.extend(skills);
    commands.truncate(MAX_ENTRIES);
    commands
}

/// Global + workspace skill roots in shadow-priority order. Codex's
/// `.system` skills sit one level deeper inside the same home.
fn skill_roots(cwd: &Path) -> Vec<(PathBuf, &'static str)> {
    let mut roots = vec![(
        cwd.join(".claude").join("skills"),
        "workspace",
    )];
    if let Some(h) = claude_home() {
        roots.push((h.join("skills"), "global"));
    }
    if let Some(h) = codex_home() {
        roots.push((h.join("skills").join(".system"), "global"));
        roots.push((h.join("skills"), "global"));
    }
    if let Some(h) = home() {
        roots.push((h.join(".agents").join("skills"), "global"));
    }
    roots
}

fn dir_exists(p: &Path) -> Option<PathBuf> {
    p.is_dir().then(|| p.to_path_buf())
}

/// Recursively collect `.md` command files. Directory segments become
/// `:`-joined name prefixes; frontmatter may override the name and
/// contributes the description/argument hint.
fn scan_commands(root: &Path, source: &'static str, out: &mut Vec<SlashEntry>) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(path);
                continue;
            }
            if path
                .extension()
                .and_then(|e| e.to_str())
                .is_none_or(|e| !e.eq_ignore_ascii_case("md"))
            {
                continue;
            }
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            if stem.is_empty() || stem.eq_ignore_ascii_case("readme") {
                continue;
            }
            let mut parts: Vec<String> = path
                .strip_prefix(root)
                .ok()
                .and_then(|rel| rel.parent())
                .map(|parent| {
                    parent
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            parts.push(stem.to_string());
            let derived = parts.join(":");
            let (name, description, argument_hint) =
                match read_frontmatter(&path, Some(&derived)) {
                    Some(f) => (f.0, f.1, f.2),
                    None => (derived, None, None),
                };
            let name = name.trim().trim_start_matches('/').to_string();
            if name.is_empty() {
                continue;
            }
            out.push(SlashEntry {
                name,
                description,
                argument_hint,
                source,
                kind: "command",
            });
        }
    }
}

/// Collect child directories containing `SKILL.md`.
fn scan_skills(root: &Path, source: &'static str, out: &mut Vec<SlashEntry>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !path.join("SKILL.md").is_file() {
            continue;
        }
        let (_, description, _) = read_frontmatter(&path.join("SKILL.md"), Some(name))
            .unwrap_or((name.to_string(), None, None));
        out.push(SlashEntry {
            name: name.to_string(),
            description,
            argument_hint: None,
            source,
            kind: "skill",
        });
    }
}

/// Parse a command file's frontmatter. Returns `(name, description,
/// argument_hint)` where `name` falls back to `fallback` when absent.
/// An unterminated block yields the fallback with no metadata — the
/// file stays discoverable by its path-derived name.
fn read_frontmatter(
    path: &Path,
    fallback: Option<&str>,
) -> Option<(String, Option<String>, Option<String>)> {
    let bytes = std::fs::read(path).ok()?;
    let head: &[u8] = &bytes[..bytes.len().min(8 * 1024)];
    let text = String::from_utf8_lossy(head);
    let mut lines = text.lines();
    if lines.next()?.trim_end_matches(['\r', '\n']) != "---" {
        return fallback.map(|f| (f.to_string(), None, None));
    }
    let mut name = None;
    let mut description = None;
    let mut argument_hint = None;
    for line in lines {
        let line = line.trim_end_matches(['\r', '\n']);
        if line == "---" {
            return Some((
                name.or_else(|| fallback.map(String::from)).unwrap_or_default(),
                description,
                argument_hint,
            ));
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = match line.split_once(':') {
            Some((k, v)) => (k.trim().to_ascii_lowercase(), strip_quotes(v.trim())),
            None => continue,
        };
        match key.as_str() {
            "name" => name = value,
            "description" => description = value,
            "argument-hint" | "argument_hint" | "argumenthint" => argument_hint = value,
            _ => {}
        }
    }
    // Unterminated frontmatter — path-derived identity only.
    fallback.map(|f| (f.to_string(), None, None))
}

/// Strip one layer of matching quotes; empty becomes `None`.
fn strip_quotes(v: &str) -> Option<String> {
    let v = v
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
        .unwrap_or(v);
    (!v.is_empty()).then(|| v.to_string())
}

/// First-wins dedup on lowercase name (scan order already puts
/// workspace before global), then name sort.
fn dedup_and_sort(mut entries: Vec<SlashEntry>) -> Vec<SlashEntry> {
    let mut seen = HashSet::new();
    entries.retain(|e| seen.insert(e.name.to_lowercase()));
    entries.sort_by_key(|e| e.name.to_lowercase());
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_join_directory_segments_and_read_frontmatter() {
        let root = std::env::temp_dir().join(format!(
            "damon-slash-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let cmds = root.join(".claude").join("commands");
        std::fs::create_dir_all(cmds.join("aimax")).unwrap();
        std::fs::write(
            cmds.join("aimax").join("plan.md"),
            "---\ndescription: \"Plan the work\"\nargument-hint: [goal]\n---\nBody",
        )
        .unwrap();
        std::fs::write(cmds.join("plain.md"), "no frontmatter").unwrap();

        let entries = catalog(&root);
        let plan = entries.iter().find(|e| e.name == "aimax:plan").unwrap();
        assert_eq!(plan.source, "workspace");
        assert_eq!(plan.kind, "command");
        assert_eq!(plan.description.as_deref(), Some("Plan the work"));
        assert_eq!(plan.argument_hint.as_deref(), Some("[goal]"));
        assert!(entries.iter().any(|e| e.name == "plain" && e.description.is_none()));
    }

    #[test]
    fn skills_need_a_skill_md_and_read_their_description() {
        let root = std::env::temp_dir().join(format!(
            "damon-slash-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let skills = root.join(".claude").join("skills");
        std::fs::create_dir_all(skills.join("review")).unwrap();
        std::fs::create_dir_all(skills.join("empty")).unwrap();
        std::fs::write(
            skills.join("review").join("SKILL.md"),
            "---\ndescription: Review diffs\n---\nBody",
        )
        .unwrap();

        let entries = catalog(&root);
        let review = entries.iter().find(|e| e.name == "review").unwrap();
        assert_eq!(review.kind, "skill");
        assert_eq!(review.description.as_deref(), Some("Review diffs"));
        assert!(!entries.iter().any(|e| e.name == "empty"));
    }

    #[test]
    fn unterminated_frontmatter_falls_back_to_the_path_name() {
        let root = std::env::temp_dir().join(format!(
            "damon-slash-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let cmds = root.join(".claude").join("commands");
        std::fs::create_dir_all(&cmds).unwrap();
        std::fs::write(cmds.join("broken.md"), "---\ndescription: never closed").unwrap();

        let entries = catalog(&root);
        let broken = entries.iter().find(|e| e.name == "broken").unwrap();
        assert!(broken.description.is_none());
    }
}
