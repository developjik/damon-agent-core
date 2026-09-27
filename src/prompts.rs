//! Prompt library: reusable markdown prompts with light frontmatter,
//! stored where the user edits them — `<workspace>/.damon/prompts/`
//! (scope "workspace", gated through the same `allowed_dirs` jail as
//! every other client-supplied cwd) and `<data_dir>/prompts/` (scope
//! "global"). Not to be confused with `prompt.add`/`prompt.recent`,
//! which record the history of prompts already sent; this module is
//! the library of prompts yet to send, surfaced in the composer's
//! `!` picker.
//!
//! The format deliberately matches what the agent CLIs accept for
//! command files: `---`-delimited frontmatter with `description` and
//! `argument-hint` (aliases accepted on read, canonical spellings on
//! write), body below. An unterminated frontmatter block means the
//! whole file is body — the same rule `slash_catalog` applies.

use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::skills_hub::{HubCode, HubError};

/// Largest single prompt body accepted (create/update).
pub const MAX_CONTENT_BYTES: usize = 64 * 1024;

/// The two storage roots. `workspace` is `None` only when no cwd
/// could be resolved — global-only operations still work.
#[derive(Debug, Clone)]
pub struct PromptRoots {
    pub workspace: Option<PathBuf>,
    pub global: PathBuf,
}

/// Resolve the roots. The global root honors `$DAMON_PROMPTS_HOME`
/// (tests, sandboxes) before falling back to `<data_dir>/prompts`.
pub fn roots(workspace_cwd: Option<&Path>, data_dir: Option<&Path>) -> PromptRoots {
    let global = crate::skills_hub::targets::env_dir("DAMON_PROMPTS_HOME").unwrap_or_else(|| {
        let base = data_dir
            .map(Path::to_path_buf)
            .unwrap_or_else(crate::config::default_data_dir);
        base.join("prompts")
    });
    PromptRoots {
        workspace: workspace_cwd.map(|c| c.join(".damon").join("prompts")),
        global,
    }
}

/// One pickable prompt row.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptEntry {
    pub name: String,
    pub description: Option<String>,
    pub argument_hint: Option<String>,
    /// "workspace" | "global".
    pub scope: &'static str,
    /// Absolute path — the mutation handle for update/delete/move.
    pub path: String,
}

/// Read-side field lookup with the alias spellings.
fn field<'a>(
    map: &'a std::collections::BTreeMap<String, String>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter().find_map(|k| map.get(*k).map(String::as_str))
}

/// Parse one prompt file into (description, argument-hint, body).
/// Missing/unreadable → the file still lists with no metadata; an
/// unterminated frontmatter block treats the whole file as body.
fn parse_prompt(path: &Path) -> (Option<String>, Option<String>, String) {
    let Ok(bytes) = std::fs::read(path) else {
        return (None, None, String::new());
    };
    let text = String::from_utf8_lossy(&bytes).into_owned();
    // Prompt bodies are capped at write time, so parsing the whole
    // text (no head slice) is bounded by construction.
    let Some(map) = crate::skills_hub::scan::parse_frontmatter(&text) else {
        return (None, None, text);
    };
    let description = field(&map, &["description"]).map(String::from);
    let hint = field(&map, &["argument-hint", "argument_hint", "argumenthint"]).map(String::from);
    (description, hint, body_after_fence(&text))
}

/// The text after a closed frontmatter block; the whole text when
/// there is no leading fence (byte-precise via split_inclusive, so
/// CRLF and unicode survive).
fn body_after_fence(text: &str) -> String {
    let mut consumed = 0usize;
    let mut first = true;
    for line in text.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if first {
            first = false;
            if trimmed != "---" {
                return text.to_string();
            }
        } else if trimmed == "---" {
            return text[consumed + line.len()..].to_string();
        }
        consumed += line.len();
    }
    text.to_string()
}

/// Serialize a prompt back to disk form: frontmatter only when at
/// least one field is set, values double-quoted with escapes.
fn build_contents(description: &Option<String>, hint: &Option<String>, body: &str) -> String {
    let quote = |v: &str| format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\""));
    let mut out = String::new();
    if description.is_some() || hint.is_some() {
        out.push_str("---\n");
        if let Some(d) = description {
            out.push_str(&format!("description: {}\n", quote(d)));
        }
        if let Some(h) = hint {
            out.push_str(&format!("argument-hint: {}\n", quote(h)));
        }
        out.push_str("---\n");
    }
    out.push_str(body);
    out
}

/// A prompt name: trimmed, non-empty, no whitespace (it becomes a
/// filename and a picker key), no separators.
pub fn validate_name(name: &str) -> Result<String, HubError> {
    let name = name.trim();
    if name.is_empty() || name.chars().any(char::is_whitespace) {
        return Err(HubError::new(
            HubCode::InvalidInput,
            "prompt name must be non-empty and contain no whitespace",
        ));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(HubError::new(
            HubCode::InvalidInput,
            "prompt name must not contain path separators",
        ));
    }
    Ok(name.to_string())
}

fn scope_root(roots: &PromptRoots, scope: &str) -> Result<PathBuf, HubError> {
    match scope {
        "workspace" => roots.workspace.clone().ok_or_else(|| {
            HubError::new(
                HubCode::InvalidInput,
                "no workspace root for this connection",
            )
        }),
        "global" => Ok(roots.global.clone()),
        other => Err(HubError::new(
            HubCode::InvalidInput,
            format!("unknown scope {other:?} (workspace|global)"),
        )),
    }
}

/// Lexical-absolute fallback for roots that do not exist yet (a
/// fresh workspace has no `.damon/prompts`).
fn abs(p: &Path) -> PathBuf {
    std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf())
}

fn canonical_root(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| abs(p))
}

/// Canonicalize both sides and require containment: `..` segments
/// and symlinked-parent escapes fail closed before any write.
fn ensure_within_roots(roots: &PromptRoots, path: &str) -> Result<PathBuf, HubError> {
    let p = Path::new(path);
    if !p.is_absolute() {
        return Err(HubError::new(
            HubCode::InvalidInput,
            "promptPath must be absolute",
        ));
    }
    let contained = |c: &Path| {
        roots
            .workspace
            .iter()
            .chain(std::iter::once(&roots.global))
            .any(|root| c.starts_with(canonical_root(root)))
    };
    // On-disk paths verify against resolved symlinks. A path that no
    // longer exists (already deleted — removal is idempotent)
    // resolves its deepest existing ancestor and re-appends the
    // tail, so a `/var` → `/private/var` style symlinked root still
    // matches. `..` components never satisfy the component-wise
    // prefix match, so escapes still fail closed.
    if let Ok(canon) = p.canonicalize() {
        if contained(&canon) {
            return Ok(canon);
        }
        return Err(HubError::new(
            HubCode::InvalidInput,
            "promptPath escapes the prompt roots",
        ));
    }
    let lexical = abs(p);
    let candidate = match (lexical.parent(), lexical.file_name()) {
        (Some(parent), Some(name)) => parent
            .canonicalize()
            .map(|pc| pc.join(name))
            .unwrap_or_else(|_| lexical.clone()),
        _ => lexical.clone(),
    };
    if contained(&candidate) {
        return Ok(candidate);
    }
    Err(HubError::new(
        HubCode::InvalidInput,
        "promptPath escapes the prompt roots",
    ))
}

fn scope_of(roots: &PromptRoots, canon: &Path) -> Option<&'static str> {
    if roots
        .workspace
        .as_deref()
        .is_some_and(|w| canon.starts_with(canonical_root(w)))
    {
        return Some("workspace");
    }
    (canon.starts_with(canonical_root(&roots.global))).then_some("global")
}

/// List prompts: workspace first (it shadows), each scope name-sorted.
/// Only regular `*.md` files (case-insensitive extension).
pub fn list(roots: &PromptRoots) -> Vec<PromptEntry> {
    let mut out = Vec::new();
    for (root, scope) in roots
        .workspace
        .iter()
        .map(|w| (w, "workspace"))
        .chain(std::iter::once((&roots.global, "global")))
    {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        let mut names: Vec<PathBuf> = entries
            .flatten()
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file() && !t.is_symlink()))
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|e| e.eq_ignore_ascii_case("md"))
            })
            .collect();
        names.sort();
        for path in names {
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if stem.is_empty() || stem.eq_ignore_ascii_case("readme") {
                continue;
            }
            let (description, hint, _) = parse_prompt(&path);
            out.push(PromptEntry {
                name: stem.to_string(),
                description,
                argument_hint: hint,
                scope,
                path: path.to_string_lossy().into_owned(),
            });
        }
    }
    out
}

/// One prompt in full — metadata plus the body, for the `!` picker's
/// insert-into-composer action.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptDetail {
    pub name: String,
    pub description: Option<String>,
    pub argument_hint: Option<String>,
    pub scope: &'static str,
    pub content: String,
}

/// Read one prompt by its mutation handle.
pub fn get(roots: &PromptRoots, prompt_path: &str) -> Result<PromptDetail, HubError> {
    let canon = ensure_within_roots(roots, prompt_path)?;
    if !canon.is_file() {
        return Err(HubError::new(HubCode::NotFound, "prompt not found"));
    }
    let name = canon
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string();
    let scope = scope_of(roots, &canon).unwrap_or("global");
    let (description, argument_hint, content) = parse_prompt(&canon);
    Ok(PromptDetail {
        name,
        description,
        argument_hint,
        scope,
        content,
    })
}

/// Create a prompt. An existing same-named file is a conflict, not
/// an overwrite — the library is user-authored content.
pub fn create(
    roots: &PromptRoots,
    scope: &str,
    name: &str,
    description: Option<String>,
    argument_hint: Option<String>,
    content: &str,
) -> Result<PathBuf, HubError> {
    let name = validate_name(name)?;
    if content.len() > MAX_CONTENT_BYTES {
        return Err(HubError::new(
            HubCode::InvalidInput,
            format!("content exceeds the {} KiB cap", MAX_CONTENT_BYTES / 1024),
        ));
    }
    let root = scope_root(roots, scope)?;
    std::fs::create_dir_all(&root).map_err(HubError::from)?;
    let dest = root.join(format!("{name}.md"));
    if dest.symlink_metadata().is_ok() {
        return Err(HubError::new(HubCode::Conflict, "Prompt already exists."));
    }
    let description = description.filter(|d| !d.trim().is_empty());
    let argument_hint = argument_hint.filter(|h| !h.trim().is_empty());
    std::fs::write(&dest, build_contents(&description, &argument_hint, content))
        .map_err(HubError::from)?;
    Ok(dest)
}

/// Update fields. `None` keeps the stored value; `Some("")` clears a
/// meta field; a `name` change renames the file (write-new then
/// remove-old, so a crash cannot lose the original).
pub fn update(
    roots: &PromptRoots,
    prompt_path: &str,
    name: Option<String>,
    description: Option<String>,
    argument_hint: Option<String>,
    content: Option<String>,
) -> Result<PathBuf, HubError> {
    let canon = ensure_within_roots(roots, prompt_path)?;
    if !canon.is_file() {
        return Err(HubError::new(HubCode::NotFound, "prompt not found"));
    }
    let (mut cur_desc, mut cur_hint, mut body) = parse_prompt(&canon);
    if let Some(d) = description {
        cur_desc = (!d.trim().is_empty()).then(|| d.trim().to_string());
    }
    if let Some(h) = argument_hint {
        cur_hint = (!h.trim().is_empty()).then(|| h.trim().to_string());
    }
    if let Some(c) = content {
        if c.len() > MAX_CONTENT_BYTES {
            return Err(HubError::new(
                HubCode::InvalidInput,
                format!("content exceeds the {} KiB cap", MAX_CONTENT_BYTES / 1024),
            ));
        }
        body = c;
    }
    let mut final_path = canon.clone();
    if let Some(name) = name {
        let name = validate_name(&name)?;
        let stem = canon
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if name != stem {
            let Some(parent) = canon.parent() else {
                return Err(HubError::new(HubCode::Internal, "prompt has no parent dir"));
            };
            let dest = parent.join(format!("{name}.md"));
            if dest.symlink_metadata().is_ok() {
                return Err(HubError::new(
                    HubCode::Conflict,
                    "a prompt with that name exists",
                ));
            }
            std::fs::write(&dest, build_contents(&cur_desc, &cur_hint, &body))
                .map_err(HubError::from)?;
            std::fs::remove_file(&canon).map_err(HubError::from)?;
            final_path = dest;
            return Ok(final_path);
        }
    }
    std::fs::write(&final_path, build_contents(&cur_desc, &cur_hint, &body))
        .map_err(HubError::from)?;
    Ok(final_path)
}

/// Delete. A missing file is `Ok(false)` — idempotent removal.
pub fn delete(roots: &PromptRoots, prompt_path: &str) -> Result<bool, HubError> {
    let canon = ensure_within_roots(roots, prompt_path)?;
    if !canon.exists() {
        return Ok(false);
    }
    std::fs::remove_file(&canon).map_err(HubError::from)?;
    Ok(true)
}

/// Move between scopes. Same-scope is an error (nothing to do);
/// destination collisions are errors; cross-device renames fall back
/// to copy+delete.
pub fn move_prompt(
    roots: &PromptRoots,
    prompt_path: &str,
    to_scope: &str,
) -> Result<PathBuf, HubError> {
    let canon = ensure_within_roots(roots, prompt_path)?;
    let Some(from_scope) = scope_of(roots, &canon) else {
        return Err(HubError::new(
            HubCode::InvalidInput,
            "promptPath escapes the prompt roots",
        ));
    };
    if from_scope == to_scope {
        return Err(HubError::new(
            HubCode::InvalidInput,
            "prompt is already in that scope",
        ));
    }
    let dest_root = scope_root(roots, to_scope)?;
    std::fs::create_dir_all(&dest_root).map_err(HubError::from)?;
    let file_name = canon
        .file_name()
        .ok_or_else(|| HubError::new(HubCode::Internal, "prompt has no file name"))?
        .to_owned();
    let dest = dest_root.join(&file_name);
    if dest.symlink_metadata().is_ok() {
        return Err(HubError::new(
            HubCode::Conflict,
            "a prompt with that name exists",
        ));
    }
    if std::fs::rename(&canon, &dest).is_err() {
        copy_and_delete(&canon, &dest)?;
    }
    Ok(dest)
}

fn copy_and_delete(src: &Path, dst: &Path) -> Result<(), HubError> {
    std::fs::copy(src, dst).map_err(HubError::from)?;
    std::fs::remove_file(src).map_err(|e| {
        // The copy exists; report the stale original without hiding it.
        HubError::from(io::Error::other(format!(
            "moved the copy but could not remove the original: {e}"
        )))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_roots(name: &str) -> (PathBuf, PromptRoots) {
        let dir = std::env::temp_dir().join(format!(
            "damon-prompts-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let ws = dir.join("ws");
        let global = dir.join("global");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::create_dir_all(&global).unwrap();
        (
            dir.clone(),
            PromptRoots {
                workspace: Some(ws),
                global,
            },
        )
    }

    #[test]
    fn create_list_update_move_delete_round_trip() {
        let (_dir, roots) = tmp_roots("rt");
        let p = create(
            &roots,
            "workspace",
            "review",
            Some("Review the diff".into()),
            Some("[file]".into()),
            "Please review:\n- correctness\n",
        )
        .unwrap();
        assert!(p.to_string_lossy().ends_with("review.md"));

        let listed = list(&roots);
        let row = listed.iter().find(|e| e.name == "review").unwrap();
        assert_eq!(row.scope, "workspace");
        assert_eq!(row.description.as_deref(), Some("Review the diff"));
        assert_eq!(row.argument_hint.as_deref(), Some("[file]"));

        // Some("") clears meta; content replaces body.
        let path = update(
            &roots,
            &row.path,
            None,
            Some(String::new()),
            None,
            Some("New body".into()),
        )
        .unwrap();
        let (desc, hint, body) = parse_prompt(&path);
        assert_eq!(desc, None);
        assert_eq!(hint.as_deref(), Some("[file]"), "untouched fields survive");
        assert_eq!(body, "New body");

        // Rename writes-new-then-removes-old.
        let renamed = update(
            &roots,
            &path.to_string_lossy(),
            Some("deep-review".into()),
            None,
            None,
            None,
        )
        .unwrap();
        assert!(!path.exists());
        assert!(renamed.exists());

        // Move to global, then delete.
        let moved = move_prompt(&roots, &renamed.to_string_lossy(), "global").unwrap();
        assert!(
            moved
                .canonicalize()
                .unwrap()
                .starts_with(canonical_root(&roots.global)),
            "moved into the global root: {}",
            moved.display()
        );
        assert!(!renamed.exists());
        assert!(delete(&roots, &moved.to_string_lossy()).unwrap());
        assert!(!moved.exists());
        assert!(
            !delete(&roots, &moved.to_string_lossy()).unwrap(),
            "idempotent"
        );
    }

    #[test]
    fn names_are_validated_and_collisions_conflict() {
        let (_dir, roots) = tmp_roots("names");
        create(&roots, "global", "one", None, None, "x").unwrap();
        for bad in ["", "  ", "two words", "a/b", "a\\b"] {
            assert!(
                create(&roots, "global", bad, None, None, "x").is_err(),
                "{bad:?}"
            );
        }
        let err = create(&roots, "global", "one", None, None, "x").unwrap_err();
        assert_eq!(err.code, HubCode::Conflict);
        assert!(create(&roots, "nowhere-scope", "x", None, None, "y").is_err());
    }

    #[test]
    fn prompt_path_escapes_fail_closed() {
        let (dir, roots) = tmp_roots("jail");
        let outside = dir.join("outside.md");
        std::fs::write(&outside, "x").unwrap();
        let err = delete(&roots, &outside.to_string_lossy()).unwrap_err();
        assert_eq!(err.code, HubCode::InvalidInput);
        // Nonexistent and outside the roots: the jail answers first.
        let err = update(
            &roots,
            &dir.join("nope.md").to_string_lossy(),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(err.code, HubCode::InvalidInput);
        // Nonexistent but inside a root: a plain not-found.
        let inside = roots.global.join("nope.md");
        let err = update(&roots, &inside.to_string_lossy(), None, None, None, None).unwrap_err();
        assert_eq!(err.code, HubCode::NotFound);
        // Relative paths never reach the filesystem.
        let err = delete(&roots, "relative.md").unwrap_err();
        assert_eq!(err.code, HubCode::InvalidInput);
    }

    #[test]
    fn unterminated_frontmatter_is_all_body() {
        let (_dir, roots) = tmp_roots("fm");
        let p = create(
            &roots,
            "global",
            "broken",
            None,
            None,
            "---\ndescription: never closed",
        )
        .unwrap();
        let (desc, hint, body) = parse_prompt(&p);
        assert_eq!(desc, None);
        assert_eq!(hint, None);
        assert!(body.starts_with("---"), "whole file is body");
    }
}
