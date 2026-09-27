//! Gitignore-aware file index of one workspace root, backing the
//! composer's `@path` mention picker.
//!
//! One walk of the picker root (the session cwd) with ripgrep's
//! `ignore` walker: `.gitignore`/`.ignore`/global excludes honored
//! even outside a git repo, hidden entries skipped, `node_modules`
//! and `target` pruned unconditionally. Symlinks are never followed,
//! so the index cannot wander outside the root. The response is
//! bounded by construction — entry count and a serialized-size
//! budget both land well under the 1 MiB frame cap, with `truncated`
//! telling the client the walk stopped early.

use std::path::Path;

/// Hard entry cap.
pub const MAX_ENTRIES: usize = 10_000;
/// Serialized-size budget — the sum of `rel.len() + 32` (the JSON
/// overhead envelope per entry) stays under half the frame budget.
pub const MAX_BYTES: usize = 512 * 1024;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileIndexEntry {
    /// Root-relative, `/`-separated.
    pub rel: String,
    pub is_dir: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileIndex {
    pub entries: Vec<FileIndexEntry>,
    pub truncated: bool,
}

/// Build the index. `root` must already be validated by the caller
/// (resolve_picker_root + canonicalize) — this function only walks.
pub fn build(root: &Path) -> std::io::Result<FileIndex> {
    build_capped(root, MAX_ENTRIES, MAX_BYTES)
}

fn build_capped(root: &Path, max_entries: usize, max_bytes: usize) -> std::io::Result<FileIndex> {
    let walker = ignore::WalkBuilder::new(root)
        .hidden(true)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .ignore(true)
        .require_git(false)
        .follow_links(false)
        .filter_entry(|e| {
            if !e.file_type().is_some_and(|t| t.is_dir()) {
                return true;
            }
            // Pruned even when the ignore filters are fully active —
            // node_modules/target are always noise, never targets.
            !(e.file_name() == "node_modules" || e.file_name() == "target")
        })
        .build();
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    let mut truncated = false;
    for entry in walker.flatten() {
        if entry.depth() == 0 {
            continue; // the root itself
        }
        let Some(rel) = entry
            .path()
            .strip_prefix(root)
            .ok()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
        else {
            continue;
        };
        if rel.is_empty() {
            continue;
        }
        if entries.len() >= max_entries || bytes + rel.len() + 32 > max_bytes {
            truncated = true;
            break;
        }
        bytes += rel.len() + 32;
        entries.push(FileIndexEntry {
            rel,
            is_dir: entry.file_type().is_some_and(|t| t.is_dir()),
        });
    }
    Ok(FileIndex { entries, truncated })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("damon-fidx-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn honors_gitignore_and_prunes_unconditionally() {
        let dir = scratch("ignore");
        std::fs::write(dir.join(".gitignore"), "*.log\n").unwrap();
        std::fs::write(dir.join("keep.rs"), "fn main() {}").unwrap();
        std::fs::write(dir.join("drop.log"), "x").unwrap();
        std::fs::create_dir_all(dir.join("node_modules").join("pkg")).unwrap();
        std::fs::write(dir.join("node_modules").join("pkg").join("x.js"), "x").unwrap();
        std::fs::create_dir_all(dir.join("sub")).unwrap();

        let idx = build(&dir).unwrap();
        let rels: Vec<&str> = idx.entries.iter().map(|e| e.rel.as_str()).collect();
        assert!(rels.contains(&"keep.rs"), "{rels:?}");
        assert!(rels.contains(&"sub"), "{rels:?}");
        assert!(!rels.iter().any(|r| r.ends_with(".log")), "{rels:?}");
        assert!(!rels.iter().any(|r| r.contains("node_modules")), "{rels:?}");
        assert!(idx.entries.iter().find(|e| e.rel == "sub").unwrap().is_dir);
        assert!(!idx.truncated);
    }

    #[test]
    fn hidden_entries_are_skipped() {
        let dir = scratch("hidden");
        std::fs::write(dir.join("vis.txt"), "x").unwrap();
        std::fs::write(dir.join(".env"), "x").unwrap();

        let idx = build(&dir).unwrap();
        let rels: Vec<&str> = idx.entries.iter().map(|e| e.rel.as_str()).collect();
        assert_eq!(rels, vec!["vis.txt"]);
    }

    #[test]
    fn caps_report_truncated() {
        let dir = scratch("cap");
        for i in 0..5 {
            std::fs::write(dir.join(format!("f{i}.txt")), "x").unwrap();
        }
        let idx = build_capped(&dir, 2, usize::MAX).unwrap();
        assert_eq!(idx.entries.len(), 2);
        assert!(idx.truncated);

        let idx = build_capped(&dir, usize::MAX, 100).unwrap();
        // "fN.txt" rels cost len+32 = 38 each; the third entry would
        // push past 100, so the budget bites at two.
        assert_eq!(idx.entries.len(), 2);
        assert!(idx.truncated, "byte budget must bite");
    }
}
