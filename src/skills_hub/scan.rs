//! Reading skill directories off disk: SKILL.md marker discovery,
//! frontmatter metadata (including block-scalar descriptions — the
//! upstream skill ecosystem ships those), bounded scans, and the two
//! content hashes the hub compares to decide "changed" —
//! `hash_directory` for the local copy, `source_signature` for the
//! repo-side tree.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// File and directory names that never contribute to a content hash
/// — VCS noise and OS droppings change without the skill changing.
const HASH_IGNORE: &[&str] = &[".git", ".DS_Store", "Thumbs.db", ".gitignore"];

/// Depth bound for scanning skill directories: skills nest their
/// payload, but nobody ships a skill four directories of grouping
/// deep, and the bound keeps a pathological tree from walking
/// forever.
pub const MAX_SCAN_DEPTH: usize = 3;

/// The marker filenames that make a directory a skill, in priority
/// order. On disk the casing is exact — `SKILL.md` is the spec'd
/// shape, `skill.md` the tolerated one.
const MARKERS: &[&str] = &["SKILL.md", "skill.md"];

/// Metadata read from a skill's frontmatter.
#[derive(Debug, Clone)]
pub struct SkillMeta {
    pub name: String,
    pub description: Option<String>,
}

/// The marker file inside `dir`, preferring `SKILL.md`.
pub fn find_skill_marker(dir: &Path) -> Option<PathBuf> {
    MARKERS.iter().map(|m| dir.join(m)).find(|p| p.is_file())
}

pub fn has_skill_marker(dir: &Path) -> bool {
    find_skill_marker(dir).is_some()
}

/// Read a skill's name/description. `fallback` is the directory
/// name callers already trust; frontmatter `name` wins when present,
/// and a file with no parseable frontmatter at all keeps the
/// fallback with no metadata — the skill stays listable.
pub fn read_skill_metadata(path: &Path, fallback: &str) -> SkillMeta {
    let bytes = std::fs::read(path).ok();
    let text = bytes
        .as_deref()
        .map(|b| String::from_utf8_lossy(head(b)).into_owned());
    match text.as_deref().and_then(parse_frontmatter) {
        Some(map) => SkillMeta {
            name: map
                .get("name")
                .cloned()
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(|| fallback.to_string()),
            description: map.get("description").cloned(),
        },
        None => SkillMeta {
            name: fallback.to_string(),
            description: None,
        },
    }
}

/// Cap the read at 8 KiB — frontmatter lives at the top, and a
/// multi-megabyte SKILL.md must not be slurped to read two fields.
fn head(b: &[u8]) -> &[u8] {
    &b[..b.len().min(8 * 1024)]
}

/// Parse `---`-delimited frontmatter into the fields the hub reads.
/// Values are inline scalars (one layer of quotes stripped, empty →
/// absent) or block scalars (`|`/`>` families: the indented lines
/// join with spaces — good enough for a description field and
/// exactly what the upstream manager did). A `#` line or a blank
/// line inside the block still belongs to it. Crate-shared: the
/// prompt library reads the same dialect.
pub(crate) fn parse_frontmatter(text: &str) -> Option<BTreeMap<String, String>> {
    let mut lines = text.lines();
    if lines.next()?.trim_end() != "---" {
        return None;
    }
    let mut map = BTreeMap::new();
    let mut pending: Option<(String, Vec<String>)> = None;
    for line in lines {
        let line = line.trim_end();
        if pending.is_some() {
            // Block scalar continuation: any indented line joins;
            // a non-indented one closes the block (and still gets
            // processed as a normal line below via the loop's next
            // iteration trick).
            if line.starts_with(' ') || line.starts_with('\t') || line.is_empty() {
                if let Some((_, acc)) = pending.as_mut()
                    && !line.trim().is_empty()
                {
                    acc.push(line.trim().to_string());
                }
                continue;
            }
            if let Some((key, acc)) = pending.take()
                && !acc.is_empty()
            {
                map.insert(key, acc.join(" "));
            }
        }
        if line == "---" {
            return Some(map);
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        if value.starts_with('|') || value.starts_with('>') {
            pending = Some((key, Vec::new()));
            continue;
        }
        if value.is_empty() {
            continue;
        }
        let stripped = value
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .or_else(|| value.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
            .unwrap_or(value);
        if !stripped.is_empty() {
            map.insert(key, stripped.to_string());
        }
    }
    // Unterminated frontmatter: the whole file is body — no partial
    // metadata, same rule `slash_catalog` applies to command files.
    // A "description:" that never found its closing fence is prose.
    None
}

/// Collect skill directories under `root`, depth-bounded, skipping
/// dot-dirs and never recursing through symlinked directories (a
/// linked grouping folder counts only if it is itself a skill).
pub fn scan_skill_directories(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
        if depth >= MAX_SCAN_DEPTH {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        let mut children: Vec<PathBuf> = entries
            .flatten()
            .filter(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                !name.starts_with('.')
            })
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir() && !t.is_symlink()))
            .map(|e| e.path())
            .collect();
        children.sort();
        for child in children {
            if has_skill_marker(&child) {
                out.push(child.clone());
            }
            walk(&child, depth + 1, out);
        }
    }
    walk(root, 0, &mut out);
    out
}

/// SHA-256 over the directory's sorted regular files. Per file:
/// `rel\0 exec-bit \0 bytes \0` — the exec bit participates because
/// a skill that ships a runnable script changed meaningfully when it
/// gains or loses `+x`. `HASH_IGNORE` names drop out; read failures
/// still contribute their header (a file we cannot read is part of
/// the content identity, not a reason to hash nothing). Returns
/// `None` when the directory cannot be walked at all.
pub fn hash_directory(dir: &Path) -> Option<String> {
    if !dir.is_dir() {
        return None;
    }
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    collect_files(dir, dir, &mut files)?;
    files.sort_by(|a, b| a.0.cmp(&b.0));
    let mut hasher = Sha256::new();
    for (rel, path) in &files {
        hasher.update(rel.as_bytes());
        hasher.update([0]);
        hasher.update(exec_bit(path).to_string().as_bytes());
        hasher.update([0]);
        // An unreadable file still contributes its header — a file we
        // cannot read is part of the content identity.
        if let Ok(bytes) = std::fs::read(path) {
            hasher.update(&bytes);
        }
        hasher.update([0]);
    }
    Some(format!("{:x}", hasher.finalize()))
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Option<()> {
    for entry in std::fs::read_dir(dir).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if HASH_IGNORE.contains(&name.as_str()) {
            continue;
        }
        let path = entry.path();
        let ft = entry.file_type().ok()?;
        if ft.is_dir() && !ft.is_symlink() {
            collect_files(root, &path, out)?;
        } else if ft.is_file() {
            let rel = path.strip_prefix(root).ok()?;
            out.push((rel.to_string_lossy().replace('\\', "/"), path.clone()));
        }
    }
    Some(())
}

#[cfg(unix)]
fn exec_bit(path: &Path) -> u8 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| u8::from(m.permissions().mode() & 0o111 != 0))
        .unwrap_or(0)
}

#[cfg(not(unix))]
fn exec_bit(_path: &Path) -> u8 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-scan-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn frontmatter_inline_and_block_scalars() {
        let t = tmp("fm");
        let f = t.join("SKILL.md");
        std::fs::write(
            &f,
            "---\nname: \"PDF Tools\"\ndescription: |\n  Create, edit and\n  inspect PDFs.\nargument-hint: [file]\n---\nbody",
        )
        .unwrap();
        let meta = read_skill_metadata(&f, "fallback");
        assert_eq!(meta.name, "PDF Tools");
        assert_eq!(
            meta.description.as_deref(),
            Some("Create, edit and inspect PDFs.")
        );
        let _ = std::fs::remove_dir_all(&t);
    }

    #[test]
    fn missing_or_broken_frontmatter_keeps_the_fallback() {
        let t = tmp("fm2");
        let f = t.join("SKILL.md");
        std::fs::write(&f, "no frontmatter here").unwrap();
        assert_eq!(read_skill_metadata(&f, "pdf").name, "pdf");
        // Unterminated fence: the whole file is body, no metadata.
        std::fs::write(&f, "---\nname: 'quoted'\ndescription: never closed").unwrap();
        let meta = read_skill_metadata(&f, "pdf");
        assert_eq!(meta.name, "pdf");
        assert_eq!(meta.description, None);
        let _ = std::fs::remove_dir_all(&t);
    }

    #[test]
    fn hash_is_stable_across_mtime_and_ignores_noise() {
        let t = tmp("hash");
        let s = t.join("skill");
        std::fs::create_dir_all(s.join("sub")).unwrap();
        std::fs::write(s.join("SKILL.md"), "body").unwrap();
        std::fs::write(s.join("sub").join("run.sh"), "#!/bin/sh\n").unwrap();
        std::fs::write(s.join(".DS_Store"), "noise").unwrap();
        let h1 = hash_directory(&s).unwrap();

        // Same content, different timestamps and extra ignored file.
        std::fs::write(s.join("SKILL.md"), "body").unwrap(); // bumps mtime
        std::fs::write(s.join(".gitignore"), "noise2").unwrap();
        assert_eq!(hash_directory(&s).unwrap(), h1);

        std::fs::write(s.join("SKILL.md"), "changed").unwrap();
        assert_ne!(hash_directory(&s).unwrap(), h1);
        let _ = std::fs::remove_dir_all(&t);
    }

    #[test]
    fn scan_respects_depth_and_dots() {
        let t = tmp("scan");
        let deep = t.join("a").join("b").join("c").join("d");
        std::fs::create_dir_all(&deep).unwrap();
        std::fs::create_dir_all(t.join("plain")).unwrap();
        std::fs::write(t.join("plain").join("SKILL.md"), "x").unwrap();
        std::fs::write(deep.join("SKILL.md"), "x").unwrap();
        std::fs::create_dir_all(t.join(".hidden")).unwrap();
        std::fs::write(t.join(".hidden").join("SKILL.md"), "x").unwrap();

        let found = scan_skill_directories(&t);
        assert_eq!(found, vec![t.join("plain")]);
        let _ = std::fs::remove_dir_all(&t);
    }
}
