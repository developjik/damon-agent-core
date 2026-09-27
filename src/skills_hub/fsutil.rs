//! Filesystem primitives for the skills hub. Everything that touches
//! skill directories funnels through here so the trust rules hold in
//! one place: names arriving from the network or a client are
//! sanitized before they ever reach a path join, removal never
//! follows a symlink out of the managed store, and copies refuse to
//! write through a surviving link.

use std::io;
use std::path::{Path, PathBuf};

use base64::Engine as _;

use super::{HubCode, HubError};

/// Milliseconds since the unix epoch — the hub's clock domain
/// (installed_at, trashed_at, cache timestamps).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One path segment of a skill directory name: non-empty, no `.`,
/// `..`, separators, or NUL. A leading `.` is allowed (skills may
/// legitimately be dot-named); nesting is rejected because install
/// names are flat.
pub fn sanitize_path_segment(s: &str) -> Option<String> {
    if s.is_empty()
        || s == "."
        || s == ".."
        || s.contains('/')
        || s.contains('\\')
        || s.contains('\0')
    {
        return None;
    }
    Some(s.to_string())
}

/// A relative path inside a skill source: backslashes fold to
/// forward slashes, and absolute paths (posix or `X:\` windows),
/// NUL, `.`, `..`, and any `:`-bearing segment are rejected. The
/// `:` guard keeps windows drive spelling out of what must be a
/// repo-relative path.
pub fn sanitize_relative_path(s: &str) -> Option<String> {
    let s = s.replace('\\', "/");
    if s.is_empty() || s.contains('\0') {
        return None;
    }
    if s.starts_with('/') {
        return None;
    }
    if s.len() >= 2 && s.as_bytes()[1] == b':' && s.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    let mut out = String::new();
    for seg in s.split('/') {
        if seg.is_empty() {
            continue;
        }
        // The `:` guard keeps windows drive spelling (and stream
        // syntax) out of what must be a repo-relative path.
        if seg.contains(':') {
            return None;
        }
        let seg = sanitize_path_segment(seg)?;
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(&seg);
    }
    Some(out)
}

/// A local skill directory (for `local:` imports): a relative path
/// whose segments additionally reject dot-prefixes — the SSOT never
/// hides installed skills inside dot-directories.
pub fn sanitize_local_skill_path(s: &str) -> Option<String> {
    let s = s.replace('\\', "/");
    if s.is_empty() || s.contains('\0') {
        return None;
    }
    let mut out = String::new();
    for seg in s.split('/') {
        if seg.is_empty() {
            continue;
        }
        if seg.starts_with('.') {
            return None;
        }
        let seg = sanitize_path_segment(seg)?;
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(&seg);
    }
    Some(out)
}

/// The flat install name for a (possibly nested) source directory:
/// the sanitized leaf. `a/b/pdf` installs as `pdf`.
pub fn install_name_from_directory(dir: &str) -> Option<String> {
    let rel = sanitize_relative_path(dir)?;
    let leaf = rel.rsplit('/').next().unwrap_or_default();
    sanitize_path_segment(leaf).filter(|s| !s.is_empty())
}

/// Remove a path whatever it is — file, directory, or symlink. A
/// symlink is removed as a link and its target is never touched:
/// `remove_dir_all` on a symlink would follow it, and what symlinks
/// point at here is the managed store. Windows directory links answer
/// only to the directory call, so links try `remove_dir` first. A
/// fresh tree can briefly lose a delete race to a scanner's open
/// handle (windows defender), so refusals retry a few times before
/// giving up.
pub fn remove_path(p: &Path) {
    let attempt = || -> io::Result<()> {
        if let Ok(m) = p.symlink_metadata() {
            if m.file_type().is_symlink() {
                // Removes the link itself — never what it points at.
                return std::fs::remove_dir(p).or_else(|_| std::fs::remove_file(p));
            }
            if m.is_dir() {
                return std::fs::remove_dir_all(p);
            }
        }
        std::fs::remove_file(p)
    };
    for try_index in 0..5u32 {
        match attempt() {
            Ok(()) => return,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return,
            Err(_) if try_index + 1 < 5 => {
                std::thread::sleep(std::time::Duration::from_millis(
                    10 * u64::from(try_index + 1),
                ));
            }
            Err(_) => return,
        }
    }
}

/// Recursively copy a directory tree. Refuses to start when the
/// destination is still a symlink: the sync flow removes the
/// destination first, and a link that survived that removal means
/// something else owns it — writing "into" it would land in whatever
/// it points at (the SSOT or the user's original source).
pub fn copy_dir(src: &Path, dst: &Path) -> io::Result<()> {
    if dst
        .symlink_metadata()
        .is_ok_and(|m| m.file_type().is_symlink())
    {
        return Err(io::Error::other(format!(
            "destination {} is a symlink — refusing to copy through it",
            dst.display()
        )));
    }
    if !src.is_dir() {
        return Err(io::Error::other(format!(
            "source {} is not a directory",
            src.display()
        )));
    }
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let ft = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ft.is_dir() && !ft.is_symlink() {
            copy_dir(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?; // files and symlinks-by-copy
        }
    }
    Ok(())
}

/// Delete empty parent directories upward until (excluding) `stop`.
/// Keeps the managed store tidy after nested-path removals without
/// ever crossing the store boundary.
pub fn remove_empty_ancestors(p: &Path, stop: &Path) {
    let mut cur = p.to_path_buf();
    while cur.starts_with(stop) && cur != stop {
        let Some(parent) = cur.parent().map(Path::to_path_buf) else {
            return;
        };
        if std::fs::remove_dir(&cur).is_err() {
            return; // not empty (or gone) — stop climbing
        }
        cur = parent;
    }
}

/// URL-safe base64 without padding — trash entries name their source
/// directory this way so any skill name survives as one path
/// component.
pub fn base64url_no_pad(s: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.as_bytes())
}

/// Reject source/destination pairs that live inside each other — a
/// sync whose destination contains its source would recurse through
/// the copy forever. Equal paths are allowed (no-op territory for
/// the callers that check first).
pub fn assert_not_nested(a: &Path, b: &Path) -> Result<(), HubError> {
    if a == b {
        return Ok(());
    }
    if a.starts_with(b) || b.starts_with(a) {
        return Err(HubError::new(
            HubCode::InvalidInput,
            format!("{} and {} are nested", a.display(), b.display()),
        ));
    }
    Ok(())
}

/// The guarded candidate path for `dir` under `base`: lexically
/// resolved, strictly contained in `base`, with every *intermediate*
/// ancestor lstat'd — a symlink or non-directory mid-path yields
/// `None`, because following it could escape the target root. The
/// final segment may be anything (including the sync's own symlink —
/// removal and classification must address it, not follow it). The
/// root itself must be a directory if it exists.
pub fn target_skill_path(base: &Path, dir: &str) -> Option<PathBuf> {
    if !base.is_absolute() {
        return None;
    }
    let rel = sanitize_relative_path(dir)?;
    let candidate = base.join(&rel);
    // Lexical containment: no `..` may survive (sanitize already
    // rejects those segments, this is the belt to the suspenders).
    if !candidate.starts_with(base) {
        return None;
    }
    if let Ok(m) = base.symlink_metadata()
        && !m.is_dir()
    {
        return None;
    }
    let segments: Vec<&str> = rel.split('/').collect();
    let mut cur = base.to_path_buf();
    for (i, seg) in segments.iter().enumerate() {
        cur.push(seg);
        if i + 1 == segments.len() {
            break; // the destination itself may be a link or missing
        }
        if let Ok(m) = cur.symlink_metadata()
            && (m.file_type().is_symlink() || !m.is_dir())
        {
            return None;
        }
    }
    Some(candidate)
}

/// Write JSON the hub's way: pretty (diff-able, merge-able), one
/// trailing newline, 0600 — registry and cache files sit in the
/// user's data dir next to credentials-grade neighbors.
pub fn write_json_private<T: serde::Serialize>(path: &Path, value: &T) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut body = serde_json::to_string_pretty(value)
        .map_err(|e| io::Error::other(format!("serialize: {e}")))?;
    body.push('\n');
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, body.as_bytes())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a directory symlink, cross-platform. Windows needs the
    /// directory flavor (and SeCreateSymbolicLinkPrivilege — absent
    /// for unprivileged local runs), so callers skip only their
    /// link-dependent assertions when this returns false; CI runners
    /// on both platforms create links for real.
    fn dir_symlink(target: &Path, link: &Path) -> bool {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(target, link).is_ok()
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_dir(target, link).is_ok()
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (target, link);
            false
        }
    }

    #[test]
    fn segment_sanitizer_rejects_the_escape_shapes() {
        assert_eq!(sanitize_path_segment("pdf"), Some("pdf".into()));
        assert_eq!(sanitize_path_segment(".hidden"), Some(".hidden".into()));
        for bad in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
            assert!(sanitize_path_segment(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn relative_sanitizer_folds_and_rejects() {
        assert_eq!(
            sanitize_relative_path("document-skills/pdf"),
            Some("document-skills/pdf".into())
        );
        assert_eq!(sanitize_relative_path("a\\b"), Some("a/b".into()));
        for bad in ["/abs", "C:\\win", "a/../b", "..", "a/b: c", "a\0"] {
            assert!(sanitize_relative_path(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn local_paths_reject_dot_segments() {
        assert_eq!(sanitize_local_skill_path("pdf"), Some("pdf".into()));
        assert!(sanitize_local_skill_path(".git/hooks").is_none());
        assert!(sanitize_local_skill_path("a/.b").is_none());
    }

    #[test]
    fn install_name_is_the_sanitized_leaf() {
        assert_eq!(
            install_name_from_directory("document-skills/pdf"),
            Some("pdf".into())
        );
        assert_eq!(install_name_from_directory("pdf"), Some("pdf".into()));
        assert_eq!(install_name_from_directory("../pdf"), None);
    }

    #[test]
    fn remove_path_never_follows_a_symlink() {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-fsutil-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let target = dir.join("target");
        let link = dir.join("link");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(target.join("keep.txt"), "x").unwrap();
        if !dir_symlink(&target, &link) {
            let _ = std::fs::remove_dir_all(&dir);
            return; // no link privilege — the guard paths are untestable
        }

        super::super::fsutil::remove_path(&link);
        assert!(!link.exists());
        assert!(target.join("keep.txt").exists(), "target must survive");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn copy_dir_refuses_a_surviving_symlink_destination() {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-fsutil-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let src = dir.join("src");
        let dst_target = dir.join("elsewhere");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::create_dir_all(&dst_target).unwrap();
        if !dir_symlink(&dst_target, &dir.join("dst")) {
            let _ = std::fs::remove_dir_all(&dir);
            return; // no link privilege — the refusal path is untestable
        }

        let err = copy_dir(&src, &dir.join("dst")).unwrap_err();
        assert!(err.to_string().contains("symlink"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn target_skill_path_blocks_symlinked_ancestors() {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-fsutil-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let base = dir.join("base");
        let real = dir.join("real");
        std::fs::create_dir_all(&real).unwrap();
        std::fs::create_dir_all(&base).unwrap();
        let linked = dir_symlink(&real, &base.join("hop"));

        assert!(target_skill_path(&base, "pdf").is_some());
        if linked {
            assert!(target_skill_path(&base, "hop/pdf").is_none());
        }
        assert!(target_skill_path(&base, "../escape").is_none());
        // Not-yet-existing leaf is fine (sync pre-creates nothing).
        assert!(target_skill_path(&base, "fresh/skill").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn nested_paths_are_rejected_both_ways() {
        let a = Path::new("/hub/managed/pdf");
        let b = Path::new("/hub/managed");
        assert!(assert_not_nested(a, b).is_err());
        assert!(assert_not_nested(b, a).is_err());
        assert!(assert_not_nested(a, a).is_ok());
        assert!(assert_not_nested(a, Path::new("/elsewhere")).is_ok());
    }
}
