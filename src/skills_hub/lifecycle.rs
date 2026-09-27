//! Install, sync, uninstall, restore — the hub's mutation surface.
//! Every entry point takes the hub root, a fetcher, and the scan
//! roots so the whole flow is testable against local fixtures; the
//! `SkillsHub` wrapper adds the production wiring (env roots,
//! reqwest fetch, serialized mutations).

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use super::fetch::{
    Fetch, blobs_under, fetch_tree_with_fallback, raw_url, source_signature_from_tree,
};
use super::fsutil;
use super::registry::{self, Registry, RepoEntry, SkillEntry};
use super::scan;
use super::targets;
use super::{HubCode, HubError, ScanRoots, TargetRoot};

/// Per-target outcome of a sync/removal sweep. Failures are visible,
/// never swallowed — a mutation's `ok` describes the registry change;
/// these rows describe the filesystem.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetOpResult {
    pub target: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Removal only: the user's preserved source copy was skipped.
    #[serde(skip_serializing_if = "is_false")]
    pub kept: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// An install request. `directory` is the source path inside the
/// repo; the flat install name is its sanitized leaf unless
/// `install_name` overrides.
#[derive(Debug, Clone)]
pub struct InstallRequest {
    pub repo_owner: String,
    pub repo_name: String,
    pub repo_branch: String,
    pub directory: String,
    pub install_name: Option<String>,
    pub targets: Vec<String>,
    pub force: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallOutcome {
    pub skill: SkillEntry,
    pub target_results: Vec<TargetOpResult>,
}

// -- sync engine -----------------------------------------------------------

/// Validate target ids against the table — unknown ids are errors,
/// never silently dropped (a typo must not look like a noop).
fn validate_target_ids(ids: &[String]) -> Result<(), HubError> {
    for id in ids {
        if targets::target(id).is_none() {
            return Err(HubError::new(
                HubCode::InvalidInput,
                format!("unknown target {id:?} (see skills.targets)"),
            ));
        }
    }
    Ok(())
}

/// One target root for a chosen id, resolved from the scan roots
/// (the test seam keeps unit tests off the real `~/.claude`).
fn root_for<'a>(roots: &'a [TargetRoot], id: &str) -> Option<&'a TargetRoot> {
    roots.iter().find(|r| r.id == id)
}

/// Sync the managed copy of `name` into one target: remove whatever
/// sits at the destination, then symlink to the managed store —
/// copy fallback where symlinks are unavailable (windows without
/// privilege). A target whose engine home does not exist reports
/// failure instead of creating the home (the never-create-homes
/// rule).
fn sync_skill_to_target(
    source: &Path,
    root: &TargetRoot,
    name: &str,
    preserved: Option<&Path>,
) -> TargetOpResult {
    let fail = |msg: String| TargetOpResult {
        target: root.id.clone(),
        ok: false,
        error: Some(msg),
        kept: false,
    };
    let Some(engine_home) = root.dir.parent() else {
        return fail("target has no engine home".into());
    };
    if !engine_home.is_dir() {
        return fail(format!(
            "engine home {} does not exist — install the CLI first",
            engine_home.display()
        ));
    }
    let Some(dest) = fsutil::target_skill_path(&root.dir, name) else {
        return fail(format!("unsafe destination for {name:?}"));
    };
    if preserved == Some(dest.as_path()) {
        return TargetOpResult {
            target: root.id.clone(),
            ok: true,
            error: None,
            kept: true,
        };
    }
    if let Err(e) = fsutil::assert_not_nested(source, &dest) {
        return fail(e.to_string());
    }
    fsutil::remove_path(&dest);
    if let Some(parent) = dest.parent()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        return fail(e.to_string());
    }
    #[cfg(unix)]
    let link = std::os::unix::fs::symlink(source, &dest);
    #[cfg(windows)]
    let link = std::os::windows::fs::symlink_dir(source, &dest);
    if link.is_err() {
        // No symlink privilege (or platform support): fall back to a
        // real copy. Updates re-copy because the destination was
        // removed above.
        if let Err(e) = fsutil::copy_dir(source, &dest) {
            return fail(e.to_string());
        }
    }
    TargetOpResult {
        target: root.id.clone(),
        ok: true,
        error: None,
        kept: false,
    }
}

/// Remove the copy of `name` from one target. The preserved source
/// (a local import's original directory) is kept and reported.
fn remove_skill_from_target(
    root: &TargetRoot,
    name: &str,
    preserved: Option<&Path>,
) -> TargetOpResult {
    let Some(dest) = fsutil::target_skill_path(&root.dir, name) else {
        return TargetOpResult {
            target: root.id.clone(),
            ok: true, // nothing addressable there
            error: None,
            kept: false,
        };
    };
    if preserved == Some(dest.as_path()) {
        return TargetOpResult {
            target: root.id.clone(),
            ok: true,
            error: None,
            kept: true,
        };
    }
    fsutil::remove_path(&dest);
    TargetOpResult {
        target: root.id.clone(),
        ok: true,
        error: None,
        kept: false,
    }
}

/// Sync into the chosen target ids.
fn sync_targets(
    hub: &Path,
    roots: &[TargetRoot],
    name: &str,
    ids: &[String],
    preserved: Option<&Path>,
) -> Vec<TargetOpResult> {
    let source = registry::managed_dir(hub).join(name);
    ids.iter()
        .filter_map(|id| root_for(roots, id))
        .map(|root| sync_skill_to_target(&source, root, name, preserved))
        .collect()
}

/// Remove from every target root (including ones the registry forgot
/// — a lost copy is still cleaned up).
fn remove_targets(
    roots: &[TargetRoot],
    name: &str,
    preserved: Option<&Path>,
) -> Vec<TargetOpResult> {
    roots
        .iter()
        .map(|root| remove_skill_from_target(root, name, preserved))
        .collect()
}

// -- install ---------------------------------------------------------------

/// Install a skill from a GitHub repo: stage, swap, re-read metadata,
/// upsert the registry, sync the chosen targets.
pub fn install_skill(
    hub: &Path,
    fetch: &dyn Fetch,
    roots: &ScanRoots,
    req: InstallRequest,
) -> Result<InstallOutcome, HubError> {
    validate_target_ids(&req.targets)?;
    if req.repo_owner.trim().is_empty() || req.repo_name.trim().is_empty() {
        return Err(HubError::new(
            HubCode::InvalidInput,
            "owner and name are required",
        ));
    }
    // "." installs the repo root as one skill, named after the repo.
    let is_root = matches!(req.directory.as_str(), "." | "./");
    let source_directory = if is_root {
        ".".to_string()
    } else {
        fsutil::sanitize_relative_path(&req.directory).ok_or_else(|| {
            HubError::new(
                HubCode::InvalidInput,
                format!("bad directory {:?}", req.directory),
            )
        })?
    };
    let install_name = match &req.install_name {
        Some(n) => fsutil::sanitize_path_segment(n)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| HubError::new(HubCode::InvalidInput, "bad installName"))?,
        None => {
            if is_root {
                fsutil::sanitize_path_segment(&req.repo_name)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| HubError::new(HubCode::InvalidInput, "bad repo name"))?
            } else {
                fsutil::install_name_from_directory(&req.directory).ok_or_else(|| {
                    HubError::new(
                        HubCode::InvalidInput,
                        format!("bad directory {:?}", req.directory),
                    )
                })?
            }
        }
    };

    let mut reg = Registry::load(hub);
    reg.purge_expired_trash(hub, fsutil::now_ms());

    // Directory conflicts: another repo (or a local import) already
    // owns this flat name.
    let new_id = format!("{}/{}:{}", req.repo_owner, req.repo_name, source_directory);
    for existing in &reg.skills {
        if existing.is_trashed() {
            continue;
        }
        if existing.directory.eq_lowercase(&install_name) && existing.id != new_id {
            return Err(HubError::new(
                HubCode::Conflict,
                format!(
                    "skill directory {install_name:?} is already managed by {} — uninstall it first",
                    existing.id
                ),
            ));
        }
    }

    // Tree + files under the source directory. "." means the repo
    // root (install name then defaults to the repo name via caller).
    let (tree, branch) =
        fetch_tree_with_fallback(fetch, &req.repo_owner, &req.repo_name, &req.repo_branch)?;
    let files = blobs_under(&tree, &source_directory);
    let has_marker = files
        .iter()
        .any(|f| f.eq_ignore_ascii_case("SKILL.md") || f.eq_ignore_ascii_case("skill.md"));
    if files.is_empty() || !has_marker {
        return Err(HubError::new(
            HubCode::InvalidInput,
            format!(
                "SKILL.md not found in {}/{}@{branch}:{source_directory}",
                req.repo_owner, req.repo_name
            ),
        ));
    }

    let dest = registry::managed_dir(hub).join(&install_name);
    // Local-modification guard: the managed copy drifted from the
    // hash recorded at install time — reinstalling would overwrite
    // user edits unless `force` says otherwise.
    if dest.is_dir() && !req.force {
        let drifted = reg
            .skills
            .iter()
            .find(|s| s.directory.eq_lowercase(&install_name))
            .and_then(|s| s.content_hash.as_deref())
            .is_some_and(|recorded| {
                scan::hash_directory(&dest).is_none_or(|current| current != recorded)
            });
        if drifted {
            return Err(HubError::new(
                HubCode::Conflict,
                "the managed copy has local changes; reinstalling would overwrite them (force to confirm)",
            ));
        }
    }

    // Stage every file, then swap atomically.
    let staging = registry::tmp_dir(hub).join(format!("{install_name}-{}", fsutil::now_ms()));
    fsutil::remove_path(&staging);
    std::fs::create_dir_all(&staging).map_err(HubError::from)?;
    for rel in &files {
        let Some(safe_rel) = fsutil::sanitize_relative_path(rel) else {
            continue; // unsafe repo paths are skipped, not fatal
        };
        // `files` are directory-relative; raw URLs are repo-absolute.
        let full_path = if source_directory == "." {
            rel.clone()
        } else {
            format!("{source_directory}/{rel}")
        };
        let url = raw_url(&req.repo_owner, &req.repo_name, &branch, &full_path);
        let body = fetch.get_bytes(&url).map_err(|e| {
            fsutil::remove_path(&staging);
            HubError::from(e)
        })?;
        let target_file = staging.join(&safe_rel);
        if let Some(parent) = target_file.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                fsutil::remove_path(&staging);
                HubError::from(e)
            })?;
        }
        std::fs::write(&target_file, body).map_err(|e| {
            fsutil::remove_path(&staging);
            HubError::from(e)
        })?;
    }
    fsutil::remove_path(&dest);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(HubError::from)?;
    }
    std::fs::rename(&staging, &dest).map_err(|e| {
        fsutil::remove_path(&staging);
        HubError::from(e)
    })?;

    // Re-read metadata from the downloaded copy — the source of
    // truth for name/description is the skill itself.
    let meta = scan::find_skill_marker(&dest)
        .map(|m| scan::read_skill_metadata(&m, &install_name))
        .unwrap_or_else(|| scan::SkillMeta {
            name: install_name.clone(),
            description: None,
        });
    let entry = SkillEntry {
        id: new_id,
        key: Some(format!(
            "{}/{}:{}",
            req.repo_owner, req.repo_name, source_directory
        )),
        name: meta.name,
        description: meta.description,
        directory: install_name.clone(),
        source_directory: Some(source_directory.clone()),
        readme_url: Some(format!(
            "https://github.com/{}/{}/blob/{}/{}SKILL.md",
            req.repo_owner,
            req.repo_name,
            branch,
            if source_directory == "." {
                String::new()
            } else {
                format!("{}/", source_directory)
            }
        )),
        repo_owner: Some(req.repo_owner.clone()),
        repo_name: Some(req.repo_name.clone()),
        repo_branch: Some(branch.clone()),
        installed_at: fsutil::now_ms(),
        content_hash: scan::hash_directory(&dest),
        source_signature: source_signature_from_tree(&tree, &source_directory),
        targets: req.targets.clone(),
        source_path: None,
        trashed_at: None,
        trashed_directory: None,
        previous_targets: None,
    };
    reg.skills
        .retain(|s| s.id != entry.id && !s.directory.eq_lowercase(&entry.directory));
    let id = entry.id.clone();
    reg.skills.push(entry.clone());
    reg.save(hub).map_err(HubError::from)?;

    let results = sync_targets(hub, &roots.targets, &install_name, &req.targets, None);
    registry::append_activity(
        hub,
        json!({"kind": "install", "id": id, "directory": install_name, "targets": req.targets}),
    );
    Ok(InstallOutcome {
        skill: entry,
        target_results: results,
    })
}

// -- uninstall / restore ---------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UninstallOutcome {
    pub trashed: bool,
    /// The id to pass to `skills.restore` within the TTL.
    pub restore_id: String,
    pub ttl_ms: u64,
    pub target_results: Vec<TargetOpResult>,
}

/// Uninstall: remove from every target, move the managed copy to the
/// trash (5-minute restore window), purge expired entries.
pub fn uninstall_skill(
    hub: &Path,
    roots: &ScanRoots,
    id: &str,
) -> Result<UninstallOutcome, HubError> {
    let mut reg = Registry::load(hub);
    reg.purge_expired_trash(hub, fsutil::now_ms());
    let Some(entry) = reg.find(id).filter(|s| !s.is_trashed()) else {
        return Err(HubError::new(
            HubCode::NotFound,
            format!("skill {id:?} not installed"),
        ));
    };
    let entry = entry.clone();
    let preserved = entry.source_path.as_deref().map(Path::new);
    let results = remove_targets(&roots.targets, &entry.directory, preserved);

    let managed = registry::managed_dir(hub).join(&entry.directory);
    let mut trashed = false;
    let now = fsutil::now_ms();
    let trash_dir = registry::trash_dir(hub);
    let trash_name = format!("{}-{now}", fsutil::base64url_no_pad(&entry.directory));
    if managed.is_dir() {
        let _ = std::fs::create_dir_all(&trash_dir);
        if std::fs::rename(&managed, trash_dir.join(&trash_name)).is_ok() {
            trashed = true;
        } else {
            // Cannot trash (read-only fs, cross-device): hard-remove.
            fsutil::remove_path(&managed);
        }
    } else {
        fsutil::remove_path(&managed);
    }

    if trashed {
        if let Some(e) = reg.find_mut(id) {
            e.trashed_at = Some(now);
            e.trashed_directory = Some(trash_name);
            e.previous_targets = Some(e.targets.clone());
            e.targets = Vec::new();
        }
    } else {
        reg.skills.retain(|s| s.id != entry.id);
    }
    reg.save(hub).map_err(HubError::from)?;
    reg.purge_expired_trash(hub, fsutil::now_ms());
    registry::append_activity(hub, json!({"kind": "uninstall", "id": entry.id}));
    Ok(UninstallOutcome {
        trashed,
        restore_id: entry.id.clone(),
        ttl_ms: registry::TRASH_TTL_MS,
        target_results: results,
    })
}

/// Restore a trashed skill: rename back, re-sync its previous
/// targets. Past the TTL the trash copy is gone — conflict.
pub fn restore_skill(hub: &Path, roots: &ScanRoots, id: &str) -> Result<InstallOutcome, HubError> {
    let mut reg = Registry::load(hub);
    reg.purge_expired_trash(hub, fsutil::now_ms());
    let Some(entry) = reg.find(id) else {
        return Err(HubError::new(
            HubCode::NotFound,
            format!("skill {id:?} not found"),
        ));
    };
    let Some(trashed_at) = entry.trashed_at else {
        return Err(HubError::new(
            HubCode::InvalidInput,
            format!("skill {id:?} is not trashed"),
        ));
    };
    if fsutil::now_ms().saturating_sub(trashed_at) > registry::TRASH_TTL_MS {
        return Err(HubError::new(HubCode::Conflict, "restore window expired"));
    }
    let Some(trash_name) = entry.trashed_directory.clone() else {
        return Err(HubError::new(HubCode::NotFound, "no trash copy recorded"));
    };
    let trash_copy = registry::trash_dir(hub).join(&trash_name);
    if !trash_copy.is_dir() {
        return Err(HubError::new(HubCode::NotFound, "trash copy is gone"));
    }
    let previous = entry.previous_targets.clone().unwrap_or_default();
    let directory = entry.directory.clone();
    let dest = registry::managed_dir(hub).join(&directory);
    fsutil::remove_path(&dest);
    std::fs::rename(&trash_copy, &dest).map_err(HubError::from)?;
    if let Some(e) = reg.find_mut(id) {
        e.trashed_at = None;
        e.trashed_directory = None;
        e.previous_targets = None;
        e.targets = previous.clone();
        e.content_hash = scan::hash_directory(&dest);
    }
    reg.save(hub).map_err(HubError::from)?;
    let restored = reg.find(id).cloned();
    let results = sync_targets(hub, &roots.targets, &directory, &previous, None);
    registry::append_activity(hub, json!({"kind": "restore", "id": id}));
    Ok(InstallOutcome {
        skill: restored
            .ok_or_else(|| HubError::new(HubCode::Internal, "entry vanished mid-restore"))?,
        target_results: results,
    })
}

/// Change the synced target set: newly selected targets sync,
/// deselected ones are removed — iterating every target so copies in
/// forgotten roots still clear.
pub fn set_targets(
    hub: &Path,
    roots: &ScanRoots,
    id: &str,
    new_targets: &[String],
) -> Result<Vec<TargetOpResult>, HubError> {
    validate_target_ids(new_targets)?;
    let mut reg = Registry::load(hub);
    reg.purge_expired_trash(hub, fsutil::now_ms());
    let Some(entry) = reg.find_mut(id).filter(|s| !s.is_trashed()) else {
        return Err(HubError::new(
            HubCode::NotFound,
            format!("skill {id:?} not installed"),
        ));
    };
    let old: Vec<String> = entry.targets.clone();
    let directory = entry.directory.clone();
    let preserved = entry
        .source_path
        .as_deref()
        .map(Path::new)
        .map(Path::to_path_buf);
    entry.targets = new_targets.to_vec();
    reg.save(hub).map_err(HubError::from)?;

    let source = registry::managed_dir(hub).join(&directory);
    let mut results = Vec::new();
    for root in &roots.targets {
        let wants = new_targets.iter().any(|t| t == &root.id);
        let had = old.iter().any(|t| t == &root.id);
        if wants {
            results.push(sync_skill_to_target(
                &source,
                root,
                &directory,
                preserved.as_deref(),
            ));
        } else if had {
            results.push(remove_skill_from_target(
                root,
                &directory,
                preserved.as_deref(),
            ));
        }
    }
    registry::append_activity(
        hub,
        json!({"kind": "set_targets", "id": id, "targets": new_targets}),
    );
    Ok(results)
}

// -- local import / delete -------------------------------------------------

/// Where a local (unmanaged) skill directory lives: the first target
/// root containing a marker-bearing child of that name. Read-only
/// sources are reported so the caller can refuse them.
fn find_local_skill_source(
    roots: &ScanRoots,
    directory: &str,
) -> Result<Option<(PathBuf, Vec<String>)>, HubError> {
    let name = fsutil::sanitize_local_skill_path(directory).ok_or_else(|| {
        HubError::new(
            HubCode::InvalidInput,
            format!("bad directory {directory:?}"),
        )
    })?;
    for r in &roots.readonly {
        if r.dir.join(&name).join("SKILL.md").is_file()
            || r.dir.join(&name).join("skill.md").is_file()
        {
            return Err(HubError::new(
                HubCode::Readonly,
                format!("{directory:?} is a read-only source ({})", r.label),
            ));
        }
    }
    let mut found: Option<(PathBuf, Vec<String>)> = None;
    for t in &roots.targets {
        let Some(candidate) = fsutil::target_skill_path(&t.dir, &name) else {
            continue;
        };
        if scan::has_skill_marker(&candidate) {
            let entry = found.get_or_insert_with(|| (candidate.clone(), Vec::new()));
            entry.1.push(t.id.clone());
        }
    }
    Ok(found)
}

/// Import a local skill into the managed store: *copied*, never
/// symlinked, with `source_path` recorded so every later sync and
/// removal leaves the user's original untouched.
pub fn import_local(
    hub: &Path,
    roots: &ScanRoots,
    directory: &str,
    targets: &[String],
) -> Result<InstallOutcome, HubError> {
    validate_target_ids(targets)?;
    let Some((source, discovered)) = find_local_skill_source(roots, directory)? else {
        return Err(HubError::new(
            HubCode::NotFound,
            format!("no local skill {directory:?} found"),
        ));
    };
    let name = source
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| HubError::new(HubCode::Internal, "source has no name"))?
        .to_string();
    let mut reg = Registry::load(hub);
    reg.purge_expired_trash(hub, fsutil::now_ms());
    if let Some(existing) = reg
        .skills
        .iter()
        .find(|s| !s.is_trashed() && s.directory == name)
    {
        if existing.source_path.is_some() {
            // Already imported: adjust targets, leave the store alone.
            // set_targets reloaded and saved the registry itself, so
            // read the entry back fresh for the response.
            let id = existing.id.clone();
            let new_targets: Vec<String> = if targets.is_empty() {
                existing.targets.clone()
            } else {
                targets.to_vec()
            };
            return set_targets(hub, roots, &id, &new_targets).map(|target_results| {
                let fresh = Registry::load(hub);
                InstallOutcome {
                    skill: fresh.find(&id).cloned().unwrap_or_default(),
                    target_results,
                }
            });
        }
        return Err(HubError::new(
            HubCode::Conflict,
            format!(
                "{name:?} is managed by {} — uninstall it first",
                existing.id
            ),
        ));
    }

    let dest = registry::managed_dir(hub).join(&name);
    fsutil::remove_path(&dest);
    std::fs::create_dir_all(registry::managed_dir(hub)).map_err(HubError::from)?;
    fsutil::copy_dir(&source, &dest).map_err(HubError::from)?;
    let meta = scan::find_skill_marker(&dest)
        .map(|m| scan::read_skill_metadata(&m, &name))
        .unwrap_or_else(|| scan::SkillMeta {
            name: name.clone(),
            description: None,
        });
    let chosen: Vec<String> = if targets.is_empty() {
        discovered.clone()
    } else {
        targets.to_vec()
    };
    let entry = SkillEntry {
        id: format!("local:{name}"),
        key: Some(format!("local:{name}")),
        name: meta.name,
        description: meta.description,
        directory: name.clone(),
        source_directory: None,
        readme_url: None,
        repo_owner: None,
        repo_name: None,
        repo_branch: None,
        installed_at: fsutil::now_ms(),
        content_hash: scan::hash_directory(&dest),
        source_signature: None,
        targets: chosen.clone(),
        source_path: Some(source.to_string_lossy().into_owned()),
        trashed_at: None,
        trashed_directory: None,
        previous_targets: None,
    };
    let id = entry.id.clone();
    reg.skills.retain(|s| s.directory != name);
    reg.skills.push(entry.clone());
    reg.save(hub).map_err(HubError::from)?;
    let results = sync_targets(hub, &roots.targets, &name, &chosen, Some(&source));
    registry::append_activity(
        hub,
        json!({"kind": "import", "id": id, "directory": name, "source": source.to_string_lossy()}),
    );
    Ok(InstallOutcome {
        skill: entry,
        target_results: results,
    })
}

/// Delete a local (unmanaged) skill from its target roots. Managed
/// entries refuse — `skills.uninstall` is that path.
pub fn delete_local(
    hub: &Path,
    roots: &ScanRoots,
    directory: &str,
    targets: &[String],
) -> Result<Vec<TargetOpResult>, HubError> {
    validate_target_ids(targets)?;
    let Some((source, _discovered)) = find_local_skill_source(roots, directory)? else {
        return Err(HubError::new(
            HubCode::NotFound,
            format!("no local skill {directory:?} found"),
        ));
    };
    let name = source
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default()
        .to_string();
    let reg = Registry::load(hub);
    if reg
        .skills
        .iter()
        .any(|s| !s.is_trashed() && s.directory == name)
    {
        return Err(HubError::new(
            HubCode::Conflict,
            format!("{name:?} is registry-managed — use skills.uninstall"),
        ));
    }
    let selected: Vec<&TargetRoot> = if targets.is_empty() {
        roots.targets.iter().collect()
    } else {
        roots
            .targets
            .iter()
            .filter(|t| targets.contains(&t.id))
            .collect()
    };
    let mut results = Vec::new();
    for root in selected {
        results.push(remove_skill_from_target(root, &name, Some(&source)));
    }
    registry::append_activity(hub, json!({"kind": "delete_local", "directory": name}));
    Ok(results)
}

// -- repos -----------------------------------------------------------------

/// `owner/name@branch` — the config spelling for extra repos.
pub fn parse_repo_spec(spec: &str) -> Result<RepoEntry, HubError> {
    let (slug, branch) = match spec.split_once('@') {
        Some((slug, branch)) => (slug, branch),
        None => (spec, "main"),
    };
    let Some((owner, name)) = slug.split_once('/') else {
        return Err(HubError::new(
            HubCode::InvalidInput,
            format!("repo spec {spec:?} must be owner/name[@branch]"),
        ));
    };
    (RepoEntry {
        owner: owner.into(),
        name: name.into(),
        branch: branch.into(),
        enabled: true,
    })
    .validated()
}

impl RepoEntry {
    /// GitHub component rules: one leading alphanumeric, then the
    /// url-safe set, ≤100 chars.
    pub fn validated(self) -> Result<Self, HubError> {
        let ok = |s: &str| {
            let mut chars = s.chars();
            chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
                && s.len() <= 100
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        };
        if !ok(&self.owner) || !ok(&self.name) || !ok(&self.branch) {
            return Err(HubError::new(
                HubCode::InvalidInput,
                format!(
                    "invalid repo coordinates {}/{}@{}",
                    self.owner, self.name, self.branch
                ),
            ));
        }
        Ok(self)
    }
}

/// Add or replace a repo (same `owner/name`). Invalidates the
/// discover cache — the repo set changed.
pub fn add_repo(hub: &Path, repo: RepoEntry) -> Result<(), HubError> {
    let repo = repo.validated()?;
    let mut reg = Registry::load(hub);
    reg.repos
        .retain(|r| !r.slug().eq_ignore_ascii_case(&repo.slug()));
    reg.repos.push(repo.clone());
    reg.save(hub).map_err(HubError::from)?;
    let _ = std::fs::remove_file(registry::cache_path(hub, "discover-cache"));
    registry::append_activity(
        hub,
        json!({"kind": "addRepo", "repo": repo.slug(), "branch": repo.branch}),
    );
    Ok(())
}

/// Remove a repo. Installed skills from it stay managed — the SSOT
/// outlives its source.
pub fn remove_repo(hub: &Path, owner: &str, name: &str) -> Result<(), HubError> {
    let mut reg = Registry::load(hub);
    let slug = format!("{owner}/{name}").to_lowercase();
    let before = reg.repos.len();
    reg.repos.retain(|r| r.slug().to_lowercase() != slug);
    if reg.repos.len() == before {
        return Err(HubError::new(
            HubCode::NotFound,
            format!("repo {slug} not registered"),
        ));
    }
    reg.save(hub).map_err(HubError::from)?;
    let _ = std::fs::remove_file(registry::cache_path(hub, "discover-cache"));
    registry::append_activity(hub, json!({"kind": "removeRepo", "repo": slug}));
    Ok(())
}

/// The registry's repos plus config extras not already present —
/// extras are advisory seeds, mutation stays in the registry.
pub fn effective_repos(reg: &Registry, extras: &[RepoEntry]) -> Vec<RepoEntry> {
    let mut out = reg.repos.clone();
    for extra in extras {
        if !out
            .iter()
            .any(|r| r.slug().eq_ignore_ascii_case(&extra.slug()))
        {
            out.push(extra.clone());
        }
    }
    out
}

// -- the production wrapper ------------------------------------------------

/// Daemon-side hub state: the root, the production fetcher, and the
/// mutation lock. Constructed once on `AppState`; reads stay lock
/// free, mutations serialize (the registry is read-modify-write and
/// concurrent installs would lose one).
pub struct SkillsHub {
    root: PathBuf,
    fetch: std::sync::Arc<dyn Fetch>,
    ops: std::sync::Mutex<()>,
}

impl SkillsHub {
    pub fn new(root: PathBuf) -> Self {
        SkillsHub {
            root,
            fetch: std::sync::Arc::new(super::fetch::ReqwestFetch),
            ops: std::sync::Mutex::new(()),
        }
    }

    fn roots() -> ScanRoots {
        ScanRoots::from_environment()
    }

    pub fn install(&self, req: InstallRequest) -> Result<InstallOutcome, HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        install_skill(&self.root, self.fetch.as_ref(), &Self::roots(), req)
    }

    pub fn uninstall(&self, id: &str) -> Result<UninstallOutcome, HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        uninstall_skill(&self.root, &Self::roots(), id)
    }

    pub fn restore(&self, id: &str) -> Result<InstallOutcome, HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        restore_skill(&self.root, &Self::roots(), id)
    }

    pub fn set_targets(
        &self,
        id: &str,
        targets: &[String],
    ) -> Result<Vec<TargetOpResult>, HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        set_targets(&self.root, &Self::roots(), id, targets)
    }

    pub fn import_local(
        &self,
        directory: &str,
        targets: &[String],
    ) -> Result<InstallOutcome, HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        import_local(&self.root, &Self::roots(), directory, targets)
    }

    pub fn delete_local(
        &self,
        directory: &str,
        targets: &[String],
    ) -> Result<Vec<TargetOpResult>, HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        delete_local(&self.root, &Self::roots(), directory, targets)
    }

    pub fn add_repo(&self, repo: RepoEntry) -> Result<(), HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        add_repo(&self.root, repo)
    }

    pub fn remove_repo(&self, owner: &str, name: &str) -> Result<(), HubError> {
        let _guard = self.ops.lock().unwrap_or_else(|e| e.into_inner());
        remove_repo(&self.root, owner, name)
    }

    pub fn activity(&self, limit: usize) -> Vec<Value> {
        registry::read_activity(&self.root, limit)
    }

    // Discovery reads run without the mutation lock — caches and the
    // registry degrade gracefully under a concurrent install.

    pub fn discover(
        &self,
        repos: Vec<RepoEntry>,
        force: bool,
    ) -> Result<super::discover::DiscoverOutcome, HubError> {
        super::discover::discover_skills(&self.root, self.fetch.as_ref(), repos, force)
    }

    pub fn search(
        &self,
        q: &str,
        limit: u64,
        offset: u64,
    ) -> Result<(u64, Vec<super::discover::SearchSkill>), HubError> {
        super::discover::search_skills(self.fetch.as_ref(), q, limit, offset)
    }

    pub fn popular(
        &self,
        limit: u64,
        force: bool,
    ) -> Result<super::discover::PopularOutcome, HubError> {
        super::discover::popular_skills(&self.root, self.fetch.as_ref(), limit, force)
    }

    pub fn updates(&self, force: bool) -> Result<super::discover::UpdatesOutcome, HubError> {
        super::discover::check_updates(&self.root, self.fetch.as_ref(), force)
    }

    pub fn content(&self, directory: &str) -> Result<super::discover::SkillContent, HubError> {
        super::discover::skill_content(&self.root, &Self::roots(), directory)
    }

    pub fn remote_content(
        &self,
        owner: &str,
        name: &str,
        branch: &str,
        directory: &str,
    ) -> Result<super::discover::RemoteSkillContent, HubError> {
        super::discover::remote_skill_content(self.fetch.as_ref(), owner, name, branch, directory)
    }
}

// -- test helper -----------------------------------------------------------

/// Case-insensitive equality on directory names — installs are flat
/// and `Pdf` vs `pdf` would collide on case-insensitive filesystems.
trait EqLowercase {
    fn eq_lowercase(&self, other: &str) -> bool;
}
impl EqLowercase for str {
    fn eq_lowercase(&self, other: &str) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skills_hub::fetch::LocalFileFetch;
    use crate::skills_hub::{ReadonlyRootRef, TargetRoot};

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-life-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A fixture repo served by LocalFileFetch: tree JSON + two files.
    fn fixture_repo(name: &str) -> (PathBuf, LocalFileFetch) {
        let root = tmp(name);
        let raw = root.join("raw.githubusercontent.com").join("o").join("r");
        let branch_dir = raw.join("main");
        let skill_dir = branch_dir.join("skills").join("pdf");
        std::fs::create_dir_all(&skill_dir).unwrap();
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: PDF\ndescription: PDF tools\n---\nUse me",
        )
        .unwrap();
        std::fs::write(skill_dir.join("run.sh"), "#!/bin/sh\n").unwrap();
        let tree = root
            .join("api.github.com")
            .join("repos")
            .join("o")
            .join("r")
            .join("git")
            .join("trees")
            .join("main");
        std::fs::create_dir_all(tree.parent().unwrap()).unwrap();
        std::fs::write(
            &tree,
            serde_json::json!({"tree": [
                {"type": "blob", "path": "skills/pdf/SKILL.md", "sha": "aaa"},
                {"type": "blob", "path": "skills/pdf/run.sh", "sha": "bbb"},
                {"type": "blob", "path": "README.md", "sha": "ccc"},
            ]})
            .to_string(),
        )
        .unwrap();
        let fetch = LocalFileFetch { root: root.clone() };
        (root, fetch)
    }

    fn scan_roots(home: &Path) -> ScanRoots {
        let claude = home.join("claude-home");
        let codex = home.join("codex-home");
        std::fs::create_dir_all(claude.join("skills")).unwrap();
        std::fs::create_dir_all(codex.join("skills")).unwrap();
        ScanRoots {
            targets: vec![
                TargetRoot {
                    id: "claude".into(),
                    dir: claude.join("skills"),
                    visible: true,
                },
                TargetRoot {
                    id: "codex".into(),
                    dir: codex.join("skills"),
                    visible: true,
                },
            ],
            readonly: Vec::new(),
        }
    }

    fn req(directory: &str, targets: &[&str]) -> InstallRequest {
        InstallRequest {
            repo_owner: "o".into(),
            repo_name: "r".into(),
            repo_branch: "main".into(),
            directory: directory.into(),
            install_name: None,
            targets: targets.iter().map(|s| s.to_string()).collect(),
            force: false,
        }
    }

    #[test]
    fn install_stages_syncs_and_records() {
        let (root, fetch) = fixture_repo("install");
        let home = tmp("home1");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let roots = scan_roots(&home);

        let out = install_skill(&hub, &fetch, &roots, req("skills/pdf", &["claude"])).unwrap();
        assert_eq!(out.skill.name, "PDF");
        assert_eq!(out.skill.directory, "pdf");
        assert!(out.skill.content_hash.is_some());
        assert!(out.skill.source_signature.is_some());
        let claude_link = roots.targets[0].dir.join("pdf");
        assert!(claude_link.is_dir(), "symlink resolves");
        assert!(claude_link.join("run.sh").is_file());
        assert!(
            !roots.targets[1].dir.join("pdf").exists(),
            "codex untouched"
        );
        assert!(out.target_results.iter().all(|r| r.ok));

        // Registry round-trip keeps it listed.
        let reg = Registry::load(&hub);
        assert!(reg.find("o/r:skills/pdf").is_some());
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn reinstall_conflicts_on_local_changes_until_forced() {
        let (root, fetch) = fixture_repo("conflict");
        let home = tmp("home2");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let roots = scan_roots(&home);
        install_skill(&hub, &fetch, &roots, req("skills/pdf", &[])).unwrap();

        // User edit inside the managed copy.
        let managed = registry::managed_dir(&hub).join("pdf");
        std::fs::write(managed.join("SKILL.md"), "---\nname: PDF\n---\nedited").unwrap();
        let err = install_skill(&hub, &fetch, &roots, req("skills/pdf", &[])).unwrap_err();
        assert_eq!(err.code, HubCode::Conflict);

        let mut forced = req("skills/pdf", &[]);
        forced.force = true;
        install_skill(&hub, &fetch, &roots, forced).unwrap();
        assert!(
            std::fs::read_to_string(managed.join("SKILL.md"))
                .unwrap()
                .contains("Use me")
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn directory_conflicts_across_repos() {
        let (root, fetch) = fixture_repo("dirconflict");
        let home = tmp("home3");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let roots = scan_roots(&home);
        install_skill(&hub, &fetch, &roots, req("skills/pdf", &[])).unwrap();

        // Same leaf from a different repo id.
        let mut other = req("skills/pdf", &[]);
        other.repo_owner = "someone".into();
        let err = install_skill(&hub, &fetch, &roots, other).unwrap_err();
        assert_eq!(err.code, HubCode::Conflict);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn uninstall_trashes_then_restores() {
        let (root, fetch) = fixture_repo("uninstall");
        let home = tmp("home4");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let roots = scan_roots(&home);
        install_skill(
            &hub,
            &fetch,
            &roots,
            req("skills/pdf", &["claude", "codex"]),
        )
        .unwrap();

        let out = uninstall_skill(&hub, &roots, "o/r:skills/pdf").unwrap();
        assert!(out.trashed);
        assert!(!roots.targets[0].dir.join("pdf").exists(), "links removed");
        assert!(
            !registry::managed_dir(&hub).join("pdf").exists(),
            "managed moved"
        );
        assert_eq!(registry::trash_dir(&hub).read_dir().unwrap().count(), 1);
        let reg = Registry::load(&hub);
        assert!(reg.find("o/r:skills/pdf").unwrap().is_trashed());

        let restored = restore_skill(&hub, &roots, "o/r:skills/pdf").unwrap();
        assert_eq!(restored.skill.targets.len(), 2);
        assert!(roots.targets[0].dir.join("pdf").exists(), "re-synced");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn set_targets_validates_and_reconciles() {
        let (root, fetch) = fixture_repo("settargets");
        let home = tmp("home5");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let roots = scan_roots(&home);
        install_skill(&hub, &fetch, &roots, req("skills/pdf", &["claude"])).unwrap();

        let err = set_targets(&hub, &roots, "o/r:skills/pdf", &["nope".into()]).unwrap_err();
        assert_eq!(err.code, HubCode::InvalidInput);

        set_targets(&hub, &roots, "o/r:skills/pdf", &["codex".into()]).unwrap();
        assert!(
            !roots.targets[0].dir.join("pdf").exists(),
            "deselected removed"
        );
        assert!(
            roots.targets[1].dir.join("pdf").exists(),
            "newly selected synced"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn import_local_copies_and_preserves_the_source() {
        let home = tmp("home6");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let roots = scan_roots(&home);
        // A user-authored skill sitting in the claude target.
        let source = roots.targets[0].dir.join("mine");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("SKILL.md"), "---\ndescription: mine\n---\nbody").unwrap();

        let out = import_local(
            &hub,
            &roots,
            "mine",
            &["claude".to_string(), "codex".to_string()],
        )
        .unwrap();
        assert_eq!(out.skill.id, "local:mine");
        assert!(out.skill.source_path.is_some());
        assert!(source.join("SKILL.md").is_file(), "original preserved");
        assert!(
            registry::managed_dir(&hub).join("mine").is_dir(),
            "copy in the store"
        );
        assert!(
            roots.targets[1].dir.join("mine").exists(),
            "synced to codex"
        );

        // Uninstall keeps the source, clears the copies.
        let un = uninstall_skill(&hub, &roots, "local:mine").unwrap();
        assert!(
            un.target_results.iter().any(|r| r.kept),
            "preserved source reported kept"
        );
        assert!(source.join("SKILL.md").is_file());
        assert!(!roots.targets[1].dir.join("mine").exists());
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn readonly_sources_refuse_import() {
        let home = tmp("home7");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let mut roots = scan_roots(&home);
        let sys = home.join("codex-home").join("skills").join(".system");
        std::fs::create_dir_all(sys.join("sys-skill")).unwrap();
        std::fs::write(sys.join("sys-skill").join("SKILL.md"), "x").unwrap();
        roots.readonly = vec![ReadonlyRootRef {
            kind: "system",
            label: "codex:.system".into(),
            dir: sys.clone(),
        }];
        let err = import_local(&hub, &roots, "sys-skill", &[]).unwrap_err();
        assert_eq!(err.code, HubCode::Readonly);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn repos_round_trip_and_specs_parse() {
        let home = tmp("home8");
        let hub = home.join("hub");
        std::fs::create_dir_all(&hub).unwrap();
        let repo = parse_repo_spec("someone/cool@dev").unwrap();
        assert_eq!(repo.branch, "dev");
        add_repo(&hub, repo).unwrap();
        let reg = Registry::load(&hub);
        assert_eq!(reg.repos.len(), 5, "4 seeds + 1");
        assert!(reg.repos.iter().any(|r| r.slug() == "someone/cool"));

        // Extras merge without duplicating.
        let extra = parse_repo_spec("someone/cool@dev").unwrap();
        assert_eq!(effective_repos(&reg, &[extra]).len(), 5);

        remove_repo(&hub, "someone", "cool").unwrap();
        assert!(remove_repo(&hub, "someone", "cool").is_err());
        assert!(parse_repo_spec("../evil").is_err());
        assert!(parse_repo_spec("no-slash").is_err());
        let _ = std::fs::remove_dir_all(&home);
    }
}
