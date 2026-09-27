//! Skills hub: install agent-skill packages once, sync them into
//! every installed CLI's native skill directory, and keep the
//! composer's `/` picker honest about what is there. Design ported
//! from desktop-cc-gui's `skills_hub` (itself from TokenTracker's
//! skills-manager, MIT), adapted to daemon conventions: flat RPC
//! methods instead of a GUI command bus, serialized mutations
//! instead of single-process assumptions, and an injectable fetch
//! layer so installs are testable without the network.
//!
//! Trust boundary: every directory this module writes is chosen by
//! the daemon itself (the user's engine homes and the hub's data
//! dir), resolved through the daemon's own environment — never from
//! client input. Client-supplied text only ever names things *inside*
//! those roots, and it is sanitized before it reaches a path join.

pub mod discover;
pub mod fetch;
pub mod fsutil;
pub mod lifecycle;
pub mod registry;
pub mod scan;
pub mod targets;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

pub use lifecycle::{InstallRequest, SkillsHub, TargetOpResult};
pub use registry::{RepoEntry, SkillEntry};

/// Upper bound on `skills.installed` entries — a pathological scan
/// must not push the response over the RPC frame budget.
pub const MAX_INSTALLED: usize = 1000;

/// Error taxonomy carried to clients as `error.data.code` so they can
/// branch (conflict → offer a forced overwrite, rate_limited → back
/// off) without parsing messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubCode {
    InvalidInput,
    NotFound,
    Conflict,
    Readonly,
    Permission,
    Network,
    Http,
    RateLimited,
    Internal,
}

impl HubCode {
    pub fn as_str(self) -> &'static str {
        match self {
            HubCode::InvalidInput => "invalid_input",
            HubCode::NotFound => "not_found",
            HubCode::Conflict => "conflict",
            HubCode::Readonly => "readonly",
            HubCode::Permission => "permission",
            HubCode::Network => "network",
            HubCode::Http => "http",
            HubCode::RateLimited => "rate_limited",
            HubCode::Internal => "internal",
        }
    }

    /// The JSON-RPC code this taxonomy maps onto. `InvalidInput` is
    /// the one clients already treat specially; everything else is
    /// a server-side condition with no reserved code.
    pub fn rpc_code(self) -> i64 {
        match self {
            HubCode::InvalidInput => crate::rpc::error_code::INVALID_PARAMS,
            _ => crate::rpc::error_code::INTERNAL_ERROR,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct HubError {
    pub code: HubCode,
    pub message: String,
}

impl HubError {
    pub fn new(code: HubCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Wrap as `anyhow::Error` for `?` plumbing; `error_payload`
    /// finds it in the chain and carries the taxonomy to the wire.
    pub fn error(code: HubCode, message: impl Into<String>) -> anyhow::Error {
        anyhow::Error::new(Self::new(code, message))
    }
}

impl From<std::io::Error> for HubError {
    fn from(e: std::io::Error) -> Self {
        let code = match e.kind() {
            std::io::ErrorKind::NotFound => HubCode::NotFound,
            std::io::ErrorKind::PermissionDenied => HubCode::Permission,
            _ => HubCode::Internal,
        };
        HubError::new(code, e.to_string())
    }
}

/// The hub's root directory: `$DAMON_SKILLS_HUB_HOME` (tests,
/// sandboxes) else `<data_dir>/skills-hub`.
pub fn hub_root(data_dir: Option<&Path>) -> PathBuf {
    if let Some(env) = targets::env_dir("DAMON_SKILLS_HUB_HOME") {
        return env;
    }
    let base = data_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(crate::config::default_data_dir);
    base.join("skills-hub")
}

// -- installed listing ----------------------------------------------------

/// Per-target sync state of one skill directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// The candidate path exists (a symlink counts — it resolves).
    Synced,
    /// A dangling symlink: the SSOT copy is gone but the target
    /// still points at it.
    Orphan,
    /// No copy in this target.
    Off,
}

impl SyncState {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncState::Synced => "synced",
            SyncState::Orphan => "orphan",
            SyncState::Off => "off",
        }
    }
}

/// Classify `dir` inside one skills root.
fn classify_in(base: &Path, dir: &str) -> SyncState {
    let Some(candidate) = fsutil::target_skill_path(base, dir) else {
        return SyncState::Off;
    };
    if candidate.exists() {
        return SyncState::Synced;
    }
    if candidate.symlink_metadata().is_ok() {
        return SyncState::Orphan; // dangling link
    }
    SyncState::Off
}

/// One scannable skills root for the listing (a test seam: the real
/// roots come from the target table, tests bring their own).
#[derive(Debug, Clone)]
pub struct TargetRoot {
    pub id: String,
    pub dir: PathBuf,
    pub visible: bool,
}

#[derive(Debug, Clone)]
pub struct ReadonlyRootRef {
    pub kind: &'static str,
    pub label: String,
    pub dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ScanRoots {
    pub targets: Vec<TargetRoot>,
    pub readonly: Vec<ReadonlyRootRef>,
}

impl ScanRoots {
    /// The real roots: every target's skills dir plus the read-only
    /// sources that exist right now.
    pub fn from_environment() -> Self {
        let targets = targets::TARGETS
            .iter()
            .filter_map(|t| {
                targets::target_skill_dir(t).map(|dir| TargetRoot {
                    id: t.id.to_string(),
                    dir,
                    visible: t.visible,
                })
            })
            .collect();
        let readonly = targets::readonly_sources()
            .into_iter()
            .map(|r| ReadonlyRootRef {
                kind: r.kind,
                label: r.label,
                dir: r.dir,
            })
            .collect();
        ScanRoots { targets, readonly }
    }
}

/// One row of `skills.installed`. Managed entries come from the
/// registry; unmanaged ones from scanning the roots. Everything is
/// camelCase on the wire.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InstalledSkill {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub directory: String,
    pub managed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readme_url: Option<String>,
    /// `owner/name@branch` for GitHub-sourced skills.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub installed_at: Option<u64>,
    /// Synced target ids.
    pub targets: Vec<String>,
    /// Every target → synced|orphan|off. `off` for a target the
    /// registry intends reports as `orphan` (a lost copy), matching
    /// what a user needs to see before re-syncing.
    pub target_states: BTreeMap<String, String>,
    /// Unmanaged only: `local` | `system` | `plugin`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<&'static str>,
    /// Unmanaged read-only hits (system/plugin): the roots they were
    /// found in, as labels.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub target_paths: Vec<String>,
    /// True when the hub refuses import/delete for this entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readonly: Option<bool>,
}

/// `skills.targets` rows.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetInfo {
    pub id: String,
    pub label: String,
    pub path: String,
    pub available: bool,
}

pub fn list_targets() -> Vec<TargetInfo> {
    targets::TARGETS
        .iter()
        .filter(|t| t.visible)
        .map(|t| TargetInfo {
            id: t.id.to_string(),
            label: t.label.to_string(),
            path: targets::target_skill_dir(t)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            available: targets::target_available(t),
        })
        .collect()
}

/// List installed skills: purge expired trash, project the registry
/// onto the real roots, then fold in unmanaged directories found in
/// target roots and read-only sources.
pub fn installed(hub: &Path) -> Vec<InstalledSkill> {
    let mut reg = registry::Registry::load(hub);
    reg.purge_expired_trash(hub, fsutil::now_ms());
    let roots = ScanRoots::from_environment();
    let mut skills = installed_with(hub, &reg, &roots);
    skills.sort_by_key(|a| a.name.to_lowercase());
    skills.truncate(MAX_INSTALLED);
    skills
}

/// The listing over caller-chosen roots — the unit-test seam.
pub fn installed_with(
    _hub: &Path,
    reg: &registry::Registry,
    roots: &ScanRoots,
) -> Vec<InstalledSkill> {
    let mut out: Vec<InstalledSkill> = reg
        .skills
        .iter()
        .filter(|s| !s.is_trashed())
        .map(|s| {
            let mut target_states = BTreeMap::new();
            let mut synced = Vec::new();
            for t in &roots.targets {
                let mut state = classify_in(&t.dir, &s.directory);
                if state == SyncState::Off && s.targets.iter().any(|id| id == &t.id) {
                    state = SyncState::Orphan; // intended but missing
                }
                if state == SyncState::Synced {
                    synced.push(t.id.clone());
                }
                target_states.insert(t.id.clone(), state.as_str().to_string());
            }
            let repo = match (&s.repo_owner, &s.repo_name) {
                (Some(o), Some(n)) => Some(match &s.repo_branch {
                    Some(b) => format!("{o}/{n}@{b}"),
                    None => format!("{o}/{n}"),
                }),
                _ => None,
            };
            InstalledSkill {
                id: s.id.clone(),
                name: s.name.clone(),
                description: s.description.clone(),
                directory: s.directory.clone(),
                managed: true,
                readme_url: s.readme_url.clone(),
                repo,
                installed_at: (s.installed_at > 0).then_some(s.installed_at),
                targets: synced,
                target_states,
                source_kind: None,
                target_paths: Vec::new(),
                readonly: None,
            }
        })
        .collect();

    // Unmanaged: directories present in the roots but not managed
    // (non-trashed) by the registry. Merged by lowercase directory
    // with source priority system > plugin > local.
    let managed: std::collections::HashSet<String> = reg
        .skills
        .iter()
        .filter(|s| !s.is_trashed())
        .map(|s| s.directory.to_lowercase())
        .collect();
    let meta_for = |dir: &Path, name: &str| {
        scan::find_skill_marker(dir)
            .map(|m| scan::read_skill_metadata(&m, name))
            .unwrap_or_else(|| scan::SkillMeta {
                name: name.to_string(),
                description: None,
            })
    };
    let mut unmanaged: BTreeMap<String, InstalledSkill> = BTreeMap::new();
    for t in &roots.targets {
        for dir in scan::scan_skill_directories(&t.dir) {
            let Some(name) = dir.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let dir_key = name.to_lowercase();
            if managed.contains(&dir_key) {
                continue;
            }
            let meta = meta_for(&dir, name);
            let entry = unmanaged.entry(dir_key).or_insert_with(|| InstalledSkill {
                id: format!("local:{name}"),
                name: meta.name.clone(),
                description: meta.description.clone(),
                directory: name.to_string(),
                managed: false,
                readme_url: None,
                repo: None,
                installed_at: None,
                targets: Vec::new(),
                target_states: BTreeMap::new(),
                source_kind: Some("local"),
                target_paths: Vec::new(),
                readonly: Some(false),
            });
            // A local find never displaces a system/plugin owner of
            // the same name; it still contributes its presence.
            entry
                .target_states
                .insert(t.id.clone(), SyncState::Synced.as_str().to_string());
            if !entry.targets.contains(&t.id) {
                entry.targets.push(t.id.clone());
            }
        }
    }
    for r in &roots.readonly {
        for dir in scan::scan_skill_directories(&r.dir) {
            let Some(name) = dir.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let dir_key = name.to_lowercase();
            if managed.contains(&dir_key) {
                continue;
            }
            let meta = meta_for(&dir, name);
            match unmanaged.get_mut(&dir_key) {
                // Read-only sources outrank a local find of the same
                // name: the entry flips readonly and gains the label.
                Some(e) => {
                    if kind_priority(r.kind) > kind_priority(e.source_kind.unwrap_or("local")) {
                        e.source_kind = Some(r.kind);
                        e.readonly = Some(true);
                    }
                    e.target_paths.push(r.label.clone());
                }
                None => {
                    unmanaged.insert(
                        dir_key,
                        InstalledSkill {
                            id: format!("local:{name}"),
                            name: meta.name,
                            description: meta.description,
                            directory: name.to_string(),
                            managed: false,
                            readme_url: None,
                            repo: None,
                            installed_at: None,
                            targets: Vec::new(),
                            target_states: BTreeMap::new(),
                            source_kind: Some(r.kind),
                            target_paths: vec![r.label.clone()],
                            readonly: Some(true),
                        },
                    );
                }
            }
        }
    }
    out.extend(unmanaged.into_values());
    out
}

fn kind_priority(kind: &str) -> u8 {
    match kind {
        "system" => 3,
        "plugin" => 2,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-mod-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_skill(dir: &Path, name: &str, description: &str) {
        let s = dir.join(name);
        std::fs::create_dir_all(&s).unwrap();
        std::fs::write(
            s.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {description}\n---\nbody"),
        )
        .unwrap();
    }

    fn roots(t1: &Path, t2: &Path) -> ScanRoots {
        ScanRoots {
            targets: vec![
                TargetRoot {
                    id: "claude".into(),
                    dir: t1.join("skills"),
                    visible: true,
                },
                TargetRoot {
                    id: "codex".into(),
                    dir: t2.join("skills"),
                    visible: true,
                },
            ],
            readonly: Vec::new(),
        }
    }

    #[test]
    fn managed_entries_report_states_and_orphan_uplift() {
        let hub = tmp("managed");
        let t1 = tmp("t1");
        let t2 = tmp("t2");
        // "pdf" synced into claude; "lost" intended everywhere, present nowhere.
        make_skill(&t1.join("skills"), "pdf", "PDF tools");
        let mut reg = registry::Registry::default();
        reg.skills.push(SkillEntry {
            id: "o/r:pdf".into(),
            name: "pdf".into(),
            directory: "pdf".into(),
            targets: vec!["claude".into(), "codex".into()],
            ..Default::default()
        });
        reg.skills.push(SkillEntry {
            id: "o/r:lost".into(),
            name: "lost".into(),
            directory: "lost".into(),
            targets: vec!["claude".into()],
            ..Default::default()
        });

        let list = installed_with(&hub, &reg, &roots(&t1, &t2));
        let pdf = list.iter().find(|s| s.directory == "pdf").unwrap();
        assert_eq!(pdf.targets, vec!["claude".to_string()]);
        assert_eq!(pdf.target_states["claude"], "synced");
        assert_eq!(pdf.target_states["codex"], "orphan", "intended but missing");
        let lost = list.iter().find(|s| s.directory == "lost").unwrap();
        assert_eq!(lost.target_states["claude"], "orphan");
        let _ = std::fs::remove_dir_all(&hub);
        let _ = std::fs::remove_dir_all(&t1);
        let _ = std::fs::remove_dir_all(&t2);
    }

    #[test]
    fn dangling_symlink_is_orphan_not_synced() {
        let hub = tmp("dangling");
        let t1 = tmp("d1");
        let t2 = tmp("d2");
        let skills = t1.join("skills");
        std::fs::create_dir_all(&skills).unwrap();
        // A managed skill whose SSOT copy vanished: the target still
        // holds a link pointing nowhere.
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            registry::managed_dir(&hub).join("ghost"),
            skills.join("ghost"),
        )
        .unwrap();
        let mut reg = registry::Registry::default();
        reg.skills.push(SkillEntry {
            id: "o/r:ghost".into(),
            name: "ghost".into(),
            directory: "ghost".into(),
            targets: vec!["claude".into()],
            ..Default::default()
        });

        let list = installed_with(&hub, &reg, &roots(&t1, &t2));
        let ghost = list.iter().find(|s| s.directory == "ghost").unwrap();
        assert_eq!(
            ghost.target_states["claude"], "orphan",
            "dangling link, not synced"
        );
        assert!(ghost.targets.is_empty());
        let _ = std::fs::remove_dir_all(&hub);
        let _ = std::fs::remove_dir_all(&t1);
        let _ = std::fs::remove_dir_all(&t2);
    }

    #[test]
    fn unmanaged_merge_prefers_readonly_and_keeps_local_states() {
        let hub = tmp("unmanaged");
        let t1 = tmp("u1");
        let t2 = tmp("u2");
        make_skill(&t1.join("skills"), "mine", "local one");
        make_skill(&t2.join("skills"), "mine", "local two");
        make_skill(&t2.join("skills"), "sys", "system one");

        let ro = tmp("ro");
        let ro_dir = ro.join("skills");
        make_skill(&ro_dir, "sys", "system one");
        let scan_roots = ScanRoots {
            targets: roots(&t1, &t2).targets,
            readonly: vec![ReadonlyRootRef {
                kind: "system",
                label: "codex:.system".into(),
                dir: ro_dir,
            }],
        };
        let reg = registry::Registry::default();
        let list = installed_with(&hub, &reg, &scan_roots);

        let mine = list.iter().find(|s| s.directory == "mine").unwrap();
        assert!(!mine.managed);
        assert_eq!(mine.source_kind, Some("local"));
        assert_eq!(mine.readonly, Some(false));
        assert_eq!(mine.targets.len(), 2, "found in both target roots");

        let sys = list.iter().find(|s| s.directory == "sys").unwrap();
        assert_eq!(
            sys.source_kind,
            Some("system"),
            "system outranks the local find"
        );
        assert_eq!(sys.readonly, Some(true));
        assert_eq!(sys.target_paths, vec!["codex:.system".to_string()]);
        let _ = std::fs::remove_dir_all(&hub);
        let _ = std::fs::remove_dir_all(&t1);
        let _ = std::fs::remove_dir_all(&t2);
        let _ = std::fs::remove_dir_all(&ro);
    }

    #[test]
    fn hub_root_honors_env_then_data_dir() {
        // Only the data-dir branch is deterministic here (env is
        // process-global); the env branch is exercised in rpc tests.
        assert_eq!(
            hub_root(Some(Path::new("/data"))),
            PathBuf::from("/data/skills-hub")
        );
    }
}
