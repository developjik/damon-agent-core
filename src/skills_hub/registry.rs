//! The hub's on-disk state: `registry.json` (repos + installed
//! skills), the trash ledger, and the activity log. Deliberately a
//! plain JSON file rather than a store table — it travels with the
//! `managed/` directory it describes, and a corrupt file degrades to
//! defaults instead of poisoning the daemon.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::fsutil;

/// How long an uninstalled skill stays restorable before its trash
/// copy is purged.
pub const TRASH_TTL_MS: u64 = 5 * 60 * 1000;

/// Activity log bounds: trim to the newest 500 lines once the file
/// passes 256 KiB.
const ACTIVITY_MAX_LINES: usize = 500;
const ACTIVITY_TRIM_BYTES: u64 = 256 * 1024;

/// One discoverable source repo.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RepoEntry {
    pub owner: String,
    pub name: String,
    #[serde(default = "default_branch")]
    pub branch: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_branch() -> String {
    "main".into()
}

fn default_true() -> bool {
    true
}

impl RepoEntry {
    /// `owner/name` — the identity used for dedup and removal.
    pub fn slug(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

/// One installed (or trashed) skill in the registry. GitHub-sourced
/// entries carry the repo coordinates; `local:` imports carry the
/// `source_path` the sync must never touch.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SkillEntry {
    /// `owner/name:sourceDir` (GitHub) or `local:<dir>`.
    pub id: String,
    #[serde(default)]
    pub key: Option<String>,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Flat install name under `managed/`.
    pub directory: String,
    #[serde(default)]
    pub source_directory: Option<String>,
    #[serde(default)]
    pub readme_url: Option<String>,
    #[serde(default)]
    pub repo_owner: Option<String>,
    #[serde(default)]
    pub repo_name: Option<String>,
    #[serde(default)]
    pub repo_branch: Option<String>,
    #[serde(default)]
    pub installed_at: u64,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub source_signature: Option<String>,
    #[serde(default)]
    pub targets: Vec<String>,
    /// Local imports only: the user's own directory, preserved on
    /// every sync and removal.
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub trashed_at: Option<u64>,
    #[serde(default)]
    pub trashed_directory: Option<String>,
    #[serde(default)]
    pub previous_targets: Option<Vec<String>>,
}

impl SkillEntry {
    pub fn is_trashed(&self) -> bool {
        self.trashed_at.is_some()
    }
}

/// The seed repos a fresh registry starts from — the same defaults
/// the upstream manager ships, all enabled.
pub fn default_repos() -> Vec<RepoEntry> {
    [
        ("anthropics", "skills", "main"),
        ("ComposioHQ", "awesome-claude-skills", "master"),
        ("cexll", "myclaude", "master"),
        ("JimLiu", "baoyu-skills", "main"),
    ]
    .into_iter()
    .map(|(owner, name, branch)| RepoEntry {
        owner: owner.into(),
        name: name.into(),
        branch: branch.into(),
        enabled: true,
    })
    .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    #[serde(default = "default_repos")]
    pub repos: Vec<RepoEntry>,
    #[serde(default)]
    pub skills: Vec<SkillEntry>,
}

impl Default for Registry {
    /// A fresh registry starts from the seed repos — derived
    /// `Default` would hand back an empty list and silently disable
    /// discovery.
    fn default() -> Self {
        Registry {
            repos: default_repos(),
            skills: Vec::new(),
        }
    }
}

// -- hub directory layout ------------------------------------------------

pub fn registry_path(hub: &Path) -> PathBuf {
    hub.join("registry.json")
}

pub fn managed_dir(hub: &Path) -> PathBuf {
    hub.join("managed")
}

pub fn trash_dir(hub: &Path) -> PathBuf {
    hub.join(".trash")
}

pub fn tmp_dir(hub: &Path) -> PathBuf {
    hub.join("tmp")
}

pub fn activity_path(hub: &Path) -> PathBuf {
    hub.join("activity.jsonl")
}

/// A cache file (`discover-cache.json`, `popular-cache.json`, …).
pub fn cache_path(hub: &Path, name: &str) -> PathBuf {
    hub.join(format!("{name}.json"))
}

impl Registry {
    /// Load with degradation instead of failure: a missing,
    /// unparseable, or wrong-shaped file yields defaults (seed
    /// repos, no skills). The registry is derived state — the
    /// `managed/` directory is the real source of truth, and a
    /// hand-mangled registry must not take the hub down.
    pub fn load(hub: &Path) -> Registry {
        let Ok(text) = std::fs::read_to_string(registry_path(hub)) else {
            return Registry::default();
        };
        let Value::Object(mut root) = text.parse().unwrap_or(Value::Null) else {
            return Registry::default();
        };
        let repos = match root.remove("repos") {
            Some(Value::Array(items)) => items
                .into_iter()
                .filter_map(|v| serde_json::from_value::<RepoEntry>(v).ok())
                .collect(),
            _ => default_repos(),
        };
        let skills = match root.remove("skills") {
            Some(Value::Array(items)) => items
                .into_iter()
                .filter_map(|v| serde_json::from_value::<SkillEntry>(v).ok())
                .collect(),
            _ => Vec::new(),
        };
        Registry { repos, skills }
    }

    pub fn save(&self, hub: &Path) -> std::io::Result<()> {
        fsutil::write_json_private(&registry_path(hub), &self)
    }

    /// Find a skill by id or key — both are accepted as the mutation
    /// handle because discovery listings key one way and installs
    /// another.
    pub fn find(&self, id_or_key: &str) -> Option<&SkillEntry> {
        self.skills
            .iter()
            .find(|s| s.id == id_or_key || s.key.as_deref() == Some(id_or_key))
    }

    pub fn find_mut(&mut self, id_or_key: &str) -> Option<&mut SkillEntry> {
        self.skills
            .iter_mut()
            .find(|s| s.id == id_or_key || s.key.as_deref() == Some(id_or_key))
    }

    /// Drop expired trash: delete the trash copy, drop the entry.
    /// Best-effort by design — a purge failure never fails the read
    /// that triggered it. Returns the purged ids.
    pub fn purge_expired_trash(&mut self, hub: &Path, now: u64) -> Vec<String> {
        let mut purged = Vec::new();
        self.skills.retain(|s| {
            let Some(trashed_at) = s.trashed_at else {
                return true;
            };
            if now.saturating_sub(trashed_at) < TRASH_TTL_MS {
                return true;
            }
            if let Some(dir) = &s.trashed_directory {
                fsutil::remove_path(&trash_dir(hub).join(dir));
            }
            purged.push(s.id.clone());
            false
        });
        purged
    }
}

// -- activity log ---------------------------------------------------------

/// Append one activity event (`{ts, kind, ...}`). Trims the file to
/// the newest [`ACTIVITY_MAX_LINES`] lines once it passes the byte
/// bound. Never fails the caller — the log is diagnostic.
pub fn append_activity(hub: &Path, mut event: Value) {
    let path = activity_path(hub);
    if !event.as_object().is_some_and(|o| o.contains_key("ts")) {
        event["ts"] = fsutil::now_ms().into();
    }
    let mut line = event.to_string();
    line.push('\n');
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| {
            use std::io::Write as _;
            f.write_all(line.as_bytes())
        })
        .is_err()
    {
        return;
    }
    // Trim only when the file has grown past the bound.
    if std::fs::metadata(&path).is_ok_and(|m| m.len() > ACTIVITY_TRIM_BYTES) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            return;
        };
        let lines: Vec<&str> = text.lines().collect();
        let keep = lines.len().saturating_sub(ACTIVITY_MAX_LINES)..lines.len();
        let trimmed: String = lines[keep].iter().fold(String::new(), |mut acc, l| {
            acc.push_str(l);
            acc.push('\n');
            acc
        });
        let _ = std::fs::write(&path, trimmed.as_bytes());
    }
}

/// Read activity newest-first, capped.
pub fn read_activity(hub: &Path, limit: usize) -> Vec<Value> {
    let limit = limit.clamp(1, ACTIVITY_MAX_LINES.max(100));
    let Ok(text) = std::fs::read_to_string(activity_path(hub)) else {
        return Vec::new();
    };
    let mut events: Vec<Value> = text
        .lines()
        .filter_map(|l| l.parse::<Value>().ok())
        .collect();
    events.reverse();
    events.truncate(limit);
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_hub(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-reg-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn skill(id: &str, directory: &str) -> SkillEntry {
        SkillEntry {
            id: id.into(),
            name: directory.into(),
            directory: directory.into(),
            ..Default::default()
        }
    }

    #[test]
    fn round_trips_through_disk() {
        let hub = tmp_hub("rt");
        let mut reg = Registry::default();
        reg.repos[0].branch = "dev".into();
        reg.skills.push(skill("a/b:pdf", "pdf"));
        reg.save(&hub).unwrap();

        let back = Registry::load(&hub);
        assert_eq!(back.repos[0].branch, "dev");
        assert_eq!(back.skills.len(), 1);
        assert_eq!(back.find("a/b:pdf").unwrap().directory, "pdf");
        assert!(back.find("missing").is_none());
        let _ = std::fs::remove_dir_all(&hub);
    }

    #[test]
    fn corrupt_or_wrong_shaped_files_degrade_to_defaults() {
        let hub = tmp_hub("corrupt");
        for body in ["not json", "[1,2,3]", "{\"repos\": 5, \"skills\": \"x\"}"] {
            std::fs::write(registry_path(&hub), body).unwrap();
            let reg = Registry::load(&hub);
            assert_eq!(reg.repos, default_repos(), "body: {body}");
            assert!(reg.skills.is_empty(), "body: {body}");
        }
        // Missing file too.
        let _ = std::fs::remove_file(registry_path(&hub));
        assert_eq!(Registry::load(&hub).repos, default_repos());
        let _ = std::fs::remove_dir_all(&hub);
    }

    #[test]
    fn trash_purges_only_past_ttl_and_deletes_the_copy() {
        let hub = tmp_hub("trash");
        let now = fsutil::now_ms();
        let trash_name = format!("{}-{}", fsutil::base64url_no_pad("pdf"), now);
        let trash_copy = trash_dir(&hub).join(&trash_name);
        std::fs::create_dir_all(&trash_copy).unwrap();

        let mut reg = Registry::default();
        reg.skills.push(SkillEntry {
            id: "local:pdf".into(),
            name: "pdf".into(),
            directory: "pdf".into(),
            trashed_at: Some(now - TRASH_TTL_MS - 1),
            trashed_directory: Some(trash_name.clone()),
            ..Default::default()
        });
        reg.skills.push(SkillEntry {
            id: "local:fresh".into(),
            name: "fresh".into(),
            directory: "fresh".into(),
            trashed_at: Some(now), // just trashed — survives
            ..Default::default()
        });
        let purged = reg.purge_expired_trash(&hub, now);
        assert_eq!(purged, vec!["local:pdf".to_string()]);
        assert!(!trash_copy.exists());
        assert_eq!(reg.skills.len(), 1);
        assert_eq!(reg.skills[0].id, "local:fresh");
        let _ = std::fs::remove_dir_all(&hub);
    }

    #[test]
    fn activity_appends_trims_and_reads_newest_first() {
        let hub = tmp_hub("act");
        for i in 0..5 {
            append_activity(&hub, serde_json::json!({"kind": "install", "i": i}));
        }
        let events = read_activity(&hub, 3);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0]["i"], 4, "newest first");
        assert!(events[0]["ts"].as_u64().is_some(), "ts is stamped");
        let _ = std::fs::remove_dir_all(&hub);
    }
}
