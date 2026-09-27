//! Network discovery: what can be installed. GitHub trees enumerate
//! the SKILL.md blobs of the registered repos (frontmatter fetched
//! from raw, bounded concurrency), skills.sh powers search and the
//! popular list, and an update check re-hashes the repos installed
//! skills came from. Every network path runs behind the `Fetch`
//! trait with disk caches (`fingerprint + TTL`) so the hub stays
//! responsive and rate-limit-friendly.

use std::collections::VecDeque;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::fetch::{Fetch, fetch_tree_with_fallback, raw_url, source_signature_from_tree};
use super::fsutil;
use super::registry::{self, Registry};
use super::scan;
use super::{HubCode, HubError, ScanRoots};

/// Per-repo discovery bound — GitHub trees are huge and nobody has
/// more skills than this in one repo.
pub const DISCOVER_MAX_PER_REPO: usize = 200;
/// Concurrent raw-content fetches during discovery / popular scans.
pub const DISCOVER_CONCURRENCY: usize = 4;
/// Concurrent repo fetches during update checks — gentler, since a
/// check fans out over many repos.
pub const UPDATE_CONCURRENCY: usize = 2;
pub const DISCOVER_CACHE_TTL_MS: u64 = 60 * 60 * 1000;
pub const UPDATE_CACHE_TTL_MS: u64 = 60 * 60 * 1000;
pub const POPULAR_CACHE_TTL_MS: u64 = 6 * 60 * 60 * 1000;
/// Largest SKILL.md served whole to a client.
pub const SKILL_CONTENT_MAX_BYTES: usize = 512 * 1024;
/// The popular list merges seed queries and caps the cache here.
pub const POPULAR_CACHE_CAP: usize = 200;
const POPULAR_SEED_QUERIES: &[&str] = &[
    "agent", "code", "test", "review", "git", "web", "design", "data", "docs", "python", "api",
    "deploy",
];

// -- shared plumbing -------------------------------------------------------

/// A thread-pool map that preserves input order — the blocking-pool
/// cousin of a bounded `buffer_unordered`. `f` must be callable from
/// several threads; results align with `items` by index.
pub fn map_with_concurrency<T, R>(items: Vec<T>, limit: usize, f: impl Fn(T) -> R + Sync) -> Vec<R>
where
    T: Send,
    R: Send,
{
    let total = items.len();
    if total == 0 {
        return Vec::new();
    }
    let queue: std::sync::Mutex<VecDeque<(usize, T)>> =
        std::sync::Mutex::new(items.into_iter().enumerate().collect());
    let results: std::sync::Mutex<Vec<std::mem::MaybeUninit<R>>> = std::sync::Mutex::new(
        (0..total)
            .map(|_| std::mem::MaybeUninit::uninit())
            .collect(),
    );
    let workers = limit.clamp(1, total);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let next = queue.lock().unwrap().pop_front();
                    let Some((idx, item)) = next else { break };
                    let value = f(item);
                    // MaybeUninit::write is safe; each index is written
                    // exactly once by the thread that dequeued it.
                    results.lock().unwrap()[idx].write(value);
                }
            });
        }
    });
    // Safety: every slot was written before the scope returned.
    unsafe {
        results
            .into_inner()
            .unwrap()
            .into_iter()
            .map(|slot| slot.assume_init())
            .collect()
    }
}

/// Is this tree path a skill marker (case-insensitive)?
fn is_skill_md_path(path: &str) -> bool {
    Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.eq_ignore_ascii_case("skill.md"))
}

fn now_ms() -> u64 {
    fsutil::now_ms()
}

/// Cache freshness: file parses, fingerprint matches, and the TTL
/// has not elapsed.
fn cache_fresh(path: &Path, ttl_ms: u64, fingerprint: &str) -> bool {
    let Ok(text) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return false;
    };
    v.get("fingerprint").and_then(|f| f.as_str()) == Some(fingerprint)
        && v.get("generatedAt")
            .and_then(|t| t.as_u64())
            .is_some_and(|t| now_ms().saturating_sub(t) < ttl_ms)
}

// -- repo discovery --------------------------------------------------------

/// One installable skill found in a registered repo. `directory` is
/// the source path inside the repo — the exact `directory` an
/// install takes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverSkill {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub directory: String,
    #[serde(default)]
    pub readme_url: Option<String>,
    pub repo_owner: String,
    pub repo_name: String,
    pub repo_branch: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverOutcome {
    pub skills: Vec<DiscoverSkill>,
    pub cached: bool,
    pub generated_at: u64,
}

fn repos_fingerprint(repos: &[super::RepoEntry]) -> String {
    let mut specs: Vec<String> = repos
        .iter()
        .map(|r| format!("{}@{}", r.slug().to_lowercase(), r.branch))
        .collect();
    specs.sort();
    specs.join("|")
}

/// Discover skills across the enabled repos. Empty repo sets short
/// -circuit; a full failure that yields nothing raises the
/// rate-limit error instead of an empty success.
pub fn discover_skills(
    hub: &Path,
    fetch: &dyn Fetch,
    repos: Vec<super::RepoEntry>,
    force: bool,
) -> Result<DiscoverOutcome, HubError> {
    let enabled: Vec<_> = repos.into_iter().filter(|r| r.enabled).collect();
    let fp = repos_fingerprint(&enabled);
    let cache = registry::cache_path(hub, "discover-cache");
    if !force
        && cache_fresh(&cache, DISCOVER_CACHE_TTL_MS, &fp)
        && let Ok(v) = std::fs::read_to_string(&cache)
        && let Ok(outcome) = serde_json::from_str::<DiscoverCache>(&v)
    {
        return Ok(DiscoverOutcome {
            skills: outcome.skills,
            cached: true,
            generated_at: outcome.generated_at,
        });
    }
    if enabled.is_empty() {
        let generated_at = now_ms();
        write_discover_cache(&cache, &fp, &[], generated_at);
        return Ok(DiscoverOutcome {
            skills: Vec::new(),
            cached: false,
            generated_at,
        });
    }

    // Per repo: tree → SKILL.md paths (capped) → metadata, with all
    // repos scanned concurrently under the discovery bound.
    let any_rate_limited = std::sync::atomic::AtomicBool::new(false);
    let per_repo: Vec<Vec<DiscoverSkill>> =
        map_with_concurrency(enabled, 2, |repo| -> Vec<DiscoverSkill> {
            let Ok((tree, branch)) =
                fetch_tree_with_fallback(fetch, &repo.owner, &repo.name, &repo.branch)
            else {
                return Vec::new();
            };
            let mut paths: Vec<String> = tree["tree"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter(|e| e["type"] == "blob")
                        .filter_map(|e| e["path"].as_str())
                        .filter(|p| is_skill_md_path(p))
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            paths.sort();
            paths.truncate(DISCOVER_MAX_PER_REPO);
            map_with_concurrency(paths, DISCOVER_CONCURRENCY, |path| {
                let dir = parent_of(&path).unwrap_or_else(|| ".".into());
                let fallback = leaf_of(&path);
                let meta = match fetch.get_text(&raw_url(&repo.owner, &repo.name, &branch, &path)) {
                    Ok(text) => scan::parse_frontmatter(&text).map(|m| scan::SkillMeta {
                        name: m.get("name").cloned().unwrap_or_else(|| fallback.clone()),
                        description: m.get("description").cloned(),
                    }),
                    // A failed fetch keeps the entry discoverable by
                    // its directory name; a rate limit is remembered so
                    // an empty result reports honestly instead of
                    // pretending the repos hold nothing.
                    Err(super::fetch::FetchError::RateLimited { .. }) => {
                        any_rate_limited.store(true, std::sync::atomic::Ordering::Relaxed);
                        None
                    }
                    Err(_) => None,
                };
                let (name, description) = match meta {
                    Some(m) => (m.name, m.description),
                    None => (fallback, None),
                };
                DiscoverSkill {
                    key: format!("{}:{dir}", repo.slug()),
                    name,
                    description,
                    directory: dir,
                    readme_url: Some(format!(
                        "https://github.com/{}/{}/blob/{}/{}",
                        repo.owner, repo.name, branch, path
                    )),
                    repo_owner: repo.owner.clone(),
                    repo_name: repo.name.clone(),
                    repo_branch: branch.clone(),
                }
            })
        });

    // Merge by lowercase key, first position wins, sort by name.
    let mut merged: Vec<DiscoverSkill> = Vec::new();
    for repo_skills in per_repo {
        for skill in repo_skills {
            match merged
                .iter()
                .position(|s| s.key.to_lowercase() == skill.key.to_lowercase())
            {
                Some(idx) => merged[idx] = skill,
                None => merged.push(skill),
            }
        }
    }
    merged.sort_by_key(|a| a.name.to_lowercase());
    if merged.is_empty() && any_rate_limited.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(HubError::new(
            HubCode::RateLimited,
            "discovery was rate-limited and found nothing — try again later",
        ));
    }
    let generated_at = now_ms();
    write_discover_cache(&cache, &fp, &merged, generated_at);
    Ok(DiscoverOutcome {
        skills: merged,
        cached: false,
        generated_at,
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiscoverCache {
    fingerprint: String,
    generated_at: u64,
    skills: Vec<DiscoverSkill>,
}

fn write_discover_cache(path: &Path, fp: &str, skills: &[DiscoverSkill], generated_at: u64) {
    let _ = fsutil::write_json_private(
        path,
        &DiscoverCache {
            fingerprint: fp.to_string(),
            generated_at,
            skills: skills.to_vec(),
        },
    );
}

fn parent_of(path: &str) -> Option<String> {
    Path::new(path)
        .parent()?
        .to_str()
        .map(String::from)
        .filter(|p| !p.is_empty())
}

fn leaf_of(path: &str) -> String {
    Path::new(path)
        .parent()
        .and_then(Path::file_name)
        .and_then(|n| n.to_str())
        .unwrap_or("skill")
        .to_string()
}

// -- skills.sh search & popular -------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchSkill {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub directory: String,
    #[serde(default)]
    pub installs: Option<u64>,
    pub repo_owner: String,
    pub repo_name: String,
    pub repo_branch: String,
}

/// UTF-16 length — the JS-side short-circuit, kept so CJK/emoji
/// queries behave identically upstream and here.
fn utf16_len(s: &str) -> usize {
    s.chars().map(char::len_utf16).sum()
}

/// Search skills.sh. Sub-2-char queries (UTF-16) return empty.
pub fn search_skills(
    fetch: &dyn Fetch,
    q: &str,
    limit: u64,
    offset: u64,
) -> Result<(u64, Vec<SearchSkill>), HubError> {
    let q = q.trim();
    if utf16_len(q) < 2 {
        return Ok((0, Vec::new()));
    }
    let limit = limit.clamp(1, 50);
    let url = format!(
        "https://skills.sh/api/search?q={}&limit={limit}&offset={offset}",
        form_encode(q)
    );
    let value = fetch.get_json(&url).map_err(HubError::from)?;
    let entries = value["results"]
        .as_array()
        .or_else(|| value["data"].as_array())
        .or_else(|| value["skills"].as_array())
        .cloned()
        .unwrap_or_default();
    let mut skills = Vec::new();
    for e in entries {
        let Some(source) = e["source"].as_str().map(String::from) else {
            continue;
        };
        let Some((owner, name)) = source.split_once('/') else {
            continue;
        };
        // A dot in either side is a host, not a repo slug.
        if owner.contains('.') || name.contains('.') {
            continue;
        }
        let skill_name = e["name"].as_str().unwrap_or_default().to_string();
        let skill_id = e["skillId"].as_str().map(String::from);
        let id = e["id"].as_str().map(String::from);
        let directory = skill_id.clone().unwrap_or_else(|| skill_name.clone());
        let key = id.unwrap_or_else(|| format!("{owner}/{name}:{directory}"));
        skills.push(SearchSkill {
            key,
            name: skill_name,
            description: e["description"].as_str().map(String::from),
            directory,
            installs: e["installs"].as_u64(),
            repo_owner: owner.to_string(),
            repo_name: name.to_string(),
            repo_branch: "main".into(),
        });
    }
    let total = value["total"]
        .as_u64()
        .or(value["totalCount"].as_u64())
        .unwrap_or(skills.len() as u64);
    skills.truncate(limit as usize);
    Ok((total, skills))
}

fn form_encode(v: &str) -> String {
    let mut out = String::new();
    for b in v.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(b))
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PopularOutcome {
    pub skills: Vec<SearchSkill>,
    pub cached: bool,
    pub generated_at: u64,
}

/// The popular list: seeded searches merged by install count, cached
/// for six hours. The response is a slice of the cache.
pub fn popular_skills(
    hub: &Path,
    fetch: &dyn Fetch,
    limit: u64,
    force: bool,
) -> Result<PopularOutcome, HubError> {
    let cache = registry::cache_path(hub, "popular-cache");
    if !force
        && cache_fresh(&cache, POPULAR_CACHE_TTL_MS, "popular")
        && let Ok(v) = std::fs::read_to_string(&cache)
        && let Ok(cached) = serde_json::from_str::<PopularCache>(&v)
    {
        let limit = limit.clamp(1, POPULAR_CACHE_CAP as u64) as usize;
        return Ok(PopularOutcome {
            skills: cached.skills.into_iter().take(limit).collect(),
            cached: true,
            generated_at: cached.generated_at,
        });
    }
    let queries: Vec<&str> = POPULAR_SEED_QUERIES.to_vec();
    let per_query: Vec<Vec<SearchSkill>> =
        map_with_concurrency(queries, DISCOVER_CONCURRENCY, |q| {
            search_skills(fetch, q, 30, 0)
                .map(|(_, s)| s)
                .unwrap_or_default()
        });
    // Merge by key keeping the larger install count; sort desc.
    let mut merged: Vec<SearchSkill> = Vec::new();
    for batch in per_query {
        for skill in batch {
            match merged
                .iter_mut()
                .find(|s| s.key.to_lowercase() == skill.key.to_lowercase())
            {
                Some(existing) => {
                    if skill.installs.unwrap_or(0) > existing.installs.unwrap_or(0) {
                        *existing = skill;
                    }
                }
                None => merged.push(skill),
            }
        }
    }
    merged.sort_by_key(|s| std::cmp::Reverse(s.installs.unwrap_or(0)));
    merged.truncate(POPULAR_CACHE_CAP);
    let generated_at = now_ms();
    let _ = fsutil::write_json_private(
        &cache,
        &PopularCache {
            fingerprint: "popular".into(),
            generated_at,
            skills: merged.clone(),
        },
    );
    let limit = limit.clamp(1, POPULAR_CACHE_CAP as u64) as usize;
    Ok(PopularOutcome {
        skills: merged.into_iter().take(limit).collect(),
        cached: false,
        generated_at,
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PopularCache {
    fingerprint: String,
    generated_at: u64,
    skills: Vec<SearchSkill>,
}

// -- update check ----------------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatesOutcome {
    pub updates: std::collections::BTreeMap<String, bool>,
    pub checked_at: u64,
    pub cached: bool,
}

/// Compare each managed repo-sourced skill's recorded
/// `source_signature` against the repo's current tree. Repo failures
/// that are not rate limits skip that repo silently — one broken
/// repo must not blank the whole check.
pub fn check_updates(
    hub: &Path,
    fetch: &dyn Fetch,
    force: bool,
) -> Result<UpdatesOutcome, HubError> {
    let reg = Registry::load(hub);
    let tracked: Vec<(String, String, String, String, String)> = reg // (id, owner, name, branch, source_dir + signature)
        .skills
        .iter()
        .filter(|s| !s.is_trashed() && s.repo_owner.is_some() && s.source_signature.is_some())
        .filter_map(|s| {
            Some((
                s.id.clone(),
                s.repo_owner.clone()?,
                s.repo_name.clone()?,
                s.repo_branch.clone().unwrap_or_else(|| "main".into()),
                format!(
                    "{}@{}",
                    s.source_directory.clone().unwrap_or_default(),
                    s.source_signature.clone()?
                ),
            ))
        })
        .collect();
    let mut fp: Vec<String> = tracked
        .iter()
        .map(|(id, _, _, _, sig)| format!("{id}@{sig}"))
        .collect();
    fp.sort();
    let fp = fp.join("|");
    let cache = registry::cache_path(hub, "updates-cache");
    if !force
        && cache_fresh(&cache, UPDATE_CACHE_TTL_MS, &fp)
        && let Ok(v) = std::fs::read_to_string(&cache)
        && let Ok(cached) = serde_json::from_str::<UpdatesCache>(&v)
    {
        return Ok(UpdatesOutcome {
            updates: cached.updates,
            checked_at: cached.checked_at,
            cached: true,
        });
    }
    if tracked.is_empty() {
        return Ok(UpdatesOutcome {
            updates: std::collections::BTreeMap::new(),
            checked_at: now_ms(),
            cached: false,
        });
    }

    // Group by repo so each repo's tree is fetched once.
    let mut groups: Vec<(String, String, String, Vec<usize>)> = Vec::new(); // owner, name, branch, indexes
    for (i, (_, owner, name, branch, _)) in tracked.iter().enumerate() {
        let group_key = format!("{owner}/{name}@{branch}").to_lowercase();
        match groups
            .iter_mut()
            .find(|(o, n, b, _)| format!("{o}/{n}@{b}").to_lowercase() == group_key)
        {
            Some((_, _, _, idxs)) => idxs.push(i),
            None => groups.push((owner.clone(), name.clone(), branch.clone(), vec![i])),
        }
    }
    let updates = std::sync::Mutex::new(std::collections::BTreeMap::<String, bool>::new());
    map_with_concurrency(groups, UPDATE_CONCURRENCY, |(owner, name, branch, idxs)| {
        let Ok((tree, _)) = fetch_tree_with_fallback(fetch, &owner, &name, &branch) else {
            return;
        };
        let mut map = updates.lock().unwrap();
        for i in idxs {
            let (id, _, _, _, dir_sig) = &tracked[i];
            let Some((dir, sig)) = dir_sig.split_once('@') else {
                continue;
            };
            let current = source_signature_from_tree(&tree, dir);
            map.insert(id.clone(), current.as_deref() != Some(sig));
        }
    });
    let updates = updates.into_inner().unwrap();
    let checked_at = now_ms();
    let _ = fsutil::write_json_private(
        &cache,
        &UpdatesCache {
            fingerprint: fp,
            checked_at,
            updates: updates.clone(),
        },
    );
    Ok(UpdatesOutcome {
        updates,
        checked_at,
        cached: false,
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdatesCache {
    fingerprint: String,
    checked_at: u64,
    updates: std::collections::BTreeMap<String, bool>,
}

// -- content viewers -------------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillContent {
    pub directory: String,
    pub path: String,
    pub markdown: String,
    pub truncated: bool,
}

/// Read a locally-present skill's SKILL.md: the managed store first,
/// then the target roots, then read-only sources. 512 KiB cap.
pub fn skill_content(
    hub: &Path,
    roots: &ScanRoots,
    directory: &str,
) -> Result<SkillContent, HubError> {
    let dir = fsutil::sanitize_relative_path(directory).ok_or_else(|| {
        HubError::new(
            HubCode::InvalidInput,
            format!("bad directory {directory:?}"),
        )
    })?;
    let mut candidates: Vec<(String, std::path::PathBuf)> = vec![(
        "managed".into(),
        registry::managed_dir(hub).join(&dir).join("SKILL.md"),
    )];
    for t in &roots.targets {
        candidates.push((t.id.clone(), t.dir.join(&dir).join("SKILL.md")));
    }
    for r in &roots.readonly {
        candidates.push((r.label.clone(), r.dir.join(&dir).join("SKILL.md")));
    }
    for (label, path) in candidates {
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let (markdown, truncated) = cap_markdown(bytes);
        return Ok(SkillContent {
            directory: dir,
            path: format!("{label}:{}", path.display()),
            markdown,
            truncated,
        });
    }
    Err(HubError::new(
        HubCode::NotFound,
        format!("no SKILL.md for {directory:?}"),
    ))
}

fn cap_markdown(bytes: Vec<u8>) -> (String, bool) {
    let truncated = bytes.len() > SKILL_CONTENT_MAX_BYTES;
    let slice = &bytes[..bytes.len().min(SKILL_CONTENT_MAX_BYTES)];
    (String::from_utf8_lossy(slice).into_owned(), truncated)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteSkillContent {
    pub name: String,
    pub description: Option<String>,
    pub directory: String,
    pub markdown: String,
    pub truncated: bool,
    pub branch: String,
}

/// Fetch a remote skill's SKILL.md. The skills.sh-style id is
/// matched against the repo tree by directory-name score: exact (3),
/// prefix-stripped (2), ties to the shallowest path.
pub fn remote_skill_content(
    fetch: &dyn Fetch,
    owner: &str,
    name: &str,
    branch: &str,
    directory: &str,
) -> Result<RemoteSkillContent, HubError> {
    let normalized = directory.to_lowercase().replace(':', "-");
    let (tree, branch) = fetch_tree_with_fallback(fetch, owner, name, branch)?;
    let mut skill_paths: Vec<String> = tree["tree"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|e| e["type"] == "blob")
                .filter_map(|e| e["path"].as_str())
                .filter(|p| is_skill_md_path(p))
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    skill_paths.sort();

    let score = |dir: &str| -> Option<i32> {
        let dir = dir.to_lowercase();
        if dir == normalized {
            return Some(3);
        }
        // One side is the other minus a `-`-joined prefix
        // (`vercel-react-best-practices` vs `react-best-practices`).
        let strip_prefix = |a: &str, b: &str| {
            a.split_once('-')
                .is_some_and(|(_, rest)| rest.trim_start_matches('-') == b)
        };
        if strip_prefix(&dir, &normalized) || strip_prefix(&normalized, &dir) {
            return Some(2);
        }
        None
    };
    let best = skill_paths
        .iter()
        .filter_map(|p| {
            let dir = parent_of(p)?;
            score(&dir).map(|s| (s, dir.split('/').count(), p.clone()))
        })
        .min_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
    let path = match best {
        Some((_, _, p)) => p,
        None => {
            // A repo whose root itself is one skill.
            if skill_paths.len() == 1 && parent_of(&skill_paths[0]).is_none() {
                skill_paths.swap_remove(0)
            } else {
                return Err(HubError::new(
                    HubCode::NotFound,
                    format!("{directory:?} not in {owner}/{name}"),
                ));
            }
        }
    };
    let directory = parent_of(&path).unwrap_or_else(|| ".".into());
    let bytes = fetch
        .get_bytes(&raw_url(owner, name, &branch, &path))
        .map_err(HubError::from)?;
    let (markdown, truncated) = cap_markdown(bytes);
    let meta = scan::parse_frontmatter(&markdown)
        .map(|m| scan::SkillMeta {
            name: m.get("name").cloned().unwrap_or_else(|| leaf_of(&path)),
            description: m.get("description").cloned(),
        })
        .unwrap_or_else(|| scan::SkillMeta {
            name: leaf_of(&path),
            description: None,
        });
    Ok(RemoteSkillContent {
        name: meta.name,
        description: meta.description,
        directory,
        markdown,
        truncated,
        branch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skills_hub::fetch::LocalFileFetch;

    fn tmp(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "damon-hub-disc-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn repo_fixture(root: &Path) {
        // Tree for o/r@main with two skills.
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
                {"type": "blob", "path": "skills/xlsx/SKILL.md", "sha": "ccc"},
                {"type": "blob", "path": "README.md", "sha": "ddd"},
            ]})
            .to_string(),
        )
        .unwrap();
        for skill in ["pdf", "xlsx"] {
            let dir = root
                .join("raw.githubusercontent.com")
                .join("o")
                .join("r")
                .join("main")
                .join("skills")
                .join(skill);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("SKILL.md"),
                format!("---\nname: {skill}-tools\ndescription: The {skill} skill\n---\nbody"),
            )
            .unwrap();
        }
    }

    #[test]
    fn discover_lists_repo_skills_and_caches() {
        let root = tmp("discover");
        repo_fixture(&root);
        let fetch = LocalFileFetch { root: root.clone() };
        let hub = root.join("hub");
        std::fs::create_dir_all(&hub).unwrap();

        let out = discover_skills(
            &hub,
            &fetch,
            vec![super::super::RepoEntry {
                owner: "o".into(),
                name: "r".into(),
                branch: "main".into(),
                enabled: true,
            }],
            false,
        )
        .unwrap();
        assert!(!out.cached);
        assert_eq!(out.skills.len(), 2);
        let pdf = out
            .skills
            .iter()
            .find(|s| s.directory == "skills/pdf")
            .unwrap();
        assert_eq!(pdf.name, "pdf-tools");
        assert_eq!(pdf.key, "o/r:skills/pdf");
        assert_eq!(pdf.description.as_deref(), Some("The pdf skill"));

        // Second call is cached.
        let cached = discover_skills(
            &hub,
            &fetch,
            vec![super::super::RepoEntry {
                owner: "o".into(),
                name: "r".into(),
                branch: "main".into(),
                enabled: true,
            }],
            false,
        )
        .unwrap();
        assert!(cached.cached);
        assert_eq!(cached.skills.len(), 2);

        // A changed fingerprint invalidates.
        let mut repo2 = super::super::RepoEntry {
            owner: "o".into(),
            name: "r".into(),
            branch: "main".into(),
            enabled: true,
        };
        repo2.branch = "dev".into();
        let fresh = discover_skills(&hub, &fetch, vec![repo2], false).unwrap();
        assert!(!fresh.cached, "different repos miss the cache");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn search_parses_skills_sh_shapes_and_guards() {
        let root = tmp("search");
        let api = root.join("skills.sh").join("api").join("search");
        std::fs::create_dir_all(api.parent().unwrap()).unwrap();
        std::fs::write(
            &api,
            serde_json::json!({"total": 2, "results": [
                {"source": "anthropics/skills", "name": "pdf", "skillId": "pdf", "installs": 42},
                {"source": "evil.example.com/x", "name": "bad"},
            ]})
            .to_string(),
        )
        .unwrap();
        let fetch = LocalFileFetch { root: root.clone() };
        let (total, skills) = search_skills(&fetch, "pdf tool", 20, 0).unwrap();
        assert_eq!(total, 2);
        assert_eq!(skills.len(), 1, "host-looking sources dropped");
        assert_eq!(skills[0].key, "anthropics/skills:pdf");
        assert_eq!(skills[0].installs, Some(42));

        // Short queries short-circuit without touching the fixture.
        assert!(search_skills(&fetch, "p", 20, 0).unwrap().1.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn map_with_concurrency_preserves_order() {
        let out = map_with_concurrency((0..50).collect::<Vec<u32>>(), 4, |n| n * 2);
        assert_eq!(out, (0..50).map(|n| n * 2).collect::<Vec<u32>>());
        assert!(map_with_concurrency(Vec::<u32>::new(), 4, |n| n).is_empty());
    }

    #[test]
    fn remote_content_scores_directories() {
        let root = tmp("remote");
        repo_fixture(&root);
        // A longer-named sibling to exercise prefix scoring.
        let tree_path = root
            .join("api.github.com")
            .join("repos")
            .join("o")
            .join("r")
            .join("git")
            .join("trees")
            .join("main");
        let mut tree: Value =
            serde_json::from_str(&std::fs::read_to_string(&tree_path).unwrap()).unwrap();
        tree["tree"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "type": "blob", "path": "vercel-react-best-practices/SKILL.md", "sha": "eee"
            }));
        std::fs::write(&tree_path, tree.to_string()).unwrap();
        let dir = root
            .join("raw.githubusercontent.com")
            .join("o")
            .join("r")
            .join("main")
            .join("vercel-react-best-practices");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("SKILL.md"), "---\nname: react\n---\nreact body").unwrap();

        let fetch = LocalFileFetch { root: root.clone() };
        let out = remote_skill_content(&fetch, "o", "r", "main", "react-best-practices").unwrap();
        assert_eq!(
            out.directory, "vercel-react-best-practices",
            "prefix score wins"
        );
        assert!(out.markdown.contains("react body"));
        let direct = remote_skill_content(&fetch, "o", "r", "main", "skills-pdf").unwrap_err();
        // "skills-pdf" is not an exact or prefix match for either dir.
        assert_eq!(direct.code, HubCode::NotFound);
        let _ = std::fs::remove_dir_all(&root);
    }
}
