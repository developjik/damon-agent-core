//! The fetch layer: everything the hub downloads funnels through one
//! trait so installs and discovery are testable against a local
//! filesystem fixture instead of the network. The production impl is
//! a blocking reqwest client — hub operations already run on the
//! blocking pool, and a sync trait stays object-safe.

use std::path::PathBuf;
use std::sync::OnceLock;

use serde_json::Value;

use super::{HubCode, HubError};

/// Request timeout for every hub fetch — a stalled download must not
/// pin an install slot forever.
pub const FETCH_TIMEOUT_SECS: u64 = 20;

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("rate-limited by the remote (HTTP {code}); try again later")]
    RateLimited { code: u16 },
    #[error("HTTP {code} from {url}")]
    Http { code: u16, url: String },
    #[error("network error: {0}")]
    Network(String),
}

impl From<FetchError> for HubError {
    fn from(e: FetchError) -> Self {
        let code = match &e {
            FetchError::RateLimited { .. } => HubCode::RateLimited,
            FetchError::Http { .. } => HubCode::Http,
            FetchError::Network(_) => HubCode::Network,
        };
        HubError::new(code, e.to_string())
    }
}

/// Sync fetch — see the module doc for why not async. The primary
/// method is `get_bytes` (skills ship binaries); text and json are
/// conveniences over it.
pub trait Fetch: Send + Sync {
    fn get_bytes(&self, url: &str) -> Result<Vec<u8>, FetchError>;
    fn get_text(&self, url: &str) -> Result<String, FetchError> {
        Ok(String::from_utf8_lossy(&self.get_bytes(url)?).into_owned())
    }
    fn get_json(&self, url: &str) -> Result<Value, FetchError> {
        let text = self.get_text(url)?;
        text.parse()
            .map_err(|e| FetchError::Network(format!("bad json from {url}: {e}")))
    }
}

/// GitHub Trees API URL for one repo+branch, recursive.
pub fn github_tree_url(owner: &str, name: &str, branch: &str) -> String {
    format!("https://api.github.com/repos/{owner}/{name}/git/trees/{branch}?recursive=1")
}

/// Raw content URL for one file.
pub fn raw_url(owner: &str, name: &str, branch: &str, path: &str) -> String {
    let encoded: Vec<String> = path.split('/').map(url_encode_segment).collect();
    format!(
        "https://raw.githubusercontent.com/{owner}/{name}/{branch}/{}",
        encoded.join("/")
    )
}

/// Percent-encode one path segment the way a URL builder would —
/// keeps `/` out so a crafted filename cannot escape the download
/// directory on the fixture side and matches raw.githubusercontent's
/// expectations for spaces and unicode.
fn url_encode_segment(seg: &str) -> String {
    let mut out = String::new();
    for b in seg.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(b))
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Production fetcher: one shared blocking client, rustls, 20s
/// timeout. The client builds lazily so constructing the hub can
/// never fail.
#[derive(Debug, Default)]
pub struct ReqwestFetch;

impl ReqwestFetch {
    fn client() -> Result<&'static reqwest::blocking::Client, FetchError> {
        // get_or_try_init is unstable; the Result-carrying cell keeps
        // construction infallible while a build failure still
        // surfaces per-request.
        static CLIENT: OnceLock<Result<reqwest::blocking::Client, String>> = OnceLock::new();
        CLIENT
            .get_or_init(|| {
                reqwest::blocking::Client::builder()
                    .user_agent(concat!("damon-skills/", env!("CARGO_PKG_VERSION")))
                    .timeout(std::time::Duration::from_secs(FETCH_TIMEOUT_SECS))
                    .build()
                    .map_err(|e| e.to_string())
            })
            .as_ref()
            .map_err(|e| FetchError::Network(format!("client build: {e}")))
    }

    fn send(&self, url: &str, accept: &str) -> Result<reqwest::blocking::Response, FetchError> {
        let resp = ReqwestFetch::client()?
            .get(url)
            .header("Accept", accept)
            .send()
            .map_err(|e| FetchError::Network(e.to_string()))?;
        let status = resp.status().as_u16();
        // GitHub rate-limits anonymous API use with 403 as well as
        // 429; both surface as typed rate-limits rather than retries.
        if status == 429 || status == 403 {
            return Err(FetchError::RateLimited { code: status });
        }
        if !(200..300).contains(&status) {
            return Err(FetchError::Http {
                code: status,
                url: url.to_string(),
            });
        }
        Ok(resp)
    }
}

impl Fetch for ReqwestFetch {
    fn get_bytes(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        let accept = if url.starts_with("https://api.github.com/") {
            "application/vnd.github+json"
        } else {
            "text/plain"
        };
        let resp = self.send(url, accept)?;
        resp.bytes()
            .map(|b| b.to_vec())
            .map_err(|e| FetchError::Network(format!("bad body from {url}: {e}")))
    }
}

/// Test fetcher: maps `https://host/path?query` onto
/// `<root>/<host>/<path>` (query stripped). Fixtures mirror the real
/// URL space — the same code paths run against files.
#[derive(Debug)]
pub struct LocalFileFetch {
    pub root: PathBuf,
}

impl Fetch for LocalFileFetch {
    fn get_bytes(&self, url: &str) -> Result<Vec<u8>, FetchError> {
        let path = self.map_url(url)?;
        std::fs::read(&path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => FetchError::Http {
                code: 404,
                url: url.to_string(),
            },
            _ => FetchError::Network(e.to_string()),
        })
    }
}

impl LocalFileFetch {
    fn map_url(&self, url: &str) -> Result<PathBuf, FetchError> {
        let rest = url
            .strip_prefix("https://")
            .ok_or_else(|| FetchError::Network(format!("non-https fixture url: {url}")))?;
        let rest = rest.split(['?', '#']).next().unwrap_or(rest);
        let mut path = self.root.clone();
        for seg in rest.split('/') {
            if seg.is_empty() || seg == "." || seg == ".." {
                return Err(FetchError::Network(format!("bad fixture url: {url}")));
            }
            path.push(seg);
        }
        Ok(path)
    }
}

/// Fetch a repo tree with the branch fallback chain: the configured
/// branch (unless it is case-insensitively `head`), then `main`, then
/// `master`. Returns the tree JSON plus the branch that worked.
pub fn fetch_tree_with_fallback(
    fetch: &dyn Fetch,
    owner: &str,
    name: &str,
    branch: &str,
) -> Result<(Value, String), HubError> {
    let mut chain: Vec<String> = Vec::new();
    for candidate in [branch, "main", "master"] {
        if candidate.eq_ignore_ascii_case("head") {
            continue;
        }
        if chain.iter().any(|c| c.eq_ignore_ascii_case(candidate)) {
            continue;
        }
        chain.push(candidate.to_string());
    }
    let mut last: Option<HubError> = None;
    for candidate in &chain {
        match fetch.get_json(&github_tree_url(owner, name, candidate)) {
            Ok(v) if v.get("tree").is_some_and(|t| t.is_array()) => {
                return Ok((v, candidate.clone()));
            }
            Ok(_) => {
                last = Some(HubError::new(
                    HubCode::Http,
                    format!("{owner}/{name}@{candidate}: tree response has no tree array"),
                ));
            }
            Err(e) => {
                let hub: HubError = e.into();
                // A rate limit aborts the chain — trying more branches
                // just burns more of the budget.
                if hub.code == HubCode::RateLimited {
                    return Err(hub);
                }
                last = Some(hub);
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        HubError::new(
            HubCode::NotFound,
            format!("no branch found for {owner}/{name}"),
        )
    }))
}

/// Blob paths under `dir` (`.` = whole repo), sorted. A tree entry is
/// a blob when `type == "blob"`.
pub fn blobs_under(tree: &Value, dir: &str) -> Vec<String> {
    let prefix = if dir == "." {
        String::new()
    } else {
        format!("{}/", dir.trim_end_matches('/'))
    };
    let mut out: Vec<String> = tree["tree"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter(|e| e["type"] == "blob")
                .filter_map(|e| e["path"].as_str())
                .filter(|p| p.starts_with(&prefix))
                .map(|p| p[prefix.len()..].to_string())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// `path:sha` lines (sorted) hashed with SHA-256 — the repo-side
/// signature compared across update checks.
pub fn source_signature_from_tree(tree: &Value, dir: &str) -> Option<String> {
    use sha2::{Digest, Sha256};
    let prefix = if dir == "." {
        String::new()
    } else {
        format!("{}/", dir.trim_end_matches('/'))
    };
    let mut lines: Vec<String> = tree["tree"]
        .as_array()?
        .iter()
        .filter(|e| e["type"] == "blob")
        .filter_map(|e| {
            let p = e["path"].as_str()?;
            let sha = e["sha"].as_str()?;
            Some(format!("{p}:{sha}"))
        })
        .filter(|l| l.starts_with(&prefix) && l.len() > prefix.len())
        .collect();
    if lines.is_empty() {
        return None;
    }
    lines.sort();
    let joined = lines.join("\n");
    Some(format!("{:x}", Sha256::digest(joined.as_bytes())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_urls_encode_segments() {
        assert_eq!(
            raw_url("o", "n", "main", "a b/skill.md"),
            "https://raw.githubusercontent.com/o/n/main/a%20b/skill.md"
        );
    }

    #[test]
    fn blobs_filter_by_dir_and_signatures_hash() {
        let tree: Value = serde_json::json!({"tree": [
            {"type": "blob", "path": "skills/pdf/SKILL.md", "sha": "aaa"},
            {"type": "blob", "path": "skills/pdf/run.sh", "sha": "bbb"},
            {"type": "blob", "path": "skills/other/SKILL.md", "sha": "ccc"},
            {"type": "tree", "path": "skills", "sha": "ddd"},
        ]});
        assert_eq!(
            blobs_under(&tree, "skills/pdf"),
            vec!["SKILL.md".to_string(), "run.sh".to_string()]
        );
        assert_eq!(blobs_under(&tree, ".").len(), 3);
        let sig = source_signature_from_tree(&tree, "skills/pdf").unwrap();
        assert_eq!(sig.len(), 64);
        assert_ne!(
            sig,
            source_signature_from_tree(&tree, "skills/other").unwrap()
        );
        assert!(source_signature_from_tree(&tree, "missing").is_none());
    }

    #[test]
    fn local_fetch_maps_urls_to_files() {
        let root =
            std::env::temp_dir().join(format!("damon-hub-fetch-{}", uuid::Uuid::new_v4().simple()));
        let dir = root
            .join("raw.githubusercontent.com")
            .join("o")
            .join("n")
            .join("main");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("SKILL.md"), "---\nname: x\n---\nbody").unwrap();

        let fetch = LocalFileFetch { root: root.clone() };
        assert_eq!(
            fetch
                .get_text(&raw_url("o", "n", "main", "SKILL.md"))
                .unwrap(),
            "---\nname: x\n---\nbody"
        );
        let err = fetch
            .get_text(&raw_url("o", "n", "main", "missing.md"))
            .unwrap_err();
        assert!(matches!(err, FetchError::Http { code: 404, .. }));
        let err = fetch.get_text("http://plain/http").unwrap_err();
        assert!(matches!(err, FetchError::Network(_)));
        let err = fetch.get_text("https://host/../escape").unwrap_err();
        assert!(matches!(err, FetchError::Network(_)));
        let _ = std::fs::remove_dir_all(&root);
    }
}
