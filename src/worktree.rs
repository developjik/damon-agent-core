//! Git worktree workspaces — "one PR, one isolated agent workspace".
//!
//! `worktree.create` fetches a PR head (`refs/pull/<n>/head`) or a
//! branch, materializes it as a `git worktree`, and registers it as a
//! child project row (`kind = "worktree"`) whose root is the worktree
//! path — `session.create {projectId}` then lands the agent in an
//! isolated checkout, and the parent project keeps the repo grouping.
//!
//! All git goes through the CLI, never libgit2: mutations must be
//! killable mid-fetch (cancellation signals the process group), and
//! git's own stderr wording beats anything we would synthesize. The
//! `allowed_dirs` gate still applies — a worktree outside the allowed
//! roots would produce sessions no client can create.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::store::Store;

// ---------------------------------------------------------------------------
// Cancellation

/// One in-flight creation's cancel handle. `cancelled` is the sticky
/// flag checked between pipeline stages; `notify` preempts a running
/// git subprocess (a fetch can run for minutes).
pub struct CreationCancel {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

impl CreationCancel {
    fn fresh() -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        })
    }

    /// A never-signalled handle for one-shot git calls that have no
    /// interactive creator to cancel them.
    pub fn noop() -> Arc<Self> {
        Self::fresh()
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        // notify_one (not notify_waiters): a permit is stored when the
        // runner is not yet parked in select!, so a cancel that lands
        // just before the subprocess spawns still preempts it.
        self.notify.notify_one();
    }

    fn check(&self) -> Result<(), WorktreeError> {
        if self.cancelled.load(Ordering::SeqCst) {
            Err(WorktreeError::new(
                "canceled",
                "worktree creation canceled",
            ))
        } else {
            Ok(())
        }
    }
}

/// creationId → live cancel handle. Short critical sections, never
/// held across an await.
pub type CreationRegistry = parking_lot::Mutex<HashMap<String, Arc<CreationCancel>>>;

pub fn new_registry() -> CreationRegistry {
    parking_lot::Mutex::new(HashMap::new())
}

/// Register a creation id and return its cancel handle. A duplicate id
/// replaces the map entry — the orphaned run keeps its stale handle and
/// settles on its own.
pub fn register_creation(registry: &CreationRegistry, id: &str) -> Arc<CreationCancel> {
    let cancel = CreationCancel::fresh();
    registry.lock().insert(id.to_string(), cancel.clone());
    cancel
}

pub fn finish_creation(registry: &CreationRegistry, id: &str) {
    registry.lock().remove(id);
}

/// Signal a creation. False when no live creation holds that id.
pub fn cancel_creation(registry: &CreationRegistry, id: &str) -> bool {
    match registry.lock().get(id) {
        Some(c) => {
            c.cancel();
            true
        }
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Errors

/// Operation failure with a machine-readable kind. The RPC layer maps
/// this to `WORKTREE_FAILED` with `{kind, message}` in the error data
/// so clients can branch without parsing prose.
#[derive(Debug)]
pub struct WorktreeError {
    pub kind: &'static str,
    pub message: String,
}

impl WorktreeError {
    pub fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for WorktreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for WorktreeError {}

pub type WResult<T> = std::result::Result<T, WorktreeError>;

// ---------------------------------------------------------------------------
// Subprocess runner

/// Captured git output. `code: None` means the run never completed —
/// canceled (see `timed_out` for the gh deadline case).
pub struct GitOut {
    pub code: Option<i32>,
    pub timed_out: bool,
    pub stdout: String,
    pub stderr: String,
}

impl GitOut {
    fn ok(&self) -> bool {
        self.code == Some(0)
    }
}

/// Cap on captured stdout/stderr. Porcelain listings and error text are
/// small; fetch progress churns but only the shape matters.
const OUTPUT_CAP: usize = 1024 * 1024;

async fn read_capped<R: tokio::io::AsyncRead + Unpin>(
    mut r: R,
) -> std::io::Result<String> {
    use tokio::io::AsyncReadExt;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        let room = OUTPUT_CAP.saturating_sub(buf.len());
        let take = n.min(room);
        buf.extend_from_slice(&chunk[..take]);
        if take < n {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Spawn a program with piped output drained concurrently (a full pipe
/// blocks the child), and race completion against cancellation and an
/// optional deadline. A losing run gets its whole process tree killed
/// (dedicated process group on unix, `taskkill /T` on windows) — git
/// spawns ssh helpers that must not outlive a canceled fetch.
async fn run_cmd(
    program: &str,
    args: &[String],
    cancel: Option<&CreationCancel>,
    timeout: Option<Duration>,
) -> anyhow::Result<GitOut> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd
        .spawn()
        .map_err(|e| anyhow::anyhow!("cannot spawn {program}: {e}"))?;
    let mut out_pipe = child.stdout.take().expect("piped stdout");
    let mut err_pipe = child.stderr.take().expect("piped stderr");
    let out_t = tokio::spawn(async move { read_capped(&mut out_pipe).await });
    let err_t = tokio::spawn(async move { read_capped(&mut err_pipe).await });

    let mut timed_out = false;
    let status = tokio::select! {
        s = child.wait() => Some(s),
        _ = async {
            match timeout {
                Some(d) => tokio::time::sleep(d).await,
                None => std::future::pending::<()>().await,
            }
        } => {
            timed_out = true;
            None
        }
        _ = async {
            match cancel {
                Some(c) => c.notify.notified().await,
                None => std::future::pending::<()>().await,
            }
        } => None,
    };
    if status.is_none() {
        crate::backend::transport::kill_tree(&mut child).await;
        let _ = child.wait().await;
    }
    let stdout = out_t.await.unwrap_or_else(|_| Ok(String::new()))?;
    let stderr = err_t.await.unwrap_or_else(|_| Ok(String::new()))?;
    Ok(GitOut {
        code: status.and_then(|s| s.ok()).and_then(|s| s.code()),
        timed_out,
        stdout,
        stderr,
    })
}

async fn run_git(repo: Option<&Path>, args: &[&str], cancel: &CreationCancel) -> anyhow::Result<GitOut> {
    let mut full: Vec<String> = Vec::with_capacity(args.len() + 2);
    if let Some(r) = repo {
        full.push("-C".into());
        full.push(r.to_string_lossy().into_owned());
    }
    full.extend(args.iter().map(|s| (*s).to_string()));
    run_cmd("git", &full, Some(cancel), None).await
}

/// First line of stderr, whitespace-collapsed, bounded — enough to
/// surface git's wording without dumping progress spam.
fn summarize_stderr(stderr: &str) -> String {
    let line = stderr.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
    let collapsed: String = line.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(300).collect()
}

// ---------------------------------------------------------------------------
// `git worktree list --porcelain` parsing

#[derive(Debug, Clone, PartialEq)]
pub struct WorktreeInfo {
    pub path: String,
    /// Short branch name; None = detached or bare.
    pub branch: Option<String>,
    pub head: String,
    pub is_main: bool,
    pub bare: bool,
    pub locked: bool,
    pub lock_reason: Option<String>,
    pub prunable: bool,
}

pub fn parse_porcelain(s: &str) -> Vec<WorktreeInfo> {
    let mut out: Vec<WorktreeInfo> = Vec::new();
    let mut cur: Option<WorktreeInfo> = None;
    for line in s.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            if let Some(w) = cur.take() {
                out.push(w);
            }
            continue;
        }
        if let Some(path) = line.strip_prefix("worktree ") {
            if let Some(w) = cur.take() {
                out.push(w);
            }
            cur = Some(WorktreeInfo {
                path: path.to_string(),
                branch: None,
                head: String::new(),
                is_main: out.is_empty(),
                bare: false,
                locked: false,
                lock_reason: None,
                prunable: false,
            });
            continue;
        }
        let Some(w) = cur.as_mut() else { continue };
        if let Some(head) = line.strip_prefix("HEAD ") {
            w.head = head.to_string();
        } else if let Some(refs) = line.strip_prefix("branch ") {
            w.branch = Some(
                refs.strip_prefix("refs/heads/")
                    .unwrap_or(refs)
                    .to_string(),
            );
        } else if line == "detached" {
            // branch stays None
        } else if line == "bare" {
            w.bare = true;
        } else if let Some(reason) = line.strip_prefix("locked") {
            w.locked = true;
            let reason = reason.trim();
            if !reason.is_empty() {
                w.lock_reason = Some(reason.to_string());
            }
        } else if line.starts_with("prunable") {
            w.prunable = true;
        }
    }
    if let Some(w) = cur.take() {
        out.push(w);
    }
    out
}

/// Canonicalizing path compare — handles case-insensitive filesystems
/// and trailing separators without requiring existence.
pub fn same_path(a: &Path, b: &Path) -> bool {
    canon_abs(a) == canon_abs(b)
}

/// Absolute + canonicalized when possible; a missing path canonicalizes
/// its deepest existing ancestor and re-appends the remainder, so a
/// not-yet-created worktree still matches an allowed root through
/// symlinked prefixes (/var → /private/var on macOS).
fn canon_abs(p: &Path) -> PathBuf {
    let Ok(abs) = std::path::absolute(p) else {
        return p.to_path_buf();
    };
    if let Ok(c) = abs.canonicalize() {
        return c;
    }
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut cur = abs.as_path();
    while let Some(name) = cur.file_name() {
        if let Ok(c) = cur.canonicalize() {
            let mut full = c;
            for part in tail.iter().rev() {
                full.push(part);
            }
            return full;
        }
        tail.push(name.to_os_string());
        let Some(parent) = cur.parent() else {
            break;
        };
        cur = parent;
    }
    abs
}

// ---------------------------------------------------------------------------
// Names and defaults

/// Default branch for a PR flow when the caller passes none.
pub fn suggest_pr_branch(n: u64) -> String {
    format!("pr-{n}")
}

/// Dirname-safe form of a branch name (`feature/x-y` → `feature-x-y`).
fn sanitize_dirname(branch: &str) -> String {
    let mut out = String::with_capacity(branch.len());
    for c in branch.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
            out.push(c);
        } else {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "worktree".to_string()
    } else {
        trimmed
    }
}

/// Sibling layout: `<repo parent>/<repo>-worktrees/<branch>` — never
/// inside the repo itself, where the agent would trip over it.
pub fn default_worktree_path(repo_root: &Path, branch: &str) -> PathBuf {
    let parent = repo_root.parent().unwrap_or(Path::new("."));
    let name = repo_root
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    parent
        .join(format!("{name}-worktrees"))
        .join(sanitize_dirname(branch))
}

/// `"1842"`, `"#1842"`, or any URL containing `/pull/<digits>` → the
/// PR number.
pub fn parse_pr_input(input: &str) -> Option<u64> {
    let t = input.trim();
    if let Some(digits) = t.strip_prefix('#') {
        return digits.parse().ok();
    }
    if !t.is_empty() && t.chars().all(|c| c.is_ascii_digit()) {
        return t.parse().ok();
    }
    // GitHub URLs tolerate suffixes (/files, /commits, …).
    let bytes = t.as_bytes();
    let mut from = 0;
    while let Some(pos) = t[from..].find("/pull/") {
        let start = from + pos + "/pull/".len();
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end > start {
            return t[start..end].parse().ok();
        }
        from = start;
    }
    None
}

/// owner/repo from a GitHub remote URL (https, ssh, git@ forms), else
/// None — non-GitHub hosts get no PR preview.
pub fn github_owner_repo(url: &str) -> Option<String> {
    let u = url.trim();
    let path = ["https://github.com/", "http://github.com/", "ssh://git@github.com/"]
        .iter()
        .find_map(|p| u.strip_prefix(p))
        .or_else(|| u.strip_prefix("git@github.com:"))?;
    let path = path.strip_suffix(".git").unwrap_or(path);
    let mut parts = path.splitn(2, '/');
    let owner = parts.next()?.trim();
    let repo = parts.next()?.trim().split('/').next().unwrap_or("").trim();
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some(format!("{owner}/{repo}"))
}

// ---------------------------------------------------------------------------
// create pipeline

pub struct CreateArgs {
    pub repo: PathBuf,
    pub branch: Option<String>,
    pub path: Option<PathBuf>,
    pub base_ref: Option<String>,
    pub pr_number: Option<u64>,
    pub existing_branch: bool,
}

pub struct CreateOutcome {
    pub project_id: String,
    pub parent_project_id: Option<String>,
    pub path: PathBuf,
    pub branch: String,
    pub meta: Value,
    pub resumed: bool,
}

/// validate → fetch → add → register → done, with cancellation checked
/// between stages and inside every git run. `progress(stage, message)`
/// is called synchronously at each stage boundary. `allowed_dirs`
/// (empty = unrestricted) gates the resolved worktree path the same
/// way `session.create` gates its cwd — a worktree outside the jail
/// would birth sessions no client can create.
pub async fn create(
    store: &Store,
    cancel: &CreationCancel,
    progress: &(dyn Fn(&str, &str) + Sync),
    allowed_dirs: &[PathBuf],
    args: CreateArgs,
) -> WResult<CreateOutcome> {
    // --- validate ---------------------------------------------------------
    progress("validate", &args.repo.to_string_lossy());
    let repo_root = git_toplevel(&args.repo, cancel).await?;
    cancel.check()?;

    let branch = match (&args.branch, args.pr_number) {
        (Some(b), _) => b.clone(),
        (None, Some(n)) => suggest_pr_branch(n),
        (None, None) => {
            return Err(WorktreeError::new(
                "invalid_args",
                "branch or prNumber is required",
            ))
        }
    };
    check_branch_name(&repo_root, &branch, cancel).await?;
    cancel.check()?;

    let worktree_path = args
        .path
        .clone()
        .unwrap_or_else(|| default_worktree_path(&repo_root, &branch));
    let worktree_path = std::path::absolute(&worktree_path)
        .map_err(|e| WorktreeError::new("invalid_args", format!("bad path: {e}")))?;
    if !allowed_dirs.is_empty() {
        let abs = canon_abs(&worktree_path);
        if !allowed_dirs.iter().any(|root| abs.starts_with(canon_abs(root))) {
            return Err(WorktreeError::new(
                "not_allowed",
                format!(
                    "{} is outside the configured allowed_dirs — add the worktrees \
                     parent to allowed_dirs or pass an explicit path",
                    worktree_path.display()
                ),
            ));
        }
    }

    let existing = branch_exists(&repo_root, &branch, cancel).await?;
    if args.existing_branch && !existing && args.pr_number.is_none() {
        return Err(WorktreeError::new(
            "branch_not_found",
            format!("branch {branch:?} does not exist"),
        ));
    }

    let porcelain = list_worktrees(&repo_root, cancel).await?;
    // Resume first, before the branch_exists refusal: a retry of a
    // fully finished creation (or one that died after `worktree add`)
    // must adopt the existing checkout, not bounce off branch_exists.
    let mut resumed = false;
    if worktree_path.exists() {
        let hit = porcelain.iter().find(|w| {
            same_path(Path::new(&w.path), &worktree_path)
                && w.branch.as_deref() == Some(branch.as_str())
        });
        match hit {
            Some(_) => resumed = true,
            None => {
                return Err(WorktreeError::new(
                    "dir_exists",
                    format!(
                        "{} exists and is not a worktree of {branch} — pick another path",
                        worktree_path.display()
                    ),
                ))
            }
        }
    } else if !args.existing_branch && existing {
        return Err(WorktreeError::new(
            "branch_exists",
            format!(
                "branch {branch:?} already exists — pass existingBranch to check it out"
            ),
        ));
    }
    if !resumed
        && porcelain.iter().any(|w| {
            w.branch.as_deref() == Some(branch.as_str())
                && !same_path(Path::new(&w.path), &worktree_path)
        })
    {
        return Err(WorktreeError::new(
            "branch_checked_out",
            format!("branch {branch:?} is already checked out in another worktree"),
        ));
    }
    cancel.check()?;

    // --- fetch --------------------------------------------------------------
    let base_ref = args.base_ref.clone();
    let mut pr_url_opt: Option<String> = None;
    if !resumed {
        if let Some(n) = args.pr_number {
            progress("fetch", &format!("pull/{n}/head"));
            let url = remote_url(&repo_root, "origin", cancel).await?;
            pr_url_opt = github_owner_repo(&url)
                .map(|slug| format!("https://github.com/{slug}/pull/{n}"));
            let refspec = format!("+refs/pull/{n}/head:refs/heads/{branch}");
            let out = run_git(Some(&repo_root), &["fetch", "origin", &refspec], cancel)
                .await
                .map_err(|e| WorktreeError::new("fetch_failed", e.to_string()))?;
            if out.code.is_none() {
                return Err(canceled());
            }
            if !out.ok() {
                let msg = summarize_stderr(&out.stderr);
                let kind = if msg.contains("couldn't find remote ref")
                    || msg.contains("couldn't find remote branch")
                    || msg.contains("could not find remote ref")
                {
                    "pr_not_found"
                } else {
                    "fetch_failed"
                };
                return Err(WorktreeError::new(kind, msg));
            }
        } else {
            let base = base_ref.clone().unwrap_or_else(|| "HEAD".to_string());
            progress("fetch", &base);
            fetch_base(&repo_root, &base, cancel).await?;
        }
    }
    cancel.check()?;

    // --- add ------------------------------------------------------------
    if !resumed {
        progress("add", &worktree_path.to_string_lossy());
        let out = if existing || args.pr_number.is_some() {
            run_git(
                Some(&repo_root),
                &["worktree", "add", &worktree_path.to_string_lossy(), &branch],
                cancel,
            )
            .await
        } else {
            let base = base_ref.clone().unwrap_or_else(|| "HEAD".to_string());
            run_git(
                Some(&repo_root),
                &[
                    "worktree",
                    "add",
                    "--no-track",
                    "-b",
                    &branch,
                    &worktree_path.to_string_lossy(),
                    &base,
                ],
                cancel,
            )
            .await
        }
        .map_err(|e| WorktreeError::new("add_failed", e.to_string()))?;
        if out.code.is_none() {
            cleanup_partial(&repo_root, &worktree_path).await;
            return Err(canceled());
        }
        if !out.ok() {
            cleanup_partial(&repo_root, &worktree_path).await;
            let msg = summarize_stderr(&out.stderr);
            let kind = if msg.contains("already checked out") {
                "branch_checked_out"
            } else {
                "add_failed"
            };
            return Err(WorktreeError::new(kind, msg));
        }
        // A sparse-cone worktree can check out to an index full of
        // skip-worktree entries — an empty-looking workspace is worse
        // than a clean failure.
        if sparse_empty(&worktree_path, cancel).await? {
            cleanup_partial(&repo_root, &worktree_path).await;
            return Err(WorktreeError::new(
                "sparse_checkout_empty",
                format!(
                    "{} checked out empty (sparse checkout) — widen the cone or pick another path",
                    worktree_path.display()
                ),
            ));
        }
    }
    cancel.check()?;

    // --- register ---------------------------------------------------------
    progress("register", &worktree_path.to_string_lossy());
    let mut meta = json!({
        "branch": branch,
        "repo": repo_root.to_string_lossy(),
    });
    if let Some(b) = &base_ref {
        meta["baseRef"] = json!(b);
    }
    if let Some(n) = args.pr_number {
        meta["prNumber"] = json!(n);
        if let Some(u) = &pr_url_opt {
            meta["prUrl"] = json!(u);
        }
    }
    let parent_project_id = ensure_parent_project(store, &repo_root)
        .await
        .map_err(|e| WorktreeError::new("register_failed", e.to_string()))?;
    let project_id = register_row(store, &worktree_path, parent_project_id.as_deref(), &meta)
        .await
        .map_err(|e| WorktreeError::new("register_failed", e.to_string()))?;

    progress("done", &worktree_path.to_string_lossy());
    Ok(CreateOutcome {
        project_id,
        parent_project_id,
        path: worktree_path,
        branch,
        meta,
        resumed,
    })
}

fn canceled() -> WorktreeError {
    WorktreeError::new("canceled", "worktree creation canceled")
}

/// The repo's canonical root — resolves a subdirectory argument to
/// its top level so `worktree.create` works from anywhere in a repo.
async fn git_toplevel(repo: &Path, cancel: &CreationCancel) -> WResult<PathBuf> {
    let out = run_git(Some(repo), &["rev-parse", "--show-toplevel"], cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    if !out.ok() {
        return Err(WorktreeError::new(
            "not_a_repo",
            summarize_stderr(&out.stderr),
        ));
    }
    Ok(PathBuf::from(out.stdout.trim()))
}

async fn check_branch_name(repo: &Path, branch: &str, cancel: &CreationCancel) -> WResult<()> {
    let out = run_git(
        Some(repo),
        &["check-ref-format", "--branch", branch],
        cancel,
    )
    .await
    .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    if !out.ok() {
        return Err(WorktreeError::new(
            "invalid_branch",
            format!("{branch:?} is not a valid branch name"),
        ));
    }
    Ok(())
}

async fn branch_exists(repo: &Path, branch: &str, cancel: &CreationCancel) -> WResult<bool> {
    let out = run_git(
        Some(repo),
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ],
        cancel,
    )
    .await
    .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    Ok(out.ok())
}

async fn remote_url(repo: &Path, remote: &str, cancel: &CreationCancel) -> WResult<String> {
    let out = run_git(Some(repo), &["remote", "get-url", remote], cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    if !out.ok() {
        return Err(WorktreeError::new(
            "no_origin",
            format!("remote {remote:?} is not configured"),
        ));
    }
    Ok(out.stdout.trim().to_string())
}

/// Best-effort fetch of a base ref, then hard verification. Remote-
/// tracking bases (`origin/main`) fetch their remote; plain refs
/// verify locally first and only fetch as a fallback.
async fn fetch_base(repo: &Path, base: &str, cancel: &CreationCancel) -> WResult<()> {
    if let Some((remote, rest)) = split_remote(repo, base, cancel).await? {
        // Remote-tracking base: refresh it, ignore failure (the local
        // ref may still be usable).
        let _ = run_git(Some(repo), &["fetch", &remote, &rest], cancel).await;
    } else if base_resolves(repo, base, cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?
    {
        // Already resolvable locally — no fetch needed.
    } else {
        let _ = run_git(Some(repo), &["fetch", "origin", base], cancel).await;
    }
    if !base_resolves(repo, base, cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?
    {
        return Err(WorktreeError::new(
            "base_not_found",
            format!("base ref {base:?} does not resolve"),
        ));
    }
    cancel.check()
}

async fn base_resolves(repo: &Path, base: &str, cancel: &CreationCancel) -> anyhow::Result<bool> {
    let out = run_git(
        Some(repo),
        &["rev-parse", "--verify", "--quiet", &format!("{base}^{{commit}}")],
        cancel,
    )
    .await?;
    Ok(out.code == Some(0))
}

/// `origin/main` → ("origin", "main") when origin is a configured
/// remote; None otherwise.
async fn split_remote(
    repo: &Path,
    base: &str,
    cancel: &CreationCancel,
) -> WResult<Option<(String, String)>> {
    let Some((head, rest)) = base.split_once('/') else {
        return Ok(None);
    };
    if head.is_empty() || rest.is_empty() {
        return Ok(None);
    }
    let out = run_git(Some(repo), &["remote"], cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    Ok(out
        .stdout
        .lines()
        .any(|r| r.trim() == head)
        .then(|| (head.to_string(), rest.to_string())))
}

/// Undo a partial `worktree add`: drop the checkout and prune git's
/// registration. The branch ref is deliberately kept — the fetch
/// result is still good, only the checkout failed.
async fn cleanup_partial(repo: &Path, path: &Path) {
    let noop = CreationCancel::fresh();
    let _ = run_git(
        Some(repo),
        &["worktree", "remove", "--force", &path.to_string_lossy()],
        &noop,
    )
    .await;
    let _ = run_git(Some(repo), &["worktree", "prune"], &noop).await;
}

/// True when the index has entries but every one is skip-worktree (a
/// sparse cone that covers nothing here). An empty index (no files at
/// all) is not flagged — that is an honest empty repo.
async fn sparse_empty(path: &Path, cancel: &CreationCancel) -> WResult<bool> {
    let out = run_git(Some(path), &["ls-files", "-t"], cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    if !out.ok() {
        return Ok(false);
    }
    let lines: Vec<&str> = out.stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    Ok(!lines.is_empty() && lines.iter().all(|l| l.starts_with('S')))
}

/// The repo's own project row, created on demand so a worktree always
/// has a parent to nest under.
async fn ensure_parent_project(store: &Store, repo_root: &Path) -> anyhow::Result<Option<String>> {
    if let Some(row) = store.get_project_by_root(&repo_root.to_string_lossy()).await?
        && row.kind == "project"
    {
        return Ok(Some(row.id));
    }
    let id = uuid::Uuid::new_v4().to_string();
    let name = repo_root
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| repo_root.to_string_lossy().into_owned());
    let root = repo_root.to_string_lossy().into_owned();
    store.create_project(&id, &name, &root, "{}").await?;
    Ok(Some(id))
}

/// Insert (or refresh) the worktree's project row. A retry after the
/// row already landed updates meta in place rather than forking a
/// second row for the same path.
async fn register_row(
    store: &Store,
    path: &Path,
    parent_id: Option<&str>,
    meta: &Value,
) -> anyhow::Result<String> {
    let root = path.to_string_lossy().into_owned();
    let meta_json = meta.to_string();
    if let Some(row) = store.get_project_by_root(&root).await?
        && row.kind == "worktree"
    {
        store
            .set_project_meta(&row.id, parent_id, &meta_json)
            .await?;
        return Ok(row.id);
    }
    let id = uuid::Uuid::new_v4().to_string();
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| root.clone());
    store
        .create_project_full(&id, &name, &root, "{}", "worktree", parent_id, &meta_json)
        .await?;
    Ok(id)
}

// ---------------------------------------------------------------------------
// list / remove / resolve_pr / merged

pub async fn list_worktrees(repo: &Path, cancel: &CreationCancel) -> WResult<Vec<WorktreeInfo>> {
    let out = run_git(Some(repo), &["worktree", "list", "--porcelain"], cancel)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    if !out.ok() {
        return Err(WorktreeError::new(
            "not_a_repo",
            summarize_stderr(&out.stderr),
        ));
    }
    Ok(parse_porcelain(&out.stdout))
}

/// The repo a worktree belongs to, discovered from the checkout itself
/// (`git-common-dir` points at the main repo's `.git`).
pub async fn discover_repo(path: &Path, cancel: &CreationCancel) -> WResult<PathBuf> {
    let out = run_git(
        Some(path),
        &[
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
        ],
        cancel,
    )
    .await
    .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    if out.code.is_none() {
        return Err(canceled());
    }
    if !out.ok() {
        return Err(WorktreeError::new(
            "not_a_repo",
            summarize_stderr(&out.stderr),
        ));
    }
    let common = PathBuf::from(out.stdout.trim());
    common
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| WorktreeError::new("not_a_repo", "cannot resolve repo root"))
}

pub struct RemoveOutcome {
    pub orphan_directory: bool,
    pub branch_deleted: bool,
    pub branch_kept_reason: Option<String>,
}

/// Remove the worktree checkout (force), pruning git's registration;
/// optionally delete the branch. Never fails hard on git errors — the
/// outcome reports what needs manual attention instead.
pub async fn remove_worktree_git(
    repo: &Path,
    path: &Path,
    branch: Option<&str>,
    delete_branch: bool,
) -> RemoveOutcome {
    let noop = CreationCancel::fresh();
    let mut outcome = RemoveOutcome {
        orphan_directory: false,
        branch_deleted: false,
        branch_kept_reason: None,
    };
    let out = run_git(
        Some(repo),
        &["worktree", "remove", "--force", &path.to_string_lossy()],
        &noop,
    )
    .await;
    let removed = out.map(|o| o.ok()).unwrap_or(false);
    if !removed && path.exists() {
        // git refused (locked, dirty beyond --force's reach) — fall
        // back to a direct delete and let prune sweep the metadata.
        let _ = tokio::fs::remove_dir_all(path).await;
        outcome.orphan_directory = true;
    }
    let _ = run_git(Some(repo), &["worktree", "prune"], &noop).await;

    if delete_branch
        && let Some(branch) = branch
    {
        // -d refuses unmerged branches; -D is the explicit override.
        let safe = run_git(Some(repo), &["branch", "-d", branch], &noop).await;
        if safe.as_ref().map(|o| o.ok()).unwrap_or(false) {
            outcome.branch_deleted = true;
        } else {
            let force = run_git(Some(repo), &["branch", "-D", branch], &noop).await;
            if force.as_ref().map(|o| o.ok()).unwrap_or(false) {
                outcome.branch_deleted = true;
            } else {
                let stderr = force
                    .map(|o| summarize_stderr(&o.stderr))
                    .unwrap_or_default();
                outcome.branch_kept_reason = Some(
                    if stderr.contains("checked out") {
                        "checked_out_elsewhere".to_string()
                    } else {
                        "unknown".to_string()
                    },
                );
            }
        }
    }
    outcome
}

pub struct PrPreview {
    pub number: u64,
    /// "owner/repo" of the GitHub remote.
    pub repo: String,
    pub title: Option<String>,
    pub state: Option<String>,
    pub author: Option<String>,
    pub degraded: bool,
    /// The branch the create dialog should default to (caller's or
    /// `pr-<n>`), the worktree path it implies, and whether either
    /// already exists — enough to pre-flight the form without a
    /// create round-trip.
    pub suggested_branch: String,
    pub suggested_path: String,
    pub branch_conflict: bool,
    pub dir_conflict: bool,
}

/// Resolve a PR reference against the repo's GitHub remote, enriching
/// with `gh pr view` when available (3s deadline — a missing or hung
/// gh degrades to number+repo, never blocks the create dialog).
pub async fn resolve_pr(
    repo: &Path,
    cancel: &CreationCancel,
    input: &str,
    branch: Option<&str>,
) -> WResult<PrPreview> {
    let Some(number) = parse_pr_input(input) else {
        return Err(WorktreeError::new(
            "invalid_pr_input",
            format!("{input:?} is not a PR number, #number, or GitHub PR URL"),
        ));
    };
    let url = remote_url(repo, "origin", cancel).await?;
    let Some(slug) = github_owner_repo(&url) else {
        return Err(WorktreeError::new(
            "not_github",
            format!("origin {url:?} is not a GitHub remote"),
        ));
    };
    let mut preview = PrPreview {
        number,
        repo: slug.clone(),
        title: None,
        state: None,
        author: None,
        degraded: true,
        suggested_branch: branch
            .map(String::from)
            .unwrap_or_else(|| suggest_pr_branch(number)),
        suggested_path: String::new(),
        branch_conflict: false,
        dir_conflict: false,
    };
    // Suggestion/conflict pre-flight needs the canonical repo root.
    if let Ok(toplevel) = git_toplevel(repo, cancel).await {
        let suggested_path = default_worktree_path(&toplevel, &preview.suggested_branch);
        preview.dir_conflict = suggested_path.exists();
        preview.suggested_path = suggested_path.to_string_lossy().into_owned();
        preview.branch_conflict = branch_exists(&toplevel, &preview.suggested_branch, cancel)
            .await
            .unwrap_or(false);
    }
    let args: Vec<String> = vec![
        "pr".into(),
        "view".into(),
        number.to_string(),
        "--repo".into(),
        slug,
        "--json".into(),
        "title,state,author".into(),
    ];
    if let Ok(out) = run_cmd("gh", &args, Some(cancel), Some(Duration::from_secs(3))).await
        && out.code.is_some()
        && out.ok()
        && let Ok(v) = serde_json::from_str::<Value>(&out.stdout)
    {
        preview.title = v["title"].as_str().map(String::from);
        preview.state = v["state"].as_str().map(String::from);
        preview.author = v["author"]["login"].as_str().map(String::from);
        preview.degraded = preview.title.is_none() && preview.state.is_none();
    }
    Ok(preview)
}

/// Whether `branch` is fully merged into `base` — the removal UX's
/// uncommitted-work warning. Squash merges report false (no ancestry).
pub async fn branch_merged(repo: &Path, branch: &str, base: &str) -> WResult<bool> {
    let noop = CreationCancel::fresh();
    let out = run_git(Some(repo), &["merge-base", "--is-ancestor", branch, base], &noop)
        .await
        .map_err(|e| WorktreeError::new("unknown", e.to_string()))?;
    match out.code {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        Some(_) => Err(WorktreeError::new(
            "not_a_repo",
            summarize_stderr(&out.stderr),
        )),
        None => Err(canceled()),
    }
}

// ---------------------------------------------------------------------------
// tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn porcelain_main_branch_detached_locked() {
        let raw = "worktree /repo\nHEAD abc123\nbranch refs/heads/main\n\nworktree /repo-wt/pr-7\nHEAD def456\nbranch refs/heads/pr-7\n\nworktree /repo-wt/det\nHEAD 789abc\ndetached\n\nworktree /repo-wt/locked\nHEAD fedcba\nbranch refs/heads/x\nlocked reason here\nprunable\n";
        let wts = parse_porcelain(raw);
        assert_eq!(wts.len(), 4);
        assert_eq!(wts[0].path, "/repo");
        assert_eq!(wts[0].branch.as_deref(), Some("main"));
        assert!(wts[0].is_main);
        assert!(!wts[0].locked);
        assert_eq!(wts[1].branch.as_deref(), Some("pr-7"));
        assert!(!wts[1].is_main);
        assert!(wts[2].branch.is_none());
        assert!(wts[3].locked);
        assert!(wts[3].prunable);
    }

    #[test]
    fn porcelain_tolerates_missing_trailing_blank() {
        let wts = parse_porcelain("worktree /r\nHEAD a\nbranch refs/heads/m");
        assert_eq!(wts.len(), 1);
        assert_eq!(wts[0].branch.as_deref(), Some("m"));
    }

    #[test]
    fn dirname_sanitizing() {
        assert_eq!(sanitize_dirname("feature/x-y"), "feature-x-y");
        assert_eq!(sanitize_dirname("한글브랜치"), "worktree");
        assert_eq!(sanitize_dirname("--"), "worktree");
        assert_eq!(default_worktree_path(Path::new("/a/b/repo"), "pr 9").to_string_lossy(), "/a/b/repo-worktrees/pr-9");
    }

    #[test]
    fn pr_input_forms() {
        assert_eq!(parse_pr_input("1842"), Some(1842));
        assert_eq!(parse_pr_input("#1842"), Some(1842));
        assert_eq!(
            parse_pr_input("https://github.com/o/r/pull/42/files"),
            Some(42)
        );
        assert_eq!(parse_pr_input("not a pr"), None);
        assert_eq!(parse_pr_input(""), None);
    }

    #[test]
    fn github_slug_forms() {
        assert_eq!(
            github_owner_repo("https://github.com/owner/repo.git"),
            Some("owner/repo".to_string())
        );
        assert_eq!(
            github_owner_repo("git@github.com:owner/repo"),
            Some("owner/repo".to_string())
        );
        assert_eq!(
            github_owner_repo("ssh://git@github.com/owner/repo.git"),
            Some("owner/repo".to_string())
        );
        assert_eq!(github_owner_repo("https://gitlab.com/o/r"), None);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_cmd_cancel_kills_the_tree() {
        let cancel = CreationCancel::fresh();
        let handle = cancel.clone();
        let start = std::time::Instant::now();
        let task = tokio::spawn(async move {
            run_cmd(
                "sh",
                &["-c".to_string(), "sleep 30".to_string()],
                Some(&handle),
                None,
            )
            .await
            .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let out = task.await.unwrap();
        assert!(out.code.is_none());
        assert!(!out.timed_out);
        assert!(start.elapsed() < Duration::from_secs(10));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_cmd_timeout_kills() {
        let start = std::time::Instant::now();
        let out = run_cmd(
            "sh",
            &["-c".to_string(), "sleep 30".to_string()],
            None,
            Some(Duration::from_millis(100)),
        )
        .await
        .unwrap();
        assert!(out.timed_out);
        assert!(out.code.is_none());
        assert!(start.elapsed() < Duration::from_secs(10));
    }
}
