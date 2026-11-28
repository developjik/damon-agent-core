//! OS-level sandbox drivers — one declarative policy, compiled into each
//! platform's native confinement form.
//!
//! The agent runs arbitrary shell commands, so without isolation a
//! hallucinated flag, an injected instruction, or an unlucky `rm` reaches
//! straight into the host: `~/.ssh`, cloud credentials, the network. This
//! module is the seam that puts an OS isolation layer between an agent CLI
//! and the machine (issue #33).
//!
//! The shape, cut for a reviewable first PR:
//!
//! - [`SandboxRequest`] is what a user declares —
//!   `sandbox = "auto" | "bwrap" | "seatbelt" | "docker" | "none"` — at the
//!   top level of the config (the default for every backend), per backend
//!   (`[backends.X] sandbox = "..."`), or per session (`session.create`'s
//!   `sandbox` param).
//! - [`SandboxSpec`] is a request resolved against one backend's process
//!   model ([`SandboxSupport`]); [`decide`] turns it into a
//!   [`SandboxDecision`]: either confined by a [`SandboxPolicy`], or
//!   explicitly unconfined with the reason recorded ([`Unconfined`]) — never
//!   "unconfined because nobody looked".
//! - A [`SandboxDriver`] compiles a policy into its native launcher argv
//!   ([`SandboxCommand`]). [`bwrap`] is the only driver here; seatbelt and
//!   docker are follow-ups to #33, and asking for one explicitly fails with a
//!   message that says so instead of quietly running unconfined.
//!
//! Two rules the rest of the file is built around:
//!
//! 1. **An explicit request never degrades silently.** `sandbox = "bwrap"` on
//!    a backend or host that cannot honor it is an error naming the cause and
//!    the choices. Only `auto` — whose whole meaning is "use what this host
//!    has" — may fall back, and it reports the fallback through `doctor`.
//! 2. **A serve-mode backend is `unsupported`, not `none`.** `opencode` and
//!    `mimo` attach to a long-lived `serve` process instead of spawning a
//!    child per session, so there is no spawn point to wrap. Recording that
//!    as `none` would read as "isolation considered and declined" when it was
//!    never possible.

pub mod bwrap;

use std::path::{Component, Path, PathBuf};

use anyhow::{Result, bail};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// What the user asked for
// ---------------------------------------------------------------------------

/// `sandbox = "..."` — the declaration. `Auto` is the zero-config default:
/// use the platform's driver when this host can run it, and say so in
/// `doctor` when it cannot. `None` is a deliberate opt-out, never a fallback.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SandboxRequest {
    #[default]
    Auto,
    Bwrap,
    Seatbelt,
    Docker,
    None,
}

impl SandboxRequest {
    /// The wire/config spelling — also the parse accepted by
    /// `session.create`'s `sandbox` param.
    pub fn parse(raw: &str) -> Result<Self> {
        Ok(match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "bwrap" => Self::Bwrap,
            "seatbelt" => Self::Seatbelt,
            "docker" => Self::Docker,
            "none" => Self::None,
            other => bail!(
                "unknown sandbox {other:?} — expected one of auto, bwrap, seatbelt, docker, none"
            ),
        })
    }
}

impl std::fmt::Display for SandboxRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Auto => "auto",
            Self::Bwrap => "bwrap",
            Self::Seatbelt => "seatbelt",
            Self::Docker => "docker",
            Self::None => "none",
        })
    }
}

/// Egress policy inside the sandbox. `Backend` defers to what the backend
/// declares in the catalog — every agent CLI in `registry.rs` declares
/// `Allow`, because its entire job is calling its model API over HTTPS
/// (denying that at the bwrap layer leaves a CLI that cannot think).
/// `filename = "sandbox_network"`-style keys stay top-level strings; see
/// `config.rs`.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NetworkPolicy {
    /// Whatever the backend declares (agents: allow).
    #[default]
    Backend,
    Allow,
    Deny,
}

/// How a backend launches its process — the thing that decides whether an
/// isolation layer can be applied at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxSupport {
    /// One stdio child per session (claude, codex, omp, pi, qwen, droid,
    /// zcode): the spawn point is ours to wrap.
    Spawn,
    /// A long-lived server the backend attaches to (opencode, mimo `serve`).
    /// Sessions are directories inside that server, so there is no
    /// per-session child to confine yet.
    Serve,
}

// ---------------------------------------------------------------------------
// Drivers
// ---------------------------------------------------------------------------

/// The isolation mechanisms the policy can be compiled into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxKind {
    Bwrap,
    Seatbelt,
    Docker,
}

impl std::fmt::Display for SandboxKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Bwrap => "bwrap",
            Self::Seatbelt => "seatbelt",
            Self::Docker => "docker",
        })
    }
}

/// Result of checking whether a driver can actually run here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverStatus {
    pub kind: SandboxKind,
    /// The launcher binary was found on PATH.
    pub installed: bool,
    /// It ran a real confinement cycle (namespaces, mounts) successfully.
    pub usable: bool,
    /// One line for `doctor` and error messages: version, or the reason it
    /// cannot run.
    pub detail: String,
}

impl DriverStatus {
    fn unavailable(kind: SandboxKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            installed: false,
            usable: false,
            detail: detail.into(),
        }
    }
}

/// One compiled launcher invocation: what to exec instead of the bare command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxCommand {
    pub program: String,
    pub args: Vec<String>,
}

/// The thin driver seam. A driver only has to (a) say whether it works here
/// and (b) turn a policy + command into the launcher invocation. Everything
/// host-shaped lives above it, so the policy stays one source of truth.
pub trait SandboxDriver: Send + Sync {
    fn kind(&self) -> SandboxKind;
    /// Cached by the implementation — `doctor` and every session start ask.
    fn status(&self) -> DriverStatus;
    fn wrap(
        &self,
        policy: &SandboxPolicy,
        command: &str,
        args: &[String],
    ) -> Result<SandboxCommand>;
}

/// The driver for `kind`, when this release actually has one.
///
/// Seatbelt and docker are follow-ups to #33: the request parses (so a
/// config written for a later release does not turn into a typo error) and
/// the failure names the gap, instead of pretending.
pub fn driver_for(kind: SandboxKind) -> Option<Box<dyn SandboxDriver>> {
    match kind {
        SandboxKind::Bwrap => Some(Box::new(bwrap::Bwrap)),
        SandboxKind::Seatbelt | SandboxKind::Docker => None,
    }
}

/// `auto`'s per-platform choice, which driver this OS can run at all.
fn platform_supports(kind: SandboxKind) -> bool {
    match kind {
        SandboxKind::Bwrap => cfg!(target_os = "linux"),
        SandboxKind::Seatbelt => cfg!(target_os = "macos"),
        // Container sockets exist on every platform with a Docker runtime.
        SandboxKind::Docker => true,
    }
}

fn platform_of(kind: SandboxKind) -> &'static str {
    match kind {
        SandboxKind::Bwrap => "Linux",
        SandboxKind::Seatbelt => "macOS",
        SandboxKind::Docker => "a Docker host",
    }
}

/// The platform's `auto` choice, irrespective of whether this release
/// implements the driver (the caller reports that difference).
pub fn platform_default() -> Option<SandboxKind> {
    #[cfg(target_os = "linux")]
    {
        Some(SandboxKind::Bwrap)
    }
    #[cfg(target_os = "macos")]
    {
        Some(SandboxKind::Seatbelt)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

// ---------------------------------------------------------------------------
// Policy
// ---------------------------------------------------------------------------

/// What a driver needs to confine one session child. Paths are absolute and
/// symlink-resolved (see [`compile_policy`]) so the compiled mounts cannot be
/// steered by a `..` or a link somewhere else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxPolicy {
    /// Backend id, for logs and error text.
    pub backend: String,
    /// The session's working directory — writable.
    pub cwd: PathBuf,
    /// Everything bound read-write: the workdir, granted roots, the
    /// backend's own auth/state directory.
    pub writable: Vec<PathBuf>,
    /// Host credential stores masked by an empty tmpfs. Compiled *after*
    /// the writable binds, so a deny always wins over a wider allow.
    pub denied: Vec<PathBuf>,
    /// Deny entries that are regular files (`.netrc`, `.git-credentials`):
    /// a tmpfs cannot mount on a file, so these bind `/dev/null` over it.
    pub denied_files: Vec<PathBuf>,
    /// Egress inside the sandbox, already resolved (`Allow`/`Deny`).
    pub network: NetworkPolicy,
}

impl SandboxPolicy {
    /// The deny paths and how each is masked, in compile order.
    pub fn deny_overlays(&self) -> impl Iterator<Item = (&Path, DenyKind)> {
        self.denied
            .iter()
            .map(|p| (p.as_path(), DenyKind::EmptyDir))
            .chain(
                self.denied_files
                    .iter()
                    .map(|p| (p.as_path(), DenyKind::NullFile)),
            )
    }
}

/// How a deny path is masked inside the sandbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenyKind {
    /// An empty tmpfs covers a directory (whether or not it exists).
    EmptyDir,
    /// `/dev/null` is bound over a regular file.
    NullFile,
}

// ---------------------------------------------------------------------------
// Decision
// ---------------------------------------------------------------------------

/// Why a session runs *without* confinement. Every variant is a deliberate
/// reason, so `doctor` can show it instead of a blank "none".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Unconfined {
    /// `sandbox = "none"` — an explicit opt-out.
    ExplicitNone,
    /// `auto` found no driver this host (or this release) can use.
    AutoNoDriver(String),
    /// The backend has no per-session child to wrap (serve mode).
    ServeUnsupported(String),
}

impl Unconfined {
    /// One line for `doctor` / logs.
    pub fn describe(&self) -> String {
        match self {
            Self::ExplicitNone => "none — sandbox = \"none\" (explicit opt-out)".to_string(),
            Self::AutoNoDriver(why) => format!("none — auto found no usable driver ({why})"),
            Self::ServeUnsupported(why) => format!("unsupported — {why}"),
        }
    }
}

/// The resolved isolation state of one session child.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SandboxDecision {
    Confined {
        kind: SandboxKind,
        policy: Box<SandboxPolicy>,
    },
    Unconfined(Unconfined),
}

impl SandboxDecision {
    /// Rewrite `(command, args)` into the driver's launcher invocation.
    /// Unconfined decisions pass through untouched.
    pub fn confine(&self, command: &str, args: &[String]) -> Result<SandboxCommand> {
        match self {
            Self::Confined { kind, policy } => {
                let Some(driver) = driver_for(*kind) else {
                    bail!("sandbox driver {kind} is not implemented in this release");
                };
                driver.wrap(policy, command, args)
            }
            Self::Unconfined(_) => Ok(SandboxCommand {
                program: command.to_string(),
                args: args.to_vec(),
            }),
        }
    }

    pub fn is_confined(&self) -> bool {
        matches!(self, Self::Confined { .. })
    }

    /// One line for `doctor` and logs: what this session child gets.
    pub fn describe(&self) -> String {
        match self {
            Self::Confined { kind, policy } => format!("{kind}: {}", summary(policy)),
            Self::Unconfined(reason) => reason.describe(),
        }
    }
}

/// One backend session's sandbox inputs, before resolution.
pub struct SandboxSpec<'a> {
    /// Backend id (routing name).
    pub backend: &'a str,
    /// How this backend launches (catalog).
    pub support: SandboxSupport,
    /// The declaration that won: session > backend config > global default.
    pub request: SandboxRequest,
    /// Egress for this backend: the global override, or the catalog default.
    pub network: NetworkPolicy,
    pub cwd: &'a Path,
    /// Extra writable roots granted to the session (claude `--add-dir`
    /// style grants). Safe to pass even when the backend ignores them.
    pub extra_writable: &'a [PathBuf],
}

/// Turn a spec into a decision. Errors are the "explicitly requested but
/// cannot be honored" cases and always name the cause and the choices.
pub fn decide(spec: &SandboxSpec<'_>) -> Result<SandboxDecision> {
    let kind = match (spec.support, spec.request) {
        // An explicit opt-out is honored on any backend, spawn or serve.
        (_, SandboxRequest::None) => {
            return Ok(SandboxDecision::Unconfined(Unconfined::ExplicitNone));
        }
        // The honest default for serve mode: there is no per-session child.
        (SandboxSupport::Serve, SandboxRequest::Auto) => {
            return Ok(SandboxDecision::Unconfined(Unconfined::ServeUnsupported(
                serve_reason(spec.backend),
            )));
        }
        // The explicit failure: the user asked for isolation on a backend
        // that cannot have it. Warning and continuing would silently run the
        // agent exactly as unprotected as before.
        (SandboxSupport::Serve, requested) => bail!(
            "sandbox = \"{requested}\" cannot confine backend `{}`: {} To proceed, set \
             sandbox = \"none\" explicitly (accepting no isolation), or use a spawn-mode \
             backend (claude, codex, omp, pi, qwen, droid, zcode).",
            spec.backend,
            serve_reason(spec.backend)
        ),
        (SandboxSupport::Spawn, SandboxRequest::Auto) => match platform_default() {
            Some(kind) => kind,
            None => {
                return Ok(SandboxDecision::Unconfined(Unconfined::AutoNoDriver(
                    "no sandbox driver for this platform yet (bwrap on Linux, seatbelt on macOS)"
                        .to_string(),
                )));
            }
        },
        (SandboxSupport::Spawn, explicit) => match explicit {
            SandboxRequest::Bwrap => SandboxKind::Bwrap,
            SandboxRequest::Seatbelt => SandboxKind::Seatbelt,
            SandboxRequest::Docker => SandboxKind::Docker,
            SandboxRequest::Auto | SandboxRequest::None => unreachable!("handled above"),
        },
    };

    // `auto` may fall back (that is what auto means) — an explicit request
    // may not. Both start from the same platform and status checks.
    let fallback_ok = spec.request == SandboxRequest::Auto;
    if !platform_supports(kind) {
        let why = format!("the {kind} driver runs on {}", platform_of(kind));
        if fallback_ok {
            return Ok(SandboxDecision::Unconfined(Unconfined::AutoNoDriver(why)));
        }
        bail!(
            "sandbox = \"{}\" is not supported on this platform: {why}. Use sandbox = \"auto\" \
             or \"none\".",
            spec.request
        );
    }
    let Some(driver) = driver_for(kind) else {
        let why = format!(
            "the {kind} driver is not implemented in this release (seatbelt and docker follow in \
             later PRs on #33)"
        );
        if fallback_ok {
            return Ok(SandboxDecision::Unconfined(Unconfined::AutoNoDriver(why)));
        }
        bail!(
            "sandbox = \"{}\" is not available: {why}. Use sandbox = \"auto\" or \"none\", or the \
             bwrap driver on Linux.",
            spec.request
        );
    };

    let status = driver.status();
    if !status.usable {
        let why = format!("{kind}: {}", status.detail);
        if fallback_ok {
            return Ok(SandboxDecision::Unconfined(Unconfined::AutoNoDriver(why)));
        }
        bail!(
            "sandbox = \"{}\" is not usable here: {why}. Fix the driver (see `damond doctor`) or \
             set sandbox = \"none\" to run without isolation.",
            spec.request
        );
    }

    let policy = compile_policy(spec)?;
    Ok(SandboxDecision::Confined {
        kind,
        policy: Box::new(policy),
    })
}

fn serve_reason(backend: &str) -> String {
    format!(
        "`{backend}` attaches to a long-lived `{backend} serve` process that Damon probes or \
         spawns once, and sessions are directories inside it — there is no per-session child to \
         wrap."
    )
}

// ---------------------------------------------------------------------------
// Policy compilation (filesystem-aware)
// ---------------------------------------------------------------------------

/// Build the policy a driver compiles, from host paths.
///
/// This is where the policy's promises are enforced rather than assumed:
///
/// * every path is absolute (`..`-relative input is rejected, not resolved
///   against the daemon's cwd);
/// * every path is symlink-resolved, so a link inside the worktree cannot
///   turn a writable bind into a bind of `/etc` — the compiled mount names
///   the real target;
/// * the workdir cannot be `/` (that would make the read-only root a lie) and
///   cannot be, or sit inside, a credential path;
/// * credential stores are denied *after* the writable binds, so a granted
///   root that happens to contain one still sees it masked.
fn compile_policy(spec: &SandboxSpec<'_>) -> Result<SandboxPolicy> {
    compile_policy_with(spec, credential_paths())
}

/// [`compile_policy`] with the deny set supplied — the tests drive credential
/// policy with their own paths instead of the machine's real `~/.ssh`.
fn compile_policy_with(spec: &SandboxSpec<'_>, denied: Vec<PathBuf>) -> Result<SandboxPolicy> {
    let cwd = resolve_existing(spec.cwd)
        .map_err(|e| anyhow::anyhow!("sandbox workdir {} — {e}", spec.cwd.display()))?;
    if cwd == Path::new("/") {
        bail!("sandbox workdir cannot be `/`: the host root is read-only inside the sandbox");
    }

    // A tmpfs mounts on a directory only, and `.netrc`-shaped credentials
    // are plain files: those are masked by binding /dev/null over them
    // instead of by a tmpfs mount.
    let (denied_files, denied): (Vec<PathBuf>, Vec<PathBuf>) =
        denied
            .into_iter()
            .fold((Vec::new(), Vec::new()), |(mut files, mut dirs), path| {
                match std::fs::symlink_metadata(&path) {
                    Ok(meta) if meta.file_type().is_file() => files.push(path),
                    _ => dirs.push(path),
                }
                (files, dirs)
            });

    let mut writable: Vec<PathBuf> = vec![cwd.clone()];
    for root in spec.extra_writable {
        // Granted roots are advisory: a missing one (deleted workspace) must
        // not fail the session — it is simply not bound.
        if let Ok(resolved) = resolve_existing(root)
            && resolved != Path::new("/")
        {
            writable.push(resolved);
        }
    }
    for dir in backend_state_dirs(spec.backend) {
        // The CLI's own home: created when absent so a first run inside the
        // sandbox can write its login instead of failing on a read-only
        // parent.
        if std::fs::create_dir_all(&dir).is_err() {
            continue;
        }
        if let Ok(resolved) = resolve_existing(&dir) {
            writable.push(resolved);
        }
    }
    for file in backend_state_files(spec.backend) {
        if let Ok(resolved) = resolve_existing(&file) {
            writable.push(resolved);
        }
    }

    // Symlinks can make two spellings one path; keep the first of each.
    writable.sort();
    writable.dedup();

    // A writable path inside a credential store would be masked by its own
    // deny overlay — the session could not work, so say why instead of
    // handing the agent an empty directory.
    if let Some((path, deny)) = writable
        .iter()
        .find_map(|w| denied.iter().map(|d| (w, d)).find(|(w, d)| is_within(w, d)))
    {
        bail!(
            "refusing to bind {} writable: it is inside the credential path {} (which the sandbox \
             masks). Move the workspace, or set sandbox = \"none\" to run without isolation.",
            path.display(),
            deny.display()
        );
    }

    // Nested binds are redundant — the parent already covers the child.
    let mut roots: Vec<PathBuf> = Vec::with_capacity(writable.len());
    for path in writable {
        if !roots.iter().any(|r| is_within(&path, r)) {
            roots.push(path);
        }
    }

    Ok(SandboxPolicy {
        backend: spec.backend.to_string(),
        cwd,
        writable: roots,
        denied,
        denied_files,
        network: spec.network,
    })
}

/// Absolute + existing + symlink-resolved.
fn resolve_existing(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("path is empty");
    }
    if !path.is_absolute() {
        bail!(
            "path is not absolute — relative paths are rejected because they would resolve \
             against whatever cwd the daemon happens to have"
        );
    }
    std::fs::canonicalize(path).map_err(|e| anyhow::anyhow!("{e}"))
}

/// `child == parent` or `child` sits under `parent`. Both sides are expected
/// to be absolute and normalized already.
fn is_within(child: &Path, parent: &Path) -> bool {
    child == parent || child.starts_with(parent)
}

/// Collapse `.`/`..` without touching the filesystem — the fallback for deny
/// paths that do not exist on this host (a tmpfs mount point does not need to
/// exist). Traversal above `/` is clamped, so `/a/../..` becomes `/`.
pub fn lexical_normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                // `..` with nothing to pop stays (a relative path that
                // already climbs) unless it is clamped by a root.
                None | Some(Component::ParentDir) => out.push(".."),
                Some(Component::RootDir) | Some(Component::Prefix(_)) => {}
                Some(Component::Normal(_)) => {
                    out.pop();
                }
                Some(Component::CurDir) => unreachable!("CurDir is never pushed"),
            },
            other => out.push(other.as_os_str()),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Host paths: what agents need, and what must never be reachable
// ---------------------------------------------------------------------------

fn home_dir() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// The pre-seeded deny set. Deliberately a fixed list, not config: a task must
/// not be able to widen it, so the only way to widen it is a code change here.
/// Every entry is masked by an empty tmpfs inside the sandbox.
pub fn credential_paths() -> Vec<PathBuf> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    [
        ".ssh",
        ".aws",
        ".gnupg",
        ".netrc",
        ".git-credentials",
        ".npmrc",
        ".pypirc",
        ".docker",
        ".kube",
        ".password-store",
        ".config/gh",
        ".config/gcloud",
        // Damon's own config holds the auth token and the discovery file.
        ".config/damon",
        ".damon",
    ]
    .into_iter()
    .map(|rel| {
        let path = home.join(rel);
        // A symlinked credential dir (dotfile managers) is still that dir.
        std::fs::canonicalize(&path).unwrap_or_else(|_| lexical_normalize(&path))
    })
    .collect()
}

/// Env vars scrubbed inside the sandbox: the ssh-agent socket is a live
/// credential no filesystem mount can hide (a unix socket stays connectable
/// through a read-only bind), and agent CLIs never need it themselves.
pub const SCRUBBED_ENV: &[&str] = &["SSH_AUTH_SOCK"];

/// The agent CLIs' own auth/state directories, per backend — bound writable
/// so the CLI keeps its login across sessions. Env overrides win over the
/// `~/` default, mirroring `skills_hub::targets`.
pub fn backend_state_dirs(backend: &str) -> Vec<PathBuf> {
    let home = home_dir();
    let env_dir = |var: &str| {
        std::env::var_os(var)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let under_home = |rel: &str| home.as_ref().map(|h| h.join(rel));
    let dirs: Vec<Option<PathBuf>> = match backend {
        "claude" => vec![env_dir("CLAUDE_CONFIG_DIR").or_else(|| under_home(".claude"))],
        "codex" => vec![env_dir("CODEX_HOME").or_else(|| under_home(".codex"))],
        "qwen" => vec![env_dir("QWEN_HOME").or_else(|| under_home(".qwen"))],
        "droid" => vec![under_home(".factory")],
        "zcode" => vec![under_home(".zcode")],
        // omp and pi keep their agent state under one root each; the env
        // overrides name a subdirectory of it.
        "omp" => vec![
            env_dir("OMP_CODING_AGENT_DIR")
                .or_else(|| env_dir("PI_CODING_AGENT_DIR"))
                .or_else(|| under_home(".omp")),
        ],
        "pi" => vec![env_dir("PI_CODING_AGENT_DIR").or_else(|| under_home(".pi"))],
        // Serve-mode backends have no driver here, so no state to bind yet.
        _ => Vec::new(),
    };
    dirs.into_iter().flatten().collect()
}

/// State that is a single file rather than a directory (bound only when it
/// exists — a missing file is not something to create).
pub fn backend_state_files(backend: &str) -> Vec<PathBuf> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    match backend {
        // Claude Code keeps its credentials and project index in
        // ~/.claude/.claude.json.
        "claude" => vec![home.join(".claude.json")],
        _ => Vec::new(),
    }
}

/// Every writable path a decision binds, for `doctor` and logs.
pub fn summary(policy: &SandboxPolicy) -> String {
    let network = match policy.network {
        NetworkPolicy::Allow => "network allow",
        NetworkPolicy::Deny => "network deny",
        NetworkPolicy::Backend => "network backend",
    };
    let mut paths: Vec<String> = policy
        .writable
        .iter()
        .map(|p| p.display().to_string())
        .collect();
    if paths.len() > 4 {
        let extra = paths.len() - 3;
        paths.truncate(3);
        paths.push(format!("+{extra} more"));
    }
    format!("{network}; writable {}", paths.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch tree that cleans itself up: `<tmp>/damon-sandbox-<pid>/<name>`.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("damon-sandbox-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn dir(&self, rel: &str) -> PathBuf {
            let p = self.0.join(rel);
            std::fs::create_dir_all(&p).unwrap();
            p
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn spec<'a>(backend: &'a str, cwd: &'a Path, extra: &'a [PathBuf]) -> SandboxSpec<'a> {
        SandboxSpec {
            backend,
            support: SandboxSupport::Spawn,
            request: SandboxRequest::None,
            network: NetworkPolicy::Deny,
            cwd,
            extra_writable: extra,
        }
    }

    #[test]
    fn parses_every_documented_spelling() {
        for (raw, want) in [
            ("auto", SandboxRequest::Auto),
            ("bwrap", SandboxRequest::Bwrap),
            ("seatbelt", SandboxRequest::Seatbelt),
            ("docker", SandboxRequest::Docker),
            ("none", SandboxRequest::None),
            (" BWRAP ", SandboxRequest::Bwrap),
        ] {
            assert_eq!(SandboxRequest::parse(raw).unwrap(), want, "{raw}");
        }
        let err = SandboxRequest::parse("bubblewrap").unwrap_err().to_string();
        assert!(err.contains("auto, bwrap, seatbelt, docker, none"), "{err}");
    }

    /// `..` is collapsed the way a shell would, and cannot climb above `/`.
    #[test]
    fn lexical_normalize_collapses_dot_dot() {
        assert_eq!(lexical_normalize(Path::new("/a/b/../c")), Path::new("/a/c"));
        assert_eq!(lexical_normalize(Path::new("/a/./b")), Path::new("/a/b"));
        assert_eq!(lexical_normalize(Path::new("/a/../..")), Path::new("/"));
        assert_eq!(lexical_normalize(Path::new("/../etc")), Path::new("/etc"));
        assert_eq!(lexical_normalize(Path::new("../x")), Path::new("../x"));
    }

    /// A relative workdir would silently resolve against the daemon's own
    /// cwd — rejected, not resolved.
    #[test]
    fn a_relative_workdir_is_rejected() {
        let scratch = Scratch::new("relative");
        let s = spec("", Path::new("worktree"), &[]);
        let err = compile_policy_with(&s, Vec::new()).unwrap_err().to_string();
        assert!(err.contains("not absolute"), "{err}");
        drop(scratch);
    }

    /// `..` in the workdir is resolved before the promise "root is read-only"
    /// is made: `/tmp/x/../..` is `/`, and `/` cannot be bound writable.
    #[test]
    fn a_workdir_that_traverses_to_the_host_root_is_rejected() {
        let scratch = Scratch::new("traverse");
        let cwd = scratch.dir("a");
        let climb = cwd.join("..").join("..").join("..").join("..");
        // `/` on the host: the workdir resolves to the root itself.
        let root = Path::new("/");
        let s = spec("", root, &[]);
        let err = compile_policy_with(&s, Vec::new()).unwrap_err().to_string();
        assert!(err.contains("cannot be `/`"), "{err}");
        // The climb itself canonicalizes to `/` too.
        let s = spec("", &climb, &[]);
        assert!(compile_policy_with(&s, Vec::new()).is_err());
    }

    /// A symlinked writable root is bound at its target: the name cannot
    /// carry the mount somewhere the policy did not intend.
    #[test]
    fn a_symlinked_writable_root_binds_its_target() {
        let scratch = Scratch::new("symlink");
        let work = scratch.dir("work");
        let target = scratch.dir("target");
        let link = scratch.path().join("link");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let extra = vec![link.clone()];
        let s = spec("", &work, &extra);
        let policy = compile_policy_with(&s, Vec::new()).unwrap();
        let want = std::fs::canonicalize(&target).unwrap();
        assert!(policy.writable.contains(&want), "{:?}", policy.writable);
        assert!(
            !policy.writable.contains(&link),
            "the link itself is never bound"
        );
    }

    /// A link pointing at `/` must not become a writable root.
    #[test]
    fn a_symlink_to_the_host_root_is_not_writable() {
        let scratch = Scratch::new("rootlink");
        let work = scratch.dir("work");
        let link = scratch.path().join("rootlink");
        #[cfg(unix)]
        std::os::unix::fs::symlink("/", &link).unwrap();
        let extra = vec![link];
        let s = spec("", &work, &extra);
        let policy = compile_policy_with(&s, Vec::new()).unwrap();
        assert!(!policy.writable.contains(&PathBuf::from("/")));
    }

    /// A writable root that *contains* a credential store keeps the store
    /// denied: the deny list is compiled after the binds (see the bwrap
    /// golden test), and the plan stays valid.
    #[test]
    fn a_wider_writable_root_does_not_widen_the_credential_deny() {
        let scratch = Scratch::new("wider");
        let work = scratch.dir("work");
        let home = scratch.dir("home");
        let ssh = home.join(".ssh");
        std::fs::create_dir_all(&ssh).unwrap();
        let extra = vec![home.clone()];
        let s = spec("", &work, &extra);
        let policy = compile_policy_with(&s, vec![std::fs::canonicalize(&ssh).unwrap()]).unwrap();
        assert!(
            policy
                .writable
                .contains(&std::fs::canonicalize(&home).unwrap())
        );
        assert!(
            policy
                .denied
                .contains(&std::fs::canonicalize(&ssh).unwrap())
        );
    }

    /// A workdir that *is* inside a credential store cannot be served — the
    /// deny overlay would empty it. Refused with both paths named instead of
    /// handing the agent an empty directory.
    #[test]
    fn a_workdir_inside_a_credential_store_is_refused() {
        let scratch = Scratch::new("inside");
        let ssh = scratch.dir("home/.ssh");
        let s = spec("", &ssh, &[]);
        let err = compile_policy_with(&s, vec![std::fs::canonicalize(&ssh).unwrap()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("credential path"), "{err}");
        assert!(err.contains("sandbox = \"none\""), "{err}");
    }

    /// Nested writable roots collapse to the outer one — one bind, no
    /// duplicate mount of the same subtree.
    #[test]
    fn nested_writable_roots_collapse() {
        let scratch = Scratch::new("nested");
        let outer = scratch.dir("outer");
        let inner = scratch.dir("outer/inner");
        let extra = vec![inner];
        let s = spec("", &outer, &extra);
        let policy = compile_policy_with(&s, Vec::new()).unwrap();
        assert_eq!(
            policy.writable,
            vec![std::fs::canonicalize(&outer).unwrap()]
        );
    }

    /// A missing granted root (deleted workspace) is skipped, not fatal.
    #[test]
    fn a_missing_granted_root_is_skipped() {
        let scratch = Scratch::new("missing");
        let work = scratch.dir("work");
        let gone = scratch.path().join("gone");
        let extra = vec![gone];
        let s = spec("", &work, &extra);
        let policy = compile_policy_with(&s, Vec::new()).unwrap();
        assert_eq!(policy.writable.len(), 1);
    }

    #[test]
    fn credential_paths_are_absolute_and_seeded() {
        for path in credential_paths() {
            assert!(path.is_absolute(), "{}", path.display());
        }
        // The set is code, not config: widening it is a code change.
        let names: Vec<String> = credential_paths()
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        for want in [".ssh", ".aws", ".gnupg"] {
            assert!(
                names.iter().any(|n| n == want),
                "{want} missing from {names:?}"
            );
        }
    }

    /// `auto` may fall back; an explicit request may not. This is the rule the
    /// whole module exists to make reviewable, so it is asserted directly.
    #[test]
    fn auto_falls_back_but_an_explicit_request_never_does() {
        let work = Scratch::new("decide");
        let cwd = work.dir("work");
        let auto = SandboxSpec {
            backend: "claude",
            support: SandboxSupport::Spawn,
            request: SandboxRequest::Auto,
            network: NetworkPolicy::Allow,
            cwd: &cwd,
            extra_writable: &[],
        };
        // Whatever this host can do, auto answers without an error.
        let decision = decide(&auto).unwrap();
        assert!(decision.is_confined() == decision.is_confined());

        let explicit = SandboxSpec {
            request: SandboxRequest::Seatbelt,
            ..auto
        };
        match decide(&explicit) {
            // On macOS the seatbelt request is refused because this release
            // has no driver; elsewhere it is refused as the wrong platform.
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("not implemented")
                        || msg.contains("not supported on this platform"),
                    "{msg}"
                );
            }
            // A host that can run seatbelt (a later release) confines.
            Ok(d) => assert!(d.is_confined()),
        }
    }

    #[test]
    fn explicit_none_never_touches_the_host() {
        let work = Scratch::new("none");
        let cwd = work.dir("work");
        let s = spec("claude", &cwd, &[]);
        assert_eq!(
            decide(&s).unwrap(),
            SandboxDecision::Unconfined(Unconfined::ExplicitNone)
        );
    }
}
