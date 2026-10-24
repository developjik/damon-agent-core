//! CLI lifecycle — version probes, npm-latest checks, and explicit
//! update runs for the cataloged backends (adapted from upstream
//! desktop-cc-gui's cli_lifecycle).
//!
//! Probes run on explicit request only (`backend.versions`, the
//! settings drawer's refresh, `damond doctor`) — never on a timer:
//! `npm view` is a network call and every probe spawns processes, so
//! polling would keep the machine awake for nothing. Updates NEVER run
//! automatically: they execute only via `backend.update`, stream their
//! progress to the requesting connection, and can be cancelled.
//!
//! Process access sits behind [`CommandRunner`], so tests stay
//! network/process-free; version comparison and plan construction are
//! pure functions over runner output.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// `<bin> --version` probe cap — doctor and `backend.versions` share it,
/// so an oddly slow CLI degrades to "version unknown" instead of hanging
/// either surface.
pub const VERSION_TIMEOUT: Duration = Duration::from_secs(10);
/// `npm view <pkg> version` cap — anonymous registry read.
pub const NPM_VIEW_TIMEOUT: Duration = Duration::from_secs(15);
/// `npm install -g` / installer-script cap — npm cold caches can take
/// minutes; this is the upstream ceiling.
pub const INSTALL_TIMEOUT: Duration = Duration::from_secs(420);
/// Chars per streamed output line — minified progress-bar lines would
/// otherwise flood the WS channel in one event.
pub const PROGRESS_LINE_CAP: usize = 1000;
/// Chars of installer output quoted back in a failure error — the tail
/// (where npm puts its errors), never the whole log.
pub const ERROR_TAIL_CHARS: usize = 2048;
/// Per-stream capture cap for the runner. Callers need the first line
/// (version probes) or a failure tail — both fit in a few KB, and
/// normal npm output stays well under this. Without a cap a chatty or
/// stuck installer buffered unboundedly for up to INSTALL_TIMEOUT.
const CAPTURE_CAP_BYTES: usize = 1024 * 1024;

// ==================== Injectable process runner ====================

/// One finished command: `success` folds exit status and timeout (a
/// timed-out run is never a success); `stdout` is the captured text.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub success: bool,
    pub stdout: String,
    pub timed_out: bool,
}

/// The only process surface this module touches. Real impl spawns and
/// kills process groups; tests substitute canned outputs and record the
/// argv they were asked to run.
#[async_trait]
pub trait CommandRunner: Send + Sync {
    /// Run `program args` under `limit` (killed — whole process group —
    /// on timeout or cancel), streaming each output line to `on_line`
    /// (capped per [`cap_line`]) and capturing stdout for tails.
    /// Err means the program could not be spawned (e.g. missing
    /// binary); a non-zero exit is `Ok` with `success: false`.
    async fn run(
        &self,
        program: &str,
        args: &[String],
        limit: Duration,
        cancel: &CancellationToken,
        on_line: Arc<dyn for<'a> Fn(&'a str) + Send + Sync>,
    ) -> anyhow::Result<RunOutcome>;
}

/// The real runner: tokio process, own process group on Unix (the
/// timeout/cancel kill takes npm's node chain down too), line-streamed
/// and capped-captured output.
pub struct RealRunner;

#[async_trait]
impl CommandRunner for RealRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        limit: Duration,
        cancel: &CancellationToken,
        on_line: Arc<dyn for<'a> Fn(&'a str) + Send + Sync>,
    ) -> anyhow::Result<RunOutcome> {
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
            .with_context(|| format!("cannot spawn `{program}`"))?;
        let pid = child.id();
        let stdout = tokio::io::BufReader::new(child.stdout.take().context("stdout not piped")?);
        let stderr_pipe = child.stderr.take().context("stderr not piped")?;
        let stdout_task = tokio::spawn(spawn_capture(stdout, on_line.clone()));
        let stderr_task = tokio::spawn(spawn_capture(stderr_pipe, on_line));
        let (status, timed_out) = tokio::select! {
            s = child.wait() => (Some(s.context("wait on child failed")?), false),
            _ = tokio::time::sleep(limit) => {
                kill_tree(pid);
                let _ = child.start_kill();
                let _ = child.wait().await;
                (None, true)
            }
            _ = cancel.cancelled() => {
                kill_tree(pid);
                let _ = child.start_kill();
                let _ = child.wait().await;
                (None, false)
            }
        };
        // Drain the readers so the children never block on full pipes;
        // failure tails want the combined text, order-insensitive.
        let mut combined = stdout_task.await.unwrap_or_default();
        combined.push('\n');
        combined.push_str(&stderr_task.await.unwrap_or_default());
        combined = combined.trim().to_string();
        Ok(RunOutcome {
            success: status.is_some_and(|s| s.success()) && !timed_out,
            stdout: combined,
            timed_out,
        })
    }
}

/// Stream one output pipe: every line goes to `on_line` through the
/// char-safe cap while the full text is captured (bounded — the cap
/// keeps a minified progress bar from flooding, the byte bound keeps a
/// chatty installer from ballooning memory). Invalid UTF-8 ends the
/// read; captured-so-far is returned.
async fn spawn_capture<R: tokio::io::AsyncRead + Unpin>(
    pipe: R,
    on_line: Arc<dyn for<'a> Fn(&'a str) + Send + Sync>,
) -> String {
    use tokio::io::AsyncReadExt;

    let mut taken = pipe.take((CAPTURE_CAP_BYTES + 1) as u64);
    let mut buf = Vec::new();
    let _ = taken.read_to_end(&mut buf).await;
    let mut captured = String::new();
    for line in String::from_utf8_lossy(&buf).lines() {
        on_line(&cap_line(line));
        captured.push_str(line);
        captured.push('\n');
    }
    captured
}

/// Kill the whole process group — the timeout/cancel path must take
/// npm's node chain down too, not just the npm shim.
fn kill_tree(pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid.filter(|p| *p != 0) {
        // The child owns group 0 (process_group(0)), so -pid covers it.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = pid;
}

// ==================== Pure helpers ====================

/// Char-safe per-line cap — a minified progress bar loses its tail, a
/// multi-byte character never splits mid-sequence.
pub fn cap_line(line: &str) -> String {
    if line.chars().count() <= PROGRESS_LINE_CAP {
        return line.to_string();
    }
    line.chars().take(PROGRESS_LINE_CAP).collect()
}

/// Last `cap` characters of `text` at a char boundary — the failure
/// tail quoted back into update errors (npm reports at the end).
pub fn tail_chars(text: &str, cap: usize) -> String {
    let count = text.chars().count();
    if count <= cap {
        return text.trim().to_string();
    }
    let skip = count - cap;
    let start = text
        .char_indices()
        .nth(skip)
        .map(|(i, _)| i)
        .unwrap_or(text.len());
    text[start..].trim().to_string()
}

/// First version-looking run in a version string: `2.1.228 (Claude
/// Code)` → (2, 1, 228); `v1.2` → (1, 2, 0); prose → None.
pub fn parse_version(text: &str) -> Option<(u64, u64, u64)> {
    let text = text.trim();
    let start = text.find(|c: char| c.is_ascii_digit())?;
    let mut parts = text[start..].split('.');
    let major = leading_number(parts.next()?)?;
    let minor = parts.next().and_then(leading_number).unwrap_or(0);
    let patch = parts.next().and_then(leading_number).unwrap_or(0);
    Some((major, minor, patch))
}

fn leading_number(text: &str) -> Option<u64> {
    let digits: String = text.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// An update is available only when BOTH sides parse — an unparseable
/// local version (custom builds report prose) never reads as outdated.
pub fn update_available(local: Option<&str>, latest: Option<&str>) -> bool {
    match (
        local.and_then(parse_version),
        latest.and_then(parse_version),
    ) {
        (Some(local), Some(latest)) => latest > local,
        _ => false,
    }
}

/// First non-empty line of a probe output — the version line.
pub fn first_version_line(output: &str) -> Option<String> {
    output
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(String::from)
}

/// Doctor's per-backend version suffix: a version, or the honest
/// "version unknown" a timeout/probe failure degrades to.
pub fn doctor_version_suffix(version: Option<&str>) -> String {
    match version {
        Some(v) => format!("version {v}"),
        None => "version unknown".to_string(),
    }
}

/// Human-readable one-time pairing key: 8 chars from a no-vowels,
/// no-look-alikes alphabet (upstream's), so it can be read aloud and
/// typed on a phone without ambiguity.
pub fn generate_pairing_key() -> String {
    const ALPHABET: &[u8] = b"23456789BCDFGHJKLMNPQRSTVWXZ";
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).expect("system randomness unavailable");
    bytes
        .iter()
        .map(|b| ALPHABET[(*b as usize) % ALPHABET.len()] as char)
        .collect()
}

/// Device token: 32 random bytes, hex — the usable secret lives only
/// on the device; the store keeps only [`hash_device_token`] of it.
pub fn generate_device_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("system randomness unavailable");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256 hex of a device token — the at-rest form (a store leak
/// cannot mint tickets).
pub fn hash_device_token(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Platform tag for update plans.
fn platform_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(windows) {
        "windows"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        "unknown"
    }
}

/// `npm install -g <pkg>@latest` — the argv shared by the plan preview
/// and the run, so the preview can never drift from what executes.
pub fn npm_install_argv(package: &str) -> Vec<String> {
    [
        "install",
        "-g",
        "--maxsockets=1",
        "--fetch-retries=5",
        "--no-audit",
        "--no-fund",
    ]
    .iter()
    .map(|a| (*a).to_string())
    .chain(std::iter::once(format!("{package}@latest")))
    .collect()
}

// ==================== Lifecycle facts per backend ====================

/// How the update button acts for one backend: npm (package mapped),
/// manual (a verified official one-liner exists — probe-only version
/// checks, the one-liner is the update path), or unknown (no verified
/// channel — blockers only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateKind {
    /// `npm install -g <package>@latest`.
    Npm(&'static str),
    /// The CLI's own official installer one-liner (catalog text).
    Manual(&'static str),
    /// No verified distribution channel — probe-only, blockers only.
    Unknown,
}

/// Lifecycle facts for one backend id, read from the backend catalog:
/// npm (package mapped — probe latest + runnable update), manual (a
/// verified official one-liner — the update path where npm is absent),
/// or unknown (no verified channel — blockers only). The package names
/// live in `BackendSpec` and are verified against each CLI's own
/// install docs (npmjs.com pages and official quickstarts, 2026-10);
/// anything not verifiable stays Unknown rather than guessing.
pub fn update_kind(backend: &str) -> UpdateKind {
    match crate::backend::registry::BACKENDS
        .iter()
        .find(|b| b.id == backend)
    {
        Some(spec) => match (spec.npm_package, spec.manual_update) {
            (Some(package), _) => UpdateKind::Npm(package),
            (None, "") => UpdateKind::Unknown,
            (None, script) => UpdateKind::Manual(script),
        },
        None => UpdateKind::Unknown,
    }
}

/// The update plan: the exact argv `backend.update` would run, the
/// copy-paste fallback (same command joined — the preview can never
/// drift from the run), and whatever blocks execution.
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub backend: String,
    /// "update" (backends are probed before planning) — kept as a wire
    /// field for UI labels.
    pub action: &'static str,
    /// "npm" | "manual" | "unknown"
    pub kind: &'static str,
    pub command: Vec<String>,
    pub manual_command: String,
    pub can_run: bool,
    pub blockers: Vec<String>,
    pub platform: &'static str,
}

/// Build the plan for one backend. Pure — no probing: `installed`
/// shapes the action label only.
pub fn build_plan(backend: &str, installed: bool) -> UpdatePlan {
    let base = |kind: &'static str, command: Vec<String>, blockers: Vec<String>| {
        let manual_command = command.join(" ");
        UpdatePlan {
            backend: backend.to_string(),
            action: if installed { "update" } else { "install" },
            kind,
            can_run: blockers.is_empty(),
            manual_command,
            command,
            blockers,
            platform: platform_name(),
        }
    };
    match update_kind(backend) {
        UpdateKind::Npm(package) => base(
            "npm",
            once("npm").chain(npm_install_argv(package)).collect(),
            Vec::new(),
        ),
        UpdateKind::Manual(script) => base(
            "manual",
            vec!["bash".to_string(), "-lc".to_string(), script.to_string()],
            Vec::new(),
        ),
        UpdateKind::Unknown => base(
            "unknown",
            Vec::new(),
            vec![format!(
                "{backend} has no verified npm package or official update command — update it through its own installer; probes report the installed version only"
            )],
        ),
    }
}

fn once(s: &str) -> std::iter::Once<String> {
    std::iter::once(s.to_string())
}

// ==================== Probes ====================

/// `<bin> --version`: first non-empty stdout line under the cap. A
/// spawn failure means not installed; a successful run with no version
/// line still counts as installed (there is just nothing to display).
pub async fn probe_local_version(runner: &dyn CommandRunner, bin: &str) -> (bool, Option<String>) {
    let cancel = CancellationToken::new();
    let outcome = runner
        .run(
            bin,
            &["--version".to_string()],
            VERSION_TIMEOUT,
            &cancel,
            Arc::new(|_| {}),
        )
        .await;
    match outcome {
        Ok(out) if out.success || !out.stdout.is_empty() => (true, first_version_line(&out.stdout)),
        _ => (false, None),
    }
}

/// Latest published version: `npm view <package> version` under its
/// own cap; None on any failure (the wire just omits `latest`).
pub async fn probe_latest_version(runner: &dyn CommandRunner, package: &str) -> Option<String> {
    let cancel = CancellationToken::new();
    let out = runner
        .run(
            "npm",
            &[
                "view".to_string(),
                package.to_string(),
                "version".to_string(),
            ],
            NPM_VIEW_TIMEOUT,
            &cancel,
            Arc::new(|_| {}),
        )
        .await
        .ok()?;
    if !out.success {
        return None;
    }
    first_version_line(&out.stdout)
}

// ==================== Update run registry ====================

/// In-flight `backend.update` runs keyed by creationId — the cancel
/// handle lets `backend.update_cancel` kill the installer's process
/// group (the worktree CreationRegistry pattern).
#[derive(Default)]
pub struct UpdateRegistry(Mutex<HashMap<String, CancellationToken>>);

impl UpdateRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a run and take its cancel handle.
    pub fn register(&self, id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.0.lock().insert(id.to_string(), token.clone());
        token
    }

    /// Signal cancellation; false when no live run holds the id.
    pub fn cancel(&self, id: &str) -> bool {
        self.0.lock().get(id).is_some_and(|t| {
            t.cancel();
            true
        })
    }

    /// The run ended — drop the entry.
    pub fn finish(&self, id: &str) {
        self.0.lock().remove(id);
    }
}

// ==================== Tests ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_extracts_first_digit_run() {
        assert_eq!(parse_version("2.1.228 (Claude Code)"), Some((2, 1, 228)));
        assert_eq!(parse_version("v1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_version("1.2"), Some((1, 2, 0)));
        assert_eq!(parse_version("no digits"), None);
        // Prose build descriptors never read as versions.
        assert_eq!(parse_version("codex-cli (dev build)"), None);
    }

    #[test]
    fn update_available_compares_parsed_triples_only() {
        assert!(update_available(Some("2.1.9"), Some("2.1.10")));
        assert!(update_available(Some("1.9.9"), Some("2.0.0")));
        assert!(!update_available(Some("2.1.10"), Some("2.1.10")));
        assert!(!update_available(Some("2.1.11"), Some("2.1.10")));
        // Unparseable either side → no false "outdated".
        assert!(!update_available(Some("dev build"), Some("2.0.0")));
        assert!(!update_available(None, Some("2.0.0")));
        assert!(!update_available(Some("1.0.0"), None));
    }

    #[test]
    fn cap_line_truncates_at_the_char_safe_bound() {
        assert_eq!(cap_line("short"), "short");
        let long = "x".repeat(PROGRESS_LINE_CAP + 500);
        assert_eq!(cap_line(&long).chars().count(), PROGRESS_LINE_CAP);
        // Multi-byte characters never split mid-sequence.
        let cjk = "커밋".repeat(PROGRESS_LINE_CAP);
        let capped = cap_line(&cjk);
        assert_eq!(capped.chars().count(), PROGRESS_LINE_CAP);
        assert!(cjk.starts_with(&capped));
    }

    /// The npm map is verified-coverage complete: every cataloged
    /// backend resolves to Npm (package present), Manual (verified
    /// one-liner), or Unknown (honest gap) — and nothing else.
    #[test]
    fn update_kinds_cover_the_whole_catalog() {
        let expected: &[(&str, bool)] = &[
            ("claude", true),
            ("codex", true),
            ("omp", true),
            ("pi", true),
            ("qwen", true),
            ("opencode", true),
            ("droid", false),
            ("mimo", false),
            ("zcode", false),
        ];
        for (id, npm) in expected {
            match update_kind(id) {
                UpdateKind::Npm(package) => {
                    assert!(npm, "{id} resolved npm but is cataloged manual");
                    assert!(package.starts_with('@') || package.contains('-'));
                }
                UpdateKind::Manual(script) => {
                    assert!(!npm, "{id} is npm-mapped but resolved manual");
                    assert!(!script.trim().is_empty(), "{id} one-liner missing");
                }
                UpdateKind::Unknown => {
                    assert!(!npm, "{id} is npm-mapped but resolved unknown");
                }
            }
        }
        // An uncataloged id is unknown, never a guess.
        assert_eq!(update_kind("nope"), UpdateKind::Unknown);
    }

    #[test]
    fn plans_carry_the_exact_run_argv_and_blockers() {
        // npm: the plan's command IS the run's argv.
        let plan = build_plan("qwen", true);
        assert_eq!(plan.kind, "npm");
        assert_eq!(plan.action, "update");
        assert!(plan.can_run);
        let mut expected = vec!["npm".to_string()];
        expected.extend(npm_install_argv("@qwen-code/qwen-code"));
        assert_eq!(plan.command, expected);
        assert!(
            plan.command
                .contains(&"@qwen-code/qwen-code@latest".to_string())
        );
        assert_eq!(plan.manual_command, plan.command.join(" "));

        // Not-installed backends read "install" — same argv.
        assert_eq!(build_plan("opencode", false).action, "install");

        // Manual: the official one-liner under bash -lc, runnable.
        let plan = build_plan("droid", true);
        assert_eq!(plan.kind, "manual");
        assert!(plan.can_run);
        assert_eq!(
            plan.command,
            vec![
                "bash".to_string(),
                "-lc".to_string(),
                "curl -fsSL https://app.factory.ai/cli | sh".to_string()
            ]
        );

        // Unknown: blockers only — nothing runnable, nothing guessed.
        let plan = build_plan("zcode", true);
        assert_eq!(plan.kind, "unknown");
        assert!(!plan.can_run);
        assert!(plan.command.is_empty());
        assert!(plan.blockers.len() == 1);
    }

    /// A recording mock runner: no processes, no network — canned
    /// outputs keyed by program, argv visible for assertions.
    #[derive(Default)]
    struct MockRunner {
        outputs: Mutex<HashMap<String, String>>,
        spawn_errors: Mutex<Vec<String>>,
        seen: Mutex<Vec<(String, Vec<String>)>>,
    }
    impl MockRunner {
        fn new() -> Self {
            let mut outputs = HashMap::new();
            outputs.insert("claude".into(), "2.1.100 (Claude Code)\n".into());
            outputs.insert("npm".into(), "2.1.276\n".into());
            Self {
                outputs: Mutex::new(outputs),
                ..Default::default()
            }
        }
    }
    #[async_trait]
    impl CommandRunner for MockRunner {
        async fn run(
            &self,
            program: &str,
            args: &[String],
            _limit: Duration,
            _cancel: &CancellationToken,
            on_line: Arc<dyn for<'a> Fn(&'a str) + Send + Sync>,
        ) -> anyhow::Result<RunOutcome> {
            self.seen.lock().push((program.to_string(), args.to_vec()));
            if self.spawn_errors.lock().iter().any(|p| p == program) {
                anyhow::bail!("cannot spawn `{program}`");
            }
            let stdout = self
                .outputs
                .lock()
                .get(program)
                .cloned()
                .unwrap_or_default();
            for line in stdout.lines() {
                on_line(&cap_line(line));
            }
            Ok(RunOutcome {
                success: true,
                stdout,
                timed_out: false,
            })
        }
    }

    #[tokio::test]
    async fn probes_read_first_line_and_report_missing_bins() {
        let runner = MockRunner::new();
        let (installed, version) = probe_local_version(&runner, "claude").await;
        assert!(installed);
        assert_eq!(version.as_deref(), Some("2.1.100 (Claude Code)"));

        let latest = probe_latest_version(&runner, "@anthropic-ai/claude-code").await;
        assert_eq!(latest.as_deref(), Some("2.1.276"));

        // A missing binary is "not installed", not an error.
        runner.spawn_errors.lock().push("ghost".into());
        let (installed, version) = probe_local_version(&runner, "ghost").await;
        assert!(!installed);
        assert_eq!(version, None);
    }

    #[tokio::test]
    async fn probes_run_the_documented_argv_once() {
        let runner = MockRunner::new();
        let _ = probe_local_version(&runner, "claude").await;
        let _ = probe_latest_version(&runner, "@openai/codex").await;
        let seen = runner.seen.lock();
        assert_eq!(
            seen[0],
            ("claude".to_string(), vec!["--version".to_string()])
        );
        assert_eq!(
            seen[1],
            (
                "npm".to_string(),
                vec![
                    "view".to_string(),
                    "@openai/codex".to_string(),
                    "version".to_string()
                ]
            )
        );
    }

    #[test]
    fn doctor_suffix_degrades_to_version_unknown() {
        assert_eq!(doctor_version_suffix(Some("2.1.100")), "version 2.1.100");
        assert_eq!(doctor_version_suffix(None), "version unknown");
    }
}
