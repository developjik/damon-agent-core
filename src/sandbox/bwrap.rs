//! The Linux driver: bubblewrap (`bwrap`) — mount namespaces for the
//! filesystem, `unshare` for the rest, no root and no daemon.
//!
//! This file is the whole bwrap surface: a pure compiler from
//! [`SandboxPolicy`] to `bwrap` argv (unit- and golden-tested, no process
//! involved), and the availability probe a real launch depends on.
//!
//! The compiled shape is deliberately "default deny, named allows":
//!
//! ```text
//! --ro-bind / /          host root, read-only
//! --dev /dev --proc /proc  fresh device and proc trees
//! --tmpfs /tmp           writable scratch, discarded with the session
//! --bind <root> <root>   the workdir, granted roots, the CLI's own state
//! --tmpfs <credential>   each deny, compiled AFTER the binds so it wins
//! --unshare-all          pid/ipc/uts/cgroup/user (+ net unless allowed)
//! --cap-drop ALL
//! --die-with-parent --new-session
//! ```

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use super::{
    DenyKind, DriverStatus, NetworkPolicy, SCRUBBED_ENV, SandboxCommand, SandboxDriver,
    SandboxKind, SandboxPolicy,
};

/// How long the availability probe may take before it counts as unusable.
/// A working bwrap answers in milliseconds. The probe runs inline on a
/// tokio worker (the decision is sync), so the cap is short: a hung bwrap
/// must not hold a worker for a session start.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a probe answer is trusted before the next `auto` resolution
/// re-checks (see `Bwrap::status`).
const STATUS_TTL: Duration = Duration::from_secs(30);

pub struct Bwrap;

impl SandboxDriver for Bwrap {
    fn kind(&self) -> SandboxKind {
        SandboxKind::Bwrap
    }

    fn status(&self) -> DriverStatus {
        // Re-probed after a short while: `auto` reads this on every session
        // start, and a driver installed (or a sysctl flipped) while the
        // daemon runs must take effect without a restart — a forever-cached
        // "not usable" would leave a resident daemon unconfined with no way
        // to tell. The probe is one short-lived process, so the window keeps
        // session starts cheap without pinning the answer.
        static CACHE: OnceLock<Mutex<Option<(Instant, DriverStatus)>>> = OnceLock::new();
        let cache = CACHE.get_or_init(|| Mutex::new(None));
        let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((at, status)) = guard.as_ref()
            && at.elapsed() < STATUS_TTL
        {
            return status.clone();
        }
        let status = probe();
        *guard = Some((Instant::now(), status.clone()));
        status
    }

    fn wrap(
        &self,
        policy: &SandboxPolicy,
        command: &str,
        args: &[String],
    ) -> Result<SandboxCommand> {
        Ok(SandboxCommand {
            program: launcher(),
            args: compile_args(policy, command, args)?,
        })
    }
}

/// The launcher binary name. Resolved through PATH by the spawn, which is
/// also how the probe found it.
fn launcher() -> String {
    "bwrap".to_string()
}

/// Policy + command → the full `bwrap` argument list (everything after the
/// program name). Pure: no filesystem access, no process, so it is the piece
/// the golden test pins.
pub fn compile_args(policy: &SandboxPolicy, command: &str, args: &[String]) -> Result<Vec<String>> {
    if !policy.cwd.is_absolute() {
        bail!("sandbox cwd must be absolute, got {}", policy.cwd.display());
    }
    if policy.writable.iter().any(|p| p == Path::new("/")) {
        bail!("refusing to bind the host root writable");
    }
    for path in policy
        .writable
        .iter()
        .chain(policy.denied.iter())
        .chain(policy.denied_files.iter())
    {
        if !path.is_absolute() {
            bail!("sandbox path must be absolute, got {}", path.display());
        }
    }

    let mut out: Vec<String> = Vec::new();
    // Allows first, denies after them (see below). `--dev`/`--proc` take a
    // single DEST, unlike the SRC DEST mount flags.
    pair(&mut out, "--ro-bind", "/", "/");
    one(&mut out, "--dev", "/dev");
    one(&mut out, "--proc", "/proc");
    for path in &policy.writable {
        let path = path.to_string_lossy().into_owned();
        pair(&mut out, "--bind", &path, &path);
    }
    // Scratch space: writable, but a tmpfs, so nothing survives the session.
    //
    // Only when no writable path lives under /tmp. bwrap resolves a mount's
    // source in the namespace it has built so far, so a tmpfs on /tmp would
    // both hide an already-bound workdir and make its source unresolvable;
    // a workdir inside /tmp therefore keeps the host's read-only /tmp.
    // (TMPDIR-nameable scratch is the cost, and it is the safe side of the
    // trade: a read-only /tmp fails a write, it does not leak one.)
    if !policy.writable.iter().any(|p| p.starts_with("/tmp")) {
        one(&mut out, "--tmpfs", "/tmp");
    }
    // Denies last — a mount stacked on top of a wider bind, so it wins even
    // when a granted root happens to contain a credential store. A file
    // credential takes /dev/null over it (a tmpfs mounts on directories).
    for (path, kind) in policy.deny_overlays() {
        match kind {
            DenyKind::EmptyDir => one(&mut out, "--tmpfs", &path.to_string_lossy()),
            DenyKind::NullFile => pair(&mut out, "--ro-bind", "/dev/null", &path.to_string_lossy()),
        }
    }
    out.push("--unshare-all".to_string());
    if policy.network == NetworkPolicy::Allow {
        // `--unshare-all` took the network with everything else; share it
        // back when the backend needs its model API.
        out.push("--share-net".to_string());
    }
    out.push("--cap-drop".to_string());
    out.push("ALL".to_string());
    // The sandbox dies with the daemon instead of outliving it, and no
    // terminal can be injected into the child.
    out.push("--die-with-parent".to_string());
    out.push("--new-session".to_string());
    for var in SCRUBBED_ENV {
        one(&mut out, "--unsetenv", var);
    }
    one(&mut out, "--chdir", &policy.cwd.to_string_lossy());
    out.push("--".to_string());
    out.push(command.to_string());
    out.extend(args.iter().cloned());
    Ok(out)
}

/// `flag value` — one argument flag.
fn one(out: &mut Vec<String>, flag: &str, value: &str) {
    out.push(flag.to_string());
    out.push(value.to_string());
}

/// `flag src dest` — a mount flag.
fn pair(out: &mut Vec<String>, flag: &str, src: &str, dest: &str) {
    one(out, flag, src);
    out.push(dest.to_string());
}

/// Is bwrap installed here, and can it actually build a sandbox?
///
/// Two separate questions, because they fail differently and users fix them
/// differently: a missing binary is an install, a rejected `unshare` is a
/// kernel/sysctl (or packaging) problem — on many distros unprivileged user
/// namespaces are off (`kernel.unprivileged_userns_clone=0`, Ubuntu 24.04's
/// AppArmor restriction), and bwrap then cannot start at all.
fn probe() -> DriverStatus {
    let Some(path) = crate::backend::registry::which("bwrap") else {
        return DriverStatus::unavailable(
            SandboxKind::Bwrap,
            "bubblewrap is not installed (bwrap not found on PATH)",
        );
    };
    let version = run_probe(&path, &["--version"])
        .and_then(|(ok, out)| ok.then_some(out))
        .map(|out| out.trim().to_string())
        .filter(|v| !v.is_empty());
    let label = match &version {
        Some(v) => format!("bubblewrap {v}"),
        None => "bubblewrap".to_string(),
    };

    // A full confinement cycle, cheap and side-effect free: if this fails,
    // every real launch would fail the same way.
    let smoke = vec![
        "--ro-bind",
        "/",
        "/",
        "--dev",
        "/dev",
        "--proc",
        "/proc",
        // The recipe's own scratch mount, so a host where /tmp cannot be
        // masked fails here rather than at the first session.
        "--tmpfs",
        "/tmp",
        "--unshare-all",
        "--die-with-parent",
        "--new-session",
        "--",
        "/bin/true",
    ];
    match run_probe(&path, &smoke) {
        Some((true, _)) => DriverStatus {
            kind: SandboxKind::Bwrap,
            installed: true,
            usable: true,
            detail: format!("{label}, usable"),
        },
        Some((false, err)) => DriverStatus {
            kind: SandboxKind::Bwrap,
            installed: true,
            usable: false,
            detail: format!(
                "{label} installed, but it cannot create a sandbox: {}",
                explain(&err)
            ),
        },
        None => DriverStatus {
            kind: SandboxKind::Bwrap,
            installed: true,
            usable: false,
            detail: format!("{label} installed, but the launch probe timed out or hung"),
        },
    }
}

/// Turn bwrap's stderr into something a user can act on.
fn explain(stderr: &str) -> String {
    let line = stderr
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("no output");
    if line.contains("Operation not permitted") || line.to_lowercase().contains("namespace") {
        format!(
            "{line} (unprivileged user namespaces look disabled — check \
             kernel.unprivileged_userns_clone / the distro's userns policy)"
        )
    } else {
        line.to_string()
    }
}

/// Run the launcher with a hard deadline. `Some((success, combined output))`,
/// or None when it could not be started or hung past [`PROBE_TIMEOUT`].
fn run_probe(program: &Path, args: &[&str]) -> Option<(bool, String)> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    // Drain both pipes on helper threads: a probe that fills a pipe buffer
    // would otherwise block against the wait loop below.
    let mut stdout = child.stdout.take()?;
    let mut stderr = child.stderr.take()?;
    let out_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        use std::io::Read;
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    let err_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        use std::io::Read;
        let _ = stderr.read_to_end(&mut buf);
        buf
    });
    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = out_reader.join();
                let _ = err_reader.join();
                return None;
            }
        }
    };
    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    let combined = if stderr.is_empty() { stdout } else { stderr };
    Some((
        status.success(),
        String::from_utf8_lossy(&combined).into_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{SandboxDecision, SandboxRequest, SandboxSpec, Unconfined, decide};

    fn policy(network: NetworkPolicy) -> SandboxPolicy {
        SandboxPolicy {
            backend: "claude".to_string(),
            cwd: "/home/u/project".into(),
            writable: vec!["/home/u/project".into(), "/home/u/.claude".into()],
            denied: vec!["/home/u/.ssh".into(), "/home/u/.aws".into()],
            denied_files: Vec::new(),
            network,
        }
    }

    /// Golden: the compiled launcher argv, in order. Order is the policy —
    /// read-only root first, binds next, deny overlays after them, the
    /// command last — so this test is the review of the whole shape.
    #[cfg(unix)]
    #[test]
    fn compiles_the_golden_argv() {
        let args = compile_args(
            &policy(NetworkPolicy::Deny),
            "claude",
            &["-p".to_string(), "--verbose".to_string()],
        )
        .unwrap();
        assert_eq!(
            args,
            vec![
                "--ro-bind",
                "/",
                "/",
                "--dev",
                "/dev",
                "--proc",
                "/proc",
                "--bind",
                "/home/u/project",
                "/home/u/project",
                "--bind",
                "/home/u/.claude",
                "/home/u/.claude",
                "--tmpfs",
                "/tmp",
                "--tmpfs",
                "/home/u/.ssh",
                "--tmpfs",
                "/home/u/.aws",
                "--unshare-all",
                "--cap-drop",
                "ALL",
                "--die-with-parent",
                "--new-session",
                "--unsetenv",
                "SSH_AUTH_SOCK",
                "--chdir",
                "/home/u/project",
                "--",
                "claude",
                "-p",
                "--verbose",
            ]
        );
    }

    /// A workdir under /tmp keeps the host's read-only /tmp: a tmpfs there
    /// would hide the bind, and bwrap resolves a mount's source in the
    /// namespace it has built so far.
    #[cfg(unix)]
    #[test]
    fn a_tmp_workdir_does_not_shadow_itself() {
        let mut p = policy(NetworkPolicy::Allow);
        p.cwd = "/tmp/work".into();
        p.writable = vec!["/tmp/work".into()];
        let args = compile_args(&p, "sh", &[]).unwrap();
        let binds = args.iter().position(|a| a == "--bind").unwrap();
        let tmpfs_tmp = args
            .windows(2)
            .position(|w| w[0] == "--tmpfs" && w[1] == "/tmp");
        assert!(
            tmpfs_tmp.is_none_or(|i| i > binds),
            "no /tmp tmpfs when the workdir lives there: {args:?}"
        );
        // The deny overlays are still there.
        assert!(
            args.windows(2)
                .any(|w| w[0] == "--tmpfs" && w[1] == "/home/u/.ssh"),
            "{args:?}"
        );
    }

    /// Network is denied by `--unshare-all` and shared back only on ask.
    #[cfg(unix)]
    #[test]
    fn shares_the_network_only_when_the_policy_allows_it() {
        let denied = compile_args(&policy(NetworkPolicy::Deny), "sh", &[]).unwrap();
        assert!(!denied.iter().any(|a| a == "--share-net"));
        let allowed = compile_args(&policy(NetworkPolicy::Allow), "sh", &[]).unwrap();
        let pos = allowed.iter().position(|a| a == "--share-net").unwrap();
        let unshare = allowed.iter().position(|a| a == "--unshare-all").unwrap();
        assert!(pos > unshare, "--share-net must follow --unshare-all");
    }

    /// Every deny overlay is compiled after every writable bind, so a deny
    /// masks a wider allow instead of being masked by it.
    #[cfg(unix)]
    #[test]
    fn deny_overlays_come_after_the_binds_they_mask() {
        let mut p = policy(NetworkPolicy::Allow);
        // A granted root that contains a credential store: the store's
        // overlay must land on top of it.
        p.writable = vec!["/home/u".into()];
        p.denied = vec!["/home/u/.ssh".into()];
        let args = compile_args(&p, "sh", &[]).unwrap();
        let last_bind = args.iter().rposition(|a| a == "--bind").unwrap();
        let deny = args
            .iter()
            .position(|a| a == "--tmpfs" && a != "/tmp")
            .unwrap();
        assert!(
            deny > last_bind,
            "deny overlay must be compiled after the writable binds: {args:?}"
        );
    }

    /// A credential that is a plain file is masked by /dev/null, not by a
    /// tmpfs (which cannot mount on a file and would fail the whole launch).
    #[cfg(unix)]
    #[test]
    fn a_file_credential_is_masked_with_dev_null() {
        let mut p = policy(NetworkPolicy::Allow);
        p.denied_files = vec!["/home/u/.netrc".into()];
        let args = compile_args(&p, "sh", &[]).unwrap();
        let i = args.iter().position(|a| a == "/home/u/.netrc").unwrap();
        assert_eq!(
            &args[i - 2..=i],
            ["--ro-bind", "/dev/null", "/home/u/.netrc"]
        );
    }

    #[test]
    fn refuses_a_relative_cwd_or_path() {
        let mut p = policy(NetworkPolicy::Deny);
        p.cwd = "project".into();
        assert!(compile_args(&p, "sh", &[]).is_err());
        let mut p = policy(NetworkPolicy::Deny);
        p.writable.push("../etc".into());
        assert!(compile_args(&p, "sh", &[]).is_err());
    }

    #[test]
    fn refuses_to_bind_the_host_root_writable() {
        let mut p = policy(NetworkPolicy::Deny);
        p.writable = vec!["/".into()];
        assert!(compile_args(&p, "sh", &[]).is_err());
    }

    /// The decision → command path the transport uses: unconfined passes the
    /// command through untouched, confined rewrites it to bwrap.
    #[test]
    fn an_unconfined_decision_passes_the_command_through() {
        let unconfined = SandboxDecision::Unconfined(Unconfined::ExplicitNone);
        let cmd = unconfined.confine("claude", &["-p".to_string()]).unwrap();
        assert_eq!(cmd.program, "claude");
        assert_eq!(cmd.args, vec!["-p"]);
    }

    #[cfg(unix)]
    #[test]
    fn a_confined_decision_rewrites_the_command_to_the_launcher() {
        let confined = SandboxDecision::Confined {
            kind: SandboxKind::Bwrap,
            policy: Box::new(policy(NetworkPolicy::Allow)),
        };
        let cmd = confined.confine("claude", &[]).unwrap();
        assert_eq!(cmd.program, "bwrap");
        assert_eq!(cmd.args.last().unwrap(), "claude");
    }

    /// A serve backend is `unsupported`, and an explicit request on one is a
    /// hard error naming the choices — never a silent "none".
    #[test]
    fn serve_backends_cannot_be_asked_to_confine() {
        let spec = SandboxSpec {
            backend: "opencode",
            support: crate::sandbox::SandboxSupport::Serve,
            request: SandboxRequest::Auto,
            network: NetworkPolicy::Backend,
            cwd: Path::new("/tmp"),
            extra_writable: &[],
            launch_env: &[],
        };
        let decision = decide(&spec).unwrap();
        assert!(matches!(
            decision,
            SandboxDecision::Unconfined(Unconfined::ServeUnsupported(_))
        ));

        let spec = SandboxSpec {
            request: SandboxRequest::Bwrap,
            ..spec
        };
        let err = decide(&spec).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("opencode"), "{msg}");
        assert!(msg.contains("sandbox = \"none\""), "{msg}");
        // Typed, so the wire answers `not_supported` rather than a daemon
        // bug (see rpc::error_payload).
        assert!(
            err.chain().any(|c| c
                .downcast_ref::<crate::sandbox::SandboxUnsupported>()
                .is_some()),
            "the refusal must stay typed: {msg}"
        );
    }

    /// An explicit opt-out is honored everywhere, serve mode included.
    #[test]
    fn explicit_none_is_honored_on_any_backend() {
        let spec = SandboxSpec {
            backend: "opencode",
            support: crate::sandbox::SandboxSupport::Serve,
            request: SandboxRequest::None,
            network: NetworkPolicy::Backend,
            cwd: Path::new("/tmp"),
            extra_writable: &[],
            launch_env: &[],
        };
        assert_eq!(
            decide(&spec).unwrap(),
            SandboxDecision::Unconfined(Unconfined::ExplicitNone)
        );
    }

    /// The probe answers without hanging, on a host with or without bwrap.
    #[test]
    fn probe_reports_a_status() {
        let status = Bwrap.status();
        assert_eq!(status.kind, SandboxKind::Bwrap);
        assert!(!status.detail.is_empty());
        if !status.installed {
            assert!(!status.usable);
        }
    }
}
