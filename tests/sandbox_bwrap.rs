//! bwrap driver, end to end: the "fake backend" is a plain shell command, so
//! what is exercised is the compiled sandbox itself — the mount layout, the
//! deny overlays, and the namespace flags — not an agent CLI.
//!
//! Every test here guards itself: a host without bubblewrap, or with
//! unprivileged user namespaces disabled (Debian's
//! `kernel.unprivileged_userns_clone=0`, Ubuntu 24.04's AppArmor policy),
//! cannot run any of it, and the test says why and returns instead of
//! failing. CI installs bubblewrap on the Linux leg so the guards normally
//! pass there; the compiler unit tests in `src/sandbox/` run everywhere and
//! are what pins the policy shape.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use damon_core::sandbox::{
    self, NetworkPolicy, SandboxCommand, SandboxDecision, SandboxKind, SandboxPolicy,
};

/// A scratch tree under the system temp dir that cleans itself up.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("damon-sandbox-it-{}-{name}", std::process::id()));
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
        std::fs::canonicalize(p).unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// True when this host can run the driver at all; prints the reason when it
/// cannot, so a skipped test is visible in the log rather than silent.
fn bwrap_usable() -> bool {
    match sandbox::driver_for(SandboxKind::Bwrap) {
        Some(driver) => {
            let status = driver.status();
            if !status.usable {
                eprintln!("skipping: {}", status.detail);
            }
            status.usable
        }
        None => {
            eprintln!("skipping: no bwrap driver in this build");
            false
        }
    }
}

fn policy(
    work: &Path,
    writable: Vec<PathBuf>,
    denied: Vec<PathBuf>,
    net: NetworkPolicy,
) -> SandboxPolicy {
    SandboxPolicy {
        backend: "fake".to_string(),
        cwd: work.to_path_buf(),
        writable,
        denied,
        denied_files: Vec::new(),
        network: net,
    }
}

/// Compile `sh -c <script>` through the driver — the same
/// `SandboxDecision::confine` call the transport makes.
fn confined(policy: &SandboxPolicy, script: &str) -> SandboxCommand {
    let decision = SandboxDecision::Confined {
        kind: SandboxKind::Bwrap,
        policy: Box::new(policy.clone()),
    };
    decision
        .confine("sh", &["-c".to_string(), script.to_string()])
        .expect("confine")
}

fn run(policy: &SandboxPolicy, script: &str) -> std::process::Output {
    let cmd = confined(policy, script);
    assert_eq!(cmd.program, "bwrap", "the launcher is the sandbox");
    Command::new(&cmd.program)
        .args(&cmd.args)
        .output()
        .expect("run the sandboxed command")
}

/// 1/4 — the workdir is writable, and nothing outside it is.
#[test]
fn writes_inside_the_workdir_land_and_writes_outside_are_denied() {
    if !bwrap_usable() {
        return;
    }
    let s = Scratch::new("workdir");
    let work = s.dir("work");
    let outside = s.path().join("escaped.txt");
    let policy = policy(&work, vec![work.clone()], Vec::new(), NetworkPolicy::Allow);

    // Inside: the session's own worktree.
    let inside = work.join("written.txt");
    let out = run(&policy, &format!("echo inside > {}", inside.display()));
    assert!(
        out.status.success(),
        "writing the workdir must work: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(inside.exists(), "the write reached the host workdir");

    // Outside: the parent of the workdir, which the policy never named.
    let out = run(&policy, &format!("echo outside > {}", outside.display()));
    assert!(
        !out.status.success(),
        "writing outside the workdir must fail"
    );
    assert!(!outside.exists(), "nothing escaped the workdir");
}

/// 2/4 — the host root is readable and read-only.
#[test]
fn the_host_root_is_read_only() {
    if !bwrap_usable() {
        return;
    }
    let s = Scratch::new("root");
    let work = s.dir("work");
    let policy = policy(&work, vec![work.clone()], Vec::new(), NetworkPolicy::Allow);

    let read = run(&policy, "ls / >/dev/null && echo visible");
    assert!(read.status.success(), "the host root stays readable");
    assert!(String::from_utf8_lossy(&read.stdout).contains("visible"));

    // /etc is part of the read-only root: a write there must fail.
    let write = run(&policy, "touch /etc/damon-sandbox-probe 2>&1");
    assert!(
        !write.status.success(),
        "writing the read-only root must fail: {}",
        String::from_utf8_lossy(&write.stdout)
    );
    assert!(!Path::new("/etc/damon-sandbox-probe").exists());
}

/// 3/4 — a credential store is masked even though the root bind reaches it,
/// both as a directory (tmpfs) and as a plain file (/dev/null).
#[test]
fn credential_paths_are_invisible_inside_the_sandbox() {
    if !bwrap_usable() {
        return;
    }
    let s = Scratch::new("credential");
    let work = s.dir("work");
    let ssh = s.dir("home/.ssh");
    let key = ssh.join("id_probe");
    std::fs::write(&key, "TOPSECRET-KEY-MATERIAL").unwrap();
    let home = s.dir("home");
    let netrc = home.join(".netrc");
    std::fs::write(&netrc, "machine example.com password TOPSECRET-NETRC").unwrap();

    let policy = SandboxPolicy {
        backend: "fake".to_string(),
        cwd: work.clone(),
        writable: vec![work.clone()],
        denied: vec![std::fs::canonicalize(&ssh).unwrap()],
        denied_files: vec![std::fs::canonicalize(&netrc).unwrap()],
        network: NetworkPolicy::Allow,
    };

    let out = run(
        &policy,
        &format!("cat {} 2>/dev/null; echo done", key.display()),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("TOPSECRET"),
        "the credential file must not be readable: {stdout}"
    );

    // And the directory itself is empty, not merely unreadable.
    let listing = run(&policy, &format!("ls -A {} 2>/dev/null", ssh.display()));
    assert!(
        String::from_utf8_lossy(&listing.stdout).trim().is_empty(),
        "the credential directory must be masked: {}",
        String::from_utf8_lossy(&listing.stdout)
    );

    // The file-shaped credential reads as empty rather than failing the
    // launch — a tmpfs would have refused to mount there at all.
    let out = run(&policy, &format!("cat {} 2>/dev/null", netrc.display()));
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains("TOPSECRET"),
        "the file credential must be masked: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// 4/4 — egress is denied by default and shared back on request. Asserted
/// against a listener in this process, so the test needs no internet.
#[test]
fn the_network_is_denied_unless_the_policy_allows_it() {
    if !bwrap_usable() {
        return;
    }
    if Command::new("bash").arg("-c").arg("true").output().is_err() {
        eprintln!("skipping: bash (with /dev/tcp) is not available");
        return;
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
    let port = listener.local_addr().unwrap().port();
    // Accept in the background so a successful connect does not sit in the
    // backlog. The thread dies with the test.
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            if stream.is_err() {
                return;
            }
        }
    });

    let s = Scratch::new("network");
    let work = s.dir("work");
    let script = format!("bash -c 'exec 3<>/dev/tcp/127.0.0.1/{port}' && echo connected");

    let denied = policy(&work, vec![work.clone()], Vec::new(), NetworkPolicy::Deny);
    let out = run(&denied, &script);
    assert!(
        !out.status.success(),
        "a denied network must not reach the host listener"
    );

    let allowed = policy(&work, vec![work.clone()], Vec::new(), NetworkPolicy::Allow);
    let out = run(&allowed, &script);
    assert!(
        out.status.success(),
        "an allowed network must reach the host listener: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The wiring, not just the compiler: the transport spawns the fake backend
/// *through* the driver, so what the backend can see is the confined view.
#[tokio::test]
async fn the_transport_spawns_the_backend_inside_the_sandbox() {
    if !bwrap_usable() {
        return;
    }
    use damon_core::backend::transport::NdjsonTransport;

    let s = Scratch::new("wiring");
    let work = s.dir("work");
    let ssh = s.dir("home/.ssh");
    let key = ssh.join("id_probe");
    std::fs::write(&key, "TOPSECRET-KEY-MATERIAL").unwrap();

    let policy = policy(
        &work,
        vec![work.clone()],
        vec![std::fs::canonicalize(&ssh).unwrap()],
        NetworkPolicy::Allow,
    );
    let decision = SandboxDecision::Confined {
        kind: SandboxKind::Bwrap,
        policy: Box::new(policy),
    };
    // A "backend" that answers with one JSON frame describing what it saw.
    let script = format!(
        "if grep -q TOPSECRET {} 2>/dev/null; then printf '{{\"secret\":true}}\\n'; else printf '{{\"secret\":false}}\\n'; fi",
        key.display()
    );
    let transport = NdjsonTransport::spawn(
        "sh",
        &["-c".to_string(), script],
        &Default::default(),
        &work,
        Some(&decision),
    )
    .await
    .expect("spawn through the sandbox");

    let mut frames = transport.subscribe();
    let frame = tokio::time::timeout(Duration::from_secs(30), frames.recv())
        .await
        .expect("a frame from the fake backend")
        .expect("frame");
    assert_eq!(
        frame["secret"], false,
        "the backend ran inside the sandbox: {frame}"
    );
    transport.shutdown().await;
}
