//! Protocol-agnostic stdio NDJSON transport for agent backends.
//!
//! One child process, newline-delimited JSON both ways. This layer owns
//! only framing and process plumbing — spawn, capped line reads, stderr
//! draining, and a shared pending-request map. Request/response
//! correlation differs per protocol (Claude uses `request_id`, Codex
//! JSON-RPC `id`, OMP typed frames with `id`), so each backend matches
//! its own responses; the map is shared.
//!
//! Generalized from the old ACP `AgentProcess`: same spawn discipline,
//! same line caps, same stderr drain — minus every ACP method name.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{Context, bail};
use serde_json::{Value, json};
use std::process::Stdio;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, broadcast, oneshot, watch};

/// Hard cap on one stdout line. Protocol frames are small; a runaway
/// agent emitting a megabyte-long line must not grow the read buffer
/// without bound — the line is dropped, the stream lives.
const MAX_LINE_BYTES: usize = 4 * 1024 * 1024;
/// stderr lines are diagnostics, not protocol: keep the head of each
/// line and drop the tail instead of dropping the whole line.
const STDERR_LINE_CAP: usize = 8 * 1024;

/// Pending request waiters keyed by the protocol's own id shape
/// (stringified — JSON-RPC numeric ids and Claude's string request_ids
/// share the map).
type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, Value>>>>>;

/// A live backend subprocess with NDJSON plumbing.
pub struct NdjsonTransport {
    writer: Mutex<tokio::process::ChildStdin>,
    /// Every parsed stdout frame, broadcast to the owning backend.
    frames: broadcast::Sender<Value>,
    pending: PendingMap,
    closed: AtomicBool,
    child: Mutex<Child>,
    last_used: AtomicU64,
    /// Fires once the process exits (stdout EOF, read error, or
    /// shutdown) — session pumps select on it so a dead backend fails
    /// the in-flight turn instead of leaving waiters on a silent pipe.
    exited: watch::Sender<bool>,
}

impl NdjsonTransport {
    /// Spawn the process and start the stdout/stderr reader tasks.
    /// No handshake happens here — the backend drives its own.
    pub async fn spawn(
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
        cwd: &Path,
    ) -> anyhow::Result<Arc<Self>> {
        // Windows resolves `npx` to `npx.cmd`, which CreateProcess cannot
        // exec directly — batch files need the cmd interpreter.
        #[cfg(windows)]
        let (command, args) = match windows_batch(command) {
            Some((cmd, mut cmd_args)) => {
                cmd_args.extend(args.iter().cloned());
                (cmd, cmd_args)
            }
            None => (command.to_string(), args.to_vec()),
        };
        #[cfg(not(windows))]
        let (command, args) = (command.to_string(), args.to_vec());

        let mut cmd = Command::new(&command);
        cmd.args(&args)
            .envs(env)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = cmd
            .spawn()
            .with_context(|| format!("cannot spawn `{command}`"))?;
        let stdin = child.stdin.take().context("stdin not piped")?;
        let stdout = child.stdout.take().context("stdout not piped")?;
        let stderr = child.stderr.take().context("stderr not piped")?;

        let (frames, _) = broadcast::channel(512);
        let me = Arc::new(Self {
            writer: Mutex::new(stdin),
            frames: frames.clone(),
            pending: Arc::new(Mutex::new(HashMap::new())),
            closed: AtomicBool::new(false),
            child: Mutex::new(child),
            last_used: AtomicU64::new(epoch_secs()),
            exited: watch::Sender::new(false),
        });
        // stderr must never block protocol I/O: a dedicated task drains
        // it line by line into the daemon log. Auth/login failures and
        // CLI errors land here — dropping them made those look like
        // silent hangs. Bounded reads keep a chatty backend from
        // ballooning memory.
        {
            let backend = command.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut buf: Vec<u8> = Vec::new();
                loop {
                    match read_capped_line(&mut reader, &mut buf, STDERR_LINE_CAP).await {
                        Ok((true, _)) => {
                            let line = String::from_utf8_lossy(&buf).trim().to_string();
                            if !line.is_empty() {
                                tracing::warn!(backend = %backend, "backend stderr: {line}");
                            }
                        }
                        Ok((false, _)) => return, // EOF
                        Err(_) => return,
                    }
                }
            });
        }

        {
            let me = me.clone();
            tokio::spawn(async move {
                read_loop(stdout, me).await;
            });
        }

        Ok(me)
    }

    /// Send one JSON frame as a single line.
    pub async fn send(&self, frame: Value) -> anyhow::Result<()> {
        self.touch();
        let mut w = self.writer.lock().await;
        w.write_all(format!("{}\n", serde_json::to_string(&frame)?).as_bytes())
            .await
            .context("backend pipe closed")?;
        w.flush().await.context("backend pipe flush failed")
    }

    /// Subscribe to every inbound stdout frame (parsed). Lagging
    /// receivers get `RecvError::Lagged` — backends must tolerate gaps.
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.frames.subscribe()
    }

    /// A receiver that fires after the process exits — stdout EOF, read
    /// error, or `shutdown()`. Pumps use it to stop promptly instead of
    /// blocking on the still-open broadcast channel forever.
    pub fn exited(&self) -> watch::Receiver<bool> {
        self.exited.subscribe()
    }

    /// Whether the subprocess is still alive.
    pub fn is_alive(&self) -> bool {
        !self.closed.load(Ordering::SeqCst)
    }

    /// Mark the process as used right now — the idle sweep kills
    /// transports whose last touch is older than the configured window.
    pub fn touch(&self) {
        self.last_used.store(epoch_secs(), Ordering::Relaxed);
    }

    /// Seconds since the last `touch()` (or since spawn).
    pub fn idle_secs(&self) -> u64 {
        epoch_secs().saturating_sub(self.last_used.load(Ordering::Relaxed))
    }

    /// Kill the subprocess and mark it closed. Safe on an already-exited
    /// child — kill on the owned handle reaps or no-ops.
    pub async fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let _ = self.exited.send(true);
        let mut child = self.child.lock().await;
        let _ = child.kill().await;
    }

    /// Register a waiter for a response carrying `id`. Returns the
    /// receiver; the caller sends the frame itself (id shapes differ).
    /// On timeout/drop the caller removes the entry.
    pub async fn expect(&self, id: impl Into<String>) -> oneshot::Receiver<Result<Value, Value>> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.into(), tx);
        rx
    }

    /// Resolve a pending waiter with a result. Returns false when no
    /// waiter was registered for `id` (late/duplicate response).
    pub async fn resolve(&self, id: &str, result: Result<Value, Value>) -> bool {
        if let Some(tx) = self.pending.lock().await.remove(id) {
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }

    /// Drop a waiter without resolving (caller-side timeout cleanup).
    pub async fn forget(&self, id: &str) {
        self.pending.lock().await.remove(id);
    }
}

/// Line-by-line stdout reader: parses each line as JSON, broadcasts it,
/// and on EOF fails every pending request so waiters never hang on a
/// dead process. Stray non-JSON lines are tolerated (agents log noise).
async fn read_loop(stdout: tokio::process::ChildStdout, me: Arc<NdjsonTransport>) {
    let mut reader = BufReader::new(stdout);
    let mut buf: Vec<u8> = Vec::new();
    loop {
        match read_capped_line(&mut reader, &mut buf, MAX_LINE_BYTES).await {
            Ok((true, over_cap)) => {
                if over_cap {
                    tracing::warn!(bytes = buf.len(), "dropping oversized backend frame");
                    continue;
                }
                let Ok(msg) = serde_json::from_slice::<Value>(&buf) else {
                    // Stray stdout noise — tolerate, don't kill the stream.
                    continue;
                };
                // Broadcast is best-effort: no subscribers is not an error.
                let _ = me.frames.send(msg);
            }
            Ok((false, _)) => break, // EOF
            Err(e) => {
                tracing::debug!(error = %e, "backend stdout read failed");
                break;
            }
        }
    }
    me.closed.store(true, Ordering::SeqCst);
    // Unblock every pending request — a dead process never answers.
    let mut pending = me.pending.lock().await;
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(json!({"message": "backend process exited"})));
    }
    // Wake exit watchers.
    let _ = me.exited.send(true);
}

/// Read one `\n`-terminated line into `buf` (newline stripped), never
/// holding more than `cap` bytes. Returns `(true, over_cap)` with the
/// line in `buf`, `(false, _)` at EOF. A line longer than `cap` is
/// consumed and reported with only its first `cap` bytes retained.
async fn read_capped_line(
    reader: &mut BufReader<impl tokio::io::AsyncRead + Unpin>,
    buf: &mut Vec<u8>,
    cap: usize,
) -> std::io::Result<(bool, bool)> {
    use tokio::io::AsyncBufReadExt;
    buf.clear();
    let mut over_cap = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok((false, over_cap)); // EOF
        }
        let newline = available.iter().position(|b| *b == b'\n');
        let (take, done) = match newline {
            Some(pos) => (pos + 1, true),
            None => (available.len(), false),
        };
        let chunk = &available[..take];
        let keep = cap.saturating_sub(buf.len());
        if chunk.len() <= keep {
            buf.extend_from_slice(chunk);
        } else {
            buf.extend_from_slice(&chunk[..keep]);
            over_cap = true;
        }
        reader.consume(take);
        if done {
            if buf.last() == Some(&b'\n') {
                buf.pop();
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
            }
            return Ok((true, over_cap));
        }
    }
}

/// Current time as epoch seconds, for the idle bookkeeping.
fn epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// PATH lookup without shelling out to `which` — checks each PATH entry
/// for `name` plus the Windows executable extensions.
#[cfg(windows)]
fn which(name: &str) -> Option<std::path::PathBuf> {
    let path_ext: Vec<String> = std::env::var("PATHEXT")
        .unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string())
        .split(';')
        .map(|e| e.to_ascii_lowercase())
        .collect();
    for dir in std::env::split_paths(&std::env::var_os("PATH")?) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        for ext in &path_ext {
            let with_ext = dir.join(format!("{name}{ext}"));
            if with_ext.is_file() {
                return Some(with_ext);
            }
        }
    }
    None
}

/// Windows cannot exec `.cmd`/`.bat` files directly — they need the cmd
/// interpreter. Returns `("cmd.exe", ["/c", resolved_path])` when the
/// command resolves to a batch file, else None.
#[cfg(windows)]
fn windows_batch(command: &str) -> Option<(String, Vec<String>)> {
    let path = Path::new(command);
    let resolved = if path.is_absolute() || command.contains(['/', '\\']) {
        Path::new(command).to_path_buf()
    } else {
        which(command)?
    };
    let ext = resolved.extension()?.to_str()?.to_ascii_lowercase();
    if ext == "cmd" || ext == "bat" {
        Some((
            "cmd.exe".to_string(),
            vec!["/c".to_string(), resolved.to_string_lossy().into_owned()],
        ))
    } else {
        None
    }
}

/// Helper for backends: run a request/response round-trip with timeout.
/// `id` is the correlation key already embedded in `frame`.
pub async fn request_roundtrip(
    transport: &Arc<NdjsonTransport>,
    id: impl Into<String>,
    frame: Value,
    timeout: std::time::Duration,
) -> anyhow::Result<Value> {
    let id = id.into();
    let rx = transport.expect(id.clone()).await;
    transport.send(frame).await?;
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(Ok(v))) => Ok(v),
        Ok(Ok(Err(e))) => bail!("{}", format_error(&e)),
        Ok(Err(_)) => bail!("backend dropped the request"),
        Err(_) => {
            transport.forget(&id).await;
            bail!("backend timed out after {timeout:?}")
        }
    }
}

fn format_error(e: &Value) -> String {
    if let Some(m) = e["message"].as_str() {
        m.to_string()
    } else {
        e.to_string()
    }
}
