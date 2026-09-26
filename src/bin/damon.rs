use anyhow::Context;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use damon_core::client::{ClientEvent, DamonClient};
use serde_json::{Value, json};
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(name = "damon", version, about = "Damon agent core CLI client")]
struct Args {
    /// Daemon WS URL [default: ~/.damon/daemon.json, else ws://127.0.0.1:9470/ws]
    #[arg(long)]
    url: Option<String>,
    /// Auth token (or set DAMON_TOKEN)
    #[arg(long, env = "DAMON_TOKEN")]
    token: Option<String>,
    /// Connect through a relay: --relay ws://relay:8080 --relay-name mydaemon
    #[arg(long, requires = "relay_name")]
    relay: Option<String>,
    /// Daemon name registered on the relay
    #[arg(long)]
    relay_name: Option<String>,
    /// Emit machine-readable JSON instead of tab-separated text
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Daemon health check (HTTP /health)
    Health,
    /// List sessions
    Sessions,
    /// Interactive chat REPL
    Chat {
        /// Resume an existing session id
        #[arg(long)]
        session: Option<String>,
        /// Backend id (a catalog backend like `claude`/`codex`, or a config-defined one)
        #[arg(long, visible_alias = "model")]
        backend: Option<String>,
    },
    /// Resume an existing session and enter the chat REPL
    Resume {
        /// Session id to resume
        id: String,
    },
    /// Delete a session and its history
    Delete {
        /// Session id to delete
        id: String,
    },
    /// One-shot prompt
    Prompt {
        text: String,
        #[arg(long)]
        session: Option<String>,
        /// Backend id when creating a new session
        #[arg(long, visible_alias = "model")]
        backend: Option<String>,
    },
    /// Full-text search over all session history
    Search {
        /// Search query — terms AND together; "exact phrase" and AND/OR/NOT supported
        query: String,
        /// Max results
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    /// Rename a session (sets its display title)
    Rename {
        /// Session id
        id: String,
        /// New title
        title: String,
    },
    /// Token usage: per-agent totals, or one session's with --session
    Usage {
        /// Session id for per-session totals
        #[arg(long)]
        session: Option<String>,
    },
    /// Fork a session: copy its history into a new session
    Fork {
        /// Session id to fork
        id: String,
        /// Fork only up to and including this message id
        #[arg(long)]
        upto: Option<i64>,
    },
    /// List native sessions a backend made outside damon; --attach N
    /// imports one into the chat REPL
    Import {
        /// Backend id to scan (e.g. claude, codex, omp)
        backend: String,
        /// Directory to scan (default: current directory)
        #[arg(long)]
        cwd: Option<String>,
        /// 1-based index from the printed list — resume it and chat
        #[arg(long)]
        attach: Option<usize>,
    },
    Export {
        /// Session id to export
        id: String,
        /// Markdown transcript instead of JSON
        #[arg(long)]
        md: bool,
    },
    /// Snapshot the daemon's SQLite store (safe while damond runs)
    Backup {
        /// Output path (default: <data_dir>/damon-backup-YYYYMMDD-HHMMSS.db)
        #[arg(long)]
        out: Option<std::path::PathBuf>,
    },
    /// Check connectivity, auth, and agent availability
    Doctor,
    /// Print shell completions to stdout
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = Args::command().get_matches();
    let args = Args::from_arg_matches(&matches).expect("clap-validated matches");
    // A --token on argv is readable by any local user via the process
    // list; the DAMON_TOKEN env alternative is not. Keep the flag for
    // scripts, but warn once so the exposure is at least visible.
    if matches.value_source("token") == Some(clap::parser::ValueSource::CommandLine) {
        eprintln!("warning: --token is visible in process lists; prefer DAMON_TOKEN env");
    }
    // --url wins; absent that, follow the running daemon's discovery
    // file before falling back to the compiled-in default.
    let url = args
        .url
        .clone()
        .or_else(discovery_url)
        .unwrap_or_else(|| "ws://127.0.0.1:9470/ws".to_string());
    if let Cmd::Health = args.cmd {
        anyhow::ensure!(
            args.relay.is_none(),
            "--relay is not supported for `damon health` — query the daemon's /health endpoint directly"
        );
        let url = url
            .replacen("wss://", "https://", 1)
            .replacen("ws://", "http://", 1);
        let url = format!(
            "{}/health",
            url.trim_end_matches('/').trim_end_matches("/ws")
        );
        // A hung daemon must not hang the health check forever.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?;
        let resp = http.get(&url).send().await?.error_for_status()?;
        outln(resp.text().await?);
        return Ok(());
    }

    // Backup opens the store file directly (VACUUM INTO snapshot) — no
    // daemon connection, and it works whether damond is running or not.
    if let Cmd::Backup { out } = args.cmd {
        return backup(out).await;
    }

    // Completions needs no daemon connection.
    if let Cmd::Completions { shell } = args.cmd {
        let mut cmd = <Args as clap::CommandFactory>::command();
        let name = cmd.get_name().to_string();
        clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
        return Ok(());
    }

    let client = match (&args.relay, &args.relay_name) {
        (Some(relay), Some(name)) => {
            let token = args.token.as_deref().unwrap_or("");
            DamonClient::connect_relay(relay, name, token).await?
        }
        _ => DamonClient::connect(&url, args.token.as_deref()).await?,
    };
    let init = client.hello().await?;

    match args.cmd {
        Cmd::Health => unreachable!(),
        Cmd::Doctor => {
            let backends = init["backends"].as_array().map(|a| a.len()).unwrap_or(0);
            let methods = init["methods"].as_array().map(|a| a.len()).unwrap_or(0);
            if args.json {
                outln(
                    json!({
                        "ok": true,
                        "version": init["version"],
                        "backends": backends,
                        "methods": methods,
                    })
                    .to_string(),
                );
            } else {
                outln(format!(
                    "ok — damon {} · {} backends · {} methods",
                    init["version"].as_str().unwrap_or("?"),
                    backends,
                    methods,
                ));
            }
        }
        Cmd::Sessions => {
            let sessions = client.list_sessions(None, None).await?;
            if args.json {
                let arr: Vec<Value> = sessions
                    .iter()
                    .map(|(id, created, backend, title)| {
                        json!({"sessionId": id, "createdAt": created, "backend": backend, "title": title})
                    })
                    .collect();
                outln(json!(arr).to_string());
            } else {
                for (id, created, backend, title) in sessions {
                    let label = if title.is_empty() { &id } else { &title };
                    outln(format!("{id}\t{created}\t{backend}\t{label}"));
                }
            }
        }
        Cmd::Usage { session } => {
            let v = client.usage(session.as_deref()).await?;
            if args.json {
                outln(v.to_string());
            } else {
                // Rollup rows: {model, contextUsed, contextSize,
                // costUsd, turns}; the per-session result is the same
                // fields plus sessionId, at the top level.
                let fmt_ctx = |m: &Value| {
                    let used = m["contextUsed"].as_u64().unwrap_or(0);
                    let size = m["contextSize"].as_u64().unwrap_or(0);
                    if size > 0 {
                        format!("{used}/{size} ctx")
                    } else {
                        format!("{used} ctx")
                    }
                };
                let fmt_cost = |m: &Value| {
                    let c = m["costUsd"].as_f64().unwrap_or(0.0);
                    if c > 0.0 {
                        format!("${c:.4}")
                    } else {
                        "-".to_string()
                    }
                };
                if let Some(models) = v["sessions"].as_array() {
                    for m in models {
                        let turns = m["turns"].as_u64().unwrap_or(0);
                        outln(format!(
                            "{}\t{}\t{}\t{} {}",
                            m["model"].as_str().unwrap_or(""),
                            fmt_ctx(m),
                            fmt_cost(m),
                            turns,
                            if turns == 1 { "turn" } else { "turns" },
                        ));
                    }
                } else {
                    let turns = v["turns"].as_u64().unwrap_or(0);
                    outln(format!(
                        "{}\t{}\t{}\t{} {}",
                        v["sessionId"].as_str().unwrap_or(""),
                        fmt_ctx(&v),
                        fmt_cost(&v),
                        turns,
                        if turns == 1 { "turn" } else { "turns" },
                    ));
                }
            }
        }
        Cmd::Rename { id, title } => {
            client.rename_session(&id, &title).await?;
            if args.json {
                outln(json!({"renamed": id}).to_string());
            } else {
                outln(format!("renamed {id}"));
            }
        }
        Cmd::Chat { session, backend } => {
            let mut events = client.events().await;
            let session_id = match session {
                // Resume brings the session live and subscribes this
                // connection; history replays from session.messages.
                Some(s) => resume_and_replay(&client, &s).await?,
                None => {
                    client
                        .create_session(backend.as_deref(), &cwd(), None)
                        .await?
                }
            };
            chat_loop(&client, &mut events, &session_id).await?;
        }
        Cmd::Resume { id } => {
            let mut events = client.events().await;
            let session_id = resume_and_replay(&client, &id).await?;
            chat_loop(&client, &mut events, &session_id).await?;
        }
        Cmd::Delete { id } => {
            client.delete_session(&id).await?;
            if args.json {
                outln(json!({"deleted": id}).to_string());
            } else {
                outln(format!("deleted {id}"));
            }
        }
        Cmd::Fork { id, upto } => {
            let new_id = client.fork_session(&id, upto).await?;
            if args.json {
                outln(json!({"forked": id, "sessionId": new_id}).to_string());
            } else {
                outln(format!("forked {id} → {new_id}"));
            }
        }
        Cmd::Import {
            backend,
            cwd,
            attach,
        } => {
            let found = client.import_sessions(&backend, cwd.as_deref()).await?;
            match attach {
                None => {
                    if args.json {
                        outln(json!({"sessions": found}).to_string());
                    } else if found.is_empty() {
                        outln("no importable sessions");
                    } else {
                        for (i, s) in found.iter().enumerate() {
                            let title = s["title"].as_str().unwrap_or("");
                            let handle = s["handle"]["native_handle"].as_str().unwrap_or("");
                            let dir = s["cwd"].as_str().unwrap_or("");
                            outln(format!("{}: {title}\t{handle}\t{dir}", i + 1));
                        }
                        eprintln!("attach one: damon import {backend} --attach N");
                    }
                }
                Some(n) => {
                    let s = found.get(n.wrapping_sub(1)).with_context(|| {
                        format!("no session #{n} — run `damon import {backend}` to list")
                    })?;
                    let session_id = client
                        .resume_by_handle(&s["handle"], s["title"].as_str(), s["cwd"].as_str())
                        .await?;
                    let mut events = client.events().await;
                    resume_and_replay(&client, &session_id).await?;
                    chat_loop(&client, &mut events, &session_id).await?;
                }
            }
        }
        Cmd::Search { query, limit } => {
            let results = client.search(&query, limit).await?;
            if args.json {
                let arr: Vec<Value> = results
                    .iter()
                    .map(|(sid, mid, snippet)| {
                        json!({"sessionId": sid, "messageId": mid, "snippet": snippet})
                    })
                    .collect();
                outln(json!(arr).to_string());
            } else {
                for (sid, mid, snippet) in results {
                    outln(format!("{sid}:{mid}\t{snippet}"));
                }
            }
        }
        Cmd::Prompt {
            text,
            session,
            backend,
        } => {
            let session_id = match session {
                // turn.start needs a live session — resume attaches one.
                Some(s) => client.resume_session(&s).await?,
                None => {
                    client
                        .create_session(backend.as_deref(), &cwd(), None)
                        .await?
                }
            };
            let mut events = client.events().await;
            // One-shot prompt: no REPL reader exists yet — create the
            // single stdin reader here for permission answers.
            let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            let _pending = run_turn(&client, &mut events, &session_id, &text, &mut stdin).await?;
            outln("");
        }
        Cmd::Export { id, md } => {
            let messages = client.session_messages(&id, Some(u32::MAX), None).await?;
            if md {
                outln(export_markdown(&id, &messages).trim_end());
            } else {
                outln(json!({"sessionId": id, "messages": messages}).to_string());
            }
        }
        // Handled before the client connects — backup needs no daemon.
        Cmd::Backup { .. } => unreachable!("backup handled before connect"),
        // Handled before the client connects — completions needs no daemon.
        Cmd::Completions { .. } => unreachable!("completions handled before connect"),
    }
    Ok(())
}

fn cwd() -> String {
    std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default()
}

/// `damon backup` — snapshot the store the daemon uses. `VACUUM INTO`
/// takes a consistent snapshot even while damond is writing (WAL), so
/// no downtime is needed. The target is never overwritten.
async fn backup(out: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    use damon_core::config::{self, Config};
    use damon_core::store::Store;

    // Prefer the config the running daemon published — a damond booted
    // with --config keeps its store where THAT config points.
    let cfg_path = discovery_config_path().unwrap_or_else(config::default_config_path);
    let cfg = Config::load(&config::ensure_config(&cfg_path)?)?;
    let dir = cfg
        .data_dir
        .clone()
        .unwrap_or_else(config::default_data_dir);
    let db = dir.join("damon.db");
    anyhow::ensure!(
        db.exists(),
        "no store at {} — nothing to back up",
        db.display()
    );
    let out = out.unwrap_or_else(|| dir.join(format!("damon-backup-{}.db", utc_stamp())));
    anyhow::ensure!(
        !out.exists(),
        "refusing to overwrite existing file {}",
        out.display()
    );
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    let store = Store::open(&db).await?;
    store.backup(&out).await?;
    outln(format!("{}", out.display()));
    Ok(())
}

/// `YYYYMMDD-HHMMSS` in UTC for backup filenames. Local time would
/// need a timezone database and a date dependency; a filename only
/// needs second-granularity uniqueness, so UTC is enough.
fn utc_stamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (time, days) = (secs % 86_400, (secs / 86_400) as i64);
    let (h, mi, s) = (time / 3600, time % 3600 / 60, time % 60);
    // Howard Hinnant's civil_from_days: days since epoch → (y, m, d).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}{m:02}{d:02}-{h:02}{mi:02}{s:02}")
}

/// Parsed view of the daemon's `~/.damon/daemon.json` discovery file.
#[derive(serde::Deserialize)]
struct DiscoveryFile {
    port: u16,
    pid: u32,
    tls: bool,
    #[serde(rename = "configPath")]
    config_path: String,
}

/// Read `~/.damon/daemon.json` — the file damond publishes so local
/// clients can find it without parsing its config.
fn read_discovery() -> Option<DiscoveryFile> {
    let path = directories::BaseDirs::new()?
        .home_dir()
        .join(".damon/daemon.json");
    let body = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&body).ok()
}

/// Whether the pid in daemon.json is still alive — a SIGKILLed daemon
/// leaves a stale file that must not redirect clients to a dead port.
fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        // No cheap std pid probe on this platform — trust the file and
        // let the connect fail fast if the daemon is gone.
        let _ = pid;
        true
    }
}

/// Derive the daemon's WS URL from its discovery file. None when the
/// file is absent, unparsable, or belongs to a dead daemon.
fn discovery_url() -> Option<String> {
    let d = read_discovery()?;
    if !pid_alive(d.pid) {
        return None;
    }
    let scheme = if d.tls { "wss" } else { "ws" };
    Some(format!("{scheme}://127.0.0.1:{}/ws", d.port))
}

/// The config path the running daemon booted from, per its discovery
/// file. None when no live daemon published one.
fn discovery_config_path() -> Option<std::path::PathBuf> {
    let d = read_discovery()?;
    if !pid_alive(d.pid) {
        return None;
    }
    let p = std::path::PathBuf::from(d.config_path);
    p.exists().then_some(p)
}

/// Write one line to stdout, exiting quietly with 141 (128+SIGPIPE, the
/// killed-by-SIGPIPE convention) when the pipe is gone. Rust ignores
/// println! with a noisy exit 101.
fn outln(s: impl std::fmt::Display) {
    use std::io::Write;
    if writeln!(std::io::stdout(), "{s}").is_err() {
        std::process::exit(141);
    }
}

/// Like `outln` but without a trailing newline and with an explicit
/// flush — streamed turn text must reach the terminal immediately.
fn out(s: impl std::fmt::Display) {
    use std::io::Write;
    let mut stdout = std::io::stdout();
    if write!(stdout, "{s}").is_err() || stdout.flush().is_err() {
        std::process::exit(141);
    }
}

/// Attach to an existing session: `session.resume` brings it live (and
/// subscribes this connection to its events), then the stored history
/// is replayed from `session.messages` — v2 has no push-based replay.
async fn resume_and_replay(client: &DamonClient, session_id: &str) -> anyhow::Result<String> {
    let id = client.resume_session(session_id).await?;
    for m in client.session_messages(&id, Some(u32::MAX), None).await? {
        render_stored(&m);
    }
    Ok(id)
}

/// Render one stored message (a `session.messages` row: `{id,
/// session_id, role, data}`). Live turns stream text without a role
/// prefix; replay marks user turns so the transcript stays readable.
fn render_stored(m: &serde_json::Value) {
    let data = &m["data"];
    match m["role"].as_str() {
        Some("user") => {
            if let Some(t) = content_text(&data["content"]) {
                out(format!("> {t}\n"));
            }
        }
        Some("assistant") => {
            if let Some(t) = content_text(&data["content"]) {
                out(format!("{t}\n"));
            }
            // Legacy rows carry OpenAI-style tool_calls on the
            // assistant message; v2 stores each call as its own
            // "tool" row instead.
            for tc in data["tool_calls"].as_array().into_iter().flatten() {
                let name = tc["function"]["name"].as_str().unwrap_or("tool");
                eprintln!("[tool {name}]");
            }
        }
        Some("tool") => {
            // v2 rows persist the ToolCall itself; legacy rows are
            // OpenAI tool results keyed by tool_call_id.
            if let Some(name) = data["name"].as_str() {
                let status = data["status"].as_str().unwrap_or("completed");
                eprintln!("[tool {name} → {status}]");
            }
        }
        _ => {}
    }
}

/// Text of a stored message's `content`: a plain string, or the text
/// parts of a content-block array. Non-text parts (images) have no
/// terminal rendering and are skipped.
fn content_text(content: &serde_json::Value) -> Option<String> {
    match content {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Array(parts) => {
            let text: Vec<&str> = parts.iter().filter_map(|p| p["text"].as_str()).collect();
            (!text.is_empty()).then(|| text.join("\n"))
        }
        _ => None,
    }
}

/// Render a session as a Markdown transcript — v2 has no export RPC,
/// so the CLI builds it from `session.messages` rows.
fn export_markdown(id: &str, messages: &[Value]) -> String {
    let mut md = format!("# Session {id}\n");
    for m in messages {
        let data = &m["data"];
        match m["role"].as_str() {
            Some("user") => {
                if let Some(t) = content_text(&data["content"]) {
                    md.push_str(&format!("\n## User\n\n{t}\n"));
                }
            }
            Some("assistant") => {
                if let Some(t) = content_text(&data["content"]) {
                    md.push_str(&format!("\n## Assistant\n\n{t}\n"));
                }
            }
            Some("reasoning") => {
                if let Some(t) = content_text(&data["content"]) {
                    md.push_str(&format!("\n> *{t}*\n"));
                }
            }
            Some("tool") => {
                let name = data["name"].as_str().unwrap_or("tool");
                let status = data["status"].as_str().unwrap_or("completed");
                md.push_str(&format!("\n**Tool** `{name}` → {status}\n"));
            }
            _ => {}
        }
    }
    md
}

/// Interactive chat REPL over an existing session. The event receiver is
/// taken once — `events()` hands out the only consumer, so re-taking it
/// per turn would starve every turn after the first.
async fn chat_loop(
    client: &DamonClient,
    events: &mut mpsc::Receiver<ClientEvent>,
    session_id: &str,
) -> anyhow::Result<()> {
    eprintln!("session: {session_id}  (Ctrl-D to quit)");
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut pending: Option<String> = None;
    loop {
        eprint!("> ");
        // A line typed mid-turn is carried over as the next prompt —
        // the type-ahead the old blocking read gave for free.
        let line = match pending.take() {
            Some(l) => l,
            None => {
                let Some(l) = stdin.next_line().await? else {
                    break;
                };
                l
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        // A failed turn (provider error, "session not found", "prompt in
        // progress") must not kill the REPL — only a dead event stream
        // means the connection is gone for good.
        match run_turn(client, events, session_id, &line, &mut stdin).await {
            Ok(next) => pending = next,
            Err(e) => {
                if events.is_closed() {
                    return Err(e);
                }
                eprintln!("[turn error] {e:#}");
            }
        }
    }
    Ok(())
}

/// Run one prompt turn: stream text to stdout, handle permission
/// requests on stderr, return when the turn ends. `turn.start` resolves
/// at turn end — its response arrives as `ClientEvent::TurnDone`,
/// ordered after the session's `session.event` stream.
async fn run_turn(
    client: &DamonClient,
    events: &mut mpsc::Receiver<ClientEvent>,
    session_id: &str,
    text: &str,
    stdin: &mut tokio::io::Lines<tokio::io::BufReader<tokio::io::Stdin>>,
) -> anyhow::Result<Option<String>> {
    client.turn_start(session_id, text).await?;
    // Tool-call updates repeat only the call_id — remember the name
    // from the first event so status lines stay readable.
    let mut tool_names: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // A non-command line typed mid-turn becomes the next prompt once
    // this turn ends — the type-ahead the old blocking read gave.
    let mut pending: Option<String> = None;
    // EOF (`damon prompt` with no tty, piped stdin) disables the stdin
    // branch — otherwise it wins every select! and kills the turn.
    let mut stdin_open = true;
    loop {
        // Race stdin against the event stream: `/cancel` (or `cancel`)
        // stops the running turn; other input is queued for next turn.
        let event = tokio::select! {
            line = async {
                if stdin_open { stdin.next_line().await } else { std::future::pending().await }
            } => {
                match line {
                    Ok(Some(l)) => {
                        let cmd = l.trim();
                        if cmd == "/cancel" || cmd == "cancel" || cmd == "/stop" {
                            if let Err(e) = client.turn_cancel(session_id).await {
                                eprintln!("[cancel failed] {e:#}");
                            }
                        } else if !cmd.is_empty() {
                            pending = Some(l);
                        }
                        continue;
                    }
                    Ok(None) => {
                        stdin_open = false;
                        continue;
                    }
                    Err(e) => anyhow::bail!("stdin: {e}"),
                }
            }
            ev = events.recv() => ev,
        };
        match event {
            Some(ClientEvent::TurnDone {
                session_id: sid,
                result,
            }) if sid == session_id => {
                match result {
                    Ok(v) => {
                        let stop = v["stopReason"].as_str().unwrap_or("?");
                        eprintln!("\n[{stop}]");
                    }
                    Err(e) => anyhow::bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
                }
                return Ok(pending);
            }
            Some(ClientEvent::Event {
                session_id: sid,
                event,
            }) if sid == session_id => match event["type"].as_str() {
                Some("timeline") => match event["kind"].as_str() {
                    Some("assistant_message") => {
                        if let Some(t) = event["text"].as_str() {
                            out(t);
                        }
                    }
                    Some("tool_call") => {
                        let call_id = event["call_id"].as_str().unwrap_or("");
                        let name = event["name"].as_str().unwrap_or("");
                        if !name.is_empty() {
                            tool_names.insert(call_id.to_string(), name.to_string());
                        }
                        let name = tool_names.get(call_id).map(String::as_str).unwrap_or("?");
                        eprintln!(
                            "\n[tool {name} → {}]",
                            event["status"].as_str().unwrap_or("")
                        );
                    }
                    _ => {}
                },
                Some("subagent") => {
                    let f = &event["event"];
                    let name = f["name"]
                        .as_str()
                        .or_else(|| f["agent"].as_str())
                        .or_else(|| f["description"].as_str())
                        .unwrap_or("subagent");
                    let status = f["status"]
                        .as_str()
                        .or_else(|| f["state"].as_str())
                        .or_else(|| f["type"].as_str())
                        .unwrap_or("");
                    eprintln!("\n[subagent {name} {status}]");
                }
                Some("permission_requested") => {
                    answer_permission(client, session_id, &event, stdin).await?;
                }
                _ => {}
            },
            // Events/TurnDone for other sessions, and connection-state
            // mirrors: a drop mid-turn surfaces as a failed TurnDone
            // when the daemon's reply is lost; the reconnect itself
            // needs no rendering here.
            Some(_) => {}
            None => anyhow::bail!("connection closed"),
        }
    }
}

/// Prompt on stderr for a `permission_requested` event and answer it
/// via `permission.respond`. The ask's `actions` are the choices the
/// backend offered — rendered verbatim rather than inventing our own.
async fn answer_permission(
    client: &DamonClient,
    session_id: &str,
    event: &Value,
    stdin: &mut tokio::io::Lines<tokio::io::BufReader<tokio::io::Stdin>>,
) -> anyhow::Result<()> {
    let request_id = event["id"].as_str().unwrap_or_default();
    let title = event["title"]
        .as_str()
        .or(event["name"].as_str())
        .unwrap_or("?");
    let input = match &event["input"] {
        Value::Null => String::new(),
        v => v.to_string(),
    };
    let actions = event["actions"].as_array().cloned().unwrap_or_default();
    let menu: Vec<String> = actions
        .iter()
        .enumerate()
        .map(|(i, a)| format!("{}:{}", i + 1, a["label"].as_str().unwrap_or("?")))
        .collect();
    eprint!("\n[permission] {title} {input}\n[{}] ", menu.join(" "));
    use std::io::Write;
    let _ = std::io::stderr().flush();
    // Reuse the REPL's single buffered stdin — a second BufReader on
    // the same fd would race it for bytes.
    let line = stdin.next_line().await?.unwrap_or_default();
    let choice = line.trim();
    let picked = choice
        .parse::<usize>()
        .ok()
        .filter(|n| *n >= 1)
        .and_then(|n| actions.get(n - 1))
        .or_else(|| match choice {
            "y" | "Y" | "yes" | "a" | "A" | "always" => {
                actions.iter().find(|a| a["behavior"] == "allow")
            }
            _ => actions.iter().find(|a| a["behavior"] == "deny"),
        });
    let response = match picked {
        Some(a) => json!({
            "behavior": a["behavior"].as_str().unwrap_or("deny"),
            "action_id": a["id"],
        }),
        // No deny action offered — refuse without one.
        None => json!({"behavior": "deny", "interrupt": false}),
    };
    client
        .respond_to_permission(session_id, request_id, response)
        .await
}
