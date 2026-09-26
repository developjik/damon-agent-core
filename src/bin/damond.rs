use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use damon_core::api::{self, AppState};
use damon_core::config::{self, Config, SecretRef, SharedConfig};
use damon_core::store::Store;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "damond", version, about = "Damon agent core daemon")]
struct Args {
    /// Config file path [default: platform config dir]
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Print the resolved config path and exit
    #[arg(long)]
    print_config_path: bool,
    /// Print the JSON-RPC method schema and exit
    #[arg(long)]
    print_rpc_schema: bool,
    /// Manage OS service registration, or run diagnostics
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Install as a user service (launchd/systemd/Task Scheduler)
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// Check agent availability, config, auth token, and the store dir
    /// without starting the daemon
    Doctor,
    /// Show the resolved config path or print the loaded config
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(clap::Subcommand)]
enum ServiceAction {
    /// Write the service definition and enable it
    Install,
    /// Print the service definition without installing
    Print,
    /// Remove the service registration
    Uninstall,
    /// Show the current registration status
    Status,
    /// Start the registered service
    Start,
    /// Stop the registered service
    Stop,
    /// Restart the registered service
    Restart,
}

#[derive(clap::Subcommand)]
enum ConfigAction {
    /// Print the resolved config path
    Path,
    /// Print the config file verbatim
    Show,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let path = args.config.unwrap_or_else(config::default_config_path);
    // Pre-load .env BEFORE tracing init: RUST_LOG from the same .env
    // files Config::load reads must shape the EnvFilter — it used to be
    // built first and silently ignored RUST_LOG. Same precedence as
    // Config::load (config dir first, cwd only in debug); dotenvy never
    // overrides already-set vars, so the later load there stays a no-op.
    if let Some(dir) = path.parent() {
        let _ = dotenvy::from_path(dir.join(".env"));
    }
    #[cfg(debug_assertions)]
    let _ = dotenvy::from_path(Path::new(".env"));
    // Console output is unchanged; the tee also fills the in-daemon
    // ring that `logs.tail`/`logs.follow` (and `damon logs`) serve —
    // no ANSI, so ring lines stay plain text.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "damon=info".into()))
        .with_ansi(false)
        .with_writer(damon_core::logs::MakeTeeWriter::new(
            damon_core::logs::ring(),
        ))
        .init();
    // rustls needs an explicit process-level crypto provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    if args.print_config_path {
        outln(path.display());
        return Ok(());
    }

    if args.print_rpc_schema {
        outln(serde_json::to_string_pretty(
            &damon_core::rpc::rpc_methods(),
        )?);
        return Ok(());
    }

    match args.cmd {
        Some(Cmd::Service { action }) => {
            let exe = std::env::current_exe()?.display().to_string();
            let cfg_path = path.display().to_string();
            match action {
                ServiceAction::Install => {
                    outln(damon_core::service::install(&exe, &cfg_path)?);
                }
                ServiceAction::Print => {
                    out(damon_core::service::print_definition(&exe, &cfg_path));
                }
                ServiceAction::Uninstall => {
                    outln(damon_core::service::uninstall()?);
                }
                ServiceAction::Status => {
                    outln(damon_core::service::status());
                }
                ServiceAction::Start | ServiceAction::Stop | ServiceAction::Restart => {
                    let action = match action {
                        ServiceAction::Start => damon_core::service::Action::Start,
                        ServiceAction::Stop => damon_core::service::Action::Stop,
                        _ => damon_core::service::Action::Restart,
                    };
                    outln(damon_core::service::control(action)?);
                    // Act, then show where the service stands — the
                    // manager's own view, not an assumed success.
                    outln(damon_core::service::status());
                }
            }
            return Ok(());
        }
        Some(Cmd::Config { action }) => {
            match action {
                ConfigAction::Path => outln(path.display()),
                ConfigAction::Show => {
                    // Print the file verbatim — secret refs stay
                    // unresolved, so this never leaks credentials.
                    let p = config::ensure_config(&path)?;
                    out(std::fs::read_to_string(p)?);
                }
            }
            return Ok(());
        }
        Some(Cmd::Doctor) => return doctor(&path).await,
        None => {}
    }

    let path = config::ensure_config(&path)?;
    let cfg = Config::load(&path)?;
    let bind = cfg.bind;
    // Safety: non-loopback bind requires a resolvable auth_token — a
    // remote-reachable daemon without auth is a remote code execution
    // surface, and an unresolvable secret ref must fail at boot, not
    // silently open the API.
    if !bind.ip().is_loopback() {
        let has_token = cfg.auth_token.as_deref().is_some_and(|raw| {
            match SecretRef::parse(raw) {
                Ok(r) => r.resolve().is_ok(),
                // Not a secret ref → a literal token, which is valid.
                Err(_) => true,
            }
        });
        anyhow::ensure!(
            has_token,
            "refusing to bind {bind}: non-loopback requires a resolvable auth_token in config"
        );
    }
    let data_dir = cfg
        .data_dir
        .clone()
        .unwrap_or_else(config::default_data_dir);
    let shared: SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));

    let store = Store::open(&data_dir.join("damon.db")).await?;
    let state = AppState::with_bind(shared.clone(), store, bind).await;
    // Config hot reload: the watcher fires after each successful reload;
    // the backend set is refreshed in place — resolved launch lines
    // rebuild, new [backends.X] blocks appear in backend.list without a
    // restart. Only `bind` keeps requiring a restart.
    if let Some(mut reloaded) = config::watch(path.clone(), shared.clone()) {
        let sessions = state.sessions.clone();
        let shared = shared.clone();
        // The relay tunnel is spawned once below; a [relay] edit lands in
        // the shared config but never reaches the running tunnel.
        let boot_relay = shared.read().relay.clone();
        tokio::spawn(async move {
            while reloaded.recv().await.is_some() {
                let cfg = shared.read().clone();
                if cfg.relay != boot_relay {
                    warn!("relay config changes require restart; running tunnel unchanged");
                }
                sessions.refresh(&cfg);
                info!("config reloaded; backend registry refreshed");
            }
        });
    }
    // Remote relay: dial out so clients can reach us without an inbound port.
    // The registration secret passes as its RAW ref (env:/keychain:/!cmd or
    // literal) — run_tunnel re-resolves it per attempt, so rotation takes
    // effect without a restart and a resolution failure is logged loudly
    // instead of silently registering without the secret.
    if let Some(relay) = &shared.read().relay {
        let state = state.clone();
        let url = relay.url.clone();
        let name = relay.name.clone();
        let secret = relay.secret.clone();
        tokio::spawn(async move {
            damon_core::relay::run_tunnel(state, url, name, secret).await;
        });
    }
    let app = api::router(state.clone());
    // Read TLS paths into locals first — a scrutinee guard would live for
    // the whole match (i.e. the server's lifetime), deadlocking the config
    // watcher's write lock on the first hot reload.
    let tls = {
        let cfg = shared.read();
        (cfg.tls_cert.clone(), cfg.tls_key.clone())
    };
    // Discovery file (~/.damon/daemon.json): local clients read it to find
    // our port without parsing config. Written only AFTER the socket is
    // bound — a discovery entry for a port nothing listens on would send
    // clients to a dead address. The tls flag is derived from the same
    // locals the match below serves with — taking the lock a second time
    // here could race a hot reload between the two reads and advertise
    // the wrong scheme.
    let discovery_tls = tls.0.is_some() && tls.1.is_some();
    // Best-effort, mirroring the no-home path inside discovery::write:
    // discovery is additive, so a failed write degrades to a no-op Guard
    // instead of blocking boot with `?`.
    let write_discovery = || match damon_core::discovery::write(bind, discovery_tls, &path) {
        Ok(guard) => guard,
        Err(e) => {
            warn!(error = %e, "daemon.json write failed; discovery disabled");
            damon_core::discovery::Guard::noop()
        }
    };
    match tls {
        (Some(cert), Some(key)) => {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
                .await
                .context("cannot load TLS cert/key")?;
            // Bind before publishing discovery (see above). std listener
            // + from_tcp_rustls keeps the same bind-then-advertise order
            // as the plain path.
            let listener = std::net::TcpListener::bind(bind)
                .with_context(|| format!("cannot bind {bind} — is another damond running?"))?;
            listener
                .set_nonblocking(true)
                .context("cannot set listener nonblocking")?;
            let _discovery_guard = write_discovery();
            info!(bind = %bind, "damond listening (TLS)");
            let handle = axum_server::Handle::new();
            // Keep the handle so SIGTERM can drain connections instead of
            // dropping mid-request.
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
            });
            axum_server::from_tcp_rustls(listener, tls)
                .context("cannot serve TLS listener")?
                .handle(handle)
                // ConnectInfo installs the peer IP the rate limiter reads.
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await?;
            cancel_live_turns(&state).await;
        }
        (None, None) => {
            // Same axum_server path as TLS so plain HTTP gets the handle's
            // graceful drain too — axum::serve's graceful_shutdown only
            // stops accepting, it doesn't bound in-flight requests.
            let listener = std::net::TcpListener::bind(bind)
                .with_context(|| format!("cannot bind {bind} — is another damond running?"))?;
            listener
                .set_nonblocking(true)
                .context("cannot set listener nonblocking")?;
            let _discovery_guard = write_discovery();
            info!(bind = %bind, "damond listening");
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
            });
            axum_server::from_tcp(listener)
                .context("cannot serve listener")?
                .handle(handle)
                // ConnectInfo installs the peer IP the rate limiter reads.
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await?;
            cancel_live_turns(&state).await;
        }
        _ => anyhow::bail!("tls_cert and tls_key must be set together"),
    }
    info!("damond stopped");
    Ok(())
}

/// Close every live backend session — exiting mid-turn would drop the
/// agent's prompt response on the floor.
async fn cancel_live_turns(state: &Arc<AppState>) {
    state.sessions.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
}

/// `damond doctor` — check agent availability, config, auth token, and
/// the store dir without starting the daemon. Exit 0 when every check
/// passes.
async fn doctor(path: &Path) -> anyhow::Result<()> {
    let mut ok = true;

    let cfg = match Config::load(path) {
        Ok(c) => {
            report(&mut ok, true, format!("config {} — ok", path.display()));
            Some(c)
        }
        Err(e) => {
            report(&mut ok, false, format!("config {} — {e:#}", path.display()));
            None
        }
    };

    // Backends: the whole provider story. Each available backend brings
    // its own login, models, and tools.
    let cfg_for_backends = cfg
        .clone()
        .unwrap_or_else(|| toml::from_str("").expect("empty config parses"));
    let resolved = damon_core::backend::registry::resolve_backends(&cfg_for_backends.backends);
    let mut any_available = false;
    for b in &resolved {
        let status = if b.detected {
            "installed"
        } else {
            "not installed"
        };
        if b.detected {
            any_available = true;
        }
        report(
            &mut ok,
            b.detected,
            format!("backend {} — {status} ({})", b.id, b.auth_hint),
        );
    }
    if !any_available {
        report(
            &mut ok,
            false,
            "no backends available — install claude, codex, or omp, or set [backends.X]",
        );
    }

    if let Some(cfg) = &cfg {
        match &cfg.auth_token {
            None => report(&mut ok, true, "auth_token — not set (loopback only)"),
            Some(raw) => match SecretRef::parse(raw) {
                Ok(r) => match r.resolve() {
                    Ok(_) => report(&mut ok, true, format!("auth_token — {raw} resolved")),
                    Err(e) => report(&mut ok, false, format!("auth_token — {raw}: {e:#}")),
                },
                Err(_) => {
                    // A short one is offline-guessable — the relay
                    // handshake exposes token-derived proofs.
                    if raw.len() < 32 {
                        report(
                            &mut ok,
                            true,
                            "auth_token — literal token set (weak: <32 chars; use `openssl rand -hex 32`)",
                        );
                    } else {
                        report(&mut ok, true, "auth_token — literal token set");
                    }
                }
            },
        }
    }

    let data_dir = cfg
        .as_ref()
        .and_then(|c| c.data_dir.clone())
        .unwrap_or_else(config::default_data_dir);
    match writable_dir(&data_dir) {
        Ok(()) => report(
            &mut ok,
            true,
            format!("store dir {} — writable", data_dir.display()),
        ),
        Err(e) => report(
            &mut ok,
            false,
            format!("store dir {} — {e:#}", data_dir.display()),
        ),
    }

    if ok { Ok(()) } else { std::process::exit(1) }
}

/// Print one doctor check line and fold it into the overall verdict.
fn report(ok: &mut bool, pass: bool, msg: impl std::fmt::Display) {
    outln(format!("{} {msg}", if pass { "ok  " } else { "FAIL" }));
    *ok &= pass;
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
/// flush — piped output must reach the reader immediately.
fn out(s: impl std::fmt::Display) {
    use std::io::Write;
    let mut stdout = std::io::stdout();
    if write!(stdout, "{s}").is_err() || stdout.flush().is_err() {
        std::process::exit(141);
    }
}

/// Create the dir if needed, then prove writability with a probe file.
fn writable_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).context("cannot create directory")?;
    let probe = dir.join(".doctor-probe");
    std::fs::write(&probe, b"").context("cannot write probe file")?;
    std::fs::remove_file(&probe).ok();
    Ok(())
}
