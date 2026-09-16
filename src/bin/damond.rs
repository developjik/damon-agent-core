use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use damon_core::api::{self, AppState};
use damon_core::config::{self, Config, SecretRef, SharedConfig};
use damon_core::mcp::McpRegistry;
use damon_core::store::Store;
use tokio::io::AsyncBufReadExt;
use tracing::info;
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
    /// Check config, secrets, provider reachability, and store dir
    Doctor,
    /// OAuth login for a provider (stores tokens in the OS keychain)
    Login {
        /// Provider name (currently only "anthropic")
        provider: String,
    },
    /// Remove stored OAuth tokens for a provider
    Logout {
        /// Provider name
        provider: String,
    },
}
#[derive(clap::Subcommand)]
enum ServiceAction {
    /// Write the service definition and enable it
    Install,
    /// Print the service definition without installing
    Print,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "damon=info".into()),
        )
        .init();
    // rustls needs an explicit process-level crypto provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Args::parse();
    let path = args.config.unwrap_or_else(config::default_config_path);
    if args.print_config_path {
        println!("{}", path.display());
        return Ok(());
    }

    match args.cmd {
        Some(Cmd::Service { action }) => {
            let exe = std::env::current_exe()?.display().to_string();
            let cfg_path = path.display().to_string();
            match action {
                ServiceAction::Install => {
                    println!("{}", damon_core::service::install(&exe, &cfg_path)?);
                }
                ServiceAction::Print => {
                    print!("{}", damon_core::service::print_definition(&exe, &cfg_path));
                }
            }
            return Ok(());
        }
        Some(Cmd::Login { provider }) => return login(&provider).await,
        Some(Cmd::Logout { provider }) => {
            damon_core::oauth::delete(&provider)?;
            println!("logged out of {provider}");
            return Ok(());
        }
        Some(Cmd::Doctor) => return doctor(&path).await,
        None => {}
    }

    let path = config::ensure_config(&path)?;
    let cfg = Config::load(&path)?;
    let bind = cfg.bind;
    let data_dir = cfg
        .data_dir
        .clone()
        .unwrap_or_else(config::default_data_dir);
    let shared: SharedConfig = Arc::new(parking_lot::RwLock::new(cfg));

    let store = Store::open(&data_dir.join("damon.db")).await?;
    let mcp = {
        let servers = shared.read().mcp_servers.clone();
        McpRegistry::connect_all(&servers).await
    };
    let state = AppState::with_bind(shared.clone(), store, mcp, bind).await;
    if let Some(mut reloaded) = config::watch(path, shared.clone()) {
        let state = state.clone();
        tokio::spawn(async move {
            while reloaded.recv().await.is_some() {
                state.reload_providers().await;
                // MCP servers: diff and respawn changed/removed, connect new.
                let cfgs = state.config.read().mcp_servers.clone();
                state.mcp.reload(&cfgs).await;
            }
        });
    }
    // Remote relay: dial out so clients can reach us without an inbound port.
    if let Some(relay) = &shared.read().relay {
        let state = state.clone();
        let url = relay.url.clone();
        let name = relay.name.clone();
        // Resolve the registration secret once — a !cmd/keychain ref
        // resolves at boot, not per reconnect.
        let secret = relay.secret.as_deref().and_then(|s| {
            match config::SecretRef::parse(s) {
                Ok(r) => r.resolve().ok(),
                Err(_) => Some(s.to_string()),
            }
        });
        tokio::spawn(async move {
            damon_core::relay::run_tunnel(state, url, name, secret).await;
        });
    }
    // Safety: non-loopback bind requires a resolvable auth_token — a
    // remote-reachable daemon without auth is a remote code execution
    // surface, and an unresolvable secret ref must fail at boot, not
    // silently open the API.
    if !bind.ip().is_loopback() {
        let has_token = matches!(state.auth_token().await, Some(Ok(_)));
        anyhow::ensure!(
            has_token,
            "refusing to bind {bind}: non-loopback requires a resolvable auth_token in config"
        );
    }

    let app = api::router(state);
    // Read TLS paths into locals first — a scrutinee guard would live for
    // the whole match (i.e. the server's lifetime), deadlocking the config
    // watcher's write lock on the first hot reload.
    let tls = {
        let cfg = shared.read();
        (cfg.tls_cert.clone(), cfg.tls_key.clone())
    };
    match tls {
        (Some(cert), Some(key)) => {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key)
                .await
                .context("cannot load TLS cert/key")?;
            info!(bind = %bind, "damond listening (TLS)");
            axum_server::bind_rustls(bind, tls)
                .handle(axum_server::Handle::new())
                .serve(app.into_make_service())
                .await?;
        }
        (None, None) => {
            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("cannot bind {bind} — is another damond running?"))?;
            info!(bind = %listener.local_addr()?, "damond listening");
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await?;
        }
        _ => anyhow::bail!("tls_cert and tls_key must be set together"),
    }
    info!("damond stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        let _ = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
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


/// `damond doctor` — verify config, secrets, provider reachability, and the
/// store dir without starting the daemon. Exit 0 when every check passes.
async fn doctor(path: &Path) -> anyhow::Result<()> {
    let mut ok = true;

    let cfg = match Config::load(path) {
        Ok(c) => {
            report(&mut ok, true, format!("config {} — {} provider(s)", path.display(), c.providers.len()));
            Some(c)
        }
        Err(e) => {
            report(&mut ok, false, format!("config {} — {e:#}", path.display()));
            None
        }
    };

    if let Some(cfg) = &cfg {
        for (name, p) in &cfg.providers {
            match &p.api_key {
                None => report(&mut ok, true, format!("provider {name} api_key — not set (unauthenticated upstream)")),
                Some(raw) => match SecretRef::parse(raw).and_then(|r| r.resolve()) {
                    Ok(_) => report(&mut ok, true, format!("provider {name} api_key — {raw} resolved")),
                    Err(e) => report(&mut ok, false, format!("provider {name} api_key — {raw}: {e:#}")),
                },
            }

            let base = p.base_url.clone().unwrap_or_else(|| match p.api.as_str() {
                "anthropic-messages" => "https://api.anthropic.com".to_string(),
                "gemini" => "https://generativelanguage.googleapis.com".to_string(),
                _ => "https://api.openai.com/v1".to_string(),
            });
            match tcp_check(&base).await {
                Ok(()) => report(&mut ok, true, format!("provider {name} base_url — {base} reachable")),
                Err(e) => report(&mut ok, false, format!("provider {name} base_url — {base}: {e:#}")),
            }
        }

        match &cfg.auth_token {
            None => report(&mut ok, true, "auth_token — not set (loopback only)"),
            Some(raw) => match SecretRef::parse(raw) {
                Ok(r) => match r.resolve() {
                    Ok(_) => report(&mut ok, true, format!("auth_token — {raw} resolved")),
                    Err(e) => report(&mut ok, false, format!("auth_token — {raw}: {e:#}")),
                },
                Err(_) => report(&mut ok, true, "auth_token — literal token set"),
            },
        }
    }

    let data_dir = cfg
        .as_ref()
        .and_then(|c| c.data_dir.clone())
        .unwrap_or_else(config::default_data_dir);
    match writable_dir(&data_dir) {
        Ok(()) => report(&mut ok, true, format!("store dir {} — writable", data_dir.display())),
        Err(e) => report(&mut ok, false, format!("store dir {} — {e:#}", data_dir.display())),
    }

    if ok { Ok(()) } else { std::process::exit(1) }
}

fn report(ok: &mut bool, pass: bool, msg: impl std::fmt::Display) {
    println!("{} {msg}", if pass { "✓" } else { "✗" });
    *ok &= pass;
}

/// TCP-connect to the host:port implied by a base URL (3s timeout).
async fn tcp_check(base_url: &str) -> anyhow::Result<()> {
    use anyhow::bail;
    let rest = base_url
        .split_once("://")
        .map(|(_, r)| r)
        .unwrap_or(base_url);
    let authority = rest.split('/').next().unwrap_or_default();
    let default_port: u16 = if base_url.starts_with("https") { 443 } else { 80 };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if p.bytes().all(|b| b.is_ascii_digit()) && !p.is_empty() => {
            (h.to_string(), p.parse()?)
        }
        _ => (authority.to_string(), default_port),
    };
    if host.is_empty() {
        bail!("cannot parse host");
    }
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect((host.as_str(), port)),
    )
    .await
    .context("connect timed out (3s)")?
    .context("connect failed")?;
    Ok(())
}

/// Create the dir if needed, then prove writability with a probe file.
fn writable_dir(dir: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir).context("cannot create directory")?;
    let probe = dir.join(".doctor-probe");
    std::fs::write(&probe, b"").context("cannot write probe file")?;
    std::fs::remove_file(&probe).ok();
    Ok(())
}

/// `damond login anthropic` — PKCE flow. Prints the authorize URL; the user
/// pastes the code shown on the callback page back into the terminal.
async fn login(provider: &str) -> anyhow::Result<()> {
    if provider != "anthropic" {
        anyhow::bail!("OAuth login is only supported for 'anthropic' for now");
    }
    let (url, verifier) = damon_core::oauth::authorize_url();
    println!("Open this URL in your browser:\n\n  {url}\n");
    println!("After approving, paste the code shown on the callback page:");
    let mut code = String::new();
    tokio::io::BufReader::new(tokio::io::stdin())
        .read_line(&mut code)
        .await?;
    let code = code.trim();
    if code.is_empty() {
        anyhow::bail!("no code entered");
    }
    damon_core::oauth::exchange(provider, code, &verifier).await?;
    println!("logged in — tokens stored in the OS keychain");
    Ok(())
}