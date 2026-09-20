use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use damon_core::api::{self, AppState};
use damon_core::config::{self, Config, SecretRef, SharedConfig};
use damon_core::mcp::McpRegistry;
use damon_core::store::Store;
use tokio::io::AsyncBufReadExt;
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
    /// OAuth login for a provider (stores tokens in the OS keychain):
    /// anthropic = Claude Pro/Max, openai = ChatGPT Plus/Pro
    Login {
        /// Provider name ("anthropic" or "openai")
        provider: String,
    },
    /// Remove stored OAuth tokens for a provider
    Logout {
        /// Provider name
        provider: String,
    },
    /// List omp-parity provider presets and which are active right now
    Presets,
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
    /// Show whether the service is installed and running
    Status,
}

#[derive(clap::Subcommand)]
enum ConfigAction {
    /// Print the resolved config file path
    Path,
    /// Print the loaded config (secrets shown as references, never resolved)
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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "damon=info".into()))
        .init();
    // rustls needs an explicit process-level crypto provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

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
                ServiceAction::Uninstall => {
                    println!("{}", damon_core::service::uninstall()?);
                }
                ServiceAction::Status => {
                    println!("{}", damon_core::service::status());
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
        Some(Cmd::Presets) => {
            return presets_table();
        }
        Some(Cmd::Config { action }) => {
            match action {
                ConfigAction::Path => println!("{}", path.display()),
                ConfigAction::Show => {
                    // Print the file verbatim — secret refs stay
                    // unresolved, so this never leaks credentials.
                    let p = config::ensure_config(&path)?;
                    print!("{}", std::fs::read_to_string(p)?);
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
    // silently open the API. Runs before Store::open/MCP spawn so a bad
    // config bails without side effects.
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
    let mcp = {
        let servers = shared.read().mcp_servers.clone();
        McpRegistry::connect_all(&servers).await
    };
    let state = AppState::with_bind(shared.clone(), store, mcp, bind).await;
    if let Some(mut reloaded) = config::watch(path.clone(), shared.clone()) {
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
    // our port without parsing config. Written before serving starts; the
    // tls flag is derived from the same locals the match below serves
    // with — taking the lock a second time here could race a hot reload
    // between the two reads and advertise the wrong scheme.
    let discovery_tls = tls.0.is_some() && tls.1.is_some();
    // Best-effort, mirroring the no-home path inside discovery::write:
    // discovery is additive, so a failed write degrades to a no-op Guard
    // instead of blocking boot with `?`.
    let _discovery_guard = match damon_core::discovery::write(bind, discovery_tls, &path) {
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
            info!(bind = %bind, "damond listening (TLS)");
            let handle = axum_server::Handle::new();
            // Keep the handle so SIGTERM can drain connections instead of
            // dropping mid-request.
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(5)));
            });
            axum_server::bind_rustls(bind, tls)
                .handle(handle)
                // ConnectInfo installs the peer IP the rate limiter reads.
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await?;
            let prompts = state.live_prompts.lock().await;
            for (_, (_, token)) in prompts.iter() {
                token.cancel();
            }
            drop(prompts);
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        (None, None) => {
            let listener = tokio::net::TcpListener::bind(bind)
                .await
                .with_context(|| format!("cannot bind {bind} — is another damond running?"))?;
            axum::serve(
                listener,
                // ConnectInfo installs the peer IP the rate limiter reads.
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown_signal())
            .await?;
            // Cancel in-flight turns and give them a moment to persist
            // "cancelled" tool rows — exiting mid-turn would orphan the
            // assistant tool_calls and 400 every later turn.
            let prompts = state.live_prompts.lock().await;
            for (_, (_, token)) in prompts.iter() {
                token.cancel();
            }
            drop(prompts);
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        _ => anyhow::bail!("tls_cert and tls_key must be set together"),
    }
    // Tear down MCP children explicitly — rmcp's Drop only *schedules* an
    // async close, so relying on it at process exit can orphan servers.
    // Session overlays (ACP mcpServers) get the same treatment.
    for (_, reg) in state.session_mcp.lock().await.drain() {
        reg.shutdown().await;
    }
    state.mcp.shutdown().await;
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
            report(
                &mut ok,
                true,
                format!(
                    "config {} — {} provider(s)",
                    path.display(),
                    c.providers.len()
                ),
            );
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
                None => report(
                    &mut ok,
                    true,
                    format!("provider {name} api_key — not set (unauthenticated upstream)"),
                ),
                // The oauth sentinel is not a SecretRef — check the
                // keychain for a live token set instead.
                Some(raw) if raw == "oauth" || raw.starts_with("oauth:") => {
                    let flavor: anyhow::Result<String> = if raw == "oauth" {
                        damon_core::oauth::provider_for_api(&p.api)
                            .map(String::from)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "api_key \"oauth\" is not supported for api '{}'",
                                    p.api
                                )
                            })
                    } else {
                        let f = raw["oauth:".len()..].to_string();
                        damon_core::oauth::validate_flavor(&f).map(|_| f)
                    };
                    match flavor {
                        Ok(oauth_provider) => match damon_core::oauth::load(&oauth_provider) {
                            Ok(Some(t)) if !t.is_expired() => report(
                                &mut ok,
                                true,
                                format!("provider {name} — logged in via {oauth_provider} OAuth"),
                            ),
                            Ok(Some(_)) => report(
                                &mut ok,
                                true,
                                format!(
                                    "provider {name} — {oauth_provider} OAuth token \
                                         expired; it refreshes on next use"
                                ),
                            ),
                            Ok(None) => report(
                                &mut ok,
                                false,
                                format!(
                                    "provider {name} — not logged in; run `damond login \
                                         {oauth_provider}`"
                                ),
                            ),
                            Err(e) => report(
                                &mut ok,
                                false,
                                format!("provider {name} — OAuth token store: {e:#}"),
                            ),
                        },
                        Err(msg) => report(&mut ok, false, format!("provider {name} — {msg}")),
                    }
                }
                Some(raw) => match SecretRef::parse(raw).and_then(|r| r.resolve()) {
                    Ok(_) => report(
                        &mut ok,
                        true,
                        format!("provider {name} api_key — {raw} resolved"),
                    ),
                    Err(e) => report(
                        &mut ok,
                        false,
                        format!("provider {name} api_key — {raw}: {e:#}"),
                    ),
                },
            }

            let base = p.base_url.clone().unwrap_or_else(|| {
                damon_core::config::default_base_url(&p.api, p.api_key.as_deref() == Some("oauth"))
                    .to_string()
            });
            match tcp_check(&base).await {
                Ok(()) => report(
                    &mut ok,
                    true,
                    format!("provider {name} base_url — {base} reachable"),
                ),
                Err(e) => report(
                    &mut ok,
                    false,
                    format!("provider {name} base_url — {base}: {e:#}"),
                ),
            }
        }

        match &cfg.auth_token {
            None => report(&mut ok, true, "auth_token — not set (loopback only)"),
            Some(raw) => match SecretRef::parse(raw) {
                Ok(r) => match r.resolve() {
                    Ok(_) => report(&mut ok, true, format!("auth_token — {raw} resolved")),
                    Err(e) => report(&mut ok, false, format!("auth_token — {raw}: {e:#}")),
                },
                Err(_) => {
                    // SecretRef::parse rejecting the raw string means a
                    // literal token. A short one is offline-guessable —
                    // the relay handshake exposes token-derived proofs.
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
    println!("{} {msg}", if pass { "ok  " } else { "FAIL" });
    *ok &= pass;
}

/// TCP-connect to the host:port implied by a base URL (3s timeout).
async fn tcp_check(base_url: &str) -> anyhow::Result<()> {
    use anyhow::bail;
    let (scheme, rest) = base_url.split_once("://").unwrap_or(("", base_url));
    let authority = rest.split('/').next().unwrap_or_default();
    // Exact scheme match — "httpsx://…" must not count as TLS.
    let default_port: u16 = if scheme == "https" { 443 } else { 80 };
    // Bracketed IPv6 ("[::1]:8080") needs its own split — rsplit_once(':')
    // would leave the brackets on the host and break the connect.
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        match rest.split_once(']') {
            Some((h, "")) => (h.to_string(), default_port),
            Some((h, p)) => (h.to_string(), p[1..].parse()?),
            None => bail!("cannot parse host"),
        }
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) if p.bytes().all(|b| b.is_ascii_digit()) && !p.is_empty() => {
                (h.to_string(), p.parse()?)
            }
            _ => (authority.to_string(), default_port),
        }
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

/// `damond presets` — print the omp-parity catalog with a live marker:
/// key env vars that are set right now (or keyless local engines) would
/// auto-register at boot unless the id is explicitly configured.
fn presets_table() -> anyhow::Result<()> {
    use damon_core::provider::presets::PRESETS;
    println!("{:<18} {:<20} {:<28} NOTE", "ID", "API", "KEY ENV");
    for p in PRESETS {
        let active = p.env_active();
        let keys = if p.key_env.is_empty() {
            "(keyless)".to_string()
        } else {
            p.key_env.join(" | ")
        };
        println!(
            "{:<18} {:<20} {:<28} {}{}",
            p.id,
            p.api,
            keys,
            p.note,
            if active { "  [active]" } else { "" }
        );
    }
    println!(
        "\n[active] = key env var is set (keyless engines always) — the provider\n\
         auto-registers at boot unless you configure the same id explicitly."
    );
    Ok(())
}

/// `damond login <provider>` — anthropic/openai: PKCE flow, the user
/// pastes back the code (anthropic) or the full callback URL (openai,
/// where the browser lands on a dead localhost port). kimi-code,
/// xai-oauth, github-copilot: RFC 8628 device flow — the CLI polls until
/// the browser approval lands, nothing to paste.
async fn login(provider: &str) -> anyhow::Result<()> {
    if damon_core::oauth::is_device_flow(provider) {
        let auth = damon_core::oauth::device_authorization(provider).await?;
        match auth
            .verification_uri_complete
            .as_deref()
            .filter(|u| !u.is_empty())
        {
            Some(url) => println!(
                "Open this URL in your browser (code: {}):\n\n  {url}\n",
                auth.user_code
            ),
            None => println!(
                "Open {} in your browser and enter code: {}\n",
                auth.verification_uri, auth.user_code
            ),
        }
        println!("Waiting for approval… (Ctrl-C to cancel)");
        damon_core::oauth::device_poll(provider, &auth).await?;
        println!("logged in — tokens stored in the OS keychain");
        return Ok(());
    }
    let (url, verifier) = damon_core::oauth::authorize_url(provider)?;
    println!("Open this URL in your browser:\n\n  {url}\n");
    match provider {
        "openai" => println!(
            "After approving, the browser lands on a localhost page that won't load —\n\
             that's expected. Paste the FULL URL from the browser's address bar:"
        ),
        _ => println!("After approving, paste the code shown on the callback page:"),
    }
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
