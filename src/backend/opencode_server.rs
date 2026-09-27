//! opencode server lifecycle — probe, adopt, or spawn `opencode serve`.
//!
//! Verified live against opencode 1.18.32: `GET /global/health` answers
//! `{"healthy":true}`; a pre-existing server is adopted (never killed);
//! otherwise `opencode serve --hostname 127.0.0.1 --port <free>` is
//! spawned with a random `OPENCODE_SERVER_PASSWORD` so the loopback
//! listener is not left wide open. One server serves every session —
//! the directory is per-session (`POST /session?directory=…`).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use base64::Engine as _;
use tokio::sync::Mutex;

/// Default port `opencode serve` uses when spawned without one.
const DEFAULT_PORT: u16 = 4096;

pub struct OpencodeServer {
    /// Base URL without a trailing slash (e.g. http://127.0.0.1:4096).
    pub base_url: String,
    /// Basic-auth password for a server we spawned; None when we
    /// adopted an existing listener.
    pub password: Option<String>,
    /// The spawned child, when we own it. Dropped (killed) on
    /// shutdown; an adopted server outlives Damon.
    child: tokio::sync::Mutex<Option<tokio::process::Child>>,
}

impl OpencodeServer {
    /// Basic-auth header value for reqwest calls, when needed.
    pub fn auth_header(&self) -> Option<String> {
        self.password.as_ref().map(|p| {
            format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("opencode:{p}"))
            )
        })
    }

    /// Take the spawned child for shutdown (idempotent; adopted
    /// servers have none).
    pub async fn take_child(&self) -> Option<tokio::process::Child> {
        self.child.lock().await.take()
    }
}

static SERVERS: Mutex<Option<HashMap<String, Arc<OpencodeServer>>>> = Mutex::const_new(None);

/// The shared server handle for one CLI binary — probes/adopts/spawns
/// at most once per command (opencode and mimo each get their own).
pub async fn shared_server(command: &str) -> Result<Arc<OpencodeServer>> {
    let mut guard = SERVERS.lock().await;
    let map = guard.get_or_insert_with(HashMap::new);
    if let Some(s) = map.get(command) {
        return Ok(s.clone());
    }
    let server = Arc::new(ensure_server(command).await?);
    map.insert(command.to_string(), server.clone());
    Ok(server)
}

async fn healthy(client: &reqwest::Client, base: &str, auth: Option<&str>) -> bool {
    let mut req = client.get(format!("{base}/global/health"));
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    matches!(
        req.send().await,
        Ok(resp) if resp.status().is_success()
    )
}

async fn ensure_server(command: &str) -> Result<OpencodeServer> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    // Adopt a healthy default-port server first (the user's own).
    let default_base = format!("http://127.0.0.1:{DEFAULT_PORT}");
    if healthy(&client, &default_base, None).await {
        return Ok(OpencodeServer {
            base_url: default_base,
            password: None,
            child: tokio::sync::Mutex::new(None),
        });
    }

    // Spawn our own on a free port, behind a random password.
    let port = free_port().context("no free port for opencode serve")?;
    let password = uuid::Uuid::new_v4().simple().to_string();
    let mut cmd = tokio::process::Command::new(command);
    cmd.args(["serve", "--hostname", "127.0.0.1", "--port", &port.to_string()])
        .env("OPENCODE_SERVER_PASSWORD", &password)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd
        .spawn()
        .with_context(|| format!("cannot spawn `{command} serve`"))?;
    // Drain stderr in the background so a chatty server can't block.
    if let Some(err) = child.stderr.take() {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut err = err;
            let mut buf = [0u8; 4096];
            while let Ok(n) = err.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });
    }

    let base = format!("http://127.0.0.1:{port}");
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("opencode:{password}"))
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if healthy(&client, &base, Some(&auth)).await {
            return Ok(OpencodeServer {
                base_url: base,
                password: Some(password),
                child: tokio::sync::Mutex::new(Some(child)),
            });
        }
        if std::time::Instant::now() >= deadline {
            bail!("opencode serve did not become healthy within 20s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// Shutdown every spawned server (adopted servers are left alone).
pub async fn shutdown_shared() {
    let mut guard = SERVERS.lock().await;
    if let Some(map) = guard.as_mut() {
        for (_, srv) in map.drain() {
            if let Some(mut child) = srv.take_child().await {
                let _ = child.kill().await;
            }
        }
    }
}

/// Grab a free TCP port by binding port 0.
fn free_port() -> Option<u16> {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .ok()?
        .local_addr()
        .ok()?
        .port()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_header_is_basic() {
        let s = OpencodeServer {
            base_url: "http://x".into(),
            password: Some("pw".into()),
            child: tokio::sync::Mutex::new(None),
        };
        let h = s.auth_header().unwrap();
        assert!(h.starts_with("Basic "));
        assert!(h.ends_with("b3BlbmNvZGU6cHc=")); // base64("opencode:pw")
    }
}
