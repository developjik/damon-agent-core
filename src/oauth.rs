//! Anthropic OAuth (Claude Code PKCE flow). `damond login anthropic` prints
//! the authorize URL; the user pastes back the code shown on the callback
//! page. Tokens live in the OS keychain under service "damon-oauth".

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::Digest;

const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const AUTHORIZE_URL: &str = "https://claude.ai/oauth/authorize";
const TOKEN_URL: &str = "https://console.anthropic.com/v1/oauth/token";
const REDIRECT_URI: &str = "https://console.anthropic.com/oauth/code/callback";
const SCOPE: &str = "org:create_api_key user:profile user:inference";
const KEYCHAIN_SERVICE: &str = "damon-oauth";

/// Stored OAuth token set for one provider account.
#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Unix seconds when the access token expires.
    pub expires_at: i64,
}

// Redact tokens in Debug — a derived impl would print them in plaintext
// the first time anyone logs the struct.
impl std::fmt::Debug for OAuthTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthTokens")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl OAuthTokens {
    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        // 60s skew buffer.
        now >= self.expires_at - 60
    }
}

fn keychain_entry(provider: &str) -> anyhow::Result<keyring::Entry> {
    keyring::Entry::new(KEYCHAIN_SERVICE, provider).context("keychain backend unavailable")
}

/// Token endpoint. `DAMON_TEST_TOKEN_URL` overrides it — tests point this
/// at a local mock server; production always uses `TOKEN_URL`.
///
/// Test hooks are compiled out of release builds (`cfg(debug_assertions)`),
/// so the env vars have no effect there. `cargo test --release` needs a
/// profile or `RUSTFLAGS` with `debug-assertions = true` to use them.
#[cfg(debug_assertions)]
fn token_url() -> String {
    std::env::var("DAMON_TEST_TOKEN_URL").unwrap_or_else(|_| TOKEN_URL.to_string())
}

/// Release fallback — always the real endpoint (see `token_url`).
#[cfg(not(debug_assertions))]
fn token_url() -> String {
    TOKEN_URL.to_string()
}

/// Test-only file path for stored tokens. When `DAMON_TEST_TOKEN_DIR` is
/// set, tokens live in `{dir}/{provider}.json` instead of the OS keychain.
///
/// Compiled out of release builds — always `None` there (see `token_url`).
#[cfg(debug_assertions)]
fn test_token_path(provider: &str) -> Option<std::path::PathBuf> {
    std::env::var("DAMON_TEST_TOKEN_DIR")
        .ok()
        .map(|dir| std::path::Path::new(&dir).join(format!("{provider}.json")))
}

/// Release fallback — always the OS keychain (see `test_token_path`).
#[cfg(not(debug_assertions))]
fn test_token_path(_provider: &str) -> Option<std::path::PathBuf> {
    None
}

/// Load stored tokens for a provider ("anthropic").
pub fn load(provider: &str) -> anyhow::Result<Option<OAuthTokens>> {
    if let Some(path) = test_token_path(provider) {
        return match std::fs::read_to_string(&path) {
            Ok(json) => Ok(Some(
                serde_json::from_str(&json).context("corrupt OAuth token file")?,
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("token file read failed"),
        };
    }
    let entry = keychain_entry(provider)?;
    match entry.get_password() {
        Ok(json) => Ok(Some(
            serde_json::from_str(&json).context("corrupt OAuth token entry")?,
        )),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(e).context("keychain read failed"),
    }
}

fn store(provider: &str, tokens: &OAuthTokens) -> anyhow::Result<()> {
    let json = serde_json::to_string(tokens)?;
    if let Some(path) = test_token_path(provider) {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        // Test token files hold live credentials: create with 0600 from
        // the start (no world-readable window), and re-apply for files
        // that already existed — mode only applies at creation.
        #[cfg(target_family = "unix")]
        {
            use std::io::Write;
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)
                .context("token file write failed")?;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .context("token file chmod failed")?;
            file.write_all(json.as_bytes())
                .context("token file write failed")?;
            return Ok(());
        }
        #[cfg(not(target_family = "unix"))]
        return std::fs::write(&path, json).context("token file write failed");
    }
    let entry = keychain_entry(provider)?;
    entry.set_password(&json).context("keychain write failed")
}

pub fn delete(provider: &str) -> anyhow::Result<()> {
    if let Some(path) = test_token_path(provider) {
        return match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context("token file delete failed"),
        };
    }
    let entry = keychain_entry(provider)?;
    match entry.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(e).context("keychain delete failed"),
    }
}

/// Generate a PKCE verifier + challenge + state (all the same base64url
/// random value, per the Claude Code flow).
fn pkce() -> (String, String) {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("OS RNG unavailable");
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

/// Build the authorize URL the user opens in a browser.
/// Returns (url, verifier) — the verifier is needed for the exchange.
pub fn authorize_url() -> (String, String) {
    let (verifier, challenge) = pkce();
    let url = format!(
        "{AUTHORIZE_URL}?code=true&response_type=code&client_id={CLIENT_ID}\
         &redirect_uri={}&scope={}&code_challenge={challenge}\
         &code_challenge_method=S256&state={verifier}",
        urlencoding(REDIRECT_URI),
        urlencoding(SCOPE),
    );
    (url, verifier)
}

/// Exchange an authorization code (pasted from the callback page, possibly
/// "code#state") for tokens. Stores them in the keychain.
pub async fn exchange(provider: &str, code_and_state: &str, verifier: &str) -> anyhow::Result<()> {
    // The callback may append "#state" — when present it must match the
    // verifier we sent, or the code isn't ours.
    let (code, state) = match code_and_state.split_once('#') {
        Some((c, s)) => (c, Some(s)),
        None => (code_and_state, None),
    };
    if let Some(s) = state
        && s != verifier
    {
        bail!("state mismatch — the pasted code belongs to a different login attempt");
    }
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .post(token_url())
        .header("content-type", "application/json")
        .header("user-agent", "anthropic")
        .json(&serde_json::json!({
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": verifier,
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "state": verifier,
        }))
        .send()
        .await
        .context("token exchange request failed")?;
    let status = resp.status();
    // A non-JSON body (HTML error page, empty 5xx) must not bury the HTTP
    // status; on success statuses keep the plain decode error.
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) if !status.is_success() => {
            bail!("token exchange failed ({status}): non-JSON response body");
        }
        Err(e) => return Err(e.into()),
    };
    if !status.is_success() {
        bail!("token exchange failed ({status}): {v}");
    }
    let tokens = OAuthTokens {
        access_token: v["access_token"]
            .as_str()
            .context("no access_token in response")?
            .to_string(),
        refresh_token: v["refresh_token"]
            .as_str()
            .context("no refresh_token in response")?
            .to_string(),
        expires_at: now() + v["expires_in"].as_i64().unwrap_or(3600),
    };
    store(provider, &tokens)
}

/// Return a valid access token, refreshing first if expired.
/// Called by the Anthropic provider proactively.
pub async fn access_token(provider: &str) -> anyhow::Result<String> {
    let Some(tokens) = load(provider)? else {
        bail!("not logged in — run `damond login {provider}`");
    };
    if !tokens.is_expired() {
        return Ok(tokens.access_token);
    }
    refresh_locked(provider, false).await
}

/// Force a token refresh regardless of the stored expiry — used after a
/// 401, where the server rejected a token we still believed valid.
pub async fn force_refresh(provider: &str) -> anyhow::Result<String> {
    refresh_locked(provider, true).await
}

/// Single-flight refresh: concurrent 401s must not race parallel
/// refreshes — Anthropic rotates the refresh_token, so a second
/// concurrent refresh would use the just-invalidated one and fail with
/// invalid_grant, forcing a re-login.
async fn refresh_locked(provider: &str, force: bool) -> anyhow::Result<String> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = LOCK.lock().await;
    let Some(tokens) = load(provider)? else {
        bail!("not logged in — run `damond login {provider}`");
    };
    // Another task may have just refreshed while we waited on the lock.
    if !force && !tokens.is_expired() {
        return Ok(tokens.access_token);
    }
    refresh(provider, &tokens.refresh_token).await
}

async fn refresh(provider: &str, refresh_token: &str) -> anyhow::Result<String> {
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .post(token_url())
        .header("content-type", "application/json")
        .header("user-agent", "anthropic")
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
        }))
        .send()
        .await
        .context("token refresh request failed")?;
    let status = resp.status();
    let v: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) if !status.is_success() => {
            bail!("token refresh failed ({status}): non-JSON response body");
        }
        Err(e) => return Err(e.into()),
    };
    if !status.is_success() {
        bail!("token refresh failed ({status}): {v}");
    }
    let tokens = OAuthTokens {
        access_token: v["access_token"]
            .as_str()
            .context("no access_token in refresh response")?
            .to_string(),
        refresh_token: v["refresh_token"]
            .as_str()
            .unwrap_or(refresh_token)
            .to_string(),
        expires_at: now() + v["expires_in"].as_i64().unwrap_or(3600),
    };
    store(provider, &tokens)?;
    Ok(tokens.access_token)
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn urlencoding(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serve raw, non-JSON HTTP responses on an ephemeral port.
    fn serve_raw(status_line: &str, body: &str) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let status_line = status_line.to_string();
        let body = body.to_string();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            // One response per connection — exchange and refresh each open
            // their own.
            while let Ok((mut sock, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf); // drain the request head
                let resp = format!(
                    "HTTP/1.1 {status_line}\r\ncontent-type: text/html\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                if sock.write_all(resp.as_bytes()).is_err() {
                    break;
                }
            }
        });
        format!("http://{addr}/token")
    }

    #[tokio::test]
    async fn non_json_error_body_keeps_http_status() {
        let dir = std::env::temp_dir().join(format!("damon-oauth-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let url = serve_raw(
            "500 Internal Server Error",
            "<html>upstream exploded</html>",
        );
        // SAFETY: test-only hooks; this is the only test in this binary
        // touching these vars, and it removes them before returning.
        unsafe {
            std::env::set_var("DAMON_TEST_TOKEN_URL", &url);
            std::env::set_var("DAMON_TEST_TOKEN_DIR", &dir);
        }

        // exchange: decode failure on an error status must surface the 500.
        let err = exchange("unit-test", "some-code", "some-verifier")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("500"), "exchange: {err}");

        // refresh: same contract.
        store(
            "unit-test",
            &OAuthTokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
                expires_at: 0,
            },
        )
        .unwrap();
        let err = force_refresh("unit-test").await.unwrap_err();
        assert!(err.to_string().contains("500"), "refresh: {err}");

        unsafe {
            std::env::remove_var("DAMON_TEST_TOKEN_URL");
            std::env::remove_var("DAMON_TEST_TOKEN_DIR");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
