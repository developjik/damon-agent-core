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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Unix seconds when the access token expires.
    pub expires_at: i64,
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

/// Load stored tokens for a provider ("anthropic").
pub fn load(provider: &str) -> anyhow::Result<Option<OAuthTokens>> {
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
    let entry = keychain_entry(provider)?;
    entry
        .set_password(&serde_json::to_string(tokens)?)
        .context("keychain write failed")
}

pub fn delete(provider: &str) -> anyhow::Result<()> {
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
    if let Some(s) = state {
        if s != verifier {
            bail!("state mismatch — the pasted code belongs to a different login attempt");
        }
    }
    let resp = reqwest::Client::new()
        .post(TOKEN_URL)
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
    let v: serde_json::Value = resp.json().await?;
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
/// Called by the Anthropic provider on 401 and proactively.
pub async fn access_token(provider: &str) -> anyhow::Result<String> {
    let Some(tokens) = load(provider)? else {
        bail!("not logged in — run `damond login {provider}`");
    };
    if !tokens.is_expired() {
        return Ok(tokens.access_token);
    }
    refresh(provider, &tokens.refresh_token).await
}

async fn refresh(provider: &str, refresh_token: &str) -> anyhow::Result<String> {
    let resp = reqwest::Client::new()
        .post(TOKEN_URL)
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
    let v: serde_json::Value = resp.json().await?;
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
