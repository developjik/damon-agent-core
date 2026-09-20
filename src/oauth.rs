//! Provider OAuth login. Two flavors:
//!
//! - `anthropic` — Claude Code PKCE flow (Claude Pro/Max subscription).
//! - `openai` — Codex PKCE flow (ChatGPT Plus/Pro subscription). The id_token
//!   JWT carries the ChatGPT account id, sent as `chatgpt-account-id` on
//!   every backend request.
//!
//! `damond login <provider>` prints the authorize URL; the user pastes back
//! the code (anthropic) or the full callback URL (openai). Tokens live in
//! the OS keychain under service "damon-oauth".

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::Digest;

const KEYCHAIN_SERVICE: &str = "damon-oauth";

/// Which OAuth dialect a provider speaks. Keychain entries and stored token
/// sets are keyed by the flavor name ("anthropic", "openai", "kimi-code",
/// "xai-oauth", "github-copilot") — a provider's config name is
/// independent of it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flavor {
    Anthropic,
    OpenAi,
    KimiCode,
    XaiOauth,
    GitHubCopilot,
}

/// Map a provider `api` kind to the OAuth provider it authenticates with.
/// `None` — that api kind has no single OAuth flavor (API keys only), or
/// the kind carries several (openai-completions: kimi/copilot) and needs
/// the explicit `api_key = "oauth:<flavor>"` sentinel.
pub fn provider_for_api(api: &str) -> Option<&'static str> {
    match api {
        "anthropic-messages" => Some("anthropic"),
        "openai-responses" => Some("openai"),
        _ => None,
    }
}

/// The api kind a flavor pairs with — `api_key = "oauth:<flavor>"` is
/// only valid on this transport.
pub fn api_for_flavor(flavor: &str) -> Option<&'static str> {
    match flavor {
        "anthropic" => Some("anthropic-messages"),
        "openai" | "xai-oauth" => Some("openai-responses"),
        "kimi-code" | "github-copilot" => Some("openai-completions"),
        _ => None,
    }
}

/// Validate a flavor name from config. `Ok(())` when known.
pub fn validate_flavor(name: &str) -> anyhow::Result<()> {
    flavor(name).map(|_| ())
}

fn flavor(provider: &str) -> anyhow::Result<Flavor> {
    match provider {
        "anthropic" => Ok(Flavor::Anthropic),
        "openai" => Ok(Flavor::OpenAi),
        "kimi-code" => Ok(Flavor::KimiCode),
        "xai-oauth" => Ok(Flavor::XaiOauth),
        "github-copilot" => Ok(Flavor::GitHubCopilot),
        other => bail!(
            "unsupported OAuth provider '{other}' — supported: anthropic, openai, \
             kimi-code, xai-oauth, github-copilot"
        ),
    }
}

/// Per-flavor endpoints and client identity. Both clients are the public
/// PKCE clients of the respective CLIs (Claude Code / Codex); subscriptions
/// authorize through them, no API key involved.
struct FlavorCfg {
    authorize_url: &'static str,
    token_url: &'static str,
    redirect_uri: &'static str,
    scope: &'static str,
    client_id: &'static str,
    user_agent: &'static str,
}

const ANTHROPIC: FlavorCfg = FlavorCfg {
    authorize_url: "https://claude.ai/oauth/authorize",
    token_url: "https://console.anthropic.com/v1/oauth/token",
    redirect_uri: "https://console.anthropic.com/oauth/code/callback",
    scope: "org:create_api_key user:profile user:inference",
    client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
    user_agent: "anthropic",
};

const OPENAI: FlavorCfg = FlavorCfg {
    authorize_url: "https://auth.openai.com/authorize",
    token_url: "https://auth.openai.com/oauth/token",
    redirect_uri: "http://localhost:1455/auth/callback",
    scope: "openid profile email offline_access",
    client_id: "app_EMoamEEZ73f0CkXaXp7hrann",
    user_agent: "damond",
};

fn cfg(f: Flavor) -> &'static FlavorCfg {
    match f {
        Flavor::Anthropic => &ANTHROPIC,
        Flavor::OpenAi => &OPENAI,
        // Device-flow flavors never paste a code — their token/refresh
        // endpoints come from DeviceCfg instead.
        _ => unreachable!("device-flow flavors have no PKCE config"),
    }
}

/// Static literal headers → HeaderMap. Values are compile-time
/// constants, so `from_static` cannot fail.
fn static_headers(pairs: &[(&'static str, &'static str)]) -> reqwest::header::HeaderMap {
    pairs
        .iter()
        .map(|(k, v)| {
            (
                reqwest::header::HeaderName::from_bytes(k.as_bytes())
                    .expect("valid constant header name"),
                reqwest::header::HeaderValue::from_static(v),
            )
        })
        .collect()
}

/// RFC 8628 device-flow configuration for the flavors that authorize on
/// a second device (kimi-code, xai-oauth, github-copilot).
struct DeviceCfg {
    client_id: &'static str,
    /// Device authorization endpoint (POST, form-encoded).
    device_url: &'static str,
    /// Token endpoint for polling AND refresh.
    token_url: TokenUrl,
    scope: Option<&'static str>,
    /// Static tracking headers some servers expect on the auth requests.
    extra_headers: &'static [(&'static str, &'static str)],
}

/// Where the device-flow token endpoint lives.
enum TokenUrl {
    Static(&'static str),
    /// Read `token_endpoint` from `{issuer}/.well-known/openid-configuration`.
    OidcDiscovery(&'static str),
}

const KIMI_CLIENT_ID: &str = "17e5f671-d194-4dfb-9706-5516cb48c098";
const XAI_CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
const GITHUB_CLIENT_ID: &str = "Ov23li8tweQw6odWQebz";

fn device_cfg(f: Flavor) -> Option<DeviceCfg> {
    match f {
        Flavor::KimiCode => Some(DeviceCfg {
            client_id: KIMI_CLIENT_ID,
            device_url: "https://auth.kimi.com/api/oauth/device_authorization",
            token_url: TokenUrl::Static("https://auth.kimi.com/api/oauth/token"),
            scope: None,
            extra_headers: &[("X-Msh-Platform", "kimi_cli"), ("User-Agent", "kimi_cli")],
        }),
        Flavor::XaiOauth => Some(DeviceCfg {
            client_id: XAI_CLIENT_ID,
            device_url: "https://auth.x.ai/oauth2/device/code",
            token_url: TokenUrl::OidcDiscovery("https://auth.x.ai"),
            scope: Some("openid profile email offline_access grok-cli:access api:access"),
            extra_headers: &[],
        }),
        Flavor::GitHubCopilot => Some(DeviceCfg {
            client_id: GITHUB_CLIENT_ID,
            device_url: "https://github.com/login/device/code",
            token_url: TokenUrl::Static("https://github.com/login/oauth/access_token"),
            scope: Some("read:user"),
            extra_headers: &[],
        }),
        _ => None,
    }
}

/// Whether `damond login <provider>` runs the device flow (no paste).
pub fn is_device_flow(provider: &str) -> bool {
    flavor(provider).ok().and_then(device_cfg).is_some()
}

/// Test hook: overrides the device authorization endpoint.
#[cfg(debug_assertions)]
fn device_url(default: &str) -> String {
    std::env::var("DAMON_TEST_DEVICE_URL").unwrap_or_else(|_| default.to_string())
}
#[cfg(not(debug_assertions))]
fn device_url(default: &str) -> String {
    default.to_string()
}

/// Resolve the token endpoint. `OidcDiscovery` reads `token_endpoint`
/// from the issuer's well-known configuration; in debug builds the
/// `DAMON_TEST_TOKEN_URL` hook short-circuits both discovery and the
/// endpoint so tests only need one mock server.
async fn resolve_token_url(t: &TokenUrl) -> anyhow::Result<String> {
    #[cfg(debug_assertions)]
    if let Ok(url) = std::env::var("DAMON_TEST_TOKEN_URL") {
        return Ok(url);
    }
    match t {
        TokenUrl::Static(u) => Ok(u.to_string()),
        TokenUrl::OidcDiscovery(issuer) => {
            let well_known = format!("{issuer}/.well-known/openid-configuration");
            let resp = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?
                .get(&well_known)
                .header("accept", "application/json")
                .send()
                .await
                .context("OIDC discovery request failed")?;
            let status = resp.status();
            let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            if !status.is_success() {
                bail!("OIDC discovery failed ({status}): {v}");
            }
            v["token_endpoint"]
                .as_str()
                .map(String::from)
                .context("OIDC discovery carries no token_endpoint")
        }
    }
}

/// Device authorization offer returned by the provider.
#[derive(Clone, Debug)]
pub struct DeviceAuthorization {
    pub user_code: String,
    pub device_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
    /// Seconds the offer stays valid (None when the server omits it).
    pub expires_in: Option<u64>,
    /// Seconds between polls.
    pub interval: u64,
}

fn oauth_http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?)
}

/// Step 1 of the device flow: request the code the user enters on the
/// verification page.
pub async fn device_authorization(provider: &str) -> anyhow::Result<DeviceAuthorization> {
    let f = flavor(provider)?;
    let Some(d) = device_cfg(f) else {
        bail!("provider '{provider}' does not use the device flow");
    };
    let mut form = vec![("client_id", d.client_id)];
    if let Some(scope) = d.scope {
        form.push(("scope", scope));
    }
    let resp = oauth_http_client()?
        .post(device_url(d.device_url))
        .header("accept", "application/json")
        .header("content-type", "application/x-www-form-urlencoded")
        .headers(static_headers(d.extra_headers))
        .form(&form)
        .send()
        .await
        .context("device authorization request failed")?;
    let status = resp.status();
    let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        bail!("device authorization failed ({status}): {v}");
    }
    Ok(DeviceAuthorization {
        user_code: v["user_code"]
            .as_str()
            .context("no user_code in response")?
            .to_string(),
        device_code: v["device_code"]
            .as_str()
            .context("no device_code in response")?
            .to_string(),
        verification_uri: v["verification_uri"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        verification_uri_complete: v["verification_uri_complete"].as_str().map(String::from),
        expires_in: v["expires_in"].as_u64(),
        interval: v["interval"].as_u64().unwrap_or(5).max(1),
    })
}

/// Step 2: poll the token endpoint until the user approves (handling
/// `authorization_pending` / `slow_down` backoff), then store the tokens
/// and return the credential.
pub async fn device_poll(provider: &str, auth: &DeviceAuthorization) -> anyhow::Result<Credential> {
    let f = flavor(provider)?;
    let Some(d) = device_cfg(f) else {
        bail!("provider '{provider}' does not use the device flow");
    };
    let token_url = resolve_token_url(&d.token_url).await?;
    let deadline = now() + auth.expires_in.unwrap_or(600) as i64;
    let mut interval = auth.interval.max(1);
    let client = oauth_http_client()?;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        if now() >= deadline {
            bail!("device code expired — run `damond login {provider}` again");
        }
        let resp = client
            .post(&token_url)
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .headers(static_headers(d.extra_headers))
            .form(&[
                ("client_id", d.client_id),
                ("device_code", auth.device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .await
            .context("device token poll failed")?;
        let status = resp.status();
        let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        if status.is_success() {
            let Some(access) = v["access_token"].as_str() else {
                // 200 without a token — GitHub answers pending polls with
                // 200 + error field; surface anything else as broken.
                let err = v["error"].as_str().unwrap_or_default();
                match err {
                    "authorization_pending" => continue,
                    "slow_down" => {
                        interval += 5;
                        continue;
                    }
                    _ => bail!("device token response missing access_token: {v}"),
                }
            };
            let tokens = OAuthTokens {
                access_token: access.to_string(),
                // GitHub device tokens are long-lived and carry no
                // refresh token — the access token doubles as its own
                // refresh so force_refresh degrades to a no-op.
                refresh_token: v["refresh_token"].as_str().unwrap_or(access).to_string(),
                // GitHub OAuth tokens don't expire — mark far-future.
                expires_at: if f == Flavor::GitHubCopilot {
                    now() + 10 * 365 * 24 * 3600
                } else {
                    now() + v["expires_in"].as_i64().unwrap_or(3600)
                },
                account_id: None,
            };
            store(provider, &tokens)?;
            return Ok(Credential {
                access_token: tokens.access_token,
                account_id: tokens.account_id,
            });
        }
        let err = v["error"].as_str().unwrap_or_default().to_string();
        match err.as_str() {
            "authorization_pending" => continue,
            "slow_down" => {
                interval += 5;
                continue;
            }
            _ => bail!("device token poll failed ({status}): {v}"),
        }
    }
}

/// Stored OAuth token set for one provider account.
#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    /// Unix seconds when the access token expires.
    pub expires_at: i64,
    /// ChatGPT account id (openai flavor) — accompanies the bearer token as
    /// the `chatgpt-account-id` header. Absent for anthropic.
    #[serde(default)]
    pub account_id: Option<String>,
}

// Redact tokens in Debug — a derived impl would print them in plaintext
// the first time anyone logs the struct. The account id is not a secret
// (it rides inside the id_token JWT claims); keeping it visible makes
// multi-account mixups diagnosable.
impl std::fmt::Debug for OAuthTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthTokens")
            .field("access_token", &"<redacted>")
            .field("refresh_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field("account_id", &self.account_id)
            .finish()
    }
}

/// A resolved OAuth credential: the bearer token plus, for openai, the
/// ChatGPT account id that must accompany it.
#[derive(Clone, Debug)]
pub struct Credential {
    pub access_token: String,
    pub account_id: Option<String>,
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
/// at a local mock server; production always uses the flavor's endpoint.
///
/// Test hooks are compiled out of release builds (`cfg(debug_assertions)`),
/// so the env vars have no effect there. `cargo test --release` needs a
/// profile or `RUSTFLAGS` with `debug-assertions = true` to use them.
#[cfg(debug_assertions)]
fn token_url(f: Flavor) -> String {
    std::env::var("DAMON_TEST_TOKEN_URL").unwrap_or_else(|_| cfg(f).token_url.to_string())
}

/// Release fallback — always the real endpoint (see `token_url`).
#[cfg(not(debug_assertions))]
fn token_url(f: Flavor) -> String {
    cfg(f).token_url.to_string()
}

/// Test-only file path for stored tokens. When `DAMON_TEST_TOKEN_DIR` is
/// set, tokens live in `{dir}/{provider}.json` instead of the OS keychain.
///
/// Compiled out of release builds — always `None` there (see `token_url`).
#[cfg(debug_assertions)]
fn test_token_path(provider: &str) -> Option<std::path::PathBuf> {
    std::env::var("DAMON_TEST_TOKEN_DIR")
        .ok()
        .map(|dir| std::path::PathBuf::from(dir).join(format!("{provider}.json")))
}

/// Release fallback — always the OS keychain (see `test_token_path`).
#[cfg(not(debug_assertions))]
fn test_token_path(_provider: &str) -> Option<std::path::PathBuf> {
    None
}

/// Load stored tokens for a provider ("anthropic" / "openai").
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
    match keychain_entry(provider)?.get_password() {
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
        std::fs::write(&path, json)?;
        return Ok(());
    }
    keychain_entry(provider)?.set_password(&json)?;
    Ok(())
}

pub fn delete(provider: &str) -> anyhow::Result<()> {
    if let Some(path) = test_token_path(provider) {
        match std::fs::remove_file(&path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        }
    }
    match keychain_entry(provider)?.delete_credential() {
        Ok(()) => Ok(()),
        Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(e).context("keychain delete failed"),
    }
}

/// Generate a PKCE verifier + challenge: the challenge is the S256 hash of
/// the verifier (both flows), and the verifier doubles as the state value.
fn pkce() -> (String, String) {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("OS RNG unavailable");
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

/// Build the authorize URL the user opens in a browser.
/// Returns (url, verifier) — the verifier is needed for the exchange.
pub fn authorize_url(provider: &str) -> anyhow::Result<(String, String)> {
    let f = flavor(provider)?;
    let c = cfg(f);
    let (verifier, challenge) = pkce();
    let mut url = format!(
        "{auth}?response_type=code&client_id={client}&redirect_uri={redirect}\
         &scope={scope}&code_challenge={challenge}\
         &code_challenge_method=S256&state={verifier}",
        auth = c.authorize_url,
        client = c.client_id,
        redirect = urlencoding(c.redirect_uri),
        scope = urlencoding(c.scope),
    );
    if f == Flavor::Anthropic {
        // Claude's authorize endpoint keys its manual-copy page on this flag.
        url.push_str("&code=true");
    } else {
        // Codex's login request shape — both flags are optional but keep
        // the flow identical to the CLI's so backend-side gating agrees.
        url.push_str("&id_token_add_organizations=true&codex_cli_simplified_flow=true");
    }
    Ok((url, verifier))
}

/// Pull (code, state) out of what the user pasted: either a raw code with
/// an optional `#state` suffix (anthropic's callback page shows exactly
/// that) or the full callback URL — for openai the browser lands on a dead
/// localhost port, so the user copies the address bar and the code and
/// state arrive as query parameters.
fn extract_code(input: &str) -> anyhow::Result<(String, Option<String>)> {
    let input = input.trim();
    if !input.contains("code=") {
        // A pasted URL without a code parameter is a user error — only a
        // bare code (or code#state) is accepted verbatim.
        if input.starts_with("http") || input.contains('?') {
            bail!("callback URL carries no code parameter");
        }
        return Ok(match input.split_once('#') {
            Some((c, s)) => (c.to_string(), Some(s.to_string())),
            None => (input.to_string(), None),
        });
    }
    let query = input.split_once('?').map(|(_, q)| q).unwrap_or(input);
    let mut code = None;
    let mut state = None;
    for pair in query.split(['&', '#']) {
        if let Some(v) = pair.strip_prefix("code=") {
            code = Some(v.to_string());
        }
        if let Some(v) = pair.strip_prefix("state=") {
            state = Some(v.to_string());
        }
    }
    let code = code
        .filter(|c| !c.is_empty())
        .context("callback URL carries no code parameter")?;
    Ok((code, state))
}

/// Extract `https://api.openai.com/auth.chatgpt_account_id` from an
/// id_token JWT payload. `None` for any other shape — callers then keep
/// the previously stored account id.
fn account_id_from_id_token(jwt: &str) -> Option<String> {
    let payload = jwt.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    claims["https://api.openai.com/auth"]["chatgpt_account_id"]
        .as_str()
        .map(String::from)
}

/// Exchange an authorization code (pasted from the callback page, possibly
/// "code#state", or a full callback URL) for tokens. Stores them in the
/// keychain.
pub async fn exchange(provider: &str, code_and_state: &str, verifier: &str) -> anyhow::Result<()> {
    let f = flavor(provider)?;
    let c = cfg(f);
    let (code, state) = extract_code(code_and_state)?;
    // When present it must match the verifier we sent, or the code isn't
    // ours.
    if let Some(s) = state
        && s != verifier
    {
        bail!("state mismatch — the pasted code belongs to a different login attempt");
    }
    let mut body = serde_json::json!({
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": verifier,
        "client_id": c.client_id,
        "redirect_uri": c.redirect_uri,
    });
    if f == Flavor::Anthropic {
        body["state"] = serde_json::json!(verifier);
    }
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .post(token_url(f))
        .header("content-type", "application/json")
        .header("user-agent", c.user_agent)
        .json(&body)
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
        account_id: v["id_token"].as_str().and_then(account_id_from_id_token),
    };
    store(provider, &tokens)
}

/// Return a valid credential, refreshing first if expired.
pub async fn credential(provider: &str) -> anyhow::Result<Credential> {
    let Some(tokens) = load(provider)? else {
        bail!("not logged in — run `damond login {provider}`");
    };
    if !tokens.is_expired() {
        return Ok(Credential {
            access_token: tokens.access_token,
            account_id: tokens.account_id,
        });
    }
    refresh_locked(provider, false).await
}

/// Return a valid access token, refreshing first if expired.
/// Called by the Anthropic provider proactively.
pub async fn access_token(provider: &str) -> anyhow::Result<String> {
    Ok(credential(provider).await?.access_token)
}

/// Force a token refresh regardless of the stored expiry — used after a
/// 401, where the server rejected a token we still believed valid.
pub async fn force_refresh(provider: &str) -> anyhow::Result<String> {
    Ok(force_credential(provider).await?.access_token)
}

/// Like [`force_refresh`] but keeps the account id alongside the token.
pub async fn force_credential(provider: &str) -> anyhow::Result<Credential> {
    refresh_locked(provider, true).await
}

/// Single-flight refresh: concurrent 401s must not race parallel
/// refreshes — both providers rotate the refresh_token, so a second
/// concurrent refresh would use the just-invalidated one and fail with
/// invalid_grant, forcing a re-login.
async fn refresh_locked(provider: &str, force: bool) -> anyhow::Result<Credential> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = LOCK.lock().await;
    let Some(tokens) = load(provider)? else {
        bail!("not logged in — run `damond login {provider}`");
    };
    // Another task may have just refreshed while we waited on the lock.
    if !force && !tokens.is_expired() {
        return Ok(Credential {
            access_token: tokens.access_token,
            account_id: tokens.account_id,
        });
    }
    refresh(provider, &tokens.refresh_token, tokens.account_id).await
}

async fn refresh(
    provider: &str,
    refresh_token: &str,
    prev_account_id: Option<String>,
) -> anyhow::Result<Credential> {
    let f = flavor(provider)?;
    if let Some(d) = device_cfg(f) {
        return refresh_device(provider, f, &d, refresh_token).await;
    }
    let c = cfg(f);
    let mut body = serde_json::json!({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": c.client_id,
    });
    if f == Flavor::OpenAi {
        // Codex sends the scope on refresh; mirror it so backend-side
        // validation stays identical to the CLI's requests.
        body["scope"] = serde_json::json!("openid profile email");
    }
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .post(token_url(f))
        .header("content-type", "application/json")
        .header("user-agent", c.user_agent)
        .json(&body)
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
    // A refresh response whose id_token lacks the claim (or any id_token at
    // all) keeps the previously stored account id — it only changes when
    // the user re-logs into a different ChatGPT account.
    let account_id = v["id_token"]
        .as_str()
        .and_then(account_id_from_id_token)
        .or(prev_account_id);
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
        account_id,
    };
    store(provider, &tokens)?;
    Ok(Credential {
        access_token: tokens.access_token,
        account_id: tokens.account_id,
    })
}

/// Refresh for device-flow flavors: kimi/xai rotate via the refresh
/// grant at their token endpoint; GitHub Copilot tokens are long-lived
/// and carry no refresh grant, so force_refresh hands back the stored
/// token unchanged (a 401 then means re-login).
async fn refresh_device(
    provider: &str,
    f: Flavor,
    d: &DeviceCfg,
    refresh_token: &str,
) -> anyhow::Result<Credential> {
    if f == Flavor::GitHubCopilot {
        let Some(tokens) = load(provider)? else {
            bail!("not logged in — run `damond login {provider}`");
        };
        tracing::warn!(
            "github-copilot token rejected by upstream — the stored GitHub \
             token may be revoked; run `damond login github-copilot`"
        );
        return Ok(Credential {
            access_token: tokens.access_token,
            account_id: None,
        });
    }
    let token_url = resolve_token_url(&d.token_url).await?;
    let resp = oauth_http_client()?
        .post(token_url)
        .header("accept", "application/json")
        .header("content-type", "application/x-www-form-urlencoded")
        .headers(static_headers(d.extra_headers))
        .form(&[
            ("client_id", d.client_id),
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ])
        .send()
        .await
        .context("token refresh request failed")?;
    let status = resp.status();
    let v: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
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
        account_id: None,
    };
    store(provider, &tokens)?;
    Ok(Credential {
        access_token: tokens.access_token,
        account_id: tokens.account_id,
    })
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

    /// Build a fake JWT whose payload carries the given claims.
    fn fake_jwt(payload: serde_json::Value) -> String {
        let b64 = |bytes: &[u8]| URL_SAFE_NO_PAD.encode(bytes);
        let header = b64(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = b64(&serde_json::to_vec(&payload).unwrap());
        format!("{header}.{payload}.c2ln")
    }

    fn jwt_with_account(account: &str) -> String {
        fake_jwt(serde_json::json!({
            "email": "u@example.com",
            "https://api.openai.com/auth": {"chatgpt_account_id": account},
        }))
    }

    #[test]
    fn account_id_parsed_from_id_token_claim() {
        assert_eq!(
            account_id_from_id_token(&jwt_with_account("acc-1")).as_deref(),
            Some("acc-1")
        );
        // No auth claim → None (caller keeps the stored account id).
        assert_eq!(
            account_id_from_id_token(&fake_jwt(serde_json::json!({"sub": "x"}))),
            None
        );
        assert_eq!(account_id_from_id_token("not-a-jwt"), None);
    }

    #[test]
    fn extract_code_accepts_raw_code_url_and_hash_state() {
        assert_eq!(extract_code("abc123").unwrap(), ("abc123".into(), None));
        assert_eq!(
            extract_code("abc123#st").unwrap(),
            ("abc123".into(), Some("st".into()))
        );
        assert_eq!(
            extract_code("http://localhost:1455/auth/callback?code=xy&state=v").unwrap(),
            ("xy".into(), Some("v".into()))
        );
        // State before code, or a trailing fragment — both fine.
        assert_eq!(
            extract_code("http://l/?state=v&code=xy#frag").unwrap(),
            ("xy".into(), Some("v".into()))
        );
        assert!(extract_code("http://l/?state=v").is_err());
        assert!(extract_code("http://l/?code=").is_err());
    }

    #[test]
    fn authorize_url_matches_flavor() {
        let (url, verifier) = authorize_url("openai").unwrap();
        assert!(
            url.starts_with("https://auth.openai.com/authorize?"),
            "{url}"
        );
        assert!(
            url.contains("client_id=app_EMoamEEZ73f0CkXaXp7hrann"),
            "{url}"
        );
        assert!(url.contains("localhost%3A1455%2Fauth%2Fcallback"), "{url}");
        assert!(url.contains("code_challenge_method=S256"), "{url}");
        assert!(url.contains(&format!("state={verifier}")), "{url}");
        assert!(!url.contains("code=true"), "{url}");

        let (url, _) = authorize_url("anthropic").unwrap();
        assert!(
            url.starts_with("https://claude.ai/oauth/authorize?"),
            "{url}"
        );
        assert!(url.contains("code=true"), "{url}");

        assert!(authorize_url("nope").is_err());
    }

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
        let err = exchange("anthropic", "some-code", "some-verifier")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("500"), "exchange: {err}");

        // refresh: same contract.
        store(
            "anthropic",
            &OAuthTokens {
                access_token: "a".into(),
                refresh_token: "r".into(),
                expires_at: 0,
                account_id: None,
            },
        )
        .unwrap();
        let err = force_refresh("anthropic").await.unwrap_err();
        assert!(err.to_string().contains("500"), "refresh: {err}");

        unsafe {
            std::env::remove_var("DAMON_TEST_TOKEN_URL");
            std::env::remove_var("DAMON_TEST_TOKEN_DIR");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
