pub mod anthropic;
pub mod compat;
pub mod discovery;
pub mod gemini;
pub mod inband;
pub mod openai;
pub mod responses;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, bail};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;

use crate::config::{Config, ProviderConfig, SecretRef};
use crate::llm::StreamEvent;

/// How long dialing a provider may take before the request is abandoned.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Upstream asked us to back off (HTTP 429/529). Carries the server's
/// Retry-After hint so callers can wait before retrying instead of
/// failing the turn outright.
#[derive(Debug)]
pub struct RateLimited(pub std::time::Duration);

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rate limited; retry after {:.0}s", self.0.as_secs_f64())
    }
}

impl std::error::Error for RateLimited {}

/// Extract a Retry-After hint (seconds) from a response, capped at 60s.
/// `None` when the header is absent, unparsable, or not a positive finite
/// number — callers decide their own default.
pub(crate) fn retry_after_opt(resp: &reqwest::Response) -> Option<std::time::Duration> {
    let secs = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<f64>().ok())
        // from_secs_f64 panics on negative/NaN/infinite input — a
        // broken upstream hint falls back to the default instead of
        // taking the turn down.
        .filter(|s| s.is_finite() && *s > 0.0)?;
    Some(std::time::Duration::from_secs_f64(secs.min(60.0)))
}

/// Extract a Retry-After hint (seconds), capped at 60s, defaulting to 2s
/// when the header is missing or malformed.
pub(crate) fn retry_after(resp: &reqwest::Response) -> std::time::Duration {
    retry_after_opt(resp).unwrap_or(std::time::Duration::from_secs(2))
}

/// Idle budget per read on a provider response — covers time-to-first-byte
/// AND mid-stream stalls. Without it a wedged upstream (black-holed TCP,
/// hung gateway) parks a turn forever: the caller's stall timer only arms
/// once the response stream exists. Generous so slow models thinking
/// before their first token are never killed.
const READ_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Provider HTTP client with hard connect + read-idle bounds. Never
/// `Client::new()`: reqwest has no default timeout, so an unbounded await
/// on `chat_stream` cannot be cancelled or timed out.
pub(crate) fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_IDLE_TIMEOUT)
        .build()
        .expect("provider HTTP client config is valid")
}

/// A provider backend. `openai-completions` passes bytes through with
/// compat shaping; `openai-responses`/`anthropic-messages`/`gemini`
/// translate to/from the OpenAI schema.
pub enum Provider {
    OpenAiCompletions(openai::OpenAiCompat),
    OpenAiResponses(responses::OpenAiResponses),
    Anthropic(anthropic::Anthropic),
    Gemini(gemini::Gemini),
}
impl Provider {
    pub fn new(name: &str, cfg: &ProviderConfig) -> anyhow::Result<Self> {
        // "oauth" is a sentinel: the provider resolves tokens itself.
        let oauth = cfg.api_key.as_deref() == Some("oauth");
        let key = match &cfg.api_key {
            Some(raw) if !oauth => Some(
                SecretRef::parse(raw)
                    .and_then(|r| r.resolve())
                    .with_context(|| format!("provider {name}: cannot resolve api_key"))?,
            ),
            _ => None,
        };
        // Header values may be secret refs (env:/keychain:/!cmd) or literals.
        let headers: HashMap<String, String> = cfg
            .headers
            .iter()
            .map(|(k, v)| {
                let resolved = match SecretRef::parse(v) {
                    Ok(r) => r
                        .resolve()
                        .with_context(|| format!("provider {name}: cannot resolve header '{k}'"))?,
                    Err(_) => v.clone(),
                };
                Ok((k.clone(), resolved))
            })
            .collect::<anyhow::Result<_>>()?;
        let base = cfg
            .base_url
            .clone()
            .unwrap_or_else(|| match cfg.api.as_str() {
                "anthropic-messages" => "https://api.anthropic.com".to_string(),
                "gemini" => "https://generativelanguage.googleapis.com".to_string(),
                _ => "https://api.openai.com/v1".to_string(),
            });
        match cfg.api.as_str() {
            "openai-completions" => Ok(Self::OpenAiCompletions(openai::OpenAiCompat::new(
                name,
                &base,
                key,
                &headers,
                cfg.compat.clone(),
            )?)),
            "openai-responses" => Ok(Self::OpenAiResponses(responses::OpenAiResponses::new(
                name,
                &base,
                key,
                &headers,
                cfg.compat.clone(),
            )?)),
            "anthropic-messages" => Ok(Self::Anthropic(anthropic::Anthropic::new(
                name, &base, key, &headers, oauth,
            )?)),
            "gemini" => Ok(Self::Gemini(gemini::Gemini::new(
                name, &base, key, &headers,
            )?)),
            other => bail!("provider {name}: unknown api '{other}'"),
        }
    }

    /// Raw passthrough — only openai-completions supports it.
    pub async fn forward(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Bytes,
    ) -> anyhow::Result<UpstreamResponse> {
        match self {
            Self::OpenAiCompletions(p) => p.forward(method, path, body).await,
            _ => bail!("raw passthrough only supported for openai-completions providers"),
        }
    }

    /// Streaming chat completion in the normalized OpenAI event model.
    /// `body` is the OpenAI-shaped request JSON (model already rewritten).
    pub async fn chat_stream(
        &self,
        body: Value,
    ) -> anyhow::Result<Box<dyn futures::Stream<Item = anyhow::Result<StreamEvent>> + Send + Unpin>>
    {
        match self {
            Self::OpenAiCompletions(p) => p.chat_stream(body).await,
            Self::OpenAiResponses(p) => p.chat_stream(body).await,
            Self::Anthropic(p) => p.chat_stream(body).await,
            Self::Gemini(p) => p.chat_stream(body).await,
        }
    }

    /// Non-streaming chat completion — collects the stream into one response.
    pub async fn chat(&self, body: Value) -> anyhow::Result<Value> {
        match self {
            Self::OpenAiCompletions(p) => p.chat(body).await,
            Self::OpenAiResponses(p) => p.chat(body).await,
            Self::Anthropic(p) => p.chat(body).await,
            Self::Gemini(p) => p.chat(body).await,
        }
    }

    /// Model list for /v1/models.
    pub async fn list_models(&self) -> anyhow::Result<Value> {
        match self {
            Self::OpenAiCompletions(p) => p.list_models().await,
            Self::OpenAiResponses(p) => p.list_models().await,
            Self::Anthropic(_) | Self::Gemini(_) => {
                bail!("model listing not supported for this provider api")
            }
        }
    }
}

pub struct UpstreamResponse {
    pub status: reqwest::StatusCode,
    pub content_type: String,
    /// Server-provided backoff hint (HTTP 429/529 `Retry-After`), captured
    /// before the body stream takes over. `None` when the upstream sent no
    /// usable hint — callers pick their own short default then.
    pub retry_after: Option<std::time::Duration>,
    pub stream: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
}

impl UpstreamResponse {
    /// Convert into an axum Response, streaming the body through.
    pub fn into_response(self) -> axum::response::Response {
        axum::response::Response::builder()
            .status(self.status)
            .header("content-type", self.content_type)
            .body(axum::body::Body::from_stream(self.stream))
            .unwrap()
    }
}

/// Build providers from config. Individual failures are collected, not
/// fatal — the daemon still boots and serves /health without providers.
pub fn build_providers(cfg: &Config) -> (HashMap<String, Arc<Provider>>, Vec<anyhow::Error>) {
    let mut providers = HashMap::new();
    let mut errors = Vec::new();
    for (name, p) in &cfg.providers {
        match Provider::new(name, p) {
            Ok(p) => {
                providers.insert(name.clone(), Arc::new(p));
            }
            Err(e) => errors.push(e),
        }
    }
    (providers, errors)
}

/// Hard cap for `collect_stream` — bodies collected here are complete
/// JSON responses, never bulk transfers. Beyond this the stream is
/// runaway and must fail instead of exhausting memory.
const COLLECT_STREAM_MAX: usize = 64 * 1024 * 1024;

/// Collect a byte stream into a Vec, bounded by `COLLECT_STREAM_MAX`.
pub async fn collect_stream(
    mut s: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = s.next().await {
        buf.extend_from_slice(&chunk?);
        if buf.len() > COLLECT_STREAM_MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "upstream response exceeds the 64MiB collect_stream cap",
            ));
        }
    }
    Ok(buf)
}

/// Extract text from an OpenAI message `content` field: either a plain
/// string or the array form `[{type:"text",text:..}, ...]` (multimodal
/// messages carry text parts alongside images — non-text parts drop).
pub fn content_text(content: &Value) -> String {
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter_map(|p| {
                    if p["type"].as_str() == Some("text") {
                        p["text"].as_str().map(String::from)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

/// Register an original tool name and return the wire name to send.
/// `a.b` mangles to `a__b` — which a tool may also be literally named,
/// so when the mangled form is already claimed by a DIFFERENT original
/// the wire name becomes `<mangled>__<sha256[:8]>` (within the 64-char
/// provider limit). The returned name is exactly what the map restores,
/// so what goes on the wire and what comes back never disagree.
pub(crate) fn mangled_for(
    names: &mut std::collections::HashMap<String, String>,
    orig: &str,
) -> String {
    let base = orig.replace('.', "__");
    if names.get(&base).is_none_or(|claimed| claimed == orig) {
        names.insert(base.clone(), orig.to_string());
        return base;
    }
    use sha2::Digest;
    let digest = sha2::Sha256::digest(orig.as_bytes());
    let hash: String = digest[..4].iter().map(|b| format!("{b:02x}")).collect();
    let prefix: String = base.chars().take(54).collect();
    let variant = format!("{prefix}__{hash}");
    names.insert(variant.clone(), orig.to_string());
    variant
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_stream_allows_normal_bodies() {
        let s = Box::pin(futures::stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"hello")),
            Ok(Bytes::from_static(b" world")),
        ]));
        assert_eq!(
            futures::executor::block_on(collect_stream(s)).unwrap(),
            b"hello world"
        );
    }

    /// A stream beyond the 64MiB cap must fail, not buffer without
    /// bound — a runaway upstream otherwise exhausts memory.
    #[test]
    fn collect_stream_enforces_cap() {
        let chunk = Bytes::from(vec![0u8; 1024 * 1024]);
        let s = futures::stream::repeat_with(move || Ok(chunk.clone())).take(70);
        let err = futures::executor::block_on(collect_stream(Box::pin(s))).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
