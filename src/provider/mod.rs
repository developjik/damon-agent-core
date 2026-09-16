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
                    Ok(r) => r.resolve().with_context(|| {
                        format!("provider {name}: cannot resolve header '{k}'")
                    })?,
                    Err(_) => v.clone(),
                };
                Ok((k.clone(), resolved))
            })
            .collect::<anyhow::Result<_>>()?;
        let base = cfg.base_url.clone().unwrap_or_else(|| match cfg.api.as_str() {
            "anthropic-messages" => "https://api.anthropic.com".to_string(),
            "gemini" => "https://generativelanguage.googleapis.com".to_string(),
            _ => "https://api.openai.com/v1".to_string(),
        });
        match cfg.api.as_str() {
            "openai-completions" => Ok(Self::OpenAiCompletions(openai::OpenAiCompat::new(
                name, &base, key, &headers, cfg.compat.clone(),
            )?)),
            "openai-responses" => Ok(Self::OpenAiResponses(responses::OpenAiResponses::new(
                name, &base, key, &headers, cfg.compat.clone(),
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
    pub stream:
        std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
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

/// Collect a byte stream into a Vec.
pub async fn collect_stream(
    mut s: std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<Bytes>> + Send>>,
) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = s.next().await {
        buf.extend_from_slice(&chunk?);
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
