//! Runtime model discovery: probe provider endpoints for their model list.
//! Discovered ids join routing — a request whose model matches a discovered
//! id goes to that provider even without a `models` glob.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use serde_json::Value;
use tracing::{debug, warn};

use crate::config::{Config, ProviderConfig, SecretRef};
use crate::provider::Provider;

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Run discovery for every provider that configures it, plus the implicit
/// ollama probe. Returns provider-name → discovered model ids.
/// Failures are logged and skipped — discovery never blocks startup.
pub async fn discover_all(
    cfg: &Config,
    providers: &mut HashMap<String, Arc<Provider>>,
) -> HashMap<String, Vec<String>> {
    let mut out = HashMap::new();

    // Explicit discovery on configured providers.
    for (name, p) in &cfg.providers {
        let Some(kind) = p.discovery.as_deref() else {
            continue;
        };
        match discover(name, p, kind).await {
            Ok(ids) => {
                debug!(provider = %name, count = ids.len(), "discovered models");
                out.insert(name.clone(), ids);
            }
            Err(e) => warn!(provider = %name, error = %e, "model discovery failed"),
        }
    }

    // Implicit ollama: probe the default local endpoint when unconfigured.
    if !cfg.providers.contains_key("ollama") {
        let base =
            std::env::var("OLLAMA_HOST").unwrap_or_else(|_| "http://127.0.0.1:11434".to_string());
        match probe_ollama(&base).await {
            Ok(ids) if !ids.is_empty() => {
                debug!(count = ids.len(), "implicit ollama discovered");
                // Synthesize a provider so discovered ids resolve.
                let synth = ProviderConfig {
                    api: "openai-completions".to_string(),
                    base_url: Some(format!("{base}/v1")),
                    api_key: None,
                    models: vec![],
                    default_model: None,
                    headers: HashMap::new(),
                    discovery: None,
                    context_promotion_target: None,
                    compat: Default::default(),
                };
                if let Ok(p) = Provider::new("ollama", &synth) {
                    providers.insert("ollama".to_string(), Arc::new(p));
                    out.insert("ollama".to_string(), ids);
                }
            }
            Ok(_) => {}
            Err(e) => debug!(error = %e, "implicit ollama probe failed"),
        }
    }

    out
}

/// Discover model ids for one provider.
async fn discover(name: &str, cfg: &ProviderConfig, kind: &str) -> anyhow::Result<Vec<String>> {
    let base = cfg
        .base_url
        .clone()
        .unwrap_or_else(|| match cfg.api.as_str() {
            "anthropic-messages" => "https://api.anthropic.com".to_string(),
            "gemini" => "https://generativelanguage.googleapis.com".to_string(),
            _ => "https://api.openai.com/v1".to_string(),
        });
    match kind {
        "openai-models-list" => probe_openai_models(&base, cfg).await,
        "ollama" => probe_ollama(&base).await,
        other => anyhow::bail!("provider {name}: unknown discovery type '{other}'"),
    }
}

/// GET {base}/models — OpenAI-style model list.
async fn probe_openai_models(base: &str, cfg: &ProviderConfig) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::builder().timeout(PROBE_TIMEOUT).build()?;
    let mut req = client.get(format!("{}/models", base.trim_end_matches('/')));
    if let Some(raw) = &cfg.api_key {
        if let Ok(key) = SecretRef::parse(raw).and_then(|r| r.resolve()) {
            req = req.bearer_auth(key);
        }
    }
    // Custom headers (gateway tokens etc.) apply to probes too — a
    // header-authenticating proxy must not fail discovery.
    for (k, v) in &cfg.headers {
        req = req.header(k, v);
    }
    let v: Value = req
        .send()
        .await
        .context("models probe failed")?
        .json()
        .await
        .context("models probe returned invalid JSON")?;
    let ids = v["data"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|m| m["id"].as_str().map(String::from))
        .collect();
    Ok(ids)
}

/// GET {base}/api/tags — Ollama model list.
async fn probe_ollama(base: &str) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::builder().timeout(PROBE_TIMEOUT).build()?;
    let v: Value = client
        .get(format!("{}/api/tags", base.trim_end_matches('/')))
        .send()
        .await
        .context("ollama probe failed")?
        .json()
        .await
        .context("ollama probe returned invalid JSON")?;
    let ids = v["models"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|m| m["name"].as_str().map(String::from))
        .collect();
    Ok(ids)
}
