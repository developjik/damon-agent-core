//! omp-parity provider presets.
//!
//! The omp coding agent routes to a large table of hosted and local model
//! providers. Nearly all of them speak one of Damon's four wire formats at
//! a known base URL behind a known environment variable — the knowledge is
//! the missing piece, not the transport. This catalog encodes it: at boot
//! (and on every config reload) each preset whose key environment variable
//! is set and whose id is not explicitly configured is synthesized into a
//! real provider, so a project picks up its available backends with zero
//! Damon-specific config.
//!
//! Explicit `[providers.X]` config always wins over a preset with the same
//! id — presets only fill the gaps.

use std::collections::BTreeMap;

use crate::config::{ProviderCompat, ProviderConfig};

/// One known provider shape.
pub struct Preset {
    /// Provider id (also the routing prefix: `groq/llama-...`).
    pub id: &'static str,
    /// Wire API — one of Damon's four transports.
    pub api: &'static str,
    /// Default base URL. `{VAR}` placeholders are filled from the
    /// environment; an unresolved placeholder skips the preset.
    pub base_url: &'static str,
    /// When this env var is set, it replaces the base URL entirely
    /// (self-hosted / alternate-region endpoints).
    pub base_url_env: Option<&'static str>,
    /// Key env vars in priority order — first non-empty one supplies the
    /// key. Empty slice = keyless (local engines).
    pub key_env: &'static [&'static str],
    /// Discovery kind for providers that list models at `{base}/models`.
    pub discovery: Option<&'static str>,
    /// Static headers; values are SecretRef-compatible (`env:VAR`).
    pub headers: &'static [(&'static str, &'static str)],
    /// When set, the resolved key rides in this header (`env:VAR` value)
    /// instead of `api_key` — Azure's `api-key` header, not Bearer.
    pub key_header: Option<&'static str>,
    /// OAuth flavor preset: register only when the keychain already holds
    /// tokens (`damond login <flavor>`); api_key becomes the
    /// `oauth:<flavor>` sentinel instead of a key reference.
    pub oauth: Option<&'static str>,
    /// Extra compat flags.
    pub compat: fn(&mut ProviderCompat),
    /// One-line note for `damond presets`.
    pub note: &'static str,
}

impl Preset {
    fn compat(&self) -> ProviderCompat {
        let mut c = ProviderCompat::default();
        (self.compat)(&mut c);
        c
    }

    fn header_map(&self) -> std::collections::HashMap<String, String> {
        self.headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
}

fn no_compat(_: &mut ProviderCompat) {}

fn mistral_compat(c: &mut ProviderCompat) {
    c.requires_tool_result_name = true;
    c.requires_mistral_tool_ids = true;
}

fn azure_compat(c: &mut ProviderCompat) {
    c.azure_deployment_urls = true;
}

fn vertex_compat(c: &mut ProviderCompat) {
    c.vertex = true;
}

fn bedrock_bearer_compat(c: &mut ProviderCompat) {
    c.bearer_auth = true;
}

/// The omp-parity catalog. Confident entries only — a provider whose
/// endpoint contract Damon cannot honor today (bespoke OAuth transports
/// like github-copilot/cursor/qwen-portal, or SigV4 Bedrock Converse) is
/// deliberately absent rather than guessed; the generic `[providers.X]`
/// config covers custom shapes.
pub const PRESETS: &[Preset] = &[
    Preset {
        id: "google",
        api: "gemini",
        base_url: "https://generativelanguage.googleapis.com",
        base_url_env: Some("GOOGLE_GENAI_BASE_URL"),
        key_env: &["GEMINI_API_KEY", "GOOGLE_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Google Gemini API (developer API key)",
    },
    Preset {
        id: "google-vertex",
        api: "gemini",
        base_url: "https://{GOOGLE_CLOUD_LOCATION}-aiplatform.googleapis.com/v1/projects/{GOOGLE_CLOUD_PROJECT}/locations/{GOOGLE_CLOUD_LOCATION}/publishers/google",
        base_url_env: None,
        key_env: &["GOOGLE_CLOUD_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: vertex_compat,
        note: "Google Vertex AI (express API key; needs GOOGLE_CLOUD_PROJECT + GOOGLE_CLOUD_LOCATION)",
    },
    Preset {
        id: "groq",
        api: "openai-completions",
        base_url: "https://api.groq.com/openai/v1",
        base_url_env: Some("GROQ_BASE_URL"),
        key_env: &["GROQ_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Groq hosted LPU inference",
    },
    Preset {
        id: "openrouter",
        api: "openai-completions",
        base_url: "https://openrouter.ai/api/v1",
        base_url_env: Some("OPENROUTER_BASE_URL"),
        key_env: &["OPENROUTER_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "OpenRouter multi-provider gateway",
    },
    Preset {
        id: "mistral",
        api: "openai-completions",
        base_url: "https://api.mistral.ai/v1",
        base_url_env: Some("MISTRAL_BASE_URL"),
        key_env: &["MISTRAL_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: mistral_compat,
        note: "Mistral platform (tool-result name + 9-char tool ids)",
    },
    Preset {
        id: "xai",
        api: "openai-completions",
        base_url: "https://api.x.ai/v1",
        base_url_env: Some("XAI_BASE_URL"),
        key_env: &["XAI_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "xAI Grok API",
    },
    Preset {
        id: "deepseek",
        api: "openai-completions",
        base_url: "https://api.deepseek.com",
        base_url_env: Some("DEEPSEEK_BASE_URL"),
        key_env: &["DEEPSEEK_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "DeepSeek platform",
    },
    Preset {
        id: "fireworks",
        api: "openai-completions",
        base_url: "https://api.fireworks.ai/inference/v1",
        base_url_env: Some("FIREWORKS_BASE_URL"),
        key_env: &["FIREWORKS_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Fireworks AI inference",
    },
    Preset {
        id: "together",
        api: "openai-completions",
        base_url: "https://api.together.xyz/v1",
        base_url_env: Some("TOGETHER_BASE_URL"),
        key_env: &["TOGETHER_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Together AI inference",
    },
    Preset {
        id: "cerebras",
        api: "openai-completions",
        base_url: "https://api.cerebras.ai/v1",
        base_url_env: Some("CEREBRAS_BASE_URL"),
        key_env: &["CEREBRAS_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Cerebras inference",
    },
    Preset {
        id: "nvidia",
        api: "openai-completions",
        base_url: "https://integrate.api.nvidia.com/v1",
        base_url_env: Some("NVIDIA_BASE_URL"),
        key_env: &["NVIDIA_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "NVIDIA NIM catalog",
    },
    Preset {
        id: "deepinfra",
        api: "openai-completions",
        base_url: "https://api.deepinfra.com/v1/openai",
        base_url_env: Some("DEEPINFRA_BASE_URL"),
        key_env: &["DEEPINFRA_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "DeepInfra inference",
    },
    Preset {
        id: "siliconflow",
        api: "openai-completions",
        base_url: "https://api.siliconflow.com/v1",
        base_url_env: None,
        key_env: &["SILICONFLOW_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "SiliconFlow (global)",
    },
    Preset {
        id: "siliconflow-cn",
        api: "openai-completions",
        base_url: "https://api.siliconflow.cn/v1",
        base_url_env: None,
        key_env: &["SILICONFLOW_CN_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "SiliconFlow (China)",
    },
    Preset {
        id: "moonshot",
        api: "openai-completions",
        base_url: "https://api.moonshot.ai/v1",
        base_url_env: Some("MOONSHOT_BASE_URL"),
        key_env: &["MOONSHOT_API_KEY", "KIMI_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Moonshot AI / Kimi",
    },
    Preset {
        id: "zai",
        api: "openai-completions",
        base_url: "https://api.z.ai/api/coding/paas/v4",
        base_url_env: Some("ZAI_BASE_URL"),
        key_env: &["ZAI_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Z.AI coding plan (route as zai/<model>)",
    },
    Preset {
        id: "zhipu-bigmodel",
        api: "openai-completions",
        base_url: "https://open.bigmodel.cn/api/paas/v4",
        base_url_env: None,
        key_env: &["BIGMODEL_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Zhipu BigModel pay-as-you-go (<id>.<secret> key)",
    },
    Preset {
        id: "minimax",
        api: "openai-completions",
        base_url: "https://api.minimax.io/v1",
        base_url_env: None,
        key_env: &["MINIMAX_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "MiniMax (global)",
    },
    Preset {
        id: "venice",
        api: "openai-completions",
        base_url: "https://api.venice.ai/api/v1",
        base_url_env: None,
        key_env: &["VENICE_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Venice AI inference",
    },
    Preset {
        id: "huggingface",
        api: "openai-completions",
        base_url: "https://router.huggingface.co/v1",
        base_url_env: None,
        key_env: &["HUGGINGFACE_HUB_TOKEN", "HF_TOKEN"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Hugging Face Inference routers",
    },
    Preset {
        id: "vercel-ai-gateway",
        api: "openai-completions",
        base_url: "https://ai-gateway.vercel.sh/v1",
        base_url_env: None,
        key_env: &["AI_GATEWAY_API_KEY", "VERCEL_AI_GATEWAY_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Vercel AI Gateway",
    },
    Preset {
        id: "litellm",
        api: "openai-completions",
        base_url: "http://localhost:4000/v1",
        base_url_env: Some("LITELLM_BASE_URL"),
        key_env: &["LITELLM_API_KEY"],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "LiteLLM proxy (set LITELLM_BASE_URL for remote)",
    },
    Preset {
        id: "azure",
        api: "openai-completions",
        base_url: "{AZURE_OPENAI_ENDPOINT}/openai",
        base_url_env: None,
        key_env: &["AZURE_OPENAI_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: Some("api-key"),
        oauth: None,
        compat: azure_compat,
        note: "Azure OpenAI (deployment URLs; needs AZURE_OPENAI_ENDPOINT)",
    },
    Preset {
        id: "bedrock-mantle",
        api: "anthropic-messages",
        base_url: "https://bedrock-runtime.{AWS_REGION}.amazonaws.com",
        base_url_env: None,
        key_env: &["AWS_BEARER_TOKEN_BEDROCK"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: bedrock_bearer_compat,
        note: "Bedrock bearer auth, Anthropic surface (set AWS_REGION; route as bedrock-mantle/<model-id>)",
    },
    Preset {
        id: "lm-studio",
        api: "openai-completions",
        base_url: "http://127.0.0.1:1234/v1",
        base_url_env: Some("LM_STUDIO_BASE_URL"),
        key_env: &[],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "LM Studio local server (keyless; also works for oMLX)",
    },
    Preset {
        id: "llama.cpp",
        api: "openai-completions",
        base_url: "http://127.0.0.1:8080/v1",
        base_url_env: Some("LLAMA_CPP_BASE_URL"),
        key_env: &[],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "llama.cpp server (keyless)",
    },
    Preset {
        id: "kimi-code",
        api: "openai-completions",
        base_url: "https://api.kimi.com/coding/v1",
        base_url_env: Some("KIMI_CODE_BASE_URL"),
        key_env: &[],
        discovery: Some("openai-models-list"),
        headers: &[],
        key_header: None,
        oauth: Some("kimi-code"),
        compat: no_compat,
        note: "Kimi For Coding subscription (damond login kimi-code)",
    },
    Preset {
        id: "xai-oauth",
        api: "openai-responses",
        base_url: "https://api.x.ai/v1",
        base_url_env: None,
        key_env: &[],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: Some("xai-oauth"),
        compat: no_compat,
        note: "SuperGrok / X Premium+ (damond login xai-oauth; route as xai-oauth/grok-...)",
    },
    Preset {
        id: "github-copilot",
        api: "openai-completions",
        base_url: "https://api.githubcopilot.com",
        base_url_env: Some("COPILOT_BASE_URL"),
        key_env: &[],
        discovery: Some("openai-models-list"),
        headers: &[
            ("Editor-Version", "copilot/1.0.82"),
            ("Copilot-Integration-Id", "copilot-developer-cli"),
        ],
        key_header: None,
        oauth: Some("github-copilot"),
        compat: no_compat,
        note: "GitHub Copilot subscription (damond login github-copilot)",
    },
    Preset {
        id: "qwen-portal",
        api: "openai-completions",
        base_url: "https://portal.qwen.ai/v1",
        base_url_env: None,
        key_env: &["QWEN_OAUTH_TOKEN", "QWEN_PORTAL_API_KEY"],
        discovery: None,
        headers: &[],
        key_header: None,
        oauth: None,
        compat: no_compat,
        note: "Qwen Portal (token from chat.qwen.ai; models coder-model/vision-model)",
    },
];

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

impl Preset {
    /// Resolve against the current environment: `None` when a required
    /// env var is missing (no key, or an unresolved `{PLACEHOLDER}`).
    fn resolve(&self) -> Option<ProviderConfig> {
        // Key: first non-empty env var wins; keyless presets skip. A
        // `key_header` preset puts the reference in headers instead of
        // api_key (Azure's `api-key`, not `Authorization: Bearer`).
        let (api_key, headers) = match self.oauth {
            // OAuth presets need stored tokens — an unauthenticated
            // provider would only produce keychain errors per request.
            Some(flavor) => match crate::oauth::load(flavor) {
                Ok(Some(_)) => (Some(format!("oauth:{flavor}")), self.header_map()),
                _ => return None,
            },
            _ => match self.key_env {
                [] => (None, self.header_map()),
                vars => {
                    let var = vars.iter().find(|v| env_value(v).is_some())?;
                    match self.key_header {
                        Some(h) => {
                            let mut headers = self.header_map();
                            headers.insert(h.to_string(), format!("env:{var}"));
                            (None, headers)
                        }
                        None => (Some(format!("env:{var}")), self.header_map()),
                    }
                }
            },
        };
        // Base URL: full-override env var beats the template; `{VAR}`
        // placeholders must all resolve or the preset is skipped.
        let base_url = match self.base_url_env.and_then(env_value) {
            Some(url) => url,
            None => {
                let mut url = self.base_url.to_string();
                for (i, chunk) in self.base_url.split('{').enumerate() {
                    if i == 0 {
                        continue;
                    }
                    let Some((var, _)) = chunk.split_once('}') else {
                        continue;
                    };
                    let val = env_value(var)?;
                    url = url.replace(&format!("{{{var}}}"), &val);
                }
                url
            }
        };
        Some(ProviderConfig {
            api: self.api.to_string(),
            base_url: Some(base_url),
            api_key,
            models: vec![],
            default_model: None,
            headers,
            discovery: self.discovery.map(String::from),
            context_promotion_target: None,
            compat: self.compat(),
        })
    }

    /// Whether the preset would resolve right now (env-var view only —
    /// used by `damond presets` to mark availability).
    pub fn env_active(&self) -> bool {
        if let Some(flavor) = self.oauth {
            return crate::oauth::load(flavor).is_ok_and(|t| t.is_some());
        }
        (self.key_env.is_empty() || self.key_env.iter().any(|v| env_value(v).is_some()))
            && self
                .base_url
                .split('{')
                .skip(1)
                .filter_map(|c| c.split_once('}').map(|(v, _)| v))
                .all(|v| env_value(v).is_some())
    }
}

/// Synthesize providers from the catalog: every preset whose key env is
/// set (or that is keyless) and whose id is NOT explicitly configured.
/// Returns (name, config) pairs in catalog order.
pub fn resolve_env_providers(
    configured: &BTreeMap<String, ProviderConfig>,
) -> Vec<(String, ProviderConfig)> {
    PRESETS
        .iter()
        .filter(|p| !configured.contains_key(p.id))
        .filter_map(|p| p.resolve().map(|c| (p.id.to_string(), c)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize env mutation — these vars are process-global.
    static ENV_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn find(id: &str) -> &'static Preset {
        PRESETS.iter().find(|p| p.id == id).unwrap()
    }

    #[test]
    fn key_preset_resolves_from_env() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; serialized by ENV_LOCK and removed below.
        unsafe {
            std::env::set_var("GROQ_API_KEY", "sk-test");
        }
        let p = find("groq").resolve().unwrap();
        assert_eq!(p.api, "openai-completions");
        assert_eq!(
            p.base_url.as_deref(),
            Some("https://api.groq.com/openai/v1")
        );
        assert_eq!(p.api_key.as_deref(), Some("env:GROQ_API_KEY"));
        assert_eq!(p.discovery.as_deref(), Some("openai-models-list"));
        unsafe {
            std::env::remove_var("GROQ_API_KEY");
        }
    }

    #[test]
    fn missing_env_skips_preset() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; serialized by ENV_LOCK.
        unsafe {
            std::env::remove_var("GROQ_API_KEY");
        }
        assert!(find("groq").resolve().is_none());
    }

    #[test]
    fn keyless_preset_always_resolves() {
        let p = find("lm-studio").resolve().unwrap();
        assert!(p.api_key.is_none());
        assert_eq!(p.base_url.as_deref(), Some("http://127.0.0.1:1234/v1"));
        assert_eq!(p.discovery.as_deref(), Some("openai-models-list"));
    }

    #[test]
    fn base_url_env_var_overrides_template() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; serialized by ENV_LOCK and removed below.
        unsafe {
            std::env::set_var("LM_STUDIO_BASE_URL", "http://127.0.0.1:9999/v1");
        }
        let p = find("lm-studio").resolve().unwrap();
        assert_eq!(p.base_url.as_deref(), Some("http://127.0.0.1:9999/v1"));
        unsafe {
            std::env::remove_var("LM_STUDIO_BASE_URL");
        }
    }

    #[test]
    fn vertex_placeholder_requires_both_vars() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; serialized by ENV_LOCK; removed below.
        unsafe {
            std::env::remove_var("GOOGLE_CLOUD_PROJECT");
            std::env::remove_var("GOOGLE_CLOUD_LOCATION");
        }
        assert!(find("google-vertex").resolve().is_none());
        unsafe {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "proj");
            std::env::set_var("GOOGLE_CLOUD_LOCATION", "us-central1");
            std::env::set_var("GOOGLE_CLOUD_API_KEY", "key");
        }
        let p = find("google-vertex").resolve().unwrap();
        assert!(
            p.base_url
                .as_deref()
                .unwrap()
                .ends_with("/v1/projects/proj/locations/us-central1/publishers/google")
        );
        assert!(p.compat.vertex);
        unsafe {
            std::env::remove_var("GOOGLE_CLOUD_PROJECT");
            std::env::remove_var("GOOGLE_CLOUD_LOCATION");
            std::env::remove_var("GOOGLE_CLOUD_API_KEY");
        }
    }

    #[test]
    fn azure_preset_routes_key_through_header() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; serialized by ENV_LOCK; removed below.
        unsafe {
            std::env::set_var("AZURE_OPENAI_ENDPOINT", "https://res.openai.azure.com");
            std::env::set_var("AZURE_OPENAI_API_KEY", "azkey");
        }
        let p = find("azure").resolve().unwrap();
        assert!(p.api_key.is_none(), "key rides in the api-key header");
        assert_eq!(
            p.base_url.as_deref(),
            Some("https://res.openai.azure.com/openai")
        );
        assert!(p.compat.azure_deployment_urls);
        assert_eq!(
            p.headers.get("api-key").map(String::as_str),
            Some("env:AZURE_OPENAI_API_KEY")
        );
        unsafe {
            std::env::remove_var("AZURE_OPENAI_ENDPOINT");
            std::env::remove_var("AZURE_OPENAI_API_KEY");
        }
    }

    #[test]
    fn oauth_preset_requires_login() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; this var is not read elsewhere.
        unsafe {
            std::env::remove_var("KIMI_CODE_BASE_URL");
        }
        // Not logged in (fresh keychain entry) → the preset is skipped;
        // it would only produce keychain errors per request.
        assert!(find("kimi-code").resolve().is_none());
        assert!(!find("kimi-code").env_active());
    }

    #[test]
    fn env_active_mirrors_resolution_gate() {
        let _g = ENV_LOCK.lock();
        // SAFETY: test-only; serialized by ENV_LOCK and removed below.
        unsafe {
            std::env::remove_var("GROQ_API_KEY");
        }
        assert!(!find("groq").env_active());
        assert!(find("lm-studio").env_active());
    }
}
