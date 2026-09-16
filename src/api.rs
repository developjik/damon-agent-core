use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use serde_json::json;
use tracing::warn;
use futures::StreamExt;

use crate::config::SharedConfig;
use crate::mcp::McpRegistry;
use crate::provider::{Provider, build_providers};
use crate::store::Store;

pub struct AppState {
    pub config: SharedConfig,
    /// Rebuilt on config reload. Guards are never held across .await.
    pub providers: parking_lot::RwLock<HashMap<String, Arc<Provider>>>,
    /// Discovered model ids per provider (from `discovery` probes).
    /// Rebuilt alongside providers.
    pub discovered: parking_lot::RwLock<HashMap<String, Vec<String>>>,
    pub store: Store,
    pub mcp: McpRegistry,
    /// The address the server actually bound. A non-loopback bind must
    /// keep requiring a resolvable token even if a hot reload drops it.
    pub bind: std::net::SocketAddr,
    /// (raw auth_token, resolved value). Resolved once per raw value —
    /// a `!cmd` or keychain ref must not spawn a shell per request.
    /// Err is cached too: a resolution failure must fail closed, never
    /// silently open the API.
    auth_token_cache: tokio::sync::Mutex<(Option<String>, Option<Result<String, String>>)>,
}

impl AppState {
    pub async fn new(config: SharedConfig, store: Store, mcp: McpRegistry) -> Arc<Self> {
        Self::with_bind(config, store, mcp, "127.0.0.1:0".parse().unwrap()).await
    }

    pub async fn with_bind(
        config: SharedConfig,
        store: Store,
        mcp: McpRegistry,
        bind: std::net::SocketAddr,
    ) -> Arc<Self> {
        let (mut providers, errors) = {
            let cfg = config.read();
            build_providers(&cfg)
        };
        for e in &errors {
            warn!(error = %e, "provider init failed");
        }
        let discovered = {
            let cfg = config.read().clone();
            crate::provider::discovery::discover_all(&cfg, &mut providers).await
        };
        Arc::new(Self {
            config,
            providers: parking_lot::RwLock::new(providers),
            discovered: parking_lot::RwLock::new(discovered),
            store,
            mcp,
            bind,
            auth_token_cache: tokio::sync::Mutex::new((None, None)),
        })
    }

    /// Resolved auth token, cached per raw config value. A `!cmd` or
    /// keychain ref resolves once per config change, not per request.
    /// Err = configured but unresolvable — callers must fail closed.
    /// Resolution runs off the async worker: `!cmd` blocks up to 10s.
    pub async fn auth_token(&self) -> Option<Result<String, String>> {
        let raw = self.config.read().auth_token.clone();
        {
            let cache = self.auth_token_cache.lock().await;
            if cache.0 == raw {
                return cache.1.clone();
            }
        }
        let raw2 = raw.clone();
        let resolved: Option<Result<String, String>> =
            match tokio::task::spawn_blocking(move || {
                raw2.as_deref().map(|r| {
                    match crate::config::SecretRef::parse(r) {
                        Ok(s) => s.resolve().map_err(|e| e.to_string()),
                        Err(_) => Ok(r.to_string()),
                    }
                })
            })
            .await
            {
                Ok(r) => r,
                Err(e) => Some(Err(e.to_string())),
            };
        let mut cache = self.auth_token_cache.lock().await;
        // Another task may have resolved a newer raw value meanwhile —
        // only store if the raw value is still current.
        if self.config.read().auth_token == raw {
            *cache = (raw, resolved.clone());
        }
        resolved
    }

    /// Rebuild providers after a config reload. If every provider fails to
    /// build, keep the previous set rather than dropping to zero.
    pub async fn reload_providers(&self) {
        let cfg = self.config.read().clone();
        let (mut providers, errors) = build_providers(&cfg);
        for e in &errors {
            warn!(error = %e, "provider rebuild failed");
        }
        if providers.is_empty() && !errors.is_empty() {
            return;
        }
        let discovered = crate::provider::discovery::discover_all(&cfg, &mut providers).await;
        *self.providers.write() = providers;
        *self.discovered.write() = discovered;
    }

    pub fn default_provider(&self) -> Option<Arc<Provider>> {
        let cfg = self.config.read();
        cfg.default_provider()
            .map(|(name, _)| name.to_string())
            .and_then(|name| self.providers.read().get(&name).cloned())
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    let v1 = Router::new()
        .route("/models", get(models))
        .route("/chat/completions", post(chat_completions))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token));

    Router::new()
        .route("/health", get(health))
        .route("/ws", get(crate::rpc::ws_handler))
        .nest("/v1", v1)
        .with_state(state)
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "providers": state.providers.read().len(),
    }))
}

async fn models(State(state): State<Arc<AppState>>) -> Response {
    // Default provider's list, merged with discovered ids from every
    // provider that probed successfully.
    let resp = forward(&state, reqwest::Method::GET, "/models", bytes::Bytes::new()).await;
    let discovered = state.discovered.read().clone();
    if discovered.is_empty() {
        return resp;
    }
    let (parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, 1 << 20).await {
        Ok(b) => b,
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };
    let mut v: serde_json::Value =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({"object":"list","data":[]}));
    let data = v["data"].as_array_mut();
    let mut seen: std::collections::HashSet<String> = data
        .as_ref()
        .map(|d| {
            d.iter()
                .filter_map(|m| m["id"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let mut extra = Vec::new();
    for (prov, ids) in &discovered {
        for id in ids {
            if seen.insert(id.clone()) {
                extra.push(json!({
                    "id": format!("{prov}/{id}"),
                    "object": "model",
                    "owned_by": prov,
                }));
            }
        }
    }
    if let Some(d) = data {
        d.extend(extra);
    } else {
        v["data"] = json!(extra);
    }
    Json(v).into_response()
}

async fn chat_completions(State(state): State<Arc<AppState>>, body: bytes::Bytes) -> Response {
    forward(&state, reqwest::Method::POST, "/chat/completions", body).await
}

/// Route by model field → provider. openai-compat passes through raw;
/// anthropic/gemini translate to/from OpenAI shape.
async fn forward(
    state: &Arc<AppState>,
    method: reqwest::Method,
    path: &str,
    body: bytes::Bytes,
) -> Response {
    // Route on the model field for chat completions. A `:low|:medium|:high`
    // suffix is a thinking-level selector — strip it before routing and
    // carry it as `_thinking` for the adapters.
    let (model, thinking) = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v["model"].as_str().map(String::from))
        .map(|m| {
            let (m, t) = crate::config::Config::split_thinking_level(&m);
            (Some(m.to_string()), t.map(String::from))
        })
        .unwrap_or((None, None));

    let provider = {
        let cfg = state.config.read();
        let providers = state.providers.read();
        let discovered = state.discovered.read();
        // Strict route first (prefix/glob); discovered ids claim the model
        // before it falls through to the default provider.
        let routed = model.as_deref().and_then(|m| {
            cfg.route_model_strict(m)
                .map(|(name, _, upstream)| (name.to_string(), upstream))
                .or_else(|| {
                    discovered.iter().find_map(|(name, ids)| {
                        ids.iter()
                            .any(|id| id == m)
                            .then(|| (name.clone(), m.to_string()))
                    })
                })
        });
        routed
            .or_else(|| cfg.default_provider().map(|(n, _)| (n.to_string(), String::new())))
            .and_then(|(name, upstream)| {
                providers.get(&name).cloned().map(|p| (name, p, upstream))
            })
    };
    let Some((provider_name, provider, upstream_model)) = provider else {
        return openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no provider configured",
            "server_error",
        );
    };

    // Inject the thinking level into the body for the adapters.
    let body = if let Some(level) = &thinking {
        let mut v: serde_json::Value = serde_json::from_slice(&body)
            .unwrap_or_else(|_| json!({}));
        v["_thinking"] = json!(level);
        bytes::Bytes::from(v.to_string())
    } else {
        body
    };

    // Non-completions providers: translate instead of passthrough.
    if !matches!(&*provider, crate::provider::Provider::OpenAiCompletions(_)) {
        return translate_forward(state, &provider_name, &provider, path, body, upstream_model)
            .await;
    }

    match provider.forward(method.clone(), path, body.clone()).await {
        Ok(up) => {
            // Context promotion: on a context-overflow error, retry once with
            // the configured promotion target. Client errors are buffered so
            // the body can be inspected.
            if up.status.is_client_error() {
                return promote_or_error(
                    state, &provider_name, &provider, method, path, body, up,
                )
                .await;
            }
            up.into_response()
        }
        Err(e) => openai_error(
            StatusCode::BAD_GATEWAY,
            &format!("upstream error: {e}"),
            "server_error",
        ),
    }
}

/// Buffer a client-error response; if it's a context overflow and the
/// provider has a promotion target, retry once with the target model.
/// Otherwise re-emit the buffered error.
async fn promote_or_error(
    state: &Arc<AppState>,
    provider_name: &str,
    _provider: &Arc<crate::provider::Provider>,
    method: reqwest::Method,
    path: &str,
    body: bytes::Bytes,
    up: crate::provider::UpstreamResponse,
) -> Response {
    let status = up.status;
    let buf = match crate::provider::collect_stream(up.stream).await {
        Ok(b) => b,
        Err(_) => return error_response(status, ""),
    };
    let text = String::from_utf8_lossy(&buf);
    if !is_context_overflow(&text) {
        return error_response(status, &text);
    }
    let target = {
        let cfg = state.config.read();
        cfg.providers
            .get(provider_name)
            .and_then(|p| p.context_promotion_target.clone())
    };
    let Some(target) = target else {
        return error_response(status, &text);
    };
    // Resolve target: "provider/model" or bare "model" (same provider).
    let (tname, tmodel) = match target.split_once('/') {
        Some((p, m)) => (p.to_string(), m.to_string()),
        None => (provider_name.to_string(), target),
    };
    let Some(tprovider) = state.providers.read().get(&tname).cloned() else {
        return error_response(status, &text);
    };
    let mut req: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return error_response(status, &text),
    };
    req["model"] = serde_json::Value::String(tmodel.clone());
    let body = bytes::Bytes::from(req.to_string());
    // Promoted request goes through the same path (translate for non-compat).
    if !matches!(&*tprovider, crate::provider::Provider::OpenAiCompletions(_)) {
        return translate_forward(
            state,
            &tname,
            &tprovider,
            path,
            body,
            tmodel,
        )
        .await;
    }
    match tprovider.forward(method, path, body).await {
        Ok(up) => up.into_response(),
        Err(e) => openai_error(
            StatusCode::BAD_GATEWAY,
            &format!("upstream error: {e}"),
            "server_error",
        ),
    }
}



/// Common context-overflow signatures across providers.
fn is_context_overflow(text: &str) -> bool {
    let t = text.to_lowercase();
    [
        "context_length_exceeded",
        "context length",
        "context window",
        "too many tokens",
        "maximum context",
        "prompt is too long",
        "request too large",
    ]
    .iter()
    .any(|s| t.contains(s))
}

/// Rebuild a Response from a buffered upstream error body.
fn error_response(status: StatusCode, body: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// Translate OpenAI request → provider-native → OpenAI response.
async fn translate_forward(
    state: &Arc<AppState>,
    provider_name: &str,
    provider: &Arc<crate::provider::Provider>,
    path: &str,
    body: bytes::Bytes,
    upstream_model: String,
) -> Response {
    if path != "/chat/completions" {
        return openai_error(
            StatusCode::NOT_FOUND,
            "endpoint not supported for this provider type",
            "invalid_request_error",
        );
    }
    let Ok(mut req) = serde_json::from_slice::<serde_json::Value>(&body) else {
        return openai_error(
            StatusCode::BAD_REQUEST,
            "invalid JSON body",
            "invalid_request_error",
        );
    };
    if !upstream_model.is_empty() {
        req["model"] = serde_json::Value::String(upstream_model);
    }
    let stream = req["stream"].as_bool().unwrap_or(false);
    if stream {
        match provider.chat_stream(req.clone()).await {
            Ok(events) => {
                // Re-emit normalized events as OpenAI SSE chunks.
                let sse = events.map(|ev| {
                    let chunk = match ev {
                        Ok(crate::llm::StreamEvent::Text(t)) => format!(
                            "data: {}\n\n",
                            serde_json::json!({"choices":[{"delta":{"content":t}}]})
                        ),
                        Ok(crate::llm::StreamEvent::Thinking(t)) => format!(
                            "data: {}\n\n",
                            serde_json::json!({"choices":[{"delta":{"reasoning_content":t}}]})
                        ),
                        Ok(crate::llm::StreamEvent::ToolCallDelta {
                            index,
                            id,
                            name,
                            arguments,
                        }) => format!(
                            "data: {}\n\n",
                            serde_json::json!({"choices":[{"delta":{"tool_calls":[{
                                "index": index,
                                "id": id,
                                "function": {"name": name, "arguments": arguments}
                            }]}}]})
                        ),
                        Ok(crate::llm::StreamEvent::Usage { input, output }) => format!(
                            "data: {}\n\n",
                            serde_json::json!({"choices":[], "usage":{
                                "prompt_tokens": input,
                                "completion_tokens": output,
                                "total_tokens": input + output,
                            }})
                        ),
                        // Provider-internal block for history round-trip —
                        // not part of the OpenAI wire shape; skip it.
                        Ok(crate::llm::StreamEvent::ThinkingBlock(_)) => String::new(),
                        Ok(crate::llm::StreamEvent::Done(_)) => "data: [DONE]\n\n".to_string(),
                        Err(e) => format!(
                            "data: {}\n\n",
                            serde_json::json!({"error":{"message":e.to_string()}})
                        ),
                    };
                    Ok::<_, std::io::Error>(bytes::Bytes::from(chunk))
                });
                // ThinkingBlock maps to an empty chunk — drop it.
                let sse = sse.filter(|c| {
                    futures::future::ready(!c.as_ref().is_ok_and(|b| b.is_empty()))
                });
                Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(sse))
                    .unwrap()
            }
            Err(e) => {
                // Context promotion: overflow errors retry once on the target.
                if is_context_overflow(&format!("{e}")) {
                    if let Some(resp) =
                        promote_translate(state, provider_name, path, req.clone()).await
                    {
                        return resp;
                    }
                }
                openai_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream error: {e}"),
                    "server_error",
                )
            }
        }
    } else {
        match provider.chat(req.clone()).await {
            Ok(v) => Json(v).into_response(),
            Err(e) => {
                if is_context_overflow(&format!("{e}")) {
                    if let Some(resp) =
                        promote_translate(state, provider_name, path, req).await
                    {
                        return resp;
                    }
                }
                openai_error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream error: {e}"),
                    "server_error",
                )
            }
        }
    }
}

/// Retry a translate-path request on the promotion target provider.
fn promote_translate<'a>(
    state: &'a Arc<AppState>,
    provider_name: &'a str,
    path: &'a str,
    req: serde_json::Value,
) -> std::pin::Pin<Box<dyn Future<Output = Option<Response>> + Send + 'a>> {
    Box::pin(async move {
        let target = {
            let cfg = state.config.read();
            cfg.providers
                .get(provider_name)
                .and_then(|p| p.context_promotion_target.clone())
        }?;
        let (tname, tmodel) = match target.split_once('/') {
            Some((p, m)) => (p.to_string(), m.to_string()),
            None => (provider_name.to_string(), target),
        };
        let tprovider = state
            .providers
            .read()
            .get(&tname)
            .cloned()?;
        let body = bytes::Bytes::from(req.to_string());
        Some(translate_forward(state, &tname, &tprovider, path, body, tmodel).await)
    })
}

/// Bearer-token gate for /v1/*. No token configured → open on localhost.
async fn require_token(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Response {
    // Cached per raw config value — a !cmd/keychain ref resolves once,
    // not per request.
    let expected = match state.auth_token().await {
        Some(Ok(t)) => t,
        // Configured but unresolvable: fail closed, never open.
        Some(Err(e)) => {
            return openai_error(
                StatusCode::SERVICE_UNAVAILABLE,
                &format!("auth token resolution failed: {e}"),
                "server_error",
            );
        }
        // A reload may have dropped the token — a non-loopback bind
        // must not silently open the API.
        None if !state.bind.ip().is_loopback() => {
            return openai_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth token required on non-loopback bind",
                "server_error",
            );
        }
        None => return next.run(req).await,
    };
    let ok = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| crate::config::constant_time_eq(t.as_bytes(), expected.as_bytes()));
    if ok {
        next.run(req).await
    } else {
        openai_error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing bearer token",
            "authentication_error",
        )
    }
}

fn openai_error(status: StatusCode, message: &str, kind: &str) -> Response {
    (
        status,
        Json(json!({
            "error": { "message": message, "type": kind, "code": null }
        })),
    )
        .into_response()
}
