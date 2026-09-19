use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use futures::StreamExt;
use serde_json::json;
use tracing::{info, warn};

use crate::config::SharedConfig;
use crate::mcp::McpRegistry;
use crate::provider::{Provider, build_providers};
use crate::store::Store;

/// Re-resolve a `!cmd`/keychain auth token at most this often — a rotated
/// secret takes effect without a config reload, but a shell isn't spawned
/// per request.
const AUTH_TOKEN_TTL: Duration = Duration::from_secs(60);
/// One-shot WS tickets expire this long after issue.
const WS_TICKET_TTL: Duration = Duration::from_secs(60);
/// Per-source-IP request budget for /v1/* on non-loopback binds.
const RATE_LIMIT_PER_MIN: f64 = 60.0;
const RATE_LIMIT_BURST: f64 = 20.0;

/// (raw auth_token, resolved-at + value) — see `auth_token_cache` below.
type AuthTokenCache = (Option<String>, Option<(Instant, Result<String, String>)>);

/// Process-wide counters, exported as Prometheus text on GET /metrics.
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub prompts_total: AtomicU64,
    pub tokens_input: AtomicU64,
    pub tokens_output: AtomicU64,
}

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
    pub bind: SocketAddr,
    /// (raw auth_token, resolved-at + value). A `!cmd` or keychain ref
    /// must not spawn a shell per request, but a rotated secret must take
    /// effect without a config reload — entries expire after
    /// `AUTH_TOKEN_TTL`. Err is cached too: a resolution failure must
    /// fail closed, never silently open the API.
    auth_token_cache: tokio::sync::Mutex<AuthTokenCache>,
    /// Prometheus counters exported on GET /metrics.
    pub metrics: Metrics,
    /// One-shot WS auth tickets → issue time. Consumed by /ws?ticket=.
    pub ws_tickets: tokio::sync::Mutex<HashMap<String, Instant>>,
    /// Per-source-IP token buckets for /v1/* on non-loopback binds.
    rate_buckets: tokio::sync::Mutex<HashMap<IpAddr, (f64, Instant)>>,
    /// Live prompt turns by session id → (connection id, cancel token).
    /// Shared across connections: a session can have only one in-flight
    /// prompt no matter which client asked, and a disconnect cancels the
    /// turns it started instead of orphaning them.
    pub live_prompts:
        tokio::sync::Mutex<HashMap<String, (u64, tokio_util::sync::CancellationToken)>>,
}

impl AppState {
    pub async fn new(config: SharedConfig, store: Store, mcp: McpRegistry) -> Arc<Self> {
        Self::with_bind(config, store, mcp, "127.0.0.1:0".parse().unwrap()).await
    }

    pub async fn with_bind(
        config: SharedConfig,
        store: Store,
        mcp: McpRegistry,
        bind: SocketAddr,
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
        let state = Arc::new(Self {
            config,
            providers: parking_lot::RwLock::new(providers),
            discovered: parking_lot::RwLock::new(discovered),
            store,
            mcp,
            bind,
            auth_token_cache: tokio::sync::Mutex::new((None, None)),
            metrics: Metrics {
                requests_total: AtomicU64::new(0),
                prompts_total: AtomicU64::new(0),
                tokens_input: AtomicU64::new(0),
                tokens_output: AtomicU64::new(0),
            },
            ws_tickets: tokio::sync::Mutex::new(HashMap::new()),
            rate_buckets: tokio::sync::Mutex::new(HashMap::new()),
            live_prompts: tokio::sync::Mutex::new(HashMap::new()),
        });
        // Daily session-retention sweep, plus one immediate pass at boot.
        // Sessions with a live prompt turn are excluded — deleting one
        // mid-turn would orphan its message writes into a dead row.
        let retention_days = state.config.read().session_retention_days;
        if let Some(days) = retention_days {
            let store = state.store.clone();
            let state = state.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
                loop {
                    tick.tick().await;
                    let live: std::collections::HashSet<String> =
                        state.live_prompts.lock().await.keys().cloned().collect();
                    match store.cleanup_older_than(days, &live).await {
                        Ok(ids) if !ids.is_empty() => {
                            for sid in &ids {
                                state.mcp.clear_session(sid);
                            }
                            info!(removed = ids.len(), days, "session retention sweep")
                        }
                        Ok(_) => {}
                        Err(e) => warn!(error = %e, "session retention sweep failed"),
                    }
                }
            });
        }
        state
    }

    /// Resolved auth token, cached per raw config value for
    /// `AUTH_TOKEN_TTL`. A `!cmd` or keychain ref resolves at most once
    /// per TTL window, not per request — a rotated secret takes effect
    /// on the next window without a config reload. Err = configured but
    /// unresolvable — callers must fail closed. Resolution runs off the
    /// async worker: `!cmd` blocks up to 10s.
    pub async fn auth_token(&self) -> Option<Result<String, String>> {
        self.auth_token_cached(AUTH_TOKEN_TTL).await
    }

    /// `auth_token` with an explicit cache TTL — tests inject a short
    /// one to observe re-resolution without waiting a minute.
    #[doc(hidden)]
    pub async fn auth_token_cached(&self, ttl: Duration) -> Option<Result<String, String>> {
        let raw = self.config.read().auth_token.clone();
        {
            let cache = self.auth_token_cache.lock().await;
            if cache.0 == raw
                && let Some((at, resolved)) = &cache.1
                && at.elapsed() < ttl
            {
                return Some(resolved.clone());
            }
            // Nothing configured and nothing cached: stay open/closed per
            // the caller's bind check without re-resolving.
            if raw.is_none() && cache.1.is_none() {
                return None;
            }
        }
        let raw2 = raw.clone();
        let resolved: Option<Result<String, String>> =
            match tokio::task::spawn_blocking(move || {
                raw2.as_deref()
                    .map(|r| match crate::config::SecretRef::parse(r) {
                        Ok(s) => s.resolve().map_err(|e| e.to_string()),
                        // A value that LOOKS like a ref but failed to parse
                        // ("env:", "keychain:svc", "!") is a config typo —
                        // failing closed beats silently treating it as a
                        // guessable literal token.
                        Err(e)
                            if r.starts_with("env:")
                                || r.starts_with("keychain:")
                                || r.starts_with('!') =>
                        {
                            Err(e.to_string())
                        }
                        Err(_) => Ok(r.to_string()),
                    })
                    // An empty/whitespace token is a misconfiguration, not
                    // "no auth": `Bearer ` would pass constant_time_eq and
                    // the non-loopback boot gate would see Some(Ok(_)).
                    .map(|r| match r {
                        Ok(t) if t.trim().is_empty() => {
                            Err("auth_token resolved to an empty value".to_string())
                        }
                        other => other,
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
            *cache = (raw, resolved.clone().map(|r| (Instant::now(), r)));
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
        .route("/ws_ticket", post(ws_ticket))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_token))
        .route_layer(middleware::from_fn_with_state(state.clone(), rate_limit));

    Router::new()
        .route("/health", get(health))
        // /ws and /metrics get the same rate limit as /v1 — on a
        // non-loopback bind an unlimited /ws would allow online
        // brute-force of the auth token via ticket exchange.
        .route(
            "/ws",
            get(crate::rpc::ws_handler)
                .route_layer(middleware::from_fn_with_state(state.clone(), rate_limit)),
        )
        .route(
            "/metrics",
            get(metrics)
                .route_layer(middleware::from_fn_with_state(state.clone(), require_token))
                .route_layer(middleware::from_fn_with_state(state.clone(), rate_limit)),
        )
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

/// GET /metrics — Prometheus text exposition of the process counters.
async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let m = &state.metrics;
    let body = format!(
        "damon_requests_total {}\n\
         damon_prompts_total {}\n\
         damon_tokens_input_total {}\n\
         damon_tokens_output_total {}\n\
         damon_active_sessions {}\n",
        m.requests_total.load(Ordering::Relaxed),
        m.prompts_total.load(Ordering::Relaxed),
        m.tokens_input.load(Ordering::Relaxed),
        m.tokens_output.load(Ordering::Relaxed),
        state.live_prompts.lock().await.len(),
    );
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
        .into_response()
}

/// POST /v1/ws_ticket — issue a one-shot ticket usable as /ws?ticket=
/// for clients that can't set an Authorization header on a WS upgrade.
async fn ws_ticket(State(state): State<Arc<AppState>>) -> Response {
    let mut bytes = [0u8; 32];
    // Practically infallible, but a panic here kills the handler task —
    // return a 500 instead.
    if getrandom::fill(&mut bytes).is_err() {
        return openai_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "OS RNG unavailable",
            "server_error",
        );
    }
    let ticket: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let mut tickets = state.ws_tickets.lock().await;
    // Sweep expired entries so the map can't grow without bound.
    tickets.retain(|_, issued| issued.elapsed() < WS_TICKET_TTL);
    tickets.insert(ticket.clone(), Instant::now());
    Json(json!({ "ticket": ticket })).into_response()
}

async fn models(State(state): State<Arc<AppState>>) -> Response {
    // Ask the default provider for its model list. Translate-only
    // providers (anthropic/gemini) can't list — fall back to an empty
    // list rather than forwarding a GET their API doesn't have.
    let mut v = match state.default_provider() {
        Some(p) => p
            .list_models()
            .await
            .unwrap_or_else(|_| json!({"object": "list", "data": []})),
        None => json!({"object": "list", "data": []}),
    };
    // Merge discovered ids from every provider that probed successfully.
    let discovered = state.discovered.read().clone();
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
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
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
        // Discovered ids claim the model FIRST — an exact id like
        // "meta-llama/llama-3" must not be hijacked by the provider/model
        // prefix split inside route_model_strict. Then strict (prefix/glob),
        // then the default provider still gets the requested model name.
        let routed = model.as_deref().and_then(|m| {
            // HashMap order is nondeterministic — when two providers
            // discover the same id, pick the alphabetically-first so the
            // winner doesn't change per process.
            let mut names: Vec<&String> = discovered.keys().collect();
            names.sort();
            names
                .iter()
                .find_map(|name| {
                    discovered[*name]
                        .iter()
                        .any(|id| id == m)
                        .then(|| ((*name).clone(), m.to_string()))
                })
                .or_else(|| {
                    cfg.route_model_strict(m)
                        .map(|(name, _, upstream)| (name.to_string(), upstream))
                })
        });
        routed
            .or_else(|| {
                cfg.default_provider()
                    .map(|(n, _)| (n.to_string(), String::new()))
            })
            .and_then(|(name, upstream)| providers.get(&name).cloned().map(|p| (name, p, upstream)))
    };
    let Some((provider_name, provider, upstream_model)) = provider else {
        return openai_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "no provider configured",
            "server_error",
        );
    };

    // Inject the thinking level for the adapters, and rewrite the model
    // to its upstream id — otherwise the upstream receives the client's
    // `provider/model[:level]` string verbatim and rejects it. When routing
    // fell through to the default provider (upstream_model empty) the
    // stripped name still replaces the suffixed one.
    let body = if thinking.is_some() || !upstream_model.is_empty() {
        let mut v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_else(|_| json!({}));
        if let Some(level) = &thinking {
            v["_thinking"] = json!(level);
        }
        if !upstream_model.is_empty() {
            v["model"] = json!(upstream_model);
        } else if let Some(m) = &model {
            v["model"] = json!(m);
        }
        bytes::Bytes::from(v.to_string())
    } else {
        // Original bytes verbatim: nothing to rewrite, and re-serializing
        // would round integers beyond u64::MAX/i64::MIN to f64. The rewrite
        // arm above keeps that extreme-boundary loss as an accepted residual
        // edge (serde_json stays exact up to those bounds) — avoiding it
        // entirely would need byte-level splicing.
        body
    };

    // Non-completions providers: translate instead of passthrough.
    if !matches!(&*provider, crate::provider::Provider::OpenAiCompletions(_)) {
        return translate_forward(
            state,
            &provider_name,
            &provider,
            path,
            body,
            upstream_model,
            0,
        )
        .await;
    }

    match provider.forward(method.clone(), path, body.clone()).await {
        Ok(up) => {
            // Context promotion: on a context-overflow error, retry once with
            // the configured promotion target. Client errors are buffered so
            // the body can be inspected.
            if up.status.is_client_error() {
                return promote_or_error(state, &provider_name, &provider, method, path, body, up)
                    .await;
            }
            up.into_response()
        }
        Err(e) => {
            // reqwest's Display embeds the full upstream URL — echoing it
            // back would disclose internal topology; log it, return fixed text.
            warn!("upstream forward failed: {e}");
            openai_error(StatusCode::BAD_GATEWAY, "upstream error", "server_error")
        }
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
    let buf = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        crate::provider::collect_stream(up.stream),
    )
    .await
    {
        Ok(Ok(b)) => b,
        // Timeout and stream failure used to collapse into the same arm and
        // return an empty body — name the cause so a client can tell a dead
        // read apart from a real upstream error body.
        Err(_) => return error_response(status, "upstream error body read timed out"),
        Ok(Err(e)) => {
            return error_response(status, &format!("upstream error body read failed: {e}"));
        }
    };
    // An oversized body (>1 MiB) is judged and echoed from its first 64 KiB —
    // plenty for any overflow signature — instead of being discarded whole.
    // Promotion below still fires only on an overflow match.
    let text = if buf.len() > (1 << 20) {
        String::from_utf8_lossy(&buf[..64 * 1024])
    } else {
        String::from_utf8_lossy(&buf)
    };
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
        return translate_forward(state, &tname, &tprovider, path, body, tmodel, 1).await;
    }
    match tprovider.forward(method, path, body).await {
        Ok(up) => up.into_response(),
        Err(e) => {
            // Same disclosure rule as `forward`: cause to the log, fixed
            // text to the client.
            warn!("upstream promotion request failed: {e}");
            openai_error(StatusCode::BAD_GATEWAY, "upstream error", "server_error")
        }
    }
}

/// Common context-overflow signatures across providers.
fn is_context_overflow(text: &str) -> bool {
    let t = text.to_lowercase();
    // Deliberately narrow: generic phrasings like "request too large" or
    // "too many tokens" also show up in ordinary 413/4xx proxy bodies, and
    // a false positive re-sends the request to context_promotion_target —
    // billing an unrequested model while masking the real cause.
    [
        "context_length_exceeded",
        "context length",
        "context window",
        "maximum context",
        "prompt is too long",
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
    // Promotion retry depth — caps the recursion so cyclic
    // context_promotion_target configs can't loop forever.
    depth: u32,
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
                        Ok(crate::llm::StreamEvent::Done(reason)) => format!(
                            // OpenAI clients key their tool loops on
                            // finish_reason; emit the terminal chunk
                            // before [DONE] or tool_calls/length are
                            // invisible on the wire.
                            "data: {}\n\ndata: [DONE]\n\n",
                            serde_json::json!({"choices":[{
                                "delta": {},
                                "finish_reason": match reason {
                                    crate::llm::StopReason::ToolCalls => "tool_calls",
                                    crate::llm::StopReason::Length => "length",
                                    _ => "stop",
                                }
                            }]})
                        ),
                        // A mid-stream provider error still terminates the
                        // SSE stream — clients waiting on [DONE] would
                        // otherwise hang on a truncated response.
                        Err(e) => {
                            // Fixed message — same disclosure rule as the
                            // 502 paths; the real cause goes to the log.
                            warn!("upstream stream failed: {e}");
                            format!(
                                "data: {}\n\ndata: [DONE]\n\n",
                                serde_json::json!({"error":{"message":"upstream stream error"}})
                            )
                        }
                    };
                    Ok::<_, std::io::Error>(bytes::Bytes::from(chunk))
                });
                // ThinkingBlock maps to an empty chunk — drop it.
                let sse =
                    sse.filter(|c| futures::future::ready(!c.as_ref().is_ok_and(|b| b.is_empty())));
                Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(sse))
                    .unwrap()
            }
            Err(e) => {
                // A provider 429 must reach the client as 429 + Retry-After —
                // collapsing it into 502 loses the backoff signal entirely.
                if let Some(rl) = e.downcast_ref::<crate::provider::RateLimited>() {
                    return Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .header("retry-after", rl.0.as_secs().to_string())
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"error":{"message":"upstream rate limited","type":"rate_limit_error"}}).to_string(),
                        ))
                        .unwrap_or_else(|_| StatusCode::TOO_MANY_REQUESTS.into_response());
                }
                // Context promotion: overflow errors retry once on the target.
                if depth == 0
                    && is_context_overflow(&format!("{e}"))
                    && let Some(resp) =
                        promote_translate(state, provider_name, path, req.clone()).await
                {
                    return resp;
                }
                // Fixed client message — the Display may embed the upstream
                // URL; the cause goes to the log instead.
                warn!("upstream translate request failed: {e}");
                openai_error(StatusCode::BAD_GATEWAY, "upstream error", "server_error")
            }
        }
    } else {
        match provider.chat(req.clone()).await {
            Ok(v) => Json(v).into_response(),
            Err(e) => {
                // A provider 429 must reach the client as 429 + Retry-After —
                // collapsing it into 502 loses the backoff signal entirely.
                if let Some(rl) = e.downcast_ref::<crate::provider::RateLimited>() {
                    return Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .header("retry-after", rl.0.as_secs().to_string())
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"error":{"message":"upstream rate limited","type":"rate_limit_error"}}).to_string(),
                        ))
                        .unwrap_or_else(|_| StatusCode::TOO_MANY_REQUESTS.into_response());
                }
                if depth == 0
                    && is_context_overflow(&format!("{e}"))
                    && let Some(resp) = promote_translate(state, provider_name, path, req).await
                {
                    return resp;
                }
                // Fixed client message — the Display may embed the upstream
                // URL; the cause goes to the log instead.
                warn!("upstream translate request failed: {e}");
                openai_error(StatusCode::BAD_GATEWAY, "upstream error", "server_error")
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
        let tprovider = state.providers.read().get(&tname).cloned()?;
        let body = bytes::Bytes::from(req.to_string());
        Some(translate_forward(state, &tname, &tprovider, path, body, tmodel, 1).await)
    })
}

/// Per-source-IP token bucket for /v1/* — only on non-loopback binds,
/// where a remote peer could hammer provider quota. Loopback binds skip
/// entirely so local scripts are never throttled. The peer IP comes from
/// `ConnectInfo<SocketAddr>`; when the server wasn't built with
/// `into_make_service_with_connect_info` the extension is absent and the
/// limiter is a no-op rather than trusting spoofable headers.
async fn rate_limit(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    if state.bind.ip().is_loopback() {
        return next.run(req).await;
    }
    let Some(peer) = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip())
    else {
        return next.run(req).await;
    };
    let mut buckets = state.rate_buckets.lock().await;
    // Bound the map: drop idle buckets once it grows past a few pages of
    // distinct peers. A sustained flood from >1024 IPs could otherwise
    // keep every bucket "recent" and grow the map without limit — evict
    // oldest-first until we're back under the cap.
    if buckets.len() > 1024 {
        buckets.retain(|_, (_, last)| last.elapsed() < Duration::from_secs(300));
        while buckets.len() > 1024 {
            let Some((&oldest, _)) = buckets.iter().min_by_key(|(_, (_, last))| *last) else {
                break;
            };
            buckets.remove(&oldest);
        }
    }
    if bucket_allow(
        buckets
            .entry(peer)
            .or_insert((RATE_LIMIT_BURST, Instant::now())),
    ) {
        drop(buckets);
        next.run(req).await
    } else {
        openai_error(
            StatusCode::TOO_MANY_REQUESTS,
            "rate limit exceeded",
            "rate_limit_error",
        )
    }
}

/// Token bucket: refills at `RATE_LIMIT_PER_MIN`/min up to
/// `RATE_LIMIT_BURST`, consumes one token per call. Split out so tests
/// can drive it without a socket.
#[doc(hidden)]
pub fn bucket_allow(bucket: &mut (f64, Instant)) -> bool {
    let refill = bucket.1.elapsed().as_secs_f64() * (RATE_LIMIT_PER_MIN / 60.0);
    bucket.0 = (bucket.0 + refill).min(RATE_LIMIT_BURST);
    bucket.1 = Instant::now();
    if bucket.0 >= 1.0 {
        bucket.0 -= 1.0;
        true
    } else {
        false
    }
}

/// Bearer-token gate for /v1/* and /metrics. No token configured → open on localhost.
async fn require_token(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    // Cached per raw config value — a !cmd/keychain ref resolves once,
    // not per request.
    let expected = match state.auth_token().await {
        Some(Ok(t)) => t,
        // Configured but unresolvable: fail closed, never open. The
        // detailed error (which can embed a !cmd string or keychain path)
        // stays server-side — echoing it would leak the operator's
        // secret-fetch mechanics to unauthenticated callers.
        Some(Err(e)) => {
            warn!(error = %e, "auth token resolution failed");
            return openai_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "auth token unavailable",
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
        None => {
            // No token: browser requests must come from a loopback origin —
            // a foreign site could otherwise burn provider quota via
            // no-cors posts even though it can't read the response.
            if let Some(origin) = req.headers().get("origin").and_then(|v| v.to_str().ok())
                && !crate::rpc::is_localhost_origin(origin)
            {
                return openai_error(
                    StatusCode::FORBIDDEN,
                    "cross-origin requests require an auth token",
                    "authentication_error",
                );
            }
            return next.run(req).await;
        }
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
