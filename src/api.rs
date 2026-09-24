use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use serde_json::json;
use tracing::{info, warn};

use crate::config::SharedConfig;
use crate::session::SessionManager;
use crate::store::Store;

/// Re-resolve a `!cmd`/keychain auth token at most this often — a rotated
/// secret takes effect without a config reload, but a shell isn't spawned
/// per request.
const AUTH_TOKEN_TTL: Duration = Duration::from_secs(60);
/// One-shot WS tickets expire this long after issue.
pub(crate) const WS_TICKET_TTL: Duration = Duration::from_secs(60);
/// Per-source-IP request budget for token-gated routes on non-loopback
/// binds.
const RATE_LIMIT_PER_MIN: f64 = 60.0;
const RATE_LIMIT_BURST: f64 = 20.0;

/// (raw auth_token, resolved-at + value) — see `auth_token_cache` below.
type AuthTokenCache = (Option<String>, Option<(Instant, Result<String, String>)>);

/// Process-wide counters, exported as Prometheus text on GET /metrics.
#[derive(Default)]
pub struct Metrics {
    pub requests_total: std::sync::atomic::AtomicU64,
    pub prompts_total: std::sync::atomic::AtomicU64,
    pub active_sessions: std::sync::atomic::AtomicI64,
    /// Permission asks answered (any outcome) and their total wait —
    /// divide for the mean; a rising wait means users are slow or
    /// prompts stall on asks.
    pub permission_waits_total: std::sync::atomic::AtomicU64,
    pub permission_wait_ms_total: std::sync::atomic::AtomicU64,
}

/// One daemon event for the SSE fan-out: (event type, JSON data).
/// Arc'd so every subscriber clones a pointer, not the payload.
pub type DaemonEvent = Arc<(String, String)>;

/// Per-agent turn counters — `latency_ms` is a sum; divide by `turns` for
/// the mean.
#[derive(Default)]
pub struct AgentStat {
    pub turns: u64,
    pub errors: u64,
    pub latency_ms: u64,
}

/// Shared daemon state: config, store, and the session manager that
/// maps Damon session ids to live backend sessions.
pub struct AppState {
    pub config: SharedConfig,
    pub store: Store,
    /// Backend clients + live sessions.
    pub sessions: Arc<SessionManager>,
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
    /// Per-source-IP token buckets for token-gated routes on non-loopback
    /// binds.
    rate_buckets: tokio::sync::Mutex<HashMap<IpAddr, (f64, Instant)>>,
    /// Per-backend turn counters for /metrics — populated at turn end.
    /// parking_lot: the update is a few integer adds, never an await.
    pub backend_stats: parking_lot::Mutex<HashMap<String, AgentStat>>,
    /// Daemon event fan-out for GET /v1/events (SSE). Bounded broadcast —
    /// a lagging subscriber sees a gap, never stalls the daemon.
    pub events: tokio::sync::broadcast::Sender<DaemonEvent>,
}

impl AppState {
    pub async fn new(config: SharedConfig, store: Store) -> Arc<Self> {
        Self::with_bind(config, store, "127.0.0.1:0".parse().unwrap()).await
    }

    pub async fn with_bind(config: SharedConfig, store: Store, bind: SocketAddr) -> Arc<Self> {
        let sessions = {
            let cfg = config.read();
            SessionManager::from_config(&cfg)
        };
        for id in sessions.available_backends() {
            info!(backend = id, "backend available");
        }
        let state = Arc::new(Self {
            config,
            store,
            sessions: Arc::new(sessions),
            bind,
            auth_token_cache: tokio::sync::Mutex::new((None, None)),
            metrics: Metrics::default(),
            ws_tickets: tokio::sync::Mutex::new(HashMap::new()),
            rate_buckets: tokio::sync::Mutex::new(HashMap::new()),
            backend_stats: parking_lot::Mutex::new(HashMap::new()),
            // 256-event ring: a subscriber that falls behind gets a
            // Lagged error and reconnects; the daemon never waits on it.
            events: tokio::sync::broadcast::channel(256).0,
        });
        // Idle session sweep: backend sessions unused for
        // agent_idle_secs are closed (they reattach via the persistence
        // handle on next use); 0 disables. Sessions with a live turn are
        // skipped — reaping mid-turn would fail the turn.
        {
            let state = state.clone();
            tokio::spawn(async move {
                loop {
                    let idle = state.config.read().agent_idle_secs;
                    if idle == 0 {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        continue;
                    }
                    let nap = state.sessions.reap_idle(Duration::from_secs(idle)).await;
                    tokio::time::sleep(nap).await;
                }
            });
        }
        // Daily session-retention sweep, plus one immediate pass at boot.
        // Sessions with a live prompt turn are excluded — deleting one
        // mid-turn would orphan its message writes into a dead row.
        let retention_days = state.config.read().session_retention_days;
        if let Some(days) = retention_days.map(|d| u32::try_from(d).unwrap_or(u32::MAX)) {
            let store = state.store.clone();
            let state = state.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(24 * 60 * 60));
                loop {
                    tick.tick().await;
                    let live: std::collections::HashSet<String> = state.sessions.busy_ids().await;
                    match store.cleanup_older_than(days, &live).await {
                        Ok(ids) if !ids.is_empty() => {
                            for sid in &ids {
                                state.sessions.detach(sid).await;
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
    /// per TTL window, not per request — a rotated secret takes effect on
    /// the next window without a config reload. Err = configured but
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

    /// WS upgrade auth: bearer header or a single-use ?ticket=. Mirrors
    /// require_token's fail-closed rules for non-loopback binds.
    pub async fn check_auth(
        &self,
        headers: &axum::http::HeaderMap,
        ticket: Option<&str>,
        _peer: IpAddr,
    ) -> bool {
        let expected = match self.auth_token().await {
            Some(Ok(t)) => Some(t),
            // Configured but unresolvable: fail closed.
            Some(Err(_)) => return false,
            None if !self.bind.ip().is_loopback() => return false,
            None => None,
        };
        let Some(expected) = expected else {
            return true; // loopback + no token configured
        };
        if let Some(t) = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            && crate::config::constant_time_eq(t.as_bytes(), expected.as_bytes())
        {
            return true;
        }
        // Single-use ticket: consume on attempt, valid or not — a
        // replayed ticket must never authenticate twice.
        if let Some(t) = ticket {
            let mut tickets = self.ws_tickets.lock().await;
            if let Some(issued) = tickets.remove(t) {
                return issued.elapsed() < WS_TICKET_TTL;
            }
        }
        false
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    // /v1/ws_ticket keeps its historical path so the web UI and the npm
    // client keep working; the model endpoints are gone — agents answer,
    // not an OpenAI-compatible proxy.
    let v1 = Router::new()
        .route("/ws_ticket", post(ws_ticket))
        // Daemon event stream (SSE): turn/session/agent lifecycle events
        // for automations that can't hold a WS open. Token-gated like
        // the rest of /v1.
        .route("/events", get(events_sse))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_token,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit,
        ));

    Router::new()
        .route("/health", get(health))
        // Embedded chat UI — static markup, no secrets; the page
        // authenticates itself through the ws ticket flow.
        .route("/ui", get(crate::ui::ui))
        // PWA assets — same trust level as /ui.
        .route("/manifest.webmanifest", get(crate::ui::manifest))
        .route("/icon.svg", get(crate::ui::icon))
        // /ws and /metrics get the same rate limit as /v1 — on a
        // non-loopback bind an unlimited /ws would allow online
        // brute-force of the auth token via ticket exchange.
        .route(
            "/ws",
            get(crate::rpc::ws_handler).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                rate_limit,
            )),
        )
        .route(
            "/metrics",
            get(metrics)
                .route_layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    require_token,
                ))
                .route_layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    rate_limit,
                )),
        )
        .nest("/v1", v1)
        .with_state(state)
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    // A non-loopback bind leaks the minimum: version and the agent
    // roster stay loopback-only.
    if !state.bind.ip().is_loopback() {
        return Json(json!({"status": "ok"}));
    }
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "backends": state.sessions.available_backends(),
    }))
}

/// GET /metrics — Prometheus text exposition of the process counters.
async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let m = &state.metrics;
    use std::sync::atomic::Ordering::Relaxed;
    let mut body = format!(
        "damon_requests_total {}\ndamon_prompts_total {}\ndamon_active_sessions {}\ndamon_live_sessions {}\ndamon_permission_waits_total {}\ndamon_permission_wait_ms_total {}\n",
        m.requests_total.load(Relaxed),
        m.prompts_total.load(Relaxed),
        m.active_sessions.load(Relaxed),
        state.sessions.busy_ids().await.len(),
        m.permission_waits_total.load(Relaxed),
        m.permission_wait_ms_total.load(Relaxed),
    );
    // Per-backend turn stats — Prometheus label form, one line per backend.
    let stats = state.backend_stats.lock();
    let mut backends: Vec<(&String, &AgentStat)> = stats.iter().collect();
    backends.sort_by_key(|(id, _)| (*id).clone());
    for (id, s) in backends {
        let label = prom_label(id);
        body.push_str(&format!(
            "damon_turn_duration_ms_sum{{backend=\"{label}\"}} {}\ndamon_turn_duration_ms_count{{backend=\"{label}\"}} {}\ndamon_turn_errors_total{{backend=\"{label}\"}} {}\n",
            s.latency_ms, s.turns, s.errors
        ));
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
        .into_response()
}

/// Escape a value for a Prometheus label — quotes and backslashes only.
fn prom_label(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

/// GET /v1/events — Server-Sent Events fan-out of daemon lifecycle
/// events (turn/session/agent). Token-gated with the rest of /v1.
/// A lagging subscriber sees a broadcast Lagged gap, never stalls the
/// daemon; the client should reconnect to resync.
async fn events_sse(
    State(state): State<Arc<AppState>>,
) -> axum::response::sse::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive};
    let rx = state.events.subscribe();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(ev) => {
                let (ty, data) = &*ev;
                let e = Event::default().event(ty).data(data.as_str());
                Some((Ok(e), rx))
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Surface the gap as a named event so EventSource
                // consumers can listen for it and resync — a comment
                // frame would be invisible to addEventListener.
                let e = Event::default().event("lagged");
                Some((Ok(e), rx))
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
        }
    });
    axum::response::sse::Sse::new(stream).keep_alive(KeepAlive::default())
}

/// POST /v1/ws_ticket — issue a one-shot ticket usable as /ws?ticket=
/// for clients that can't set an Authorization header on a WS upgrade.
async fn ws_ticket(State(state): State<Arc<AppState>>) -> Response {
    let ticket = uuid::Uuid::new_v4().to_string();
    let mut tickets = state.ws_tickets.lock().await;
    // Sweep expired entries on insert — a ticket abandoned without a
    // /ws upgrade would otherwise linger for the daemon's lifetime.
    tickets.retain(|_, issued| issued.elapsed() < WS_TICKET_TTL);
    tickets.insert(ticket.clone(), Instant::now());
    Json(json!({ "ticket": ticket })).into_response()
}

/// Per-source-IP token bucket for token-gated routes — only on
/// non-loopback binds, where a remote peer could hammer the daemon.
/// Loopback binds skip entirely so local scripts are never throttled.
/// The peer IP comes from `ConnectInfo<SocketAddr>`; when the server
/// wasn't built with `into_make_service_with_connect_info` the extension
/// is absent and the limiter is a no-op rather than trusting spoofable
/// headers.
async fn rate_limit(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let ip = req
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    let Some(ip) = ip else {
        return next.run(req).await;
    };
    if ip.is_loopback() {
        return next.run(req).await;
    }
    let mut buckets = state.rate_buckets.lock().await;
    let now = Instant::now();
    // Evict buckets idle over 10 minutes — a long-lived daemon on a
    // public bind would otherwise grow one entry per peer IP forever.
    buckets.retain(|_, (_, at)| now.duration_since(*at) < Duration::from_secs(600));
    let bucket = buckets.entry(ip).or_insert((RATE_LIMIT_BURST, now));
    let ok = bucket_allow(bucket, now);
    if !ok {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    }
    drop(buckets);
    next.run(req).await
}

/// Token bucket: refills at `RATE_LIMIT_PER_MIN`/min up to
/// `RATE_LIMIT_BURST`, consumes one token per call. Split out so tests
/// can drive it without a socket.
#[doc(hidden)]
pub fn bucket_allow(bucket: &mut (f64, Instant), now: Instant) -> bool {
    let elapsed = now.duration_since(bucket.1).as_secs_f64();
    bucket.0 = (bucket.0 + elapsed * (RATE_LIMIT_PER_MIN / 60.0)).min(RATE_LIMIT_BURST);
    bucket.1 = now;
    if bucket.0 >= 1.0 {
        bucket.0 -= 1.0;
        true
    } else {
        false
    }
}

/// Bearer-token gate for token-gated routes. No token configured → open
/// on localhost.
async fn require_token(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    use std::sync::atomic::Ordering::Relaxed;
    state.metrics.requests_total.fetch_add(1, Relaxed);
    let expected = match state.auth_token().await {
        Some(Ok(t)) => t,
        // Configured but unresolvable: fail closed, never open.
        Some(Err(_)) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        // A reload may have dropped the token — a non-loopback bind
        // must not silently open the API (same gate as /ws).
        None if !state.bind.ip().is_loopback() => {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        None => return next.run(req).await,
    };
    let got = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match got {
        Some(t) if crate::config::constant_time_eq(t.as_bytes(), expected.as_bytes()) => {
            next.run(req).await
        }
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}
