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
/// Label cardinality is bounded by construction: turn stats key by
/// backend id (the catalog is fixed at boot plus config reloads), rpc
/// errors by wire code.
#[derive(Default)]
pub struct Metrics {
    /// HTTP requests through the token gate (historical scope).
    pub requests_total: std::sync::atomic::AtomicU64,
    /// JSON-RPC frames dispatched — WS and relay paths alike.
    pub rpc_requests_total: std::sync::atomic::AtomicU64,
    /// JSON-RPC error responses, keyed by wire error code.
    rpc_errors: parking_lot::Mutex<HashMap<String, std::sync::atomic::AtomicU64>>,
    /// Turn outcomes + wall-clock duration, keyed by backend id.
    turns: parking_lot::Mutex<HashMap<String, TurnMetrics>>,
}

/// Upper bounds (seconds) of the turn-duration histogram buckets.
const TURN_BUCKETS: [f64; 8] = [1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0];
/// `run_turn` stop reasons, in the order `TurnMetrics::status` counts
/// them.
const TURN_STATUSES: [&str; 5] = ["completed", "failed", "canceled", "timeout", "closed"];

/// Per-backend turn accounting: outcome counters plus a fixed-bucket
/// duration histogram (sum/count give the mean without a client lib).
#[derive(Default)]
struct TurnMetrics {
    status: [std::sync::atomic::AtomicU64; TURN_STATUSES.len()],
    duration_buckets: [std::sync::atomic::AtomicU64; TURN_BUCKETS.len()],
    duration_sum_ms: std::sync::atomic::AtomicU64,
    total: std::sync::atomic::AtomicU64,
}

impl Metrics {
    pub(crate) fn record_rpc_request(&self) {
        self.rpc_requests_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn record_rpc_error(&self, code: i64) {
        let mut m = self.rpc_errors.lock();
        m.entry(code.to_string())
            .or_default()
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record one finished turn: backend, `run_turn` stop reason,
    /// wall-clock duration.
    pub fn record_turn(&self, backend: &str, status: &str, duration: std::time::Duration) {
        let mut turns = self.turns.lock();
        let t = turns.entry(backend.to_string()).or_default();
        t.total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(i) = TURN_STATUSES.iter().position(|s| *s == status) {
            t.status[i].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        t.duration_sum_ms.fetch_add(
            duration.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let secs = duration.as_secs_f64();
        for (i, edge) in TURN_BUCKETS.iter().enumerate() {
            if secs <= *edge {
                t.duration_buckets[i].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// The full Prometheus text body for /metrics.
    pub fn render(&self, live_sessions: usize, busy_sessions: usize) -> String {
        use std::fmt::Write as _;
        use std::sync::atomic::Ordering::Relaxed;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP damon_requests_total HTTP requests through the token gate."
        );
        let _ = writeln!(out, "# TYPE damon_requests_total counter");
        let _ = writeln!(
            out,
            "damon_requests_total {}",
            self.requests_total.load(Relaxed)
        );
        let _ = writeln!(out, "# TYPE damon_rpc_requests_total counter");
        let _ = writeln!(
            out,
            "damon_rpc_requests_total {}",
            self.rpc_requests_total.load(Relaxed)
        );
        let _ = writeln!(out, "# TYPE damon_live_sessions gauge");
        let _ = writeln!(out, "damon_live_sessions {live_sessions}");
        let _ = writeln!(out, "# TYPE damon_busy_sessions gauge");
        let _ = writeln!(out, "damon_busy_sessions {busy_sessions}");

        let mut errors: Vec<_> = self
            .rpc_errors
            .lock()
            .iter()
            .map(|(code, n)| (code.clone(), n.load(Relaxed)))
            .collect();
        errors.sort();
        for (code, n) in errors {
            let _ = writeln!(out, "damon_rpc_errors_total{{code=\"{code}\"}} {n}");
        }

        let turns_guard = self.turns.lock();
        let mut turns: Vec<_> = turns_guard.iter().collect();
        turns.sort_by(|a, b| a.0.cmp(b.0));
        for (backend, t) in turns {
            for (i, status) in TURN_STATUSES.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "damon_turns_total{{backend=\"{backend}\",status=\"{status}\"}} {}",
                    t.status[i].load(Relaxed)
                );
            }
            for (i, edge) in TURN_BUCKETS.iter().enumerate() {
                let _ = writeln!(
                    out,
                    "damon_turn_seconds_bucket{{backend=\"{backend}\",le=\"{edge}\"}} {}",
                    t.duration_buckets[i].load(Relaxed)
                );
            }
            let total = t.total.load(Relaxed);
            let _ = writeln!(
                out,
                "damon_turn_seconds_bucket{{backend=\"{backend}\",le=\"+Inf\"}} {total}"
            );
            let _ = writeln!(
                out,
                "damon_turn_seconds_sum{{backend=\"{backend}\"}} {}",
                t.duration_sum_ms.load(Relaxed) as f64 / 1000.0
            );
            let _ = writeln!(
                out,
                "damon_turn_seconds_count{{backend=\"{backend}\"}} {total}"
            );
        }
        out
    }
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
    /// One-shot WS auth tickets → issue record. A ticket minted for a
    /// web device carries the device id, so the /ws upgrade knows
    /// which device's socket it carries — `device.revoke` cuts those
    /// live sockets and purges the device's unspent tickets.
    pub ws_tickets: tokio::sync::Mutex<HashMap<String, WsTicket>>,
    /// Per-source-IP token buckets for token-gated routes on non-loopback
    /// binds.
    rate_buckets: tokio::sync::Mutex<HashMap<IpAddr, (f64, Instant)>>,
    /// Skills hub state: root + fetcher + the mutation lock.
    pub skills: std::sync::Arc<crate::skills_hub::SkillsHub>,
    /// In-flight `worktree.create` runs keyed by creationId — the
    /// cancel handles `worktree.cancel` signals.
    pub worktrees: crate::worktree::CreationRegistry,
    /// Injectable process runner for CLI lifecycle probes/updates —
    /// tests substitute a mock so no probe ever spawns a process or
    /// touches the network. Unset = the real runner.
    pub cli_runner: std::sync::OnceLock<std::sync::Arc<dyn crate::cli_lifecycle::CommandRunner>>,
    /// In-flight `backend.update` runs keyed by creationId — the cancel
    /// handles `backend.update_cancel` signals.
    pub cli_updates: crate::cli_lifecycle::UpdateRegistry,
    /// Injectable MCP probe runner — tests substitute a mock so no
    /// dispatch test ever spawns an MCP server. Unset = real probes.
    pub mcp_probe: std::sync::OnceLock<std::sync::Arc<dyn crate::mcp_inventory::ProbeRunner>>,
    /// Non-loopback WS connections currently live — the "remote control
    /// active" badge counts exactly these: a phone or second machine on
    /// the LAN holding a direct `/ws` connection. Relayed phones ride
    /// the daemon's own outbound tunnel instead of an inbound socket,
    /// so they are not counted (documented cut — counting them needs
    /// relay tunnel state, not a connection accept).
    pub remote_ws: std::sync::atomic::AtomicUsize,
    /// Single-flight history scan — `Some(scanId)` while one runs; a
    /// second `session.rescan` returns the in-flight id.
    pub history_scan: parking_lot::Mutex<Option<String>>,
    /// Live mission runs: per-run event fanout, cancel token, and the
    /// human-respond command inbox. Persisted state lives in the store;
    /// this holds only what a LIVE scheduler needs.
    pub missions: crate::mission::MissionRegistry,
    /// Live WS connections authenticated as a web device: device id →
    /// per-connection kill handles. `device.revoke` drains a device's
    /// entries and fires every handle; each connection removes its own
    /// entry when it ends. Master-auth connections carry no device id
    /// and are never registered here.
    pub device_conns: DeviceConns,
}

/// One minted WS ticket: when it was issued (single-use, TTL-bounded)
/// and which web device it was minted for (None = master auth).
#[derive(Debug, Clone)]
pub struct WsTicket {
    pub issued: Instant,
    pub device: Option<String>,
}

/// Registry of per-device live-socket kill handles — see
/// `AppState::device_conns`.
#[derive(Default)]
pub struct DeviceConns {
    inner: parking_lot::Mutex<HashMap<String, HashMap<u64, tokio::sync::oneshot::Sender<()>>>>,
    next: std::sync::atomic::AtomicU64,
}

impl DeviceConns {
    /// Register a device connection; the returned receiver resolves
    /// when the device is revoked (drop it to be never-signaled).
    pub fn register(&self, device: &str) -> (u64, tokio::sync::oneshot::Receiver<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner
            .lock()
            .entry(device.to_string())
            .or_default()
            .insert(id, tx);
        (id, rx)
    }

    /// Remove one connection's registration (its socket ended on its
    /// own). Stale entries under a live device are impossible: revoke
    /// drains the whole device.
    pub fn unregister(&self, device: &str, id: u64) {
        let mut g = self.inner.lock();
        if let Some(m) = g.get_mut(device) {
            m.remove(&id);
            if m.is_empty() {
                g.remove(device);
            }
        }
    }

    /// Revoke-time: fire every kill handle for the device. Returns how
    /// many live sockets were signaled. Sends to dead receivers are
    /// ignored — their connections unregister themselves.
    pub fn cut(&self, device: &str) -> usize {
        let mut g = self.inner.lock();
        g.remove(device)
            .map(|m| {
                let n = m.len();
                for (_, tx) in m {
                    let _ = tx.send(());
                }
                n
            })
            .unwrap_or(0)
    }
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
        let skills = {
            let cfg = config.read();
            std::sync::Arc::new(crate::skills_hub::SkillsHub::new(
                crate::skills_hub::hub_root(cfg.data_dir.as_deref()),
            ))
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
            skills,
            worktrees: crate::worktree::new_registry(),
            cli_runner: std::sync::OnceLock::new(),
            cli_updates: crate::cli_lifecycle::UpdateRegistry::new(),
            mcp_probe: std::sync::OnceLock::new(),
            remote_ws: std::sync::atomic::AtomicUsize::new(0),
            history_scan: parking_lot::Mutex::new(None),
            missions: crate::mission::MissionRegistry::new(),
            device_conns: DeviceConns::default(),
        });
        // Crash-honest mission convergence: runs with unfinished work
        // and no end timestamp are orphans of a dead daemon — mark them
        // interrupted, cancel their non-terminal tasks, close their
        // pending inbox. Never faked as still-running.
        {
            let state2 = state.clone();
            tokio::spawn(async move {
                match state2
                    .store
                    .recover_interrupted_mission_runs(state2.missions.live_run_ids())
                    .await
                {
                    Ok(runs) if !runs.is_empty() => {
                        tracing::info!(count = runs.len(), "recovered interrupted mission runs");
                    }
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "mission recovery failed"),
                }
            });
        }
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
        // History scan: a boot pass (delayed — the daemon settles first)
        // plus an optional interval (`[history] scan_interval_secs`, 0 =
        // off). Single-flight: background sweeps share the same lock the
        // session.rescan RPC holds, so a manual scan can never race a
        // scheduled one.
        let scan_interval = state.config.read().history.scan_interval_secs;
        {
            let state = state.clone();
            tokio::spawn(async move {
                // Disabled: the automatic sweeps (boot + interval) are
                // off entirely; only an explicit session.rescan runs.
                if !state.config.read().history.enabled {
                    return;
                }
                // A few seconds in — backends settle, and an early scan
                // would race the boot log for nothing.
                tokio::time::sleep(Duration::from_secs(5)).await;
                {
                    let mut slot = state.history_scan.lock();
                    if slot.is_none() {
                        *slot = Some(uuid::Uuid::new_v4().to_string());
                    }
                }
                let report = crate::history_scan::scan_history_quiet(&state).await;
                *state.history_scan.lock() = None;
                match report {
                    Ok(r) if r.scanned > 0 || r.reparsed > 0 || r.archived > 0 => {
                        info!(
                            scanned = r.scanned,
                            reparsed = r.reparsed,
                            reused = r.reused,
                            archived = r.archived,
                            "history scan complete"
                        )
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "history scan failed"),
                }
                if scan_interval == 0 {
                    return; // boot scan only — explicit session.rescan after that
                }
                loop {
                    tokio::time::sleep(Duration::from_secs(scan_interval)).await;
                    {
                        let mut slot = state.history_scan.lock();
                        if slot.is_some() {
                            continue; // a manual scan owns the flight
                        }
                        *slot = Some(uuid::Uuid::new_v4().to_string());
                    }
                    match crate::history_scan::scan_history_quiet(&state).await {
                        Ok(r) => info!(
                            scanned = r.scanned,
                            imported = r.reparsed,
                            "scheduled history scan"
                        ),
                        Err(e) => warn!(error = %e, "scheduled history scan failed"),
                    }
                    *state.history_scan.lock() = None;
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
    /// Returns the authenticated identity — which web device the
    /// connection carries, when it rode a device ticket — or None when
    /// the upgrade must be refused.
    pub async fn check_auth(
        &self,
        headers: &axum::http::HeaderMap,
        ticket: Option<&str>,
        _peer: IpAddr,
    ) -> Option<WsAuth> {
        let expected = match self.auth_token().await {
            Some(Ok(t)) => Some(t),
            // Configured but unresolvable: fail closed.
            Some(Err(_)) => return None,
            None if !self.bind.ip().is_loopback() => return None,
            None => None,
        };
        let Some(expected) = expected else {
            return Some(WsAuth { device: None }); // loopback + no token configured
        };
        if let Some(t) = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            && crate::config::constant_time_eq(t.as_bytes(), expected.as_bytes())
        {
            return Some(WsAuth { device: None });
        }
        // Single-use ticket: consume on attempt, valid or not — a
        // replayed ticket must never authenticate twice.
        if let Some(t) = ticket {
            let mut tickets = self.ws_tickets.lock().await;
            if let Some(record) = tickets.remove(t)
                && record.issued.elapsed() < WS_TICKET_TTL
            {
                return Some(WsAuth {
                    device: record.device,
                });
            }
        }
        None
    }
}

/// The authenticated /ws identity: the web device the connection
/// belongs to (None = master auth or open loopback).
#[derive(Debug, Clone)]
pub struct WsAuth {
    pub device: Option<String>,
}

pub fn router(state: Arc<AppState>) -> Router {
    // /v1/ws_ticket keeps its historical path so the web UI and the npm
    // client keep working; the model endpoints are gone — agents answer,
    // not an OpenAI-compatible proxy. The ticket route carries its OWN
    // auth (master bearer OR an approved device token — see ws_ticket)
    // instead of the shared require_token layer, and stays rate-limited:
    // device tokens are 256-bit random, but a throttled brute-force
    // surface costs nothing to keep.
    let v1 = Router::new()
        .route("/pair", post(pair))
        .route("/pair/status", post(pair_status))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            rate_limit,
        ));

    Router::new()
        .route(
            "/v1/ws_ticket",
            post(ws_ticket).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                rate_limit,
            )),
        )
        .route("/health", get(health))
        // Embedded chat UI — static markup, no secrets; the page
        // authenticates itself through the ws ticket flow.
        .route("/ui", get(crate::ui::ui))
        .route("/relay-client.js", get(crate::ui::relay_client_js))
        .route("/stream-reveal.js", get(crate::ui::stream_reveal_js))
        .route("/hljs.js", get(crate::ui::hljs_js))
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
/// `damon_live_sessions` is the live map size; `damon_busy_sessions`
/// counts turns in flight (what the old `live_sessions` number
/// actually measured).
async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let busy = state.sessions.busy_ids().await.len();
    let body = state
        .metrics
        .render(state.sessions.live_count().await, busy);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
        .into_response()
}

/// POST /v1/ws_ticket — issue a one-shot ticket usable as /ws?ticket=.
/// Two credentials mint tickets: the master token via the Authorization
/// header (the historical path, byte-for-byte the old require_token
/// rules), or — new — an APPROVED web device's token in the JSON body
/// (`{"deviceToken": …}`): the per-device revokable credential for
/// remote surfaces. Pending and revoked devices mint nothing; a device
/// token works even while the master secret is unresolvable, because
/// the device row is its own credential. Loopback + no token
/// configured stays open.
#[derive(serde::Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct TicketRequest {
    device_token: Option<String>,
}

async fn ws_ticket(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Option<Json<TicketRequest>>,
) -> Response {
    // Master path — the exact rules require_token applied.
    let master_ok = match state.auth_token().await {
        Some(Ok(t)) => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|got| crate::config::constant_time_eq(got.as_bytes(), t.as_bytes())),
        // Configured but unresolvable: the master path fails closed —
        // an approved device's own credential still works (its row is
        // independent of the master secret).
        Some(Err(_)) => false,
        None => state.bind.ip().is_loopback(),
    };
    if master_ok {
        return issue_ticket(&state, None).await;
    }
    if let Some(token) = body
        .and_then(|Json(b)| b.device_token)
        .filter(|t| !t.trim().is_empty())
    {
        let hash = crate::cli_lifecycle::hash_device_token(token.trim());
        // Fail closed on store errors: unwrap_or(false).
        if state.store.device_token_ok(&hash).await.unwrap_or(false) {
            // Throttled last-seen — the store bounds the writes.
            let _ = state.store.touch_device(&hash).await;
            // Stamp the device id onto the ticket: the /ws upgrade
            // needs it to register the socket for revocation cutoff.
            let device = state.store.device_id_by_hash(&hash).await.ok().flatten();
            return issue_ticket(&state, device).await;
        }
    }
    StatusCode::UNAUTHORIZED.into_response()
}

/// Mint one single-use ticket (sweep-on-insert as before), stamped
/// with the device it was minted for (None = master auth).
async fn issue_ticket(state: &Arc<AppState>, device: Option<String>) -> Response {
    let ticket = uuid::Uuid::new_v4().to_string();
    let mut tickets = state.ws_tickets.lock().await;
    // Sweep expired entries on insert — a ticket abandoned without a
    // /ws upgrade would otherwise linger for the daemon's lifetime.
    tickets.retain(|_, t| t.issued.elapsed() < WS_TICKET_TTL);
    tickets.insert(
        ticket.clone(),
        WsTicket {
            issued: Instant::now(),
            device,
        },
    );
    Json(json!({ "ticket": ticket })).into_response()
}

/// POST /v1/pair — spend a one-time pairing key on one device: the key
/// is consumed AND rotated in a single locked step (a code opens
/// exactly one pairing), the device row starts `pending`, and the
/// device token — returned exactly once, here — mints nothing until a
/// trusted surface approves the row.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairRequest {
    pairing_key: String,
    name: Option<String>,
}

async fn pair(State(state): State<Arc<AppState>>, Json(body): Json<PairRequest>) -> Response {
    let Some(_fresh) = state
        .store
        .consume_pairing_key(&body.pairing_key)
        .await
        .unwrap_or(None)
    else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": "invalid pairing key"
            })),
        )
            .into_response();
    };
    let token = crate::cli_lifecycle::generate_device_token();
    let hash = crate::cli_lifecycle::hash_device_token(&token);
    let Ok(id) = state
        .store
        .create_device(body.name.as_deref().unwrap_or_default(), &hash)
        .await
    else {
        // The key was spent but the row failed — fail closed: rotate a
        // new key in so the shown one is never half-spent.
        let _ = state.store.pairing_key_ensure().await;
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    Json(json!({
        "deviceId": id,
        "token": token,
        "status": "pending",
    }))
    .into_response()
}

/// POST /v1/pair/status — the waiting page's poll. The row is matched
/// by id AND token hash (the device proves it owns the credential);
/// a missing row reads `revoked` — fail closed, and a denied/revoked
/// device stops polling with a reason.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PairStatusRequest {
    device_id: String,
    token: String,
}

async fn pair_status(
    State(state): State<Arc<AppState>>,
    Json(body): Json<PairStatusRequest>,
) -> Response {
    let hash = crate::cli_lifecycle::hash_device_token(body.token.trim());
    let status = match state
        .store
        .device_pair_status(&body.device_id, &hash)
        .await
        .unwrap_or(None)
    {
        Some(status) => status,
        None => "revoked".to_string(),
    };
    Json(json!({ "status": status })).into_response()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The rendered body is valid Prometheus text: HELP/TYPE metadata,
    /// cumulative histogram buckets ending at +Inf == count, and a sum
    /// that tracks the recorded durations. Buckets are CUMULATIVE, so a
    /// 2s turn lands in every bucket ≥ its duration and none below.
    #[test]
    fn metrics_render_turn_histogram_and_errors() {
        let m = Metrics::default();
        m.record_turn("mock", "completed", std::time::Duration::from_millis(2_100));
        m.record_turn("mock", "failed", std::time::Duration::from_millis(700));
        m.record_turn("claude", "completed", std::time::Duration::from_secs(90));
        m.record_rpc_error(-32602);
        m.record_rpc_error(-32602);
        m.record_rpc_error(-32001);

        let body = m.render(3, 1);
        assert!(body.contains("damon_live_sessions 3"), "{body}");
        assert!(body.contains("damon_busy_sessions 1"), "{body}");
        assert!(
            body.contains("damon_rpc_errors_total{code=\"-32602\"} 2"),
            "{body}"
        );
        assert!(
            body.contains("damon_rpc_errors_total{code=\"-32001\"} 1"),
            "{body}"
        );
        // mock: one 2.1s turn (first bucket edge ≥ 2.1 is 5) and one
        // 0.7s turn (edge 1) — cumulative counts per le.
        assert!(
            body.contains("damon_turns_total{backend=\"mock\",status=\"completed\"} 1"),
            "{body}"
        );
        assert!(
            body.contains("damon_turns_total{backend=\"mock\",status=\"failed\"} 1"),
            "{body}"
        );
        assert!(
            body.contains("damon_turn_seconds_bucket{backend=\"mock\",le=\"1\"} 1"),
            "{body}"
        );
        assert!(
            body.contains("damon_turn_seconds_bucket{backend=\"mock\",le=\"5\"} 2"),
            "{body}"
        );
        assert!(
            body.contains("damon_turn_seconds_bucket{backend=\"mock\",le=\"+Inf\"} 2"),
            "{body}"
        );
        // sum is seconds: 2.1 + 0.7 = 2.8.
        assert!(
            body.contains("damon_turn_seconds_sum{backend=\"mock\"} 2.8"),
            "{body}"
        );
        assert!(
            body.contains("damon_turn_seconds_count{backend=\"mock\"} 2"),
            "{body}"
        );
        // claude: the 90s turn lands past the 60 bucket.
        assert!(
            body.contains("damon_turn_seconds_bucket{backend=\"claude\",le=\"60\"} 0"),
            "{body}"
        );
        assert!(
            body.contains("damon_turn_seconds_bucket{backend=\"claude\",le=\"120\"} 1"),
            "{body}"
        );
    }

    /// An unknown stop reason still counts the turn (total/sum/count)
    /// without inventing a status label.
    #[test]
    fn metrics_record_unknown_status_still_counts_total() {
        let m = Metrics::default();
        m.record_turn("mock", "weird", std::time::Duration::from_secs(1));
        let body = m.render(0, 0);
        assert!(
            body.contains("damon_turn_seconds_count{backend=\"mock\"} 1"),
            "{body}"
        );
        assert!(!body.contains("status=\"weird\""), "{body}");
    }
}
