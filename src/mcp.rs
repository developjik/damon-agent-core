use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, Tool};
use rmcp::service::RunningService;
use rmcp::transport::TokioChildProcess;
use serde_json::Value;
use tracing::{info, warn};

use crate::config::McpServerConfig;

struct ServerSlot {
    cfg: parking_lot::RwLock<McpServerConfig>,
    conn: tokio::sync::Mutex<Option<RunningService<rmcp::RoleClient, ()>>>,
    /// Consecutive connect failures → next allowed attempt. A dead server
    /// must not respawn on every tool call — exponential backoff instead.
    backoff: parking_lot::Mutex<(u32, Option<std::time::Instant>)>,
}

/// `ensure_connected` refused to dial because the server is inside its
/// backoff window — a transient state, NOT a dead server. Callers must
/// not treat it as proof the server is gone (e.g. by dropping its tools).
#[derive(Debug, thiserror::Error)]
#[error("MCP server backing off after recent failures")]
struct Backoff;

#[derive(Default)]
struct Inner {
    servers: HashMap<String, Arc<ServerSlot>>,
    /// namespaced tool name -> (server name, tool schema)
    tools: HashMap<String, (String, Tool)>,
}

/// Registry of MCP servers and their tools. Tool names are namespaced as
/// `server.tool` to avoid collisions across servers.
///
/// Connections are lazy and self-healing: a crashed stdio child is respawned
/// on the next call, and `reload` diffs the config (new servers connect,
/// removed servers are dropped, changed configs respawn on next call).
pub struct McpRegistry {
    inner: Arc<parking_lot::RwLock<Inner>>,
    /// Per-session "always allow" grants: session id → tool names. In-memory
    /// only — approvals never persist to config or disk.
    session_approvals: parking_lot::RwLock<HashMap<String, HashSet<String>>>,
}

impl McpRegistry {
    /// Spawn every configured server and enumerate tools. Individual server
    /// failures are logged, not fatal.
    pub async fn connect_all(cfgs: &HashMap<String, McpServerConfig>) -> Self {
        let registry = Self {
            inner: Arc::new(parking_lot::RwLock::new(Inner::default())),
            session_approvals: parking_lot::RwLock::new(HashMap::new()),
        };
        registry.reload(cfgs).await;
        registry
    }

    /// Diff the registry against a new config. New servers connect eagerly
    /// (so their tools appear immediately); removed servers are dropped;
    /// changed configs keep running but respawn on next call.
    pub async fn reload(&self, cfgs: &HashMap<String, McpServerConfig>) {
        // Drop removed servers.
        let removed: Vec<String> = {
            let inner = self.inner.read();
            inner
                .servers
                .keys()
                .filter(|n| !cfgs.contains_key(*n))
                .cloned()
                .collect()
        };
        for name in &removed {
            let slot = self.inner.write().servers.remove(name);
            if let Some(slot) = slot {
                *slot.conn.lock().await = None; // drops service → kills child
            }
            self.inner.write().tools.retain(|_, (s, _)| s != name);
            info!(server = %name, "MCP server removed");
        }

        // Add or update.
        for (name, cfg) in cfgs {
            let existing = self.inner.read().servers.get(name).cloned();
            match existing {
                Some(slot) => {
                    let changed = *slot.cfg.read() != *cfg;
                    if changed {
                        *slot.cfg.write() = cfg.clone();
                        *slot.conn.lock().await = None;
                        // "Always allow" grants were issued against the OLD
                        // command/args — a swapped server must not inherit
                        // them. Drop every approval for this server's tools.
                        let prefix = format!("{name}.");
                        for tools in self.session_approvals.write().values_mut() {
                            tools.retain(|t| !t.starts_with(&prefix));
                        }
                        info!(server = %name, "MCP server config changed; will respawn");
                    }
                }
                None => {
                    let slot = Arc::new(ServerSlot {
                        cfg: parking_lot::RwLock::new(cfg.clone()),
                        conn: tokio::sync::Mutex::new(None),
                        backoff: parking_lot::Mutex::new((0, None)),
                    });
                    self.inner
                        .write()
                        .servers
                        .insert(name.clone(), slot.clone());
                    if let Err(e) = self.ensure_connected(name, &slot).await {
                        warn!(server = %name, error = %e, "MCP server failed to start");
                    }
                }
            }
        }
        let inner = self.inner.read();
        info!(
            servers = inner.servers.len(),
            tools = inner.tools.len(),
            "MCP registry ready"
        );
    }

    /// Close every server connection, awaiting each child's graceful
    /// shutdown under a bounded timeout: SHUTDOWN_TIMEOUT caps both the
    /// per-slot conn-lock wait and each close, so daemon exit stays within
    /// a few seconds per slot even while a dial (up to ~60s) holds the
    /// lock — a slot taken over mid-dial is skipped, not awaited. Dropping
    /// a `RunningService` only *schedules* an async close (its DropGuard
    /// fires cancellation but nothing awaits the child), so a daemon
    /// relying on Drop alone can orphan stdio children — call this from
    /// the daemon shutdown hook.
    pub async fn shutdown(&self) {
        // A child that ignores shutdown — or a dial holding the conn
        // lock — must not hang daemon exit.
        const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        let slots: Vec<(String, Arc<ServerSlot>)> = {
            let inner = self.inner.read();
            inner
                .servers
                .iter()
                .map(|(n, s)| (n.clone(), s.clone()))
                .collect()
        };
        for (name, slot) in slots {
            // take() empties the slot: a call racing shutdown fails fast
            // with "MCP server not connected" instead of using a dying
            // child. The lock wait itself carries the same bound: tokio
            // mutex lock futures are cancellation-safe, so an elapsed
            // timer just dequeues this waiter. Losing the wait means a
            // dial is in flight (ensure_connected holds the lock across
            // the whole dial, up to ~60s) — skip this slot's graceful
            // close rather than stalling exit behind it. Cleanup stays
            // with the dialer's own completion path: on dial failure the
            // half-built RunningService drops and rmcp's ChildWithCleanup
            // drop guard kills the child; on success the conn is
            // installed and the RunningService drop guard closes it when
            // the registry is dropped at daemon exit (kill is scheduled,
            // not awaited — the Drop-path caveat in this doc comment).
            let Ok(mut conn) = tokio::time::timeout(SHUTDOWN_TIMEOUT, slot.conn.lock()).await
            else {
                warn!(
                    server = %name,
                    "MCP server dial in progress at shutdown; skipping graceful close"
                );
                continue;
            };
            let Some(mut svc) = conn.take() else {
                continue;
            };
            // close_with_timeout cancels the service and awaits the
            // background task (incl. transport/child teardown), returning
            // Ok(None) when the child overstays the timeout — it logs
            // that case itself, so only the join error surfaces here.
            if let Err(e) = svc.close_with_timeout(SHUTDOWN_TIMEOUT).await {
                warn!(server = %name, error = %e, "MCP server shutdown failed");
            }
        }
    }

    /// Connect a server if its slot is empty, then refresh its tool map.
    /// Consecutive failures back off exponentially (1s → 30s cap) so a
    /// permanently-broken server isn't respawned on every tool call.
    async fn ensure_connected(&self, name: &str, slot: &Arc<ServerSlot>) -> anyhow::Result<()> {
        // Hold the conn mutex across the WHOLE dial. Concurrent callers
        // (e.g. N sessions racing to revive a freshly crashed server)
        // block here and then reuse whichever conn the dialer installs —
        // waiting out a CONNECT_TIMEOUT (30s) dial is the intended
        // behavior, and cheaper than N duplicate child spawns. Holding it
        // also serializes this dial against reload()'s teardown, which
        // takes the same lock to drop the conn.
        let mut conn = slot.conn.lock().await;
        if conn.is_some() {
            return Ok(());
        }
        {
            let mut b = slot.backoff.lock();
            if let Some(next) = b.1
                && std::time::Instant::now() < next
            {
                return Err(Backoff.into());
            }
            b.1 = None; // window passed — this attempt is allowed
        }
        let cfg = slot.cfg.read().clone();
        let (svc, tools) = match connect_one(name, &cfg).await {
            Ok(v) => {
                *slot.backoff.lock() = (0, None);
                v
            }
            Err(e) => {
                let mut b = slot.backoff.lock();
                b.0 = b.0.saturating_add(1);
                // 1s → 30s cap: b.0 counts consecutive failures, so the
                // first failure waits 1s (1<<0), not 2s.
                let secs = 1u64 << b.0.saturating_sub(1).min(5);
                b.1 =
                    Some(std::time::Instant::now() + std::time::Duration::from_secs(secs.min(30)));
                return Err(e);
            }
        };
        // Publish atomically: re-verify the slot is still THIS slot (a
        // reload may have removed it while we dialed) and install the conn
        // and its tools in ONE write critical section. Splitting the
        // recheck from the tools insert let a reload landing between the
        // two lock windows purge the server and have this insert
        // resurrect its tools — advertised by has_tool/openai_tools but
        // uncallable ("MCP server X not running" on every call). No .await
        // inside: the write guard must not be held across an await point.
        {
            let mut inner = self.inner.write();
            let still = inner
                .servers
                .get(name)
                .is_some_and(|cur| Arc::ptr_eq(cur, slot));
            if !still {
                // svc drops here, killing the child; the slot stays empty.
                return Err(anyhow::anyhow!(
                    "MCP server {name} was removed during connect"
                ));
            }
            inner.tools.retain(|_, (s, _)| s != name);
            for t in tools {
                inner
                    .tools
                    .insert(format!("{name}.{}", t.name), (name.to_string(), t));
            }
            // We have held the conn lock since observing None, and only
            // ensure_connected ever installs a conn — still empty here.
            *conn = Some(svc);
        }
        Ok(())
    }

    /// OpenAI `tools` array for chat completions.
    pub fn openai_tools(&self) -> Vec<Value> {
        self.inner
            .read()
            .tools
            .iter()
            .map(|(namespaced, (_, t))| {
                serde_json::json!({
                    "type": "function",
                    "function": {
                        "name": namespaced,
                        "description": t.description.as_deref().unwrap_or(""),
                        // Some providers reject null parameters — emit an
                        // empty object schema instead.
                        "parameters": serde_json::Value::Object(
                            t.input_schema.as_ref().clone(),
                        ),
                    }
                })
            })
            .collect()
    }

    /// Whether calls to this tool skip the permission round-trip via the
    /// server's `auto_approve` config flag.
    pub fn auto_approve(&self, namespaced_tool: &str) -> bool {
        let inner = self.inner.read();
        inner
            .tools
            .get(namespaced_tool)
            .and_then(|(server, _)| inner.servers.get(server))
            .is_some_and(|s| s.cfg.read().auto_approve)
    }

    /// Whether this session granted "always allow" for the tool.
    pub fn session_approved(&self, session_id: &str, namespaced_tool: &str) -> bool {
        self.session_approvals
            .read()
            .get(session_id)
            .is_some_and(|tools| tools.contains(namespaced_tool))
    }

    /// Record an "always allow" grant for a tool within one session.
    pub fn approve_for_session(&self, session_id: &str, namespaced_tool: &str) {
        self.session_approvals
            .write()
            .entry(session_id.to_string())
            .or_default()
            .insert(namespaced_tool.to_string());
    }

    /// Drop all session-scoped grants for a session (on session end).
    pub fn clear_session(&self, session_id: &str) {
        self.session_approvals.write().remove(session_id);
    }

    pub fn has_tool(&self, namespaced_tool: &str) -> bool {
        self.inner.read().tools.contains_key(namespaced_tool)
    }

    /// Whether a server with this name is configured (running or not) —
    /// used to reject session-overlay names that would shadow a global
    /// server.
    pub fn has_server(&self, name: &str) -> bool {
        self.inner.read().servers.contains_key(name)
    }

    pub async fn call(&self, namespaced_tool: &str, args: Value) -> anyhow::Result<Value> {
        let (server, tool_name) = {
            let inner = self.inner.read();
            let (server, tool) = inner
                .tools
                .get(namespaced_tool)
                .with_context(|| format!("unknown tool {namespaced_tool}"))?;
            (server.clone(), tool.name.clone())
        };
        let slot = self
            .inner
            .read()
            .servers
            .get(&server)
            .cloned()
            .with_context(|| format!("MCP server {server} not running"))?;

        // Lazy respawn: a crashed child reconnects here. A REAL reconnect
        // failure drops the tool so the model stops calling a dead server —
        // but a backoff-window refusal is transient: the tool must survive
        // it or a single-tool server loses its only tool until reload.
        if let Err(e) = self.ensure_connected(&server, &slot).await {
            if !e.is::<Backoff>() {
                // A real reconnect failure means the server is dead —
                // drop ALL its tools, not just the one that was called,
                // or openai_tools keeps advertising a dead server.
                self.inner.write().tools.retain(|_, (s, _)| s != &server);
            }
            return Err(e).context("MCP server reconnect failed");
        }

        // Tool arguments must be a JSON object — anything else (a bare
        // string, an array from malformed streamed args) would silently
        // invoke the tool with NO arguments, which is worse than failing.
        if !args.is_object() {
            anyhow::bail!("tool arguments must be a JSON object, got {args}");
        }
        let make_params = || {
            let mut p = CallToolRequestParams::new(tool_name.clone());
            if let Some(obj) = args.as_object() {
                p = p.with_arguments(obj.clone());
            }
            p
        };

        // A hung MCP child must not stall the turn forever — bound every
        // call, and bound the retry too.
        const TOOL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
        // Clone the peer handle and release the conn lock before the
        // call — holding it across a 300s timeout would serialize every
        // tool call to this server (and block reconnects) for all sessions.
        let peer = {
            let conn = slot.conn.lock().await;
            conn.as_ref()
                .context("MCP server not connected")?
                .peer()
                .clone()
        };
        let result = tokio::time::timeout(TOOL_TIMEOUT, peer.call_tool(make_params())).await;
        match result {
            Ok(Ok(r)) => Ok(serde_json::to_value(r)?),
            Ok(Err(e)) => {
                use rmcp::service::ServiceError;
                // Retry ONLY TransportSend: the request failed before it
                // could reach the child, so re-running cannot re-execute
                // a tool. TransportClosed (child died mid-call — the
                // request may have already run) and UnexpectedResponse
                // (a response WAS received) both mean the server may
                // have processed the call; retrying could double-execute
                // a side-effectful tool, so those return the error.
                if !matches!(e, ServiceError::TransportSend(_)) {
                    if matches!(e, ServiceError::TransportClosed) {
                        // The conn is a corpse — drop it so the next call
                        // respawns a fresh child instead of erroring on
                        // the dead one forever.
                        *slot.conn.lock().await = None;
                    }
                    return Err(e.into());
                }
                warn!(server = %server, error = %e, "tool call send failed; reconnecting");
                *slot.conn.lock().await = None;
                self.ensure_connected(&server, &slot).await?;
                let peer = {
                    let conn = slot.conn.lock().await;
                    conn.as_ref()
                        .context("MCP server not connected")?
                        .peer()
                        .clone()
                };
                let r = tokio::time::timeout(TOOL_TIMEOUT, peer.call_tool(make_params()))
                    .await
                    .context("tool call timed out")?
                    .context("tool call failed")?;
                Ok(serde_json::to_value(r)?)
            }
            Err(_) => {
                // A hung child stays hung — drop the conn so the next
                // call respawns instead of burning another timeout.
                *slot.conn.lock().await = None;
                Err(anyhow::anyhow!(
                    "tool call timed out after {}s",
                    TOOL_TIMEOUT.as_secs()
                ))
            }
        }
    }
}

/// A merged view over the global registry plus an optional per-session
/// overlay (ACP `session/new` mcpServers). Session tools win on lookup;
/// approvals route to the registry that owns the tool so a grant never
/// crosses the global/session boundary.
pub struct McpView<'a> {
    global: &'a McpRegistry,
    session: Option<Arc<McpRegistry>>,
}

impl<'a> McpView<'a> {
    pub fn new(global: &'a McpRegistry, session: Option<Arc<McpRegistry>>) -> Self {
        Self { global, session }
    }

    /// The registry owning `namespaced_tool` — session overlay first.
    fn owner(&self, namespaced_tool: &str) -> &McpRegistry {
        match &self.session {
            Some(s) if s.has_tool(namespaced_tool) => s,
            _ => self.global,
        }
    }

    /// OpenAI `tools` array: global tools plus session-overlay tools.
    /// A session tool shadowing a global name replaces it — the overlay
    /// is the session's explicit choice.
    pub fn openai_tools(&self) -> Vec<Value> {
        let mut tools = self.global.openai_tools();
        if let Some(s) = &self.session {
            let overlay = s.openai_tools();
            let overlay_names: HashSet<&str> = overlay
                .iter()
                .filter_map(|t| t["function"]["name"].as_str())
                .collect();
            tools.retain(|t| {
                !t["function"]["name"]
                    .as_str()
                    .is_some_and(|n| overlay_names.contains(n))
            });
            tools.extend(overlay);
        }
        tools
    }

    pub fn has_tool(&self, namespaced_tool: &str) -> bool {
        self.session
            .as_ref()
            .is_some_and(|s| s.has_tool(namespaced_tool))
            || self.global.has_tool(namespaced_tool)
    }

    pub fn auto_approve(&self, namespaced_tool: &str) -> bool {
        self.owner(namespaced_tool).auto_approve(namespaced_tool)
    }

    pub fn session_approved(&self, session_id: &str, namespaced_tool: &str) -> bool {
        self.owner(namespaced_tool)
            .session_approved(session_id, namespaced_tool)
    }

    pub fn approve_for_session(&self, session_id: &str, namespaced_tool: &str) {
        self.owner(namespaced_tool)
            .approve_for_session(session_id, namespaced_tool);
    }

    pub async fn call(&self, namespaced_tool: &str, args: Value) -> anyhow::Result<Value> {
        self.owner(namespaced_tool)
            .call(namespaced_tool, args)
            .await
    }
}

/// Bound the whole connect+initialize+list_tools sequence — a child that
/// spawns but never speaks MCP would otherwise hang daemon startup and
/// every later reconnect attempt forever.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn connect_one(
    name: &str,
    cfg: &McpServerConfig,
) -> anyhow::Result<(RunningService<rmcp::RoleClient, ()>, Vec<Tool>)> {
    let mut cmd = tokio::process::Command::new(&cfg.command);
    cmd.args(&cfg.args);
    for (k, v) in &cfg.env {
        cmd.env(k, v);
    }
    let transport =
        TokioChildProcess::new(cmd).with_context(|| format!("cannot spawn {}", cfg.command))?;
    let service = tokio::time::timeout(CONNECT_TIMEOUT, ().serve(transport))
        .await
        .with_context(|| format!("MCP server {name} did not initialize within 30s"))??;
    let tools = match tokio::time::timeout(CONNECT_TIMEOUT, service.peer().list_all_tools()).await {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            warn!(server = %name, error = %e, "tools/list failed");
            Vec::new()
        }
        Err(_) => {
            warn!(server = %name, "tools/list timed out");
            Vec::new()
        }
    };
    info!(server = %name, tools = tools.len(), "MCP server connected");
    Ok((service, tools))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: shutdown() once awaited `slot.conn.lock()` with NO
    /// timeout. ensure_connected holds that lock across a whole dial
    /// (initialize 30s + tools/list 30s), so a SIGTERM landing mid-dial
    /// stalled daemon exit ~60s per slot — far past the documented 5s
    /// exit bound. The lock wait must carry the same SHUTDOWN_TIMEOUT
    /// bound and skip the slot instead.
    #[tokio::test]
    async fn shutdown_lock_wait_is_bounded_behind_inflight_dial() {
        let registry = McpRegistry::connect_all(&HashMap::new()).await;
        let slot = Arc::new(ServerSlot {
            // Never dialed — the config only has to type-check.
            cfg: parking_lot::RwLock::new(McpServerConfig {
                command: "true".to_string(),
                args: Vec::new(),
                env: HashMap::new(),
                auto_approve: false,
            }),
            conn: tokio::sync::Mutex::new(None),
            backoff: parking_lot::Mutex::new((0, None)),
        });
        registry
            .inner
            .write()
            .servers
            .insert("slow".to_string(), slot.clone());

        // Simulate an in-flight dial: ensure_connected parks holding the
        // conn mutex for the entire dial. Hold it here and never release —
        // pre-fix, shutdown blocked on this lock forever.
        let _dial_guard = slot.conn.lock().await;

        let started = std::time::Instant::now();
        // Outer guard keeps a regression a bounded 15s failure, not a
        // hung test binary.
        let completed =
            tokio::time::timeout(std::time::Duration::from_secs(15), registry.shutdown()).await;
        let elapsed = started.elapsed();
        assert!(
            completed.is_ok(),
            "shutdown blocked {elapsed:?} on a slot whose conn lock was held by a dial"
        );
        // It waited out the SHUTDOWN_TIMEOUT window before skipping (not
        // an instant return) and stayed well inside the outer guard.
        assert!(elapsed >= std::time::Duration::from_secs(4));
        assert!(elapsed < std::time::Duration::from_secs(15));
    }
}
