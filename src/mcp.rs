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

    /// Connect a server if its slot is empty, then refresh its tool map.
    /// Consecutive failures back off exponentially (1s → 30s cap) so a
    /// permanently-broken server isn't respawned on every tool call.
    async fn ensure_connected(&self, name: &str, slot: &Arc<ServerSlot>) -> anyhow::Result<()> {
        // Check state under the lock, but connect OUTSIDE it — a 60s
        // connect_one under the conn mutex would stall reload()'s
        // slot.conn.lock() for the whole dial.
        {
            let conn = slot.conn.lock().await;
            if conn.is_some() {
                return Ok(());
            }
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
        // A reload() may have removed this server while we dialed —
        // installing its tools would resurrect a dead server's entries.
        // Re-check the slot is still registered before touching the map.
        {
            let inner = self.inner.read();
            let still = inner
                .servers
                .get(name)
                .is_some_and(|cur| Arc::ptr_eq(cur, slot));
            if !still {
                return Err(anyhow::anyhow!(
                    "MCP server {name} was removed during connect"
                ));
            }
        }
        {
            let mut inner = self.inner.write();
            inner.tools.retain(|_, (s, _)| s != name);
            for t in tools {
                inner
                    .tools
                    .insert(format!("{name}.{}", t.name), (name.to_string(), t));
            }
        }
        // Install the connection — a concurrent ensure_connected may have
        // beaten us; keep whichever is already there and drop ours.
        let mut conn = slot.conn.lock().await;
        if conn.is_none() {
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
                // A JSON-RPC error means the server processed the call —
                // retrying would double-execute a side-effectful tool.
                // Only transport failures (child died mid-call) retry.
                use rmcp::service::ServiceError;
                let transport = matches!(
                    e,
                    ServiceError::TransportSend(_)
                        | ServiceError::TransportClosed
                        | ServiceError::UnexpectedResponse
                );
                if !transport {
                    return Err(e.into());
                }
                warn!(server = %server, error = %e, "tool call transport failed; reconnecting");
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
