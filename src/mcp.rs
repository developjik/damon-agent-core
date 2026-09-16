use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, Tool};
use rmcp::service::RunningService;
use rmcp::transport::TokioChildProcess;
use serde_json::Value;
use tracing::{info, warn};

use crate::config::McpServerConfig;

/// One configured server: its config plus a lazily-reconnectable service slot.
struct ServerSlot {
    cfg: parking_lot::RwLock<McpServerConfig>,
    conn: tokio::sync::Mutex<Option<RunningService<rmcp::RoleClient, ()>>>,
}

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
}

impl McpRegistry {
    /// Spawn every configured server and enumerate tools. Individual server
    /// failures are logged, not fatal.
    pub async fn connect_all(cfgs: &HashMap<String, McpServerConfig>) -> Self {
        let registry = Self {
            inner: Arc::new(parking_lot::RwLock::new(Inner::default())),
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
                        info!(server = %name, "MCP server config changed; will respawn");
                    }
                }
                None => {
                    let slot = Arc::new(ServerSlot {
                        cfg: parking_lot::RwLock::new(cfg.clone()),
                        conn: tokio::sync::Mutex::new(None),
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
    async fn ensure_connected(&self, name: &str, slot: &Arc<ServerSlot>) -> anyhow::Result<()> {
        let mut conn = slot.conn.lock().await;
        if conn.is_some() {
            return Ok(());
        }
        let cfg = slot.cfg.read().clone();
        let (svc, tools) = connect_one(name, &cfg).await?;
        {
            let mut inner = self.inner.write();
            inner.tools.retain(|_, (s, _)| s != name);
            for t in tools {
                inner
                    .tools
                    .insert(format!("{name}.{}", t.name), (name.to_string(), t));
            }
        }
        *conn = Some(svc);
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

    /// Whether calls to this tool skip the permission round-trip.
    pub fn auto_approve(&self, namespaced_tool: &str) -> bool {
        let inner = self.inner.read();
        inner
            .tools
            .get(namespaced_tool)
            .and_then(|(server, _)| inner.servers.get(server))
            .is_some_and(|s| s.cfg.read().auto_approve)
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

        // Lazy respawn: a crashed child reconnects here. If reconnect fails,
        // drop the tool so the model stops calling into a dead server.
        if let Err(e) = self.ensure_connected(&server, &slot).await {
            self.inner.write().tools.remove(namespaced_tool);
            return Err(e).context("MCP server reconnect failed");
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
        let result = {
            let conn = slot.conn.lock().await;
            let svc = conn.as_ref().context("MCP server not connected")?;
            tokio::time::timeout(TOOL_TIMEOUT, svc.peer().call_tool(make_params())).await
        };
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
                let conn = slot.conn.lock().await;
                let svc = conn.as_ref().context("MCP server not connected")?;
                let r = tokio::time::timeout(TOOL_TIMEOUT, svc.peer().call_tool(make_params()))
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
    let service = ().serve(transport).await?;
    let tools = service.peer().list_all_tools().await.unwrap_or_default();
    info!(server = %name, tools = tools.len(), "MCP server connected");
    Ok((service, tools))
}
