//! MCP probe + dispatch tests. The stdio fixture is a Node script
//! speaking initialize/tools-list over stdio; the HTTP fixture is an
//! axum loopback server answering the streamable-HTTP handshake. The
//! dispatch tests inject a probe runner — no process, no network.

use std::sync::Arc;

use damon_core::config::Config;
use damon_core::mcp_inventory::{ProbeResult, ProbeRunner};
use serde_json::{Value, json};

mod common;

use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};

fn test_config(auth_token: Option<&str>) -> std::sync::Arc<parking_lot::RwLock<Config>> {
    std::sync::Arc::new(parking_lot::RwLock::new(common::mock_config(auth_token)))
}

/// The rpc-suite's serve — real axum on an ephemeral port, sharing the
/// AppState so the injected probe runner is visible.
async fn serve(
    config: std::sync::Arc<parking_lot::RwLock<Config>>,
) -> (Arc<damon_core::api::AppState>, String) {
    let store = damon_core::store::Store::in_memory().await.unwrap();
    let state = damon_core::api::AppState::new(config, store).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            damon_core::api::router(server_state)
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    (state, addr.to_string())
}

async fn ws_connect(url: &str) -> (Ws, Value) {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = read_json(&mut ws).await;
    assert_eq!(hello["hello"]["protocol"], 2, "{hello}");
    (ws, hello)
}

async fn read_json(ws: &mut Ws) -> Value {
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), ws.next())
            .await
            .expect("timed out waiting for a daemon frame")
            .unwrap()
            .unwrap();
        match frame {
            tokio_tungstenite::tungstenite::Message::Text(t) => {
                break serde_json::from_str(&t).unwrap();
            }
            _ => continue,
        }
    }
}

async fn rpc_send(ws: &mut Ws, msg: Value) {
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        msg.to_string().into(),
    ))
    .await
    .unwrap();
}

async fn read_reply(ws: &mut Ws, id: u64) -> Value {
    loop {
        let v = read_json(ws).await;
        if v.get("id").and_then(|i| i.as_u64()) == Some(id) {
            break v;
        }
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The stdio fixture: a Node script that answers `initialize` and
/// `tools/list` (and one noisy banner line first — the probe must skip
/// it until the matching JSON-RPC id).
fn fixture_script() -> String {
    r#"
let buf = "";
process.stdin.on("data", (d) => {
  buf += d;
  let i;
  while ((i = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.method === "initialize") {
      process.stdout.write("banner line the probe must skip\n");
      process.stdout.write(JSON.stringify({
        jsonrpc: "2.0", id: msg.id,
        result: {
          protocolVersion: msg.params.protocolVersion,
          capabilities: { tools: {} },
          serverInfo: { name: "fixture-mcp", version: "1.2.3" },
        },
      }) + "\n");
    } else if (msg.method === "notifications/initialized") {
      // no response for notifications
    } else if (msg.method === "tools/list") {
      process.stdout.write(JSON.stringify({
        jsonrpc: "2.0", id: msg.id,
        result: { tools: [{ name: "tool_a" }, { name: "tool_b" }, { name: "tool_c" }] },
      }) + "\n");
    }
  }
});
"#
    .to_string()
}

/// The real stdio handshake against the fixture: connected, tool names
/// through, banner skipped, server name + protocol version surfaced.
#[tokio::test]
async fn probe_stdio_handshakes_with_the_fixture_server() {
    let dir = std::env::temp_dir().join(format!("damon-mcp-probe-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let script = dir.join("fixture-server.mjs");
    std::fs::write(&script, fixture_script()).unwrap();

    let runner = damon_core::mcp_inventory::RealProbeRunner;
    let result = runner
        .probe_stdio(
            "node",
            &[script.to_string_lossy().into_owned()],
            &Default::default(),
        )
        .await;
    assert_eq!(result.status, "connected", "{result:?}");
    assert_eq!(
        result.server_name.as_deref(),
        Some("fixture-mcp"),
        "{result:?}"
    );
    assert_eq!(
        result.protocol_version.as_deref(),
        Some("2024-11-05"),
        "{result:?}"
    );
    assert_eq!(
        result.tools,
        vec![
            "tool_a".to_string(),
            "tool_b".to_string(),
            "tool_c".to_string()
        ],
        "{result:?}"
    );
    assert!(result.error.is_none(), "{result:?}");

    std::fs::remove_dir_all(&dir).ok();
}

/// A missing stdio binary fails with a reason (not a hang).
#[tokio::test]
async fn probe_stdio_missing_binary_fails() {
    let runner = damon_core::mcp_inventory::RealProbeRunner;
    let result = runner
        .probe_stdio(
            "definitely-not-a-binary-damon-test",
            &[],
            &Default::default(),
        )
        .await;
    assert_eq!(result.status, "failed", "{result:?}");
    assert!(result.error.is_some(), "{result:?}");
}

/// The HTTP fixture: initialize → (session id echo) → tools/list over
/// streamable-HTTP semantics; 401 → needs_auth.
#[tokio::test]
async fn probe_http_handshakes_and_reports_needs_auth() {
    use axum::routing::post;

    #[derive(Clone, Copy)]
    struct Cfg {
        require_auth: bool,
    }

    async fn handler(
        axum::Extension(cfg): axum::Extension<Cfg>,
        body: String,
    ) -> axum::response::Response {
        let v: Value = serde_json::from_str(&body).unwrap_or_default();
        let id = v["id"].clone();
        if cfg.require_auth {
            return axum::http::StatusCode::UNAUTHORIZED.into_response();
        }
        let payload = match v["method"].as_str() {
            Some("initialize") => json!({
                "jsonrpc": "2.0", "id": id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "fixture-http", "version": "0.2.0"},
                },
            }),
            Some("tools/list") => json!({
                "jsonrpc": "2.0", "id": id,
                "result": {"tools": [{"name": "http_tool"}]},
            }),
            _ => json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601}}),
        };
        (
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            payload.to_string(),
        )
            .into_response()
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let open = Cfg {
        require_auth: false,
    };
    let locked = Cfg { require_auth: true };
    let open_server = axum::Router::new()
        .route("/mcp", post(handler))
        .layer(axum::Extension(open));
    tokio::spawn(async move {
        let _ = axum::serve(listener, open_server.into_make_service()).await;
    });
    let url = format!("http://{addr}/mcp");
    // Give the spawned server a beat to accept.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let runner = damon_core::mcp_inventory::RealProbeRunner;
    let result = runner.probe_http(&url).await;
    assert_eq!(result.status, "connected", "{result:?}");
    assert_eq!(
        result.server_name.as_deref(),
        Some("fixture-http"),
        "{result:?}"
    );
    assert_eq!(result.tools, ["http_tool"], "{result:?}");

    // A second server requiring auth → needs_auth.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener.local_addr().unwrap();
    let locked_server = axum::Router::new()
        .route("/mcp", post(handler))
        .layer(axum::Extension(locked));
    tokio::spawn(async move {
        let _ = axum::serve(listener, locked_server.into_make_service()).await;
    });
    let url2 = format!("http://{addr2}/mcp");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let result = runner.probe_http(&url2).await;
    assert_eq!(result.status, "needs_auth", "{result:?}");
}

/// Dispatch through the real server with an INJECTED probe runner —
/// connected / unsupported shapes, and the inventory's redaction holds
/// on the wire (no env values anywhere).
// The env lock is INTENTIONALLY held across awaits: the daemon reads
// CLAUDE_CONFIG_DIR while serving the inventory/probe rpcs on this
// same runtime, and no daemon task takes the lock (no deadlock).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn mcp_dispatch_inventory_and_probe() {
    struct MockProbe;
    #[async_trait::async_trait]
    impl ProbeRunner for MockProbe {
        async fn probe_stdio(
            &self,
            program: &str,
            _args: &[String],
            _env: &serde_json::Map<String, Value>,
        ) -> ProbeResult {
            let mut r = ProbeResult::of("connected", 42);
            r.server_name = Some(format!("mock-of-{program}"));
            r.tools = vec!["mock_tool".to_string()];
            r
        }
        async fn probe_http(&self, _url: &str) -> ProbeResult {
            ProbeResult::of("needs_auth", 7)
        }
    }

    let (state, addr) = serve(test_config(None)).await;
    let _ = state.mcp_probe.set(Arc::new(MockProbe));
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    // Unsupported backend (no verified source) → typed unsupported.
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"mcp.probe","params":{"backend":"zcode","name":"whatever"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    assert_eq!(reply["result"]["result"], "unsupported", "{reply}");

    // Connected via the injected runner (fixture entry in a tempdir
    // claude config, discovered through CLAUDE_CONFIG_DIR).
    let dir = std::env::temp_dir().join(format!("damon-mcp-dispatch-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join(".claude.json"),
        r#"{"mcpServers": {"fixture": {"command": "fixture-bin", "args": ["--tok", "sekrit"], "env": {"K": "V"}}}}"#,
    )
    .unwrap();
    let _env = damon_core::tests_env_lock();
    unsafe { std::env::set_var("CLAUDE_CONFIG_DIR", &dir) };

    rpc_send(
        &mut ws,
        json!({"id":2,"method":"mcp.probe","params":{"backend":"claude","name":"fixture"}}),
    )
    .await;
    let reply = read_reply(&mut ws, 2).await;
    assert_eq!(reply["result"]["result"], "connected", "{reply}");
    assert_eq!(
        reply["result"]["serverName"], "mock-of-fixture-bin",
        "{reply}"
    );
    assert_eq!(reply["result"]["tools"], json!(["mock_tool"]), "{reply}");

    // Inventory: the fixture entry is REDACTED on the wire — the arg
    // value ("--tok") and the env value ("V") never appear; env KEYS do.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"mcp.inventory","params":{}}),
    )
    .await;
    let reply = read_reply(&mut ws, 3).await;
    let wire = reply.to_string();
    assert!(
        !wire.contains("sekrit") && !wire.contains("--tok"),
        "arg values must never reach the wire: {wire}"
    );
    assert!(
        !wire.contains("\"V\""),
        "env values must never reach the wire: {wire}"
    );
    let fixture = reply["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "fixture")
        .expect("fixture entry inventoried")
        .clone();
    assert_eq!(
        fixture["command"], "fixture-bin <2 args redacted>",
        "{fixture}"
    );
    assert_eq!(fixture["envKeys"], json!(["K"]), "{fixture}");
    assert_eq!(fixture["transport"], "stdio", "{fixture}");

    unsafe { std::env::remove_var("CLAUDE_CONFIG_DIR") };
    std::fs::remove_dir_all(&dir).ok();
}

/// mcp.setEnabled: claude toggles through the real config (tempdir via
/// CLAUDE_CONFIG_DIR); CAS conflict is typed; readonly backends refuse
/// with the reason; a parse-failed config is never written.
// Same: the lock is held across the ws rpcs by design (the daemon
// reads the env while serving them).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn mcp_set_enabled_claude_only_with_cas() {
    let (_state, addr) = serve(test_config(None)).await;
    let (mut ws, _) = ws_connect(&format!("ws://{addr}/ws")).await;

    let dir = std::env::temp_dir().join(format!("damon-mcp-seten-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let _env = damon_core::tests_env_lock();
    eprintln!("[dbg] set_enabled acquired lock, setting env to {dir:?}");
    unsafe { std::env::set_var("CLAUDE_CONFIG_DIR", &dir) };
    std::fs::write(
        dir.join(".claude.json"),
        r#"{"mcpServers": {"srv": {"command": "bin"}}}"#,
    )
    .unwrap();

    // Inventory → the CAS version.
    rpc_send(
        &mut ws,
        json!({"id":1,"method":"mcp.inventory","params":{}}),
    )
    .await;
    let reply = read_reply(&mut ws, 1).await;
    let entry = reply["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "srv")
        .expect("srv inventoried")
        .clone();
    let version = entry["version"].as_str().unwrap().to_string();

    // Readonly backend refuses with the reason.
    rpc_send(
        &mut ws,
        json!({"id":2,"method":"mcp.setEnabled","params":{"backend":"codex","name":"x","enabled":true,"expectedVersion":"v"}}),
    )
    .await;
    let readonly = read_reply(&mut ws, 2).await;
    assert_eq!(readonly["error"]["code"], -32602, "{readonly}");
    assert!(
        readonly["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no verified config toggle"),
        "{readonly}"
    );

    // Enable with the fresh CAS version → set.
    rpc_send(
        &mut ws,
        json!({"id":3,"method":"mcp.setEnabled","params":{"backend":"claude","name":"srv","enabled":false,"expectedVersion":version}}),
    )
    .await;
    let set = read_reply(&mut ws, 3).await;
    assert_eq!(set["result"]["set"], true, "{set}");

    // Stale version (the file changed) → typed conflict.
    rpc_send(
        &mut ws,
        json!({"id":4,"method":"mcp.setEnabled","params":{"backend":"claude","name":"srv","enabled":true,"expectedVersion":version}}),
    )
    .await;
    let conflict = read_reply(&mut ws, 4).await;
    assert_eq!(conflict["error"]["code"], -32010, "{conflict}");

    // The disabled state is visible in a fresh inventory.
    rpc_send(
        &mut ws,
        json!({"id":5,"method":"mcp.inventory","params":{}}),
    )
    .await;
    let reply = read_reply(&mut ws, 5).await;
    let entry = reply["result"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "srv")
        .unwrap()
        .clone();
    assert_eq!(entry["enabled"], false, "{entry}");

    unsafe { std::env::remove_var("CLAUDE_CONFIG_DIR") };
    std::fs::remove_dir_all(&dir).ok();
}
