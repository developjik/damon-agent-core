//! End-to-end tests for the `damon` CLI binary against the real daemon
//! router. The daemon runs in-process (axum::serve on a loopback port)
//! with the mock backend injected — the standalone mock binary is gone,
//! so the CLI exercises the full client→v2-wire→daemon path without
//! spawning damond.

mod common;

use std::sync::Arc;

use damon_core::api::{self, AppState};
use damon_core::config::SharedConfig;
use damon_core::store::Store;

/// Serve the real router with the in-process mock backend; returns the
/// ws:// URL the CLI connects to.
async fn serve_daemon() -> String {
    let shared: SharedConfig = Arc::new(parking_lot::RwLock::new(common::mock_config(None)));
    let store = Store::in_memory().await.unwrap();
    let state = AppState::new(shared, store).await;
    state
        .sessions
        .insert_client("mock".into(), common::mock_client());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // ConnectInfo installs the peer IP ws_handler's auth check reads.
        axum::serve(
            listener,
            api::router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("ws://{addr}/ws")
}

/// Run `damon` with args; returns (stdout, success).
async fn damon(url: &str, args: &[&str]) -> (String, bool) {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_damon"))
        .arg("--url")
        .arg(url)
        .args(args)
        .output()
        .await
        .expect("run damon");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        out.status.success(),
    )
}

#[tokio::test]
async fn cli_sessions_and_prompt() {
    let url = serve_daemon().await;

    // Empty session list.
    let (out, ok) = damon(&url, &["sessions"]).await;
    assert!(ok, "sessions failed: {out}");

    // One-shot prompt creates a session and gets an echo.
    let (out, ok) = damon(&url, &["prompt", "hello"]).await;
    assert!(ok, "prompt failed: {out}");
    assert!(
        out.contains("echo: hello"),
        "prompt output missing echo: {out}"
    );

    // Session list now has one entry.
    let (out, ok) = damon(&url, &["sessions"]).await;
    assert!(ok, "sessions failed: {out}");
    assert!(!out.trim().is_empty(), "no sessions listed");
}

#[tokio::test]
async fn cli_fork_and_export() {
    let url = serve_daemon().await;

    // Create a session with a prompt.
    let (out, ok) = damon(&url, &["prompt", "hello"]).await;
    assert!(ok, "prompt failed: {out}");

    // Get the session id.
    let (out, ok) = damon(&url, &["sessions"]).await;
    assert!(ok, "sessions failed: {out}");
    let sid = out.split('\t').next().unwrap().trim().to_string();
    assert!(!sid.is_empty(), "no session id");

    // Fork it.
    let (out, ok) = damon(&url, &["fork", &sid]).await;
    assert!(ok, "fork failed: {out}");
    assert!(out.contains("forked"), "no fork confirmation: {out}");

    // Export the fork as markdown — client-side rendering of
    // session.messages rows in v2.
    let fork_sid = out
        .split('→')
        .nth(1)
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    assert!(!fork_sid.is_empty(), "no fork session id in: {out}");
    let (out, ok) = damon(&url, &["export", &fork_sid, "--md"]).await;
    assert!(ok, "export failed: {out}");
    assert!(out.contains("hello"), "export missing prompt text: {out}");
    assert!(
        out.contains("echo: hello"),
        "export missing echo reply: {out}"
    );
}

#[tokio::test]
async fn cli_json_output() {
    let url = serve_daemon().await;

    let (out, ok) = damon(&url, &["prompt", "hi"]).await;
    assert!(ok, "prompt failed: {out}");

    // --json sessions returns a JSON array.
    let (out, ok) = damon(&url, &["--json", "sessions"]).await;
    assert!(ok, "sessions failed: {out}");
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert!(parsed.is_array(), "sessions not a JSON array: {out}");
    assert!(!parsed.as_array().unwrap().is_empty());

    // --json usage returns a JSON object.
    let (out, ok) = damon(&url, &["--json", "usage"]).await;
    assert!(ok, "usage failed: {out}");
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert!(parsed.is_object(), "usage not a JSON object: {out}");
}

#[tokio::test]
async fn cli_doctor() {
    let url = serve_daemon().await;

    let (out, ok) = damon(&url, &["doctor"]).await;
    assert!(ok, "doctor failed: {out}");
    assert!(out.contains("ok"), "doctor not ok: {out}");
    assert!(out.contains("backends"), "doctor missing backends: {out}");

    // --json doctor returns structured output.
    let (out, ok) = damon(&url, &["--json", "doctor"]).await;
    assert!(ok, "doctor failed: {out}");
    let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert_eq!(parsed["ok"], true, "doctor not ok: {out}");
    // The mock plus any real agent CLIs detected on this machine.
    assert!(
        parsed["backends"].as_u64().unwrap_or(0) >= 1,
        "expected at least the mock backend: {out}"
    );
}
