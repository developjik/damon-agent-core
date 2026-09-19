//! Boot-path smoke test: spawn the real damond binary against a temp
//! config and prove it binds and answers /v1/models. Guards the
//! config-parse → bind → serve chain that unit tests can't reach.

use std::io::Write;
use std::time::Duration;

/// Pick a free port, drop the listener, and let damond bind it.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn damond_boots_and_serves_models() {
    let dir = std::env::temp_dir().join(format!("damond-boot-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let cfg_path = dir.join("config.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    // Literal string (single quotes): a Windows temp path is full of
    // backslashes, which TOML basic strings would read as escapes.
    write!(
        f,
        "bind = \"127.0.0.1:{port}\"\ndata_dir = '{}'\n",
        dir.display()
    )
    .unwrap();

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_damond"))
        .arg("--config")
        .arg(&cfg_path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn damond");

    // Poll /v1/models until the listener is up (or give up after 15s).
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/v1/models");
    let mut ok = false;
    for _ in 0..150 {
        if let Ok(resp) = client.get(&url).send().await
            && resp.status().is_success()
        {
            let body: serde_json::Value = resp.json().await.unwrap();
            assert!(body["data"].is_array(), "bad /v1/models body: {body}");
            ok = true;
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("damond exited early with {status}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    child.kill().await.unwrap();
    assert!(ok, "damond never answered /v1/models on {url}");
}
