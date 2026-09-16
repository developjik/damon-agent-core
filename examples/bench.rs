//! Baseline benchmark: idle RSS + streaming passthrough overhead.
//! Run: cargo run --release --example bench
//!
//! Spawns a mock OpenAI upstream (SSE with inter-chunk delay) and a real
//! `damond` child process, then measures:
//!   - idle RSS of damond after boot
//!   - TTFB and total-time delta: direct-to-upstream vs via-damond

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::response::Response;
use axum::routing::post;
use futures::StreamExt;
use tokio_stream::wrappers::IntervalStream;

const CHUNKS: usize = 20;
const CHUNK_DELAY: Duration = Duration::from_millis(20);

#[tokio::main]
async fn main() {
    let upstream = spawn_mock_upstream().await;
    let (mut damon, damon_addr) = spawn_damond(upstream);

    let rss = idle_rss(damon.id());
    println!("idle RSS:            {:.1} MB", rss as f64 / 1e6);
    let body = serde_json::json!({"model": "bench", "stream": true}).to_string();

    // Warmup both paths (upstream path vs damond's /v1 path).
    let _ = measure_stream(&format!("http://{upstream}/chat/completions"), &body).await;
    let _ = measure_stream(&format!("http://{damon_addr}/v1/chat/completions"), &body).await;

    let mut direct_ttfb = Vec::new();
    let mut direct_total = Vec::new();
    let mut proxy_ttfb = Vec::new();
    let mut proxy_total = Vec::new();
    for _ in 0..5 {
        let (t, tot) = measure_stream(&format!("http://{upstream}/chat/completions"), &body).await;
        direct_ttfb.push(t);
        direct_total.push(tot);
        let (t, tot) =
            measure_stream(&format!("http://{damon_addr}/v1/chat/completions"), &body).await;
        proxy_ttfb.push(t);
        proxy_total.push(tot);
    }

    let d_ttfb = median(&mut direct_ttfb);
    let p_ttfb = median(&mut proxy_ttfb);
    let d_tot = median(&mut direct_total);
    let p_tot = median(&mut proxy_total);

    println!(
        "TTFB  direct:        {:>7.2} ms",
        d_ttfb.as_secs_f64() * 1e3
    );
    println!(
        "TTFB  via damond:    {:>7.2} ms",
        p_ttfb.as_secs_f64() * 1e3
    );
    println!(
        "TTFB  overhead:      {:>7.2} ms",
        (p_ttfb - d_ttfb).as_secs_f64() * 1e3
    );
    println!("total direct:        {:>7.2} ms", d_tot.as_secs_f64() * 1e3);
    println!("total via damond:    {:>7.2} ms", p_tot.as_secs_f64() * 1e3);
    println!(
        "total overhead:      {:>7.2} ms",
        (p_tot - d_tot).as_secs_f64() * 1e3
    );

    let _ = damon.kill();
    let _ = damon.wait();
}

async fn spawn_mock_upstream() -> SocketAddr {
    let app = Router::new().route(
        "/chat/completions",
        post(|| async {
            let stream = IntervalStream::new(tokio::time::interval(CHUNK_DELAY)).map(|_| {
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"data: {\"choices\":[]}\n\n"))
            });
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream.take(CHUNKS)))
                .unwrap()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

fn spawn_damond(upstream: SocketAddr) -> (Child, SocketAddr) {
    // Reserve an ephemeral port, then hand it to damond.
    let damon_addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();

    let dir = std::env::temp_dir().join(format!("damon-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg_path = dir.join("config.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "bind = \"{damon_addr}\"\n\n[providers.default]\nbase_url = \"http://{upstream}\"\n"
        ),
    )
    .unwrap();

    // Examples don't get CARGO_BIN_EXE_*; locate sibling binary in target dir.
    let exe = std::env::current_exe()
        .unwrap()
        .parent() // examples/
        .unwrap()
        .parent() // target/<profile>/
        .unwrap()
        .join("damond");
    let mut child = Command::new(exe)
        .arg("--config")
        .arg(&cfg_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn damond");

    // Wait for the "listening" log line.
    let stdout = child.stdout.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut ready = false;
    for line in BufReader::new(stdout).lines() {
        if Instant::now() > deadline {
            break;
        }
        if let Ok(l) = line {
            if l.contains("listening") {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "damond did not become ready");
    (child, damon_addr)
}

fn idle_rss(pid: u32) -> u64 {
    std::thread::sleep(Duration::from_millis(300)); // settle after boot
    let mut sys = sysinfo::System::new();
    sys.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
        true,
    );
    sys.process(sysinfo::Pid::from_u32(pid))
        .map(|p| p.memory())
        .unwrap_or(0)
}

/// Returns (time to first byte, total time). `url` is the full endpoint.
async fn measure_stream(url: &str, body: &str) -> (Duration, Duration) {
    let client = reqwest::Client::new();
    let start = Instant::now();
    let mut resp = client
        .post(url)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status {}", resp.status());
    let mut ttfb = None;
    while let Some(chunk) = resp.chunk().await.unwrap() {
        if ttfb.is_none() && !chunk.is_empty() {
            ttfb = Some(start.elapsed());
        }
    }
    (ttfb.unwrap_or_else(|| start.elapsed()), start.elapsed())
}

fn median(v: &mut [Duration]) -> Duration {
    v.sort();
    v[v.len() / 2]
}
