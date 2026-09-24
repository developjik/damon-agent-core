//! Minimal damond client: connect, hello, one turn.
//! Run: cargo run --example client -- "your prompt"

use damon_core::client::{ClientEvent, DamonClient};
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let text = std::env::args().nth(1).unwrap_or_else(|| "hi".into());
    let client = DamonClient::connect("ws://127.0.0.1:9470/ws", None).await?;
    client.hello().await?;
    let session = client.create_session(None, "/tmp", None).await?;

    // turn_start() sends the request; the turn result arrives as TurnDone
    // on the event stream, ordered after that session's session.event pushes.
    let mut events = client.events().await;
    client.turn_start(&session, &text).await?;

    loop {
        match events.recv().await {
            Some(ClientEvent::TurnDone { result, .. }) => {
                let v = result.map_err(|e| anyhow::anyhow!("{}", e["message"]))?;
                println!("\n[stop: {}]", v["stopReason"].as_str().unwrap_or("?"));
                return Ok(());
            }
            Some(ClientEvent::Event { event, .. }) => {
                // StreamEvent: timeline items carry a `kind` tag.
                if event["type"] == "timeline"
                    && event["kind"] == "assistant_message"
                    && let Some(t) = event["text"].as_str()
                {
                    println!("{t}");
                }
                if event["type"] == "permission_requested"
                    && let Some(id) = event["id"].as_str()
                {
                    // Auto-approve permission requests.
                    client
                        .respond_to_permission(&session, id, json!({"behavior": "allow"}))
                        .await?;
                }
            }
            // Connection-state mirrors — informational only here.
            Some(ClientEvent::Connected | ClientEvent::Disconnected) => {}
            None => anyhow::bail!("connection closed"),
        }
    }
}
