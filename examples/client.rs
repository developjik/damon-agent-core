//! Minimal damond client: connect, initialize, one prompt turn.
//! Run: cargo run --example client -- "your prompt"

use damon_core::client::{ClientEvent, DamonClient};
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let text = std::env::args().nth(1).unwrap_or_else(|| "hi".into());
    let client = DamonClient::connect("ws://127.0.0.1:9470/ws", None).await?;
    client.initialize().await?;
    let session = client.new_session("/tmp", None).await?;

    // prompt() sends the request; the turn result arrives as PromptDone on
    // the event stream, ordered after that session's chunk notifications.
    let mut events = client.events().await;
    client.prompt(&session, &text, None).await?;

    loop {
        match events.recv().await {
            Some(ClientEvent::PromptDone { result, .. }) => {
                let v = result.map_err(|e| anyhow::anyhow!("{}", e["message"]))?;
                println!("\n[stop: {}]", v["stopReason"].as_str().unwrap_or("?"));
                return Ok(());
            }
            Some(ClientEvent::Update(p)) => {
                let u = &p["update"];
                if u["sessionUpdate"] == "agent_message_chunk"
                    && let Some(t) = u["content"]["text"].as_str()
                {
                    print!("{t}");
                }
            }
            Some(ClientEvent::Request { id, .. }) => {
                // Auto-approve permission requests.
                client
                    .respond(
                        id,
                        json!({"outcome": {"outcome": "selected", "optionId": "allow-once"}}),
                    )
                    .await?;
            }
            None => anyhow::bail!("connection closed"),
        }
    }
}
