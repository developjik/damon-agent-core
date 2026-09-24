//! Throwaway probe: drive a real `claude` session through the new
//! backend and print normalized events. Run:
//!   cargo run --example backend_probe -- claude "reply with just: ok"

use damon_core::backend::registry::{BACKENDS, ResolvedBackend, client_for};
use damon_core::backend::types::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = std::env::args().nth(1).unwrap_or_else(|| "claude".into());
    let prompt = std::env::args().nth(2).unwrap_or_else(|| "say ok".into());

    let spec = BACKENDS.iter().find(|b| b.id == provider).expect("backend");
    let resolved = ResolvedBackend {
        id: spec.id.into(),
        title: spec.title.into(),
        command: spec.command.into(),
        args: spec.args.iter().map(|s| s.to_string()).collect(),
        env: spec
            .env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        auth_hint: spec.auth_hint.into(),
        detected: true,
    };
    let client = client_for(&resolved).expect("client");
    println!("== {} available: {}", provider, client.is_available().await);

    let session = client
        .create_session(SessionConfig {
            cwd: std::env::current_dir()?,
            ..Default::default()
        })
        .await?;
    println!("== session up, handle: {:?}", session.persistence_handle());

    let mut events = session.subscribe();
    let turn = session.start_turn(PromptInput::text(&prompt)).await?;
    println!("== turn {turn} started");

    // Print events until the turn completes.
    loop {
        match events.recv().await {
            Ok(ev) => {
                let summary = match &ev.kind {
                    StreamEventKind::Timeline(TimelineItem::AssistantMessage { text }) => {
                        format!("assistant: {}", &text[..text.len().min(80)])
                    }
                    StreamEventKind::Timeline(TimelineItem::Reasoning { text }) => {
                        format!("reasoning: {}", &text[..text.len().min(60)])
                    }
                    StreamEventKind::Timeline(TimelineItem::ToolCall(t)) => {
                        format!("tool {} {:?}", t.name, t.status)
                    }
                    other => format!("{other:?}"),
                };
                println!("  [ev] {summary}");
                if matches!(
                    ev.kind,
                    StreamEventKind::TurnCompleted { .. }
                        | StreamEventKind::TurnFailed { .. }
                        | StreamEventKind::TurnCanceled { .. }
                ) {
                    break;
                }
            }
            Err(e) => {
                println!("== events closed: {e}");
                break;
            }
        }
    }
    session.close().await?;
    println!("== done");
    Ok(())
}
