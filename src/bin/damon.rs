use anyhow::Context;
use clap::{Parser, Subcommand};
use damon_core::client::{ClientEvent, DamonClient};
use serde_json::json;
use tokio::io::AsyncBufReadExt;

#[derive(Parser)]
#[command(name = "damon", version, about = "Damon agent core CLI client")]
struct Args {
    /// Daemon WS URL
    #[arg(long, default_value = "ws://127.0.0.1:9470/ws")]
    url: String,
    /// Auth token (or set DAMON_TOKEN)
    token: Option<String>,
    /// Connect through a relay: --relay ws://relay:8080 --relay-name mydaemon
    #[arg(long)]
    relay: Option<String>,
    /// Daemon name registered on the relay
    #[arg(long)]
    relay_name: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Daemon health check (HTTP /health)
    Health,
    /// List sessions
    Sessions,
    /// Interactive chat REPL
    Chat {
        /// Resume an existing session id
        #[arg(long)]
        session: Option<String>,
    },
    /// Resume an existing session and enter the chat REPL
    Resume {
        /// Session id to resume
        id: String,
    },
    /// Delete a session and its history
    Delete {
        /// Session id to delete
        id: String,
    },
    /// One-shot prompt
    Prompt {
        text: String,
        #[arg(long)]
        session: Option<String>,
    },
    /// Full-text search over all session history
    Search {
        /// FTS5 query (e.g. 'error AND timeout', '"exact phrase"')
        query: String,
        /// Max results
        #[arg(long, default_value = "10")]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if let Cmd::Health = args.cmd {
        let url = args
            .url
            .replace("ws://", "http://")
            .replace("wss://", "https://")
            .replace("/ws", "/health");
        let resp = reqwest::get(&url).await?;
        println!("{}", resp.text().await?);
        return Ok(());
    }

    let client = match (&args.relay, &args.relay_name) {
        (Some(relay), Some(name)) => {
            let token = args.token.as_deref().unwrap_or("");
            DamonClient::connect_relay(relay, name, token).await?
        }
        _ => DamonClient::connect(&args.url, args.token.as_deref()).await?,
    };
    client.initialize().await?;

    match args.cmd {
        Cmd::Health => unreachable!(),
        Cmd::Sessions => {
            for (id, created) in client.list_sessions().await? {
                println!("{id}\t{created}");
            }
        }
        Cmd::Chat { session } => {
            let session_id = match session {
                Some(s) => s,
                None => client.new_session(&cwd()).await?,
            };
            chat_loop(&client, &session_id).await?;
        }
        Cmd::Resume { id } => {
            let session_id = client.resume_session(&id).await?;
            chat_loop(&client, &session_id).await?;
        }
        Cmd::Delete { id } => {
            client.delete_session(&id).await?;
            println!("deleted {id}");
        }
        Cmd::Search { query, limit } => {
            for (sid, mid, snippet) in client.search(&query, limit).await? {
                println!("{sid}:{mid}\t{snippet}");
            }
        }
        Cmd::Prompt { text, session } => {
            let session_id = match session {
                Some(s) => s,
                None => client.new_session(&cwd()).await?,
            };
            run_turn(&client, &session_id, &text).await?;
            println!();
        }
    }
    Ok(())
}

fn cwd() -> String {
    std::env::current_dir()
        .map(|p| p.display().to_string())
        .unwrap_or_default()
}

/// Interactive chat REPL over an existing session.
async fn chat_loop(client: &DamonClient, session_id: &str) -> anyhow::Result<()> {
    eprintln!("session: {session_id}  (Ctrl-D to quit)");
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    loop {
        eprint!("> ");
        let Some(line) = stdin.next_line().await? else { break };
        if line.trim().is_empty() {
            continue;
        }
        run_turn(client, session_id, &line).await?;
    }
    Ok(())
}

/// Run one prompt turn: stream text to stdout, handle permission
/// requests on stderr, return when the turn ends.
async fn run_turn(client: &DamonClient, session_id: &str, text: &str) -> anyhow::Result<()> {
    let mut events = client.events().await;
    let prompt = {
        let client = client.clone();
        let session_id = session_id.to_string();
        let text = text.to_string();
        tokio::spawn(async move { client.prompt(&session_id, &text).await })
    };
    tokio::pin!(prompt);

    // prompt() only errors on send; the turn result arrives as PromptDone.
    prompt.await.context("prompt task panicked")??;
    loop {
        match events.recv().await {
            Some(ClientEvent::PromptDone { result, .. }) => {
                match result {
                    Ok(v) => {
                        let stop = v["stopReason"].as_str().unwrap_or("?");
                        eprintln!("\n[{stop}]");
                    }
                    Err(e) => anyhow::bail!("{}", e["message"].as_str().unwrap_or("rpc error")),
                }
                return Ok(());
            }
            Some(ClientEvent::Update(params)) => {
                        let u = &params["update"];
                        match u["sessionUpdate"].as_str() {
                            Some("agent_message_chunk") => {
                                if let Some(t) = u["content"]["text"].as_str() {
                                    print!("{t}");
                                    use std::io::Write;
                                    let _ = std::io::stdout().flush();
                                }
                            }
                            Some("tool_call_update") => {
                                eprintln!(
                                    "\n[tool {} → {}]",
                                    u["toolCallId"].as_str().unwrap_or(""),
                                    u["status"].as_str().unwrap_or("")
                                );
                            }
                            _ => {}
                        }
                    }
                    Some(ClientEvent::Request { id, method, params }) => {
                        if method == "session/request_permission" {
                            let title = params["toolCall"]["title"].as_str().unwrap_or("?");
                            let input = params["toolCall"]["rawInput"].to_string();
                            eprint!("\n[permission] {title} {input}\nallow? [y/N] ");
                            use std::io::Write;
                            let _ = std::io::stderr().flush();
                            let mut stdin = tokio::io::BufReader::new(tokio::io::stdin());
                            let mut line = String::new();
                            let _ = stdin.read_line(&mut line).await;
                            let allow = matches!(line.trim(), "y" | "Y" | "yes");
                            client
                                .respond(
                                    id,
                                    json!({
                                        "outcome": {
                                            "outcome": "selected",
                                            "optionId": if allow { "allow-once" } else { "reject-once" }
                                        }
                                    }),
                                )
                                .await?;
                        }
                    }
            None => anyhow::bail!("connection closed"),
        }
    }
}
