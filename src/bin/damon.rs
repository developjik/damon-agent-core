use anyhow::Context;
use clap::{Parser, Subcommand};
use damon_core::client::{ClientEvent, DamonClient};
use serde_json::json;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(name = "damon", version, about = "Damon agent core CLI client")]
struct Args {
    /// Daemon WS URL
    #[arg(long, default_value = "ws://127.0.0.1:9470/ws")]
    url: String,
    /// Auth token (or set DAMON_TOKEN)
    #[arg(long, env = "DAMON_TOKEN")]
    token: Option<String>,
    /// Connect through a relay: --relay ws://relay:8080 --relay-name mydaemon
    #[arg(long, requires = "relay_name")]
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
        /// Model override (provider/model, glob id, or model:level)
        #[arg(long)]
        model: Option<String>,
    },
    /// Resume an existing session and enter the chat REPL
    Resume {
        /// Session id to resume
        id: String,
        /// Model override for this session's turns
        #[arg(long)]
        model: Option<String>,
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
        /// Model override for this turn
        #[arg(long)]
        model: Option<String>,
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
        anyhow::ensure!(
            args.relay.is_none(),
            "--relay is not supported for `damon health` — query the daemon's /health endpoint directly"
        );
        let url = args
            .url
            .replacen("wss://", "https://", 1)
            .replacen("ws://", "http://", 1);
        let url = format!(
            "{}/health",
            url.trim_end_matches('/').trim_end_matches("/ws")
        );
        // A hung daemon must not hang the health check forever.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()?;
        let resp = http.get(&url).send().await?.error_for_status()?;
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
            for (id, created, model) in client.list_sessions().await? {
                println!("{id}\t{created}\t{model}");
            }
        }
        Cmd::Chat { session, model } => {
            let session_id = match session {
                // Verify the id exists up front — prompting into a dead
                // session would only fail on the first turn.
                Some(s) => client.resume_session(&s).await?,
                None => client.new_session(&cwd(), model.as_deref()).await?,
            };
            chat_loop(&client, &session_id, model.as_deref()).await?;
        }
        Cmd::Resume { id, model } => {
            let session_id = client.resume_session(&id).await?;
            chat_loop(&client, &session_id, model.as_deref()).await?;
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
        Cmd::Prompt {
            text,
            session,
            model,
        } => {
            let session_id = match session {
                Some(s) => s,
                None => client.new_session(&cwd(), model.as_deref()).await?,
            };
            let mut events = client.events().await;
            // One-shot prompt: no REPL reader exists yet — create the
            // single stdin reader here for permission answers.
            let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            run_turn(
                &client,
                &mut events,
                &session_id,
                &text,
                model.as_deref(),
                &mut stdin,
            )
            .await?;
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

/// Interactive chat REPL over an existing session. The event receiver is
/// taken once — `events()` hands out the only consumer, so re-taking it
/// per turn would starve every turn after the first.
async fn chat_loop(
    client: &DamonClient,
    session_id: &str,
    model: Option<&str>,
) -> anyhow::Result<()> {
    eprintln!("session: {session_id}  (Ctrl-D to quit)");
    let mut events = client.events().await;
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    loop {
        eprint!("> ");
        let Some(line) = stdin.next_line().await? else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        // A failed turn (provider error, "session not found", "prompt in
        // progress") must not kill the REPL — only a dead event stream
        // means the connection is gone for good.
        if let Err(e) = run_turn(client, &mut events, session_id, &line, model, &mut stdin).await {
            if events.is_closed() {
                return Err(e);
            }
            eprintln!("[turn error] {e:#}");
        }
    }
    Ok(())
}

/// Run one prompt turn: stream text to stdout, handle permission
/// requests on stderr, return when the turn ends.
async fn run_turn(
    client: &DamonClient,
    events: &mut mpsc::Receiver<ClientEvent>,
    session_id: &str,
    text: &str,
    model: Option<&str>,
    stdin: &mut tokio::io::Lines<tokio::io::BufReader<tokio::io::Stdin>>,
) -> anyhow::Result<()> {
    let prompt = {
        let client = client.clone();
        let session_id = session_id.to_string();
        let text = text.to_string();
        let model = model.map(String::from);
        tokio::spawn(async move { client.prompt(&session_id, &text, model.as_deref()).await })
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
                    eprint!("\n[permission] {title} {input}\nallow? [y/N/a(lways)] ");
                    use std::io::Write;
                    let _ = std::io::stderr().flush();
                    // Reuse the REPL's single buffered stdin — a second
                    // BufReader on the same fd would race it for bytes.
                    let line = stdin.next_line().await?.unwrap_or_default();
                    let option = match line.trim() {
                        "a" | "A" | "always" => "allow-always",
                        "y" | "Y" | "yes" => "allow-once",
                        _ => "reject-once",
                    };
                    client
                        .respond(
                            id,
                            json!({
                                "outcome": {
                                    "outcome": "selected",
                                    "optionId": option
                                }
                            }),
                        )
                        .await?;
                } else {
                    // Unknown server-initiated request: answer with a
                    // JSON-RPC error instead of leaving the daemon
                    // waiting on a reply that never comes.
                    eprintln!("\n[unsupported request: {method}]");
                    client
                        .respond_error(id, -32601, &format!("unsupported method: {method}"))
                        .await?;
                }
            }
            None => anyhow::bail!("connection closed"),
        }
    }
}
