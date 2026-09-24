use std::sync::Arc;

use anyhow::Context;
use clap::{CommandFactory, FromArgMatches, Parser};
use damon_core::client::DamonClient;
use damon_core::telegram::{BotApi, Bridge};

#[derive(Parser)]
#[command(name = "damon-telegram", about = "Telegram thin client for damond")]
struct Args {
    /// Daemon WS URL
    #[arg(long, default_value = "ws://127.0.0.1:9470/ws")]
    url: String,
    /// Daemon auth token (or DAMON_TOKEN)
    #[arg(long, env = "DAMON_TOKEN")]
    token: Option<String>,
    /// Telegram bot token (or TELEGRAM_BOT_TOKEN)
    #[arg(long, env = "TELEGRAM_BOT_TOKEN")]
    bot_token: String,
    /// Allowed Telegram user/chat ids, comma-separated (or
    /// DAMON_ALLOW). REQUIRED — a public bot without an allowlist is
    /// unauthenticated agent access.
    #[arg(long, env = "DAMON_ALLOW", value_delimiter = ',')]
    allow: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "damon=info".into()),
        )
        .init();

    let matches = Args::command().get_matches();
    let args = Args::from_arg_matches(&matches).expect("clap-validated matches");
    // A --token on argv is readable by any local user via the process
    // list; the DAMON_TOKEN env alternative is not. Keep the flag for
    // scripts, but warn once so the exposure is at least visible.
    if matches.value_source("token") == Some(clap::parser::ValueSource::CommandLine) {
        eprintln!("warning: --token is visible in process lists; prefer DAMON_TOKEN env");
    }
    // Same exposure for the channel token — argv is world-readable.
    if matches.value_source("bot_token") == Some(clap::parser::ValueSource::CommandLine) {
        eprintln!(
            "warning: --bot-token is visible in process lists; prefer TELEGRAM_BOT_TOKEN env"
        );
    }
    let client = DamonClient::connect(&args.url, args.token.as_deref())
        .await
        .context("cannot connect to damond")?;
    let tg = Arc::new(BotApi::new(&args.bot_token));
    if args.allow.is_empty() {
        anyhow::bail!(
            "--allow is required: a public bot without an allowlist is unauthenticated agent access"
        );
    }
    let bridge = Bridge::new(tg, client);
    bridge.set_allowed(args.allow);
    bridge.run().await
}
