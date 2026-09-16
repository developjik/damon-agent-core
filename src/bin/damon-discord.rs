use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use damon_core::channel::Bridge;
use damon_core::client::DamonClient;
use damon_core::discord::{DiscordApi, DiscordChannel};

#[derive(Parser)]
#[command(name = "damon-discord", about = "Discord thin client for damond")]
struct Args {
    /// Daemon WS URL
    #[arg(long, default_value = "ws://127.0.0.1:9470/ws")]
    url: String,
    /// Daemon auth token (or DAMON_TOKEN)
    #[arg(long, env = "DAMON_TOKEN")]
    token: Option<String>,
    /// Discord bot token (or DISCORD_BOT_TOKEN)
    #[arg(long, env = "DISCORD_BOT_TOKEN")]
    bot_token: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "damon=info".into()),
        )
        .init();

    let args = Args::parse();
    let client = DamonClient::connect(&args.url, args.token.as_deref())
        .await
        .context("cannot connect to damond")?;
    let api = Arc::new(DiscordApi::new(&args.bot_token));
    Bridge::new(Arc::new(DiscordChannel::new(api)), client)
        .run()
        .await
}
