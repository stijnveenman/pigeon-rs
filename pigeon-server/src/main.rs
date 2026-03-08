mod config;
mod dur;
mod systems;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use crate::config::ServerConfig;

#[derive(Parser, Debug)]
#[command(name = "pigeon", version, author, about = "Run pegon server")]
struct Cli {
    #[arg(long, short, default_value = "server.toml")]
    config: String,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let cli = Cli::parse();

    let config = ServerConfig::load_from_file(&cli.config);

    info!("Starting with ServerConfig {config:?}");

    Ok(())
}
