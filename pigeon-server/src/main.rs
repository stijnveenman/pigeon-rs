use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use pigeon_server::{ServerConfig, SystemContext, http};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "pigeon", version, author, about = "Run pegon server")]
struct Cli {
    #[arg(long, short, default_value = "server.toml")]
    config: String,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let config = ServerConfig::load_from_file(&cli.config);

    info!("Starting with ServerConfig {config:?}");

    let system = SystemContext::initialise(Arc::new(config)).await;

    http::serve(system).await?;

    Ok(())
}
