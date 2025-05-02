use anyhow::Result;
use clap::Parser;
use pigeon_rs::{logging::set_up_logging, server, DEFAULT_PORT};
use tokio::{net::TcpListener, signal};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "pigeon", version, author, about = "Run pegon server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    let address = &format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(address).await?;

    info!("Starting listener on {}", address);

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}
