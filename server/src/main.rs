use anyhow::Result;
use clap::Parser;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "pigeon", version, author, about = "Run pegon server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,

    #[arg(long, short, action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long, short, action = clap::ArgAction::Count)]
    quiet: u8,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let _cli = Cli::parse();

    tracing_subscriber::fmt().init();

    info!("logging");
    warn!("test");

    Ok(())
}
