use clap::Parser;
use pigeon_rs::{server, DEFAULT_PORT};
use tokio::{net::TcpListener, signal};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{
    fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
    EnvFilter,
};

#[derive(Parser, Debug)]
#[command(name = "pigeon", version, author, about = "Run pegon server")]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
pub async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    let address = &format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(address).await?;

    info!("Starting listener on {}", address);

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

fn set_up_logging() -> Result<(), TryInitError> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    // Use the tracing subscriber `Registry`, or any other subscriber
    // that impls `LookupSpan`
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
