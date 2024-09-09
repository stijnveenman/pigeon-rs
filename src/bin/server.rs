use pigeon_rs::DEFAULT_PORT;
use tokio::net::TcpListener;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{
    fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
    EnvFilter,
};

#[tokio::main]
pub async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let port = DEFAULT_PORT;

    let address = &format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(address).await?;

    info!("Created listener on {}", address);

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
