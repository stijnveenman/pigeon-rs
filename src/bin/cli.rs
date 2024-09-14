use pigeon_rs::Client;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
    EnvFilter,
};

#[tokio::main]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let mut client = match Client::connect("localhost:6394").await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };

    client.create_topic("hello world").await.unwrap();

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
