use tracing::{info, warn};

use pigeon_rs::{logging::set_up_logging, Client, DEFAULT_PORT};

#[tokio::main(flavor = "current_thread")]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let mut client = Client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT)).await?;

    match client.create_topic("test".into(), 3).await {
        Ok(_) => info!("Created topic 'test'"),
        Err(_) => warn!("Topic 'test' already exists"),
    }

    Ok(())
}
