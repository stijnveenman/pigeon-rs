// Placeholder file, rust-analyzer does not work in the `examples` folder. So we can copy an
// example here to get autocomplete

use tracing::{info, warn};

use crate::{logging::set_up_logging, Client, DEFAULT_PORT};

#[tokio::main(flavor = "current_thread")]
async fn main() -> crate::Result<()> {
    set_up_logging()?;

    let mut client = Client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT)).await?;

    match client.create_topic("test".into(), 3).await {
        Ok(_) => info!("Created topic 'test'"),
        Err(_) => warn!("Topic 'test' already exists"),
    }

    Ok(())
}
