use anyhow::Result;
use pigeon_rs::{client::HttpClient, logging::set_up_logging, DEFAULT_PORT};
use tracing::info;

#[tokio::main]
pub async fn main() -> Result<()> {
    set_up_logging(0, 0)?;

    let client = HttpClient::new(format!("http://127.0.0.1:{}", DEFAULT_PORT))?;

    let topic = client.get_topic("foo").await?;

    info!("{topic:#?}");

    Ok(())
}
