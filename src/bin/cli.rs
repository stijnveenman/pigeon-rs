use pigeon_rs::{logging::set_up_logging, Client};
use tracing::info;

#[tokio::main]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let mut client = match Client::connect("localhost:6394").await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };

    let response = client.create_topic("hello world").await.unwrap();

    info!(?response);

    Ok(())
}
