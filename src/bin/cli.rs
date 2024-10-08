use pigeon_rs::{logging::set_up_logging, Client};
use tracing::info;

#[tokio::main]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let mut client = match Client::connect("localhost:6394").await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };

    let pong = client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
    info!(?pong);

    let result = client.create_topic("topic".into(), 5).await.unwrap();
    assert_eq!(b"OK", &result[..]);
    info!(?result);

    Ok(())
}
