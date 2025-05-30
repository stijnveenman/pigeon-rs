use std::time::Duration;
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use pigeon_rs::{client, logging::set_up_logging, DEFAULT_PORT};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    set_up_logging(0, 0)?;

    let mut client = client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT)).await?;

    match client.create_topic("test", 5).await {
        Ok(_) => info!("Created topic 'test'"),
        Err(_) => warn!("Topic 'test' already exists"),
    }

    let task = tokio::spawn(async {
        info!("starting consumer");
        let client = client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT))
            .await
            .expect("failed to create client");

        let consumer = client::consumer(client, "test")
            .await
            .expect("failed to start consumer");
        let messages = consumer.into_stream();

        tokio::pin!(messages);

        while let Some(msg) = messages.next().await {
            println!(
                "{}:{}",
                String::from_utf8(msg.key.to_vec()).unwrap(),
                String::from_utf8(msg.data.to_vec()).unwrap()
            )
        }
    });

    time::sleep(Duration::from_secs(2)).await;

    for i in 0..50 {
        client
            .produce("test", format!("{}", i), format!("value: {}", i))
            .await
            .expect("failed to produce message");
    }

    task.await.expect("fetch task failed");
    Ok(())
}
