use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

use pigeon_rs::{client, fetch, logging::set_up_logging, DEFAULT_PORT};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), anyhow::Error> {
    set_up_logging(0, 0)?;

    let mut client = client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT)).await?;

    match client.create_topic("test", 3).await {
        Ok(_) => info!("Created topic 'test'"),
        Err(_) => warn!("Topic 'test' already exists"),
    }

    let task = tokio::spawn(async {
        info!("starting fetch");
        let mut client = client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT))
            .await
            .expect("failed to create client");

        let config = fetch::Request {
            timeout_ms: 1000,
            topics: vec![fetch::TopicsRequest {
                topic: "test".into(),
                partitions: vec![
                    fetch::PartitionRequest {
                        partition: 0,
                        offset: 0,
                    },
                    fetch::PartitionRequest {
                        partition: 1,
                        offset: 0,
                    },
                    fetch::PartitionRequest {
                        partition: 2,
                        offset: 0,
                    },
                ],
            }],
        };
        let fetch = client.fetch(config).await;

        match fetch {
            Ok(Some(message)) => info!("Received message {:?}", message),
            Ok(None) => info!("Did not receive message"),
            Err(e) => error!("Error receiving message {}", e),
        }
    });

    time::sleep(Duration::from_secs(5)).await;

    client
        .produce("test", "hello", "world")
        .await
        .expect("failed to produce message");

    task.await.expect("fetch task failed");
    Ok(())
}
