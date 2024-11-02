use std::time::Duration;
use tracing::{error, info, warn};

use pigeon_rs::{
    logging::set_up_logging, Client, FetchConfig, FetchPartitionConfig, FetchTopicConfig,
    DEFAULT_PORT,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> pigeon_rs::Result<()> {
    set_up_logging()?;

    let mut client = Client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT)).await?;

    match client.create_topic("test".into(), 3).await {
        Ok(_) => info!("Created topic 'test'"),
        Err(_) => warn!("Topic 'test' already exists"),
    }

    let task = tokio::spawn(async {
        info!("starting fetch");
        let mut client = Client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT))
            .await
            .expect("failed to create client");

        let config = FetchConfig {
            timeout_ms: 1000,
            topics: vec![FetchTopicConfig {
                topic: "test".into(),
                partitions: vec![
                    FetchPartitionConfig {
                        partition: 0,
                        offset: 0,
                    },
                    FetchPartitionConfig {
                        partition: 1,
                        offset: 0,
                    },
                    FetchPartitionConfig {
                        partition: 2,
                        offset: 0,
                    },
                ],
            }],
        };
        let fetch = client.cfetch(config).await;

        match fetch {
            Ok(Some(message)) => info!("Received message {:?}", message),
            Ok(None) => info!("Did not receive message"),
            Err(e) => error!("Error receiving message {}", e),
        }
    });

    let mut interval = tokio::time::interval(Duration::from_secs(5));
    interval.tick().await;
    interval.tick().await;

    client
        .produce("test".into(), "hello".into(), "world".into())
        .await
        .expect("failed to produce message");

    task.await.expect("fetch task failed");
    Ok(())
}
