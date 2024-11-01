// Placeholder file, rust-analyzer does not work in the `examples` folder. So we can copy an
// example here to get autocomplete

use std::time::Duration;

use tracing::{error, info, warn};

use crate::{
    cmd::{FetchConfig, FetchPartitionConfig, FetchTopicConfig},
    logging::set_up_logging,
    Client, DEFAULT_PORT,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> crate::Result<()> {
    set_up_logging()?;

    let mut client = Client::connect(format!("{}:{}", "127.0.0.1", DEFAULT_PORT)).await?;

    match client.create_topic("test".into(), 3).await {
        Ok(_) => info!("Created topic 'test'"),
        Err(_) => warn!("Topic 'test' already exists"),
    }

    let task = tokio::spawn(async {
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

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.tick().await;

    client
        .produce("test".into(), "hello".into(), "world".into())
        .await
        .expect("failed to produce message");

    task.await.expect("fetch task failed");
    Ok(())
}
