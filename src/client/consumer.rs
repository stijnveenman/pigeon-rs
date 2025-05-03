use async_stream::stream;
use tokio_stream::Stream;

use crate::{fetch, Message};

use super::Client;

struct PartitionState {
    partition: u64,
    current_offset: u64,
}

pub struct Consumer {
    client: Client,
    topic: String,
    partitions: Vec<PartitionState>,
}

impl Consumer {
    pub async fn consume(mut client: Client, topic: String) -> Result<Consumer, super::Error> {
        let topic_description = client.describe_topic(topic).await?;

        assert_eq!(
            topic_description.partitions.len(),
            1,
            "Only single partitions topics are supported yet"
        );

        let consumer = Consumer {
            client,
            topic: topic_description.topic,
            partitions: topic_description
                .partitions
                .into_iter()
                .map(|p| PartitionState {
                    partition: p.partition_number,
                    current_offset: p.current_offset,
                })
                .collect(),
        };

        Ok(consumer)
    }

    pub async fn next_message(&mut self) -> Result<Message, super::Error> {
        loop {
            let request = fetch::Request {
                timeout_ms: 1000,
                topics: vec![fetch::TopicsRequest {
                    topic: self.topic.clone(),
                    partitions: self
                        .partitions
                        .iter()
                        .map(|p| fetch::PartitionRequest {
                            offset: p.current_offset,
                            partition: p.partition,
                        })
                        .collect(),
                }],
            };

            if let Some(message) = self.client.fetch(request).await? {
                let partition = self.partitions.iter_mut().next().unwrap();
                partition.current_offset += 1;

                return Ok(message);
            }
        }
    }

    pub fn into_stream(mut self) -> impl Stream<Item = Message> {
        stream! {
            while let Ok(message) = self.next_message().await {
                yield message;
            }
        }
    }
}
