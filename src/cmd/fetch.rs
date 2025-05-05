use std::{pin::Pin, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{select, time};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, instrument};

use crate::{db, Message};

use super::Rpc;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub timeout_ms: u64,
    pub topics: Vec<TopicsRequest>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TopicsRequest {
    pub topic: String,
    pub partitions: Vec<PartitionRequest>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PartitionRequest {
    pub partition: u64,
    pub offset: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MessageResponse {
    pub topic: String,
    pub partition: u64,
    pub message: Message,
}

impl Rpc for Request {
    type Response = Option<MessageResponse>;

    fn to_request(self) -> super::Command {
        super::Command::Fetch(self)
    }

    #[instrument(skip(self, ctx))]
    async fn apply(self, ctx: &mut super::RpcContext) -> Result<Self::Response, crate::db::Error> {
        self.fetch(ctx).await
    }
}

impl Request {
    async fn fetch(
        &self,
        ctx: &mut super::RpcContext,
    ) -> Result<Option<MessageResponse>, crate::db::Error> {
        let mut map = StreamMap::new();
        for topic in &self.topics {
            match topic.fetch(ctx).await? {
                Some(message) => {
                    return Ok(Some(MessageResponse {
                        partition: message.0,
                        message: message.1,
                        topic: topic.topic.clone(),
                    }))
                }
                None => {
                    for partition in &topic.partitions {
                        let rx = partition.fetch(ctx, &topic.topic)?;
                        map.insert((&topic.topic, partition.partition), rx);
                    }
                    continue;
                }
            }
        }

        select! {
            message = map.next() => {
                Ok(message.map(|m| MessageResponse{
                    partition: m.0.1,
                    topic: m.0.0.into(),
                    message: m.1

                }))
            },
            _ = time::sleep(Duration::from_millis(self.timeout_ms)) => {
                Ok(None)
            },
            _ = ctx.shutdown.recv() => {
                debug!("received shutdown signal waiting for fetch");
                Err(db::Error::ShuttingDown)
            }
        }
    }
}

impl TopicsRequest {
    async fn fetch(
        &self,
        ctx: &mut super::RpcContext,
    ) -> Result<Option<(u64, Message)>, crate::db::Error> {
        for partition in &self.partitions {
            if let Some(message) =
                ctx.db
                    .fetch(&self.topic, partition.partition, partition.offset)?
            {
                return Ok(Some((partition.partition, message)));
            }
        }

        Ok(None)
    }
}

impl PartitionRequest {
    fn fetch(
        &self,
        ctx: &mut super::RpcContext,
        topic: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Message> + Send>>, db::Error> {
        let mut rx = ctx.db.fetch_subscribe(topic, self.partition)?;
        let from_offset = self.offset;

        let rx = Box::pin(async_stream::stream! {
              while let Ok((offset, message)) = rx.recv().await {
                  if from_offset == 0 || offset > from_offset {
                      yield message;
                  }
              }
        }) as Pin<Box<dyn Stream<Item = Message> + Send>>;

        Ok(rx)
    }
}
