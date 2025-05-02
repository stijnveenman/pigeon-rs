use std::{pin::Pin, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{select, time};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::debug;

use crate::{db, Message};

use super::{Rpc, Shutdown};

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

impl Rpc for Request {
    type Response = Option<Message>;

    fn to_request(self) -> super::Command {
        super::Command::Fetch(self)
    }

    async fn apply(
        self,
        db: &mut super::Db,
        shutdown: &mut Shutdown,
    ) -> Result<Self::Response, crate::db::Error> {
        self.fetch(db, shutdown).await
    }
}

impl Request {
    async fn fetch(
        &self,
        db: &mut super::Db,
        shutdown: &mut Shutdown,
    ) -> Result<Option<Message>, crate::db::Error> {
        let mut map = StreamMap::new();
        for topic in &self.topics {
            match topic.fetch(db).await? {
                Some(message) => return Ok(Some(message)),
                None => {
                    for partition in &topic.partitions {
                        let rx = partition.fetch(db, &topic.topic)?;
                        map.insert((&topic.topic, partition.partition), rx);
                    }
                    continue;
                }
            }
        }

        select! {
            message = map.next() => {
                Ok(message.map(|m| m.1))
            },
            _ = time::sleep(Duration::from_millis(self.timeout_ms)) => {
                Ok(None)
            },
            _ = shutdown.recv() => {
                debug!("received shutdown signal waiting for fetch");
                Err(db::Error::ShuttingDown)
            }
        }
    }
}

impl TopicsRequest {
    async fn fetch(&self, db: &mut super::Db) -> Result<Option<Message>, crate::db::Error> {
        for partition in &self.partitions {
            if let Some(message) = db.fetch(&self.topic, partition.partition, partition.offset)? {
                return Ok(Some(message));
            }
        }

        Ok(None)
    }
}

impl PartitionRequest {
    fn fetch(
        &self,
        db: &mut super::Db,
        topic: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Message> + Send>>, db::Error> {
        let mut rx = db.fetch_subscribe(topic, self.partition)?;
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
