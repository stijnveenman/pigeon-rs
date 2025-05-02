use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::{Db, Rpc, Shutdown};
use crate::db;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Request {
    pub topic: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TopicDescription {
    pub topic: String,
    pub partitions: Vec<PartitionDescription>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PartitionDescription {
    pub partition_number: u64,
    pub current_offset: u64,
}

impl Rpc for Request {
    type Response = TopicDescription;

    fn to_request(self) -> super::Command {
        super::Command::DescribeTopic(self)
    }

    #[instrument(skip(self, db, _shutdown))]
    async fn apply(
        self,
        db: &mut Db,
        _shutdown: &mut Shutdown,
    ) -> Result<Self::Response, db::Error> {
        db.with_data_for_topic(&self.topic, |topic| TopicDescription {
            topic: self.topic.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|partition| PartitionDescription {
                    current_offset: partition.current_offset,
                    partition_number: partition.partition_number,
                })
                .collect(),
        })
    }
}
