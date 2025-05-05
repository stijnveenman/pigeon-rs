use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::Rpc;

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

    #[instrument(skip(self, ctx))]
    async fn apply(self, ctx: &mut super::RpcContext) -> Result<Self::Response, crate::db::Error> {
        ctx.db
            .with_data_for_topic(&self.topic, |topic| TopicDescription {
                topic: self.topic.clone(),
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| PartitionDescription {
                        current_offset: partition.1.current_offset,
                        partition_number: *partition.0,
                    })
                    .collect(),
            })
    }
}
