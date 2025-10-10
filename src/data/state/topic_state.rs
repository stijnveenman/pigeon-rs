use serde::{Deserialize, Serialize};

use super::partition_state::PartitionState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicState {
    pub topic_id: u64,
    pub partitions: Vec<PartitionState>,
}
