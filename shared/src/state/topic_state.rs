use serde::{Deserialize, Serialize};

use super::partition_state::PartitionState;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TopicState {
    pub name: String,
    pub topic_id: u64,
    pub partitions: Vec<PartitionState>,
}
