use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionState {
    pub partition_id: u64,
    pub current_offset: u64,
    pub segment_count: usize,
}
