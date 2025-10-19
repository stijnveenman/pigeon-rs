use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, identifier::Identifier, offset_selection::OffsetSelection};

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchCommand {
    pub encoding: Encoding,
    pub timeout_ms: u64,
    pub min_bytes: usize,
    pub topics: Vec<FetchTopicCommand>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTopicCommand {
    pub identifier: Identifier,
    pub partitions: Vec<FetchPartitionCommand>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchPartitionCommand {
    pub id: u64,
    pub offset: OffsetSelection,
}
