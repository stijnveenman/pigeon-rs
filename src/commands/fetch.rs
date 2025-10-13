use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, identifier::Identifier, offset_selection::OffsetSelection};

#[derive(Debug, Serialize, Deserialize)]
pub struct Fetch {
    pub encoding: Encoding,
    pub timeout_ms: u64,
    pub topics: Vec<FetchTopic>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchTopic {
    pub identifier: Identifier,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchPartition {
    pub id: u64,
    pub offset: OffsetSelection,
}
