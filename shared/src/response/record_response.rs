use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, timestamp::Timestamp};

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordResponse {
    pub topic_id: u64,
    pub partition_id: u64,
    pub offset: u64,
    pub timestamp: Timestamp,
    pub key: String,
    pub value: String,
    pub headers: Vec<HeaderResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeaderResponse {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FetchResponse {
    pub total_size: usize,
    pub encoding: Encoding,
    pub count: usize,
    pub records: Vec<RecordResponse>,
}

impl FetchResponse {
    pub fn push(&mut self, record: RecordResponse, record_size: usize) {
        self.records.push(record);
        self.total_size += record_size;
    }
}
