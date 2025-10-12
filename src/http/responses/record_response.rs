use serde::{Deserialize, Serialize};
use std::str;

use crate::data::{record::Record, timestamp::Timestamp};

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordResponse {
    pub offset: u64,
    pub timestamp: Timestamp,
    pub key: String,
    pub value: String, // TODO: headers
}

impl From<Record> for RecordResponse {
    fn from(value: Record) -> Self {
        Self {
            offset: value.offset,
            // TODO: error handling
            key: str::from_utf8(&value.key).expect("from_utf8").to_owned(),
            value: str::from_utf8(&value.value).expect("from_utf8").to_owned(),
            timestamp: value.timestamp,
        }
    }
}
