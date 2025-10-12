use serde::{Deserialize, Serialize};
use std::str;

use crate::data::{
    encoding::{self, Encoding},
    record::Record,
    timestamp::Timestamp,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct RecordResponse {
    pub offset: u64,
    pub timestamp: Timestamp,
    pub key: String,
    pub value: String,
    // TODO: headers
}

impl RecordResponse {
    pub fn from(value: Record, encoding: Encoding) -> Result<Self, encoding::Error> {
        Ok(Self {
            offset: value.offset,
            key: encoding.encode(value.key)?,
            value: encoding.encode(value.value)?,
            timestamp: value.timestamp,
        })
    }
}
