use serde::{Deserialize, Serialize};
use shared::data::{
    encoding::{self, Encoding},
    timestamp::Timestamp,
};
use std::str;

use crate::{commands::fetch::Fetch, data::record::Record};

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
    pub records: Vec<RecordResponse>,
}

impl From<&Fetch> for FetchResponse {
    fn from(value: &Fetch) -> Self {
        Self {
            total_size: 0,
            records: vec![],
            encoding: value.encoding,
        }
    }
}

impl FetchResponse {
    pub fn push(
        &mut self,
        record: &Record,
        topic_id: u64,
        partition_id: u64,
    ) -> Result<(), encoding::Error> {
        self.records.push(RecordResponse::from(
            record,
            &self.encoding,
            topic_id,
            partition_id,
        )?);
        self.total_size += record.size();

        Ok(())
    }
}

impl RecordResponse {
    pub fn from(
        value: &Record,
        encoding: &Encoding,
        topic_id: u64,
        partition_id: u64,
    ) -> Result<Self, encoding::Error> {
        Ok(Self {
            offset: value.offset,
            topic_id,
            partition_id,
            key: encoding.encode(&value.key)?,
            value: encoding.encode(&value.value)?,
            timestamp: value.timestamp,
            headers: value
                .headers
                .iter()
                .map(|header| {
                    Ok(HeaderResponse {
                        key: header.key.to_string(),
                        value: encoding.encode(&header.value)?,
                    })
                })
                .collect::<Result<_, encoding::Error>>()?,
        })
    }
}
