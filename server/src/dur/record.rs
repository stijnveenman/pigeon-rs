use bytes::Bytes;
use shared::{
    data::{
        encoding::{self, Encoding},
        timestamp::Timestamp,
    },
    response::record_response::{HeaderResponse, RecordResponse},
};

#[derive(Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Bytes,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub timestamp: Timestamp,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<RecordHeader>,
    // pub crc: u32,
}

impl RecordHeader {
    pub fn size(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

impl Record {
    pub fn size(&self) -> usize {
        self.key.len()
            + self.value.len()
            + self.headers.iter().map(RecordHeader::size).sum::<usize>()
    }

    pub fn to_response(
        &self,
        encoding: &Encoding,
        topic_id: u64,
        partition_id: u64,
    ) -> Result<RecordResponse, encoding::Error> {
        Ok(RecordResponse {
            offset: self.offset,
            topic_id,
            partition_id,
            key: encoding.encode(&self.key)?,
            value: encoding.encode(&self.value)?,
            timestamp: self.timestamp,
            headers: self
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

#[cfg(test)]
impl Record {
    pub fn basic(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::basic_with_offset(0, key, value)
    }

    pub fn basic_with_offset(
        offset: u64,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            headers: vec![],
            offset,
            value: value.into().into(),
            key: key.into().into(),
            timestamp: Timestamp::now(),
        }
    }
}
