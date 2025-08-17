use bytes::Bytes;

use super::timestamp::Timestamp;

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
