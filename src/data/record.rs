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
