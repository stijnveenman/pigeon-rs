use serde::Serialize;

use crate::record::Record;

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct UncommitedRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl UncommitedRecord {
    pub fn new(key: &str, value: &str) -> Self {
        Self {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }
}

impl UncommitedRecord {
    pub fn to_record(self, offset: u64) -> Record {
        Record {
            offset,
            key: self.key,
            value: self.value,
        }
    }
}
