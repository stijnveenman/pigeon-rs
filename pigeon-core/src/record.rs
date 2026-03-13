use std::string::FromUtf8Error;

use serde::Serialize;

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Record {
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(offset: u64, key: &str, value: &str) -> Record {
        Record {
            offset,
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    pub fn value_string(&self) -> Result<String, FromUtf8Error> {
        String::from_utf8(self.value.clone())
    }
}
