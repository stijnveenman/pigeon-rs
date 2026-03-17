use crate::base64_vec;
use std::string::FromUtf8Error;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub offset: u64,
    #[serde(with = "base64_vec")]
    pub key: Vec<u8>,
    #[serde(with = "base64_vec")]
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(key: &str, value: &str) -> Record {
        Record {
            offset: 0,
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    pub fn with_offset(offset: u64, key: &str, value: &str) -> Record {
        Record {
            offset,
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }

    pub fn key_text(&self) -> Result<String, FromUtf8Error> {
        String::from_utf8(self.value.clone())
    }

    pub fn text(&self) -> Result<String, FromUtf8Error> {
        String::from_utf8(self.value.clone())
    }

    pub fn json<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(&self.value)
    }

    pub fn is_commited(&self) -> bool {
        self.offset != 0
    }
}
