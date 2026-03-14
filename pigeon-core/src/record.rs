use std::string::FromUtf8Error;

use serde::{Deserialize, Serialize};

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

    pub fn value_string(&self) -> Result<String, FromUtf8Error> {
        String::from_utf8(self.value.clone())
    }

    pub fn is_commited(&self) -> bool {
        self.offset != 0
    }
}

mod base64_vec {
    use base64::{Engine, prelude::BASE64_STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(buffer: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let b64 = BASE64_STANDARD.encode(buffer);
        serializer.serialize_str(&b64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let buffer = BASE64_STANDARD
            .decode(s)
            .map_err(serde::de::Error::custom)?;
        Ok(buffer)
    }
}
