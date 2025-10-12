use base64::{prelude::BASE64_STANDARD, Engine};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::str::Utf8Error;
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub enum Encoding {
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "b64")]
    B64,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("UTF8 Decoding Error")]
    UTF8Decode(#[from] Utf8Error),
    #[error("Base64 decoding error")]
    B64Decode(#[from] base64::DecodeError),
}

impl Encoding {
    pub fn encode(&self, bytes: &Bytes) -> Result<String, Error> {
        Ok(match self {
            Encoding::Utf8 => std::str::from_utf8(bytes)?.to_string(),
            Encoding::B64 => BASE64_STANDARD.encode(bytes),
        })
    }

    pub fn decode(&self, value: &str) -> Result<Bytes, Error> {
        Ok(match self {
            Encoding::Utf8 => value.to_string().into(),
            Encoding::B64 => BASE64_STANDARD.decode(value)?.into(),
        })
    }
}
