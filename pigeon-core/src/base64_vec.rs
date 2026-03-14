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
