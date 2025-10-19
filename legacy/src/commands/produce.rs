use serde::{Deserialize, Serialize};
use shared::encoding::Encoding;

use crate::data::identifier::Identifier;

#[derive(Serialize, Deserialize)]
pub struct Produce {
    pub topic: Identifier,
    pub partition_id: u64,
    pub key: String,
    pub value: String,
    pub encoding: Encoding,
    pub headers: Option<Vec<ProduceHeader>>,
}

#[derive(Serialize, Deserialize)]
pub struct ProduceHeader {
    pub key: String,
    pub value: String,
}
