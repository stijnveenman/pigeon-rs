use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, identifier::Identifier};

#[derive(Serialize, Deserialize)]
pub struct Produce {
    pub topic: Identifier,
    pub partition_id: u64,
    pub key: String,
    pub value: String,
    pub encoding: Encoding, // TODO: support headers
}
