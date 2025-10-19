use serde::{Deserialize, Serialize};

use crate::data::{encoding::Encoding, identifier::Identifier};

#[derive(Serialize, Deserialize)]
pub struct ProduceCommand {
    pub topic: Identifier,
    pub partition_id: u64,
    pub key: String,
    pub value: String,
    pub encoding: Encoding,
    pub headers: Option<Vec<ProduceHeaderCommand>>,
}

#[derive(Serialize, Deserialize)]
pub struct ProduceHeaderCommand {
    pub key: String,
    pub value: String,
}
