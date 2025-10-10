use serde::{Deserialize, Serialize};

use crate::data::identifier::Identifier;

#[derive(Serialize, Deserialize)]
pub struct ProduceString {
    pub topic: Identifier,
    pub partition_id: u64,
    pub key: String,
    pub value: String,
    // TODO: support headers
}
