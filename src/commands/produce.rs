use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ProduceString {
    pub topic_id: u64,
    pub partition_id: u64,
    pub key: String,
    pub value: String,
    // TODO: support headers
}
