use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTopic {
    pub topic_name: String,
    pub num_partitions: Option<u64>,
}
