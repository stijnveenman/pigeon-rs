use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateTopicCommand {
    pub topic_id: Option<u64>,
    pub name: String,
    pub partitions: Option<u64>,
}
