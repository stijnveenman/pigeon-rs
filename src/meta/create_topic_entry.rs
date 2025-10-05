use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateTopicEntry {
    pub topic_id: u64,
}
