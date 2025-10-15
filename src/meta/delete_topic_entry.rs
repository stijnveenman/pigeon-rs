use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteTopicEntry {
    pub topic_id: u64,
}
