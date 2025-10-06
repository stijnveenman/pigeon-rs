use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTopicEntry {
    pub topic_id: u64,
}
