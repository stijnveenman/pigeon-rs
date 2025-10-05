use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateTopicResponse {
    pub topic_id: u64,
}
