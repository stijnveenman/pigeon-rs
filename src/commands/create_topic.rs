use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CreateTopic {
    pub topic_id: Option<u64>,
    pub name: String,
}
