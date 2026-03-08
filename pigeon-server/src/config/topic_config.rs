use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicConfig {
    pub default_partitions: u64,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            default_partitions: 1,
        }
    }
}
