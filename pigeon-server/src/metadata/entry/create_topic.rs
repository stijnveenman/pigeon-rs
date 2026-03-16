use serde::{Deserialize, Serialize};

use crate::metadata::entry::MetadataEntry;

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTopicEntry {
    pub topic_name: String,
    pub num_partitions: u64,
}

impl From<CreateTopicEntry> for MetadataEntry {
    fn from(val: CreateTopicEntry) -> Self {
        MetadataEntry::CreateTopic(val)
    }
}
