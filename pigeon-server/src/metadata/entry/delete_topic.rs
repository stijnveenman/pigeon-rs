use serde::{Deserialize, Serialize};

use crate::metadata::entry::MetadataEntry;

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteTopicEntry {
    pub topic_name: String,
}

impl From<DeleteTopicEntry> for MetadataEntry {
    fn from(val: DeleteTopicEntry) -> Self {
        MetadataEntry::DeleteTopic(val)
    }
}
