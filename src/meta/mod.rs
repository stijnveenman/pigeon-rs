pub mod create_topic_entry;

use create_topic_entry::CreateTopicEntry;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum MetadataEntry {
    CreateTopic(CreateTopicEntry),
}
