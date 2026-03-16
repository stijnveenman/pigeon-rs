use serde::{Deserialize, Serialize};

use crate::metadata::entry::create_topic::CreateTopicEntry;

pub mod create_topic;

#[derive(Debug, Serialize, Deserialize)]
pub enum MetadataEntry {
    CreateTopic(CreateTopicEntry),
}
