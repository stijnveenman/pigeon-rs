use serde::{Deserialize, Serialize};

use crate::metadata::entry::{create_topic::CreateTopicEntry, delete_topic::DeleteTopicEntry};

pub mod create_topic;
pub mod delete_topic;

#[derive(Debug, Serialize, Deserialize)]
pub enum MetadataEntry {
    CreateTopic(CreateTopicEntry),
    DeleteTopic(DeleteTopicEntry),
}
