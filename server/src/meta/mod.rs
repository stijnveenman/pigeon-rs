pub mod create_topic_entry;
pub mod delete_topic_entry;
use core::str;

use std::collections::HashMap;

use create_topic_entry::CreateTopicEntry;
use delete_topic_entry::DeleteTopicEntry;
use serde::{Deserialize, Serialize};

use crate::dur::record::Record;

#[derive(Serialize, Deserialize, Debug)]
pub enum MetadataEntry {
    CreateTopic(CreateTopicEntry),
    DeleteTopic(DeleteTopicEntry),
}

#[derive(Default, Debug)]
pub struct Metadata {
    pub topics: HashMap<u64, TopicMetadata>,
}

#[derive(Debug)]
pub struct TopicMetadata {
    pub topic_id: u64,
    pub name: String,
    pub partitions: u64,
}

impl Metadata {
    pub fn from_records(records: Vec<Record>) -> Self {
        let mut metadata = Metadata::default();

        for record in records {
            let value = str::from_utf8(&record.value).expect("Invalid UTF8 on metadata topic");

            let entry = serde_json::from_str::<MetadataEntry>(value)
                .expect("Invalid JSON on metadata topic");

            match entry {
                MetadataEntry::CreateTopic(entry) => {
                    metadata.topics.insert(entry.topic_id, TopicMetadata {
                        topic_id: entry.topic_id,
                        name: entry.name,
                        partitions: entry.partitions,
                    });
                }
                MetadataEntry::DeleteTopic(entry) => {
                    metadata.topics.remove(&entry.topic_id);
                }
            }
        }

        metadata
    }
}
