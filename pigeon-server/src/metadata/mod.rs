use std::collections::{HashMap, hash_map::Entry};

use pigeon_core::{PError, record::Record};

use crate::metadata::entry::MetadataEntry;

pub mod entry;

#[derive(Debug)]
pub struct TopicMetadata {
    pub topic_name: String,
    pub num_partitions: u64,
}

#[derive(Debug)]
pub struct Metadata {
    pub topics: HashMap<String, TopicMetadata>,
}

impl Metadata {
    pub fn apply(&mut self, entry: &MetadataEntry) -> Result<(), PError> {
        match entry {
            MetadataEntry::CreateTopic(entry) => {
                match self.topics.entry(entry.topic_name.to_string()) {
                    Entry::Occupied(_) => return Err(PError::TopicAlreadyExists),
                    Entry::Vacant(vacant) => vacant.insert(TopicMetadata {
                        topic_name: entry.topic_name.to_string(),
                        num_partitions: entry.num_partitions,
                    }),
                }
            }
        };

        Ok(())
    }

    pub async fn initialise(records: &[Record]) -> Self {
        let mut metadata = Metadata {
            topics: Default::default(),
        };

        for record in records {
            let entry = record
                .json::<MetadataEntry>()
                .expect("Failed to deserialize MetadataEntry");

            metadata
                .apply(&entry)
                .expect("Failed to apply metadata state from disk");
        }

        if !metadata.topics.contains_key(".metadata") {
            metadata.topics.insert(
                ".metadata".to_string(),
                TopicMetadata {
                    topic_name: ".metadata".to_string(),
                    num_partitions: 1,
                },
            );
        }

        metadata
    }
}
