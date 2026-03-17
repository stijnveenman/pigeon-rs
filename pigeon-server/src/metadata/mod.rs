use std::collections::{HashMap, hash_map::Entry};

use pigeon_core::PError;
use tracing::info;

use crate::{
    config::ServerConfig, metadata::entry::MetadataEntry, systems::topic_system::TopicSystem,
};

pub mod entry;

pub struct TopicMetadata {
    pub topic_name: String,
    pub num_partitions: u64,
}

pub struct Metadata {
    topics: HashMap<String, TopicMetadata>,
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

    pub async fn initialise(config: &ServerConfig, topics: &TopicSystem) -> Self {
        let records = topics.read_range(".metadata", 0, 0u64..).await;
        info!("{records:?}");

        Self {
            topics: Default::default(),
        }
    }
}
