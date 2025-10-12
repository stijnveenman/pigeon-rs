use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{debug, warn};
use tracing_subscriber::layer::Identity;

use crate::data::identifier::Identifier;
use crate::data::offset_selection::OffsetSelection;
use crate::data::record::{Record, RecordHeader};
use crate::data::state::topic_state::TopicState;
use crate::data::timestamp::Timestamp;
use crate::meta::create_topic_entry::CreateTopicEntry;
use crate::meta::MetadataEntry;
use crate::{commands::create_topic::CreateTopic, dur::topic::Topic};

use super::error::{Error, Result};
use super::AppLock;

impl AppLock {
    pub async fn create_topic(
        &mut self,
        topic_id: Option<u64>,
        name: &str,
        partition_count: Option<u64>,
    ) -> Result<u64> {
        let topic_id = match topic_id {
            Some(topic_id) => topic_id,
            None => loop {
                let topic_id = self.next_topic_id;
                self.next_topic_id += 1;
                if !self.topics.contains_key(&topic_id) {
                    break topic_id;
                }

                if topic_id == u64::MAX {
                    return Err(Error::MaxTopicIdReached);
                }
            },
        };

        if self.topics.contains_key(&topic_id) {
            return Err(Error::TopicIdInUse(topic_id));
        }

        if self.topic_ids.contains_key(name) {
            return Err(Error::TopicNameInUse(name.to_string()));
        }

        let partition_count = partition_count.unwrap_or(self.config.topic.num_partitions);

        debug!("Creating topic with topic_id: {topic_id} and name {name}");
        let topic =
            Topic::load_from_disk(self.config.clone(), topic_id, name, partition_count).await?;

        self.topics.insert(topic_id, topic);
        self.topic_ids.insert(name.to_string(), topic_id);

        self.append_metadata(MetadataEntry::CreateTopic(CreateTopicEntry {
            topic_id,
            name: name.to_string(),
            partitions: partition_count,
        }))
        .await?;

        Ok(topic_id)
    }

    pub async fn read_exact(
        &self,
        identifer: &Identifier,
        partition_id: u64,
        offset: u64,
    ) -> Result<Record> {
        let topic = self.get_topic(identifer)?;

        Ok(topic.read_exact(partition_id, offset).await?)
    }

    pub async fn read(
        &self,
        identifer: &Identifier,
        partition_id: u64,
        offset: &OffsetSelection,
    ) -> Result<Record> {
        let topic = self.get_topic(identifer)?;

        Ok(topic.read(partition_id, offset).await?)
    }

    pub fn get_topic(&self, identifer: &Identifier) -> Result<&Topic> {
        match identifer {
            Identifier::Name(name) => self.get_topic_by_name(name),
            Identifier::Id(topic_id) => self.get_topic_by_id(*topic_id),
        }
    }

    pub fn get_topic_mut(&mut self, identifer: &Identifier) -> Result<&mut Topic> {
        match identifer {
            Identifier::Name(name) => self.get_topic_by_name_mut(name),
            Identifier::Id(topic_id) => self.get_topic_by_id_mut(*topic_id),
        }
    }

    pub fn get_topic_by_id_mut(&mut self, topic_id: u64) -> Result<&mut Topic> {
        self.topics
            .get_mut(&topic_id)
            .ok_or(Error::TopicIdNotFound(topic_id))
            .inspect_err(|e| warn!("get_topic_mut {e}"))
    }

    pub fn get_topic_by_id(&self, topic_id: u64) -> Result<&Topic> {
        self.topics
            .get(&topic_id)
            .ok_or(Error::TopicIdNotFound(topic_id))
            .inspect_err(|e| warn!("get_topic {e}"))
    }

    pub fn get_topic_by_name(&self, name: &str) -> Result<&Topic> {
        self.topic_ids
            .get(name)
            .ok_or(Error::TopicNameNotFound(name.to_string()))
            .and_then(|topic_id| self.get_topic_by_id(*topic_id))
            .inspect_err(|e| warn!("get_topic_by_name {e}"))
    }

    pub fn get_topic_by_name_mut(&mut self, name: &str) -> Result<&mut Topic> {
        self.topic_ids
            .get(name)
            .ok_or(Error::TopicNameNotFound(name.to_string()))
            .cloned()
            .and_then(|topic_id| self.get_topic_by_id_mut(topic_id))
            .inspect_err(|e| warn!("get_topic_by_name {e}"))
    }

    pub async fn produce(
        &mut self,
        identifier: Identifier,
        partition_id: u64,
        key: Bytes,
        value: Bytes,
        headers: Vec<RecordHeader>,
    ) -> Result<u64> {
        let mut topic = self.get_topic_mut(&identifier)?;

        if topic.name().starts_with("__") {
            return Err(Error::InternalTopicName(topic.name().to_string()));
        }

        let record = topic
            .append(partition_id, key, value, headers)
            .await
            .inspect_err(|e| warn!("Produce error: {e}"))?;

        debug!("Appended record to {identifier} offset: {}", record.offset);

        let topic_id = topic.id();
        let offset = record.offset;
        let notify_count = self
            .listeners
            .get(&topic_id)
            .map(|sender| sender.send(Arc::new(record)).unwrap_or(0))
            .unwrap_or(0);

        debug!("Notified {notify_count} listeners for topic {topic_id}");

        Ok(offset)
    }

    pub fn subscribe(
        &mut self,
        identifer: &Identifier,
    ) -> Result<broadcast::Receiver<Arc<Record>>> {
        let topic_id = self.get_topic(identifer)?.id();

        let rx = match self.listeners.entry(topic_id) {
            Entry::Occupied(occupied_entry) => occupied_entry.get().subscribe(),
            Entry::Vacant(vacant_entry) => {
                let (tx, rx) = broadcast::channel(8);
                vacant_entry.insert(tx);
                rx
            }
        };

        Ok(rx)
    }

    pub fn topic_states(&self) -> HashMap<u64, TopicState> {
        self.topics
            .iter()
            .map(|entry| (*entry.0, entry.1.state()))
            .collect()
    }
}
