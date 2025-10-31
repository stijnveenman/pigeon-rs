use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use bytes::Bytes;
use shared::data::identifier::Identifier;
use shared::data::offset_selection::OffsetSelection;
use shared::state::topic_state::TopicState;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::dur::record::{Record, RecordHeader};
use crate::dur::topic::Topic;
use crate::meta::MetadataEntry;
use crate::meta::create_topic_entry::CreateTopicEntry;
use crate::meta::delete_topic_entry::DeleteTopicEntry;
use crate::record_batch::RecordBatch;

use super::AppLock;
use super::error::{Error, Result};

impl AppLock {
    pub(super) async fn create_topic_internal(
        &mut self,
        topic_id: Option<u64>,
        name: &str,
        partition_count: Option<u64>,
    ) -> Result<u64> {
        if name.is_empty() {
            return Err(Error::InvalidName(name.to_string()));
        }

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

        info!("Creating topic with topic_id: {topic_id} and name {name}");
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

    pub async fn create_topic(
        &mut self,
        topic_id: Option<u64>,
        name: &str,
        partition_count: Option<u64>,
    ) -> Result<u64> {
        if name.starts_with("__") {
            return Err(Error::ReservedTopicName);
        }

        self.create_topic_internal(topic_id, name, partition_count)
            .await
    }

    pub async fn delete_topic(&mut self, identifer: &Identifier) -> Result<()> {
        let topic = self.get_topic(identifer)?;

        if topic.is_internal() {
            return Err(Error::InternalTopicName(topic.name().to_string()));
        }

        let topic_id = topic.id();
        let topic_name = topic.name().to_string();

        info!("Deleting topic with topic_id: {topic_id}");

        self.append_metadata(MetadataEntry::DeleteTopic(DeleteTopicEntry { topic_id }))
            .await?;

        self.topic_ids.remove(&topic_name);
        if let Some(topic) = self.topics.remove(&topic_id) {
            topic.delete().await?;
        }

        Ok(())
    }

    pub async fn read_exact(
        &self,
        identifer: &Identifier,
        partition_id: u64,
        offset: u64,
    ) -> Result<Option<Record>> {
        let topic = self.get_topic(identifer)?;

        Ok(topic.read_exact(partition_id, offset).await?)
    }

    pub async fn read_batch(
        &self,
        batch: &mut RecordBatch,
        offset: &OffsetSelection,
        partition_id: u64,
        identifier: &Identifier,
    ) -> Result<()> {
        let topic = self.get_topic(identifier)?;

        Ok(topic.read_batch(batch, offset, partition_id).await?)
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
        let topic = self.get_topic_mut(&identifier)?;

        if topic.is_internal() {
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
            .map(|sender| sender.send((partition_id, Arc::new(record))).unwrap_or(0))
            .unwrap_or(0);

        debug!("Notified {notify_count} listeners for topic {topic_id}");

        Ok(offset)
    }

    pub fn subscribe(
        &mut self,
        identifer: &Identifier,
    ) -> Result<broadcast::Receiver<(u64, Arc<Record>)>> {
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
