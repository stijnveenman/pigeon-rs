use std::{
    collections::{hash_map::Entry, BTreeMap},
    hash::{DefaultHasher, Hasher},
};

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::sync::broadcast;
use tracing::{debug, instrument};

use crate::db;

use super::{Db, DbResult};

#[derive(Debug)]
pub struct Topic {
    pub partitions: BTreeMap<u64, Partition>,
}

#[derive(Debug)]
pub struct Partition {
    pub messages: BTreeMap<u64, Message>,
    pub current_offset: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Message {
    pub key: ByteBuf,
    pub data: ByteBuf,
}

impl Topic {
    pub(crate) fn new(partitions: u64) -> Topic {
        Topic {
            partitions: (0..partitions)
                .map(|index| {
                    (
                        index,
                        Partition {
                            current_offset: 0,
                            messages: BTreeMap::default(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl Db {
    pub fn create_topic(&mut self, name: String, partitions: u64) -> DbResult<()> {
        let mut state = self.shared.lock().unwrap();

        if state.topics.contains_key(&name) {
            return Err(db::Error::NameInUse);
        }

        debug!("creating topic with name {}", &name);
        state.topics.insert(name, Topic::new(partitions));

        Ok(())
    }

    /// Produce a message on a given topic
    ///
    /// Returns a tuple of (partition_key, offset)
    #[instrument(skip(self))]
    pub fn produce(
        &mut self,
        topic_name: &str,
        key: ByteBuf,
        data: ByteBuf,
    ) -> DbResult<(u64, u64)> {
        let mut state = self.shared.lock().unwrap();

        let topic = state.topics.get_mut(topic_name);
        let Some(topic) = topic else {
            return Err(db::Error::TopicNotFound);
        };

        let message = Message { key, data };

        let partition_key = message.hash() % topic.partitions.len() as u64;
        let partition = topic
            .partitions
            .get_mut(&partition_key)
            .expect("partition_key failed to produce a valid partition");

        let offset = partition.current_offset;
        debug!(?partition_key, ?offset);

        partition.current_offset += 1;
        partition.messages.insert(offset, message.clone());

        let count = state
            .fetches
            .get(&(topic_name.into(), partition_key))
            .map(|tx| tx.send((offset, message)).unwrap_or(0))
            .unwrap_or(0);

        debug!("notified {} fetch of a new message", count);

        Ok((partition_key, offset))
    }

    pub fn fetch(&mut self, topic: &str, partition: u64, offset: u64) -> DbResult<Option<Message>> {
        let state = self.shared.lock().unwrap();

        let topic = state.topics.get(topic);
        let Some(topic) = topic else {
            return Err(db::Error::TopicNotFound);
        };

        let partition = topic.partitions.get(&partition);
        let Some(partition) = partition else {
            return Err(db::Error::PartitionNotFound);
        };

        let message = partition.messages.get(&offset).cloned();
        Ok(message)
    }

    pub fn with_data_for_topic<T>(&self, topic: &str, f: impl FnOnce(&Topic) -> T) -> DbResult<T> {
        let state = &self.shared.lock().unwrap();

        let topic = state.topics.get(topic).ok_or(db::Error::TopicNotFound)?;

        Ok(f(topic))
    }
    pub fn fetch_subscribe(
        &mut self,
        topic: &str,
        partition: u64,
    ) -> DbResult<broadcast::Receiver<(u64, Message)>> {
        let mut state = self.shared.lock().unwrap();

        let key = (topic.to_string(), partition);

        let rx = match state.fetches.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(32);
                e.insert(tx);
                rx
            }
        };

        Ok(rx)
    }
}

impl Message {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(&self.key);
        hasher.finish()
    }
}
