use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hasher, SipHasher},
};

use bytes::Bytes;
use tracing::debug;

use super::{Db, DbErr, DbResult};

pub struct Topic {
    partitions: Vec<Partition>,
}

#[derive(Default)]
pub struct Partition {
    messages: BTreeMap<u64, Message>,
    current_offset: u64,
}

pub struct Message {
    key: Bytes,
    data: Bytes,
}

impl Topic {
    pub(crate) fn new(partitions: u64) -> Topic {
        Topic {
            partitions: (0..partitions).map(|_| Partition::default()).collect(),
        }
    }
}

impl Db {
    pub fn create_topic(&mut self, name: String, partitions: u64) -> DbResult<()> {
        let mut state = self.shared.lock().unwrap();

        if state.topics.contains_key(&name) {
            return Err(DbErr::NameInUse);
        }

        debug!("creating topic with name {}", &name);
        state.topics.insert(name, Topic::new(partitions));

        Ok(())
    }

    /// Produce a message on a given topic
    ///
    /// # Returns
    /// The offset of the message produces
    pub fn produce(&mut self, topic: String, key: Bytes, data: Bytes) -> DbResult<u64> {
        let mut state = self.shared.lock().unwrap();

        let topic = state.topics.get_mut(&topic);
        let Some(topic) = topic else {
            return Err(DbErr::NotFound);
        };

        let message = Message::new(key, data);

        let partition_key = message.hash() % topic.partitions.len() as u64;
        let partition = topic
            .partitions
            .get_mut(partition_key as usize)
            .expect("partition_key failed to produce a valid partition");

        let offset = partition.current_offset;
        partition.current_offset += 1;
        partition.messages.insert(offset, message);

        Ok(offset)
    }
}

impl Message {
    fn hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write(&self.key);
        hasher.finish()
    }

    fn new(key: Bytes, data: Bytes) -> Message {
        Message { key, data }
    }
}
