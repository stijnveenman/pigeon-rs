use tracing::{debug, warn};

use crate::data::record::Record;
use crate::data::timestamp::Timestamp;
use crate::meta::create_topic_entry::CreateTopicEntry;
use crate::meta::MetadataEntry;
use crate::{commands::create_topic::CreateTopic, dur::topic::Topic};

use super::error::{Error, Result};
use super::AppLock;

impl AppLock {
    pub async fn create_topic(&mut self, topic_id: Option<u64>, name: &str) -> Result<u64> {
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

        debug!("Creating topic with topic_id: {topic_id} and name {name}");
        let topic = Topic::load_from_disk(self.config.clone(), topic_id, name).await?;

        self.topics.insert(topic_id, topic);
        self.topic_ids.insert(name.to_string(), topic_id);

        self.append_metadata(MetadataEntry::CreateTopic(CreateTopicEntry {
            topic_id,
            name: name.to_string(),
        }))
        .await?;

        Ok(topic_id)
    }

    pub fn get_topic_mut(&mut self, topic_id: u64) -> Result<&mut Topic> {
        self.topics
            .get_mut(&topic_id)
            .ok_or(Error::TopicIdNotFound(topic_id))
            .inspect_err(|e| warn!("get_topic_mut {e}"))
    }

    pub async fn produce(
        &mut self,
        topic_id: u64,
        partition_id: u64,
        record: Record,
    ) -> Result<u64> {
        let mut topic = self.get_topic_mut(topic_id)?;

        let offset = topic
            .append(partition_id, record)
            .await
            .inspect_err(|e| warn!("Produce error: {e}"))?;

        debug!("Appended record to topic_id: {topic_id} offset: {offset}",);

        Ok(offset)
    }
}
