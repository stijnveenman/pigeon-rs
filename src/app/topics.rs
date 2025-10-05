use crate::data::record::Record;
use crate::data::timestamp::Timestamp;
use crate::{commands::create_topic::CreateTopic, dur::topic::Topic};

use super::error::{Error, Result};
use super::AppLock;

impl AppLock {
    pub async fn create_topic(&mut self, command: CreateTopic) -> Result<u64> {
        // TODO: this will overwrite topoics if we delete topics in the middle
        // metadata topic starts at 0
        let topic_id = self.topics.len() as u64 + 1;

        let topic = Topic::load_from_disk(self.config.clone(), topic_id).await?;

        self.topics.insert(topic_id, topic);

        Ok(topic_id)
    }

    pub async fn produce(
        &mut self,
        topic_id: u64,
        partition_id: u64,
        record: Record,
    ) -> Result<u64> {
        let mut topic = self
            .topics
            .get_mut(&topic_id)
            .ok_or(Error::TopicIdNotFound(topic_id))?;

        let offset = topic.append(partition_id, record).await?;

        Ok(offset)
    }
}
