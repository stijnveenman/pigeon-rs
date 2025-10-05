use crate::commands::produce::Produce;
use crate::data::record::Record;
use crate::data::timestamp::Timestamp;
use crate::{commands::create_topic::CreateTopic, dur::topic::Topic};

use super::error::{Error, Result};
use super::AppLock;

impl AppLock {
    pub async fn create_topic(&mut self, command: CreateTopic) -> Result<u64> {
        // TODO: this will overwrite topoics if we delete topics in the middle
        let topic_id = self.topics.len() as u64;

        let topic = Topic::load_from_disk(self.config.clone(), topic_id).await?;

        self.topics.insert(topic_id, topic);

        Ok(topic_id)
    }

    pub async fn produce(&mut self, command: Produce) -> Result<u64> {
        let mut topic = self
            .topics
            .get_mut(&command.topic_id)
            .ok_or(Error::TopicIdNotFound(command.topic_id))?;

        let offset = topic
            .append(
                command.partition_id,
                Record {
                    offset: 0,
                    timestamp: Timestamp::now(),
                    key: command.key,
                    value: command.value,
                    headers: vec![],
                },
            )
            .await?;

        Ok(offset)
    }
}
