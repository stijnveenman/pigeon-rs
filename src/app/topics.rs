use crate::{commands::create_topic::CreateTopic, dur::topic::Topic};

use super::error::Result;
use super::AppLock;

impl AppLock {
    pub async fn create_topic(&mut self, command: CreateTopic) -> Result<u64> {
        let topic_id = self.topics.len() as u64;

        let topic = Topic::load_from_disk(self.config.clone(), topic_id).await?;

        self.topics.insert(topic_id, topic);

        Ok(topic_id)
    }
}
