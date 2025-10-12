use std::sync::Arc;

use bytes::Bytes;

use crate::config::Config;
use crate::data::record::{Record, RecordHeader};
use crate::data::state::topic_state::TopicState;
use crate::dur::error::{Error, Result};

use super::partition::Partition;

pub struct Topic {
    topic_id: u64,
    name: String,

    pub(super) partitions: Vec<Partition>,
}

impl Topic {
    pub async fn load_from_disk(
        config: Arc<Config>,
        topic_id: u64,
        name: &str,
        partition_count: u64,
    ) -> Result<Self> {
        let mut partitions = Vec::with_capacity(partition_count as usize);
        for partition_id in 0..partition_count {
            let partition =
                Partition::load_from_disk(config.clone(), topic_id, partition_id).await?;
            partitions.push(partition);
        }

        Ok(Self {
            topic_id,
            name: name.to_string(),
            partitions,
        })
    }

    pub async fn read_all_from_partition(&mut self, partition_id: u64) -> Result<Vec<Record>> {
        // TODO: we should batch this as a single read for multiple messages
        let partition = self
            .partitions
            .get(partition_id as usize)
            .ok_or(Error::PartitionNotFound)?;

        let min_offset = partition.min_offset().unwrap_or(0);
        let max_offset = partition.max_offset().unwrap_or(0);

        let mut v = vec![];
        for offset in min_offset..=max_offset {
            match partition.read_exact(offset).await {
                Ok(record) => v.push(record),
                Err(Error::OffsetNotFound) => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(v)
    }

    pub async fn append(
        &mut self,
        partition_id: u64,
        key: Bytes,
        value: Bytes,
        headers: Vec<RecordHeader>,
    ) -> Result<Record> {
        let partition = self
            .partitions
            .get_mut(partition_id as usize)
            .ok_or(Error::PartitionNotFound)?;

        partition.append(key, value, headers).await
    }

    pub async fn read_exact(&self, partition_id: u64, offset: u64) -> Result<Record> {
        let partition = self
            .partitions
            .get(partition_id as usize)
            .ok_or(Error::PartitionNotFound)?;

        partition.read_exact(offset).await
    }

    pub fn state(&self) -> TopicState {
        TopicState {
            name: self.name.to_string(),
            topic_id: self.topic_id,
            partitions: self
                .partitions
                .iter()
                .map(|partition| partition.state())
                .collect(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> u64 {
        self.topic_id
    }
}

#[cfg(test)]
mod test {
    use super::Topic;
    use std::sync::Arc;

    use crate::{config::Config, dur::error::Error};

    #[tokio::test]
    async fn topic_basic_read_write() {
        let config = Arc::new(Config::default());

        let mut topic = Topic::load_from_disk(config, 0, "foo", 10)
            .await
            .expect("Failed to create topic");

        let record = topic
            .append(0, "foo".into(), "bar".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 0);

        let read_record = topic.read_exact(0, 0).await.expect("Failed to read record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar");
        assert_eq!(read_record.offset, 0);
    }

    #[tokio::test]
    async fn topic_continue_on_existing() {
        let config = Arc::new(Config::default());

        let mut topic = Topic::load_from_disk(config.clone(), 0, "foo", 10)
            .await
            .expect("Failed to create topic");

        let record = topic
            .append(0, "foo".into(), "bar".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 0);
        drop(topic);

        let topic = Topic::load_from_disk(config.clone(), 0, "foo", 10)
            .await
            .expect("Failed to create topic");

        let read_record = topic.read_exact(0, 0).await.expect("Failed to read record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar");
        assert_eq!(read_record.offset, 0);
    }

    #[tokio::test]
    async fn topic_multiple_partitions() {
        let config = Arc::new(Config::default());

        let mut topic = Topic::load_from_disk(config.clone(), 0, "foo", 10)
            .await
            .expect("Failed to create topic");

        let record = topic
            .append(0, "foo".into(), "bar".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 0);
        drop(topic);

        let topic = Topic::load_from_disk(config.clone(), 0, "foo", 10)
            .await
            .expect("Failed to create topic");

        let read_record = topic.read_exact(0, 0).await.expect("Failed to read record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar");
        assert_eq!(read_record.offset, 0);

        let read_record = topic.read_exact(1, 0).await;
        assert!(
            matches!(read_record, Err(Error::OffsetNotFound)),
            "expected read_record to be OffsetNotFound, got {:?}",
            read_record
        );

        let read_record = topic.read_exact(10, 0).await;
        assert!(
            matches!(read_record, Err(Error::PartitionNotFound)),
            "expected read_record to be PartitionNotFound, got {:?}",
            read_record
        );
    }
}
