// TODO: remove
#![allow(unused)]
use std::future::Future;
use std::sync::Arc;

use crate::config::Config;
use crate::data::record::Record;
use crate::dur::error::{Error, Result};

use super::partition::Partition;

pub struct Topic {
    topic_id: u64,
    config: Arc<Config>,

    partitions: Vec<Partition>,
}

impl Topic {
    pub async fn load_from_disk(config: Arc<Config>, topic_id: u64) -> Result<Self> {
        let mut partitions = Vec::with_capacity(config.topic.num_partitions as usize);
        for partition_id in (0..config.topic.num_partitions) {
            let partition =
                Partition::load_from_disk(config.clone(), topic_id, partition_id).await?;
            partitions.push(partition);
        }

        Ok(Self {
            topic_id,
            config,
            partitions,
        })
    }

    pub async fn append(&mut self, partition_id: u64, record: Record) -> Result<u64> {
        let partition = self
            .partitions
            .get_mut(partition_id as usize)
            .ok_or(Error::PartitionNotFound)?;

        partition.append(record).await
    }

    pub async fn read_exact(&self, partition_id: u64, offset: u64) -> Result<Record> {
        let partition = self
            .partitions
            .get(partition_id as usize)
            .ok_or(Error::PartitionNotFound)?;

        partition.read_exact(offset).await
    }
}

#[cfg(test)]
mod test {
    use super::Topic;
    use std::{fs::create_dir_all, path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        config::{self, Config},
        data::{record::Record, timestamp::Timestamp},
    };

    //  TODO: move into record behind cfg(test)
    fn basic_record(key: &str, value: &str) -> Record {
        Record {
            headers: vec![],
            offset: 0,
            value: value.to_string().into(),
            key: key.to_string().into(),
            timestamp: Timestamp::now(),
        }
    }

    // TODO: move into config
    fn create_config() -> (tempfile::TempDir, Arc<config::Config>) {
        let dir = tempdir().expect("failed to create tempdir");

        let config = Config {
            path: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let partition_path = config.partition_path(0, 0);

        create_dir_all(Path::new(&partition_path)).expect("failed to create partition_path");

        (dir, Arc::new(config))
    }

    #[tokio::test]
    async fn topic_basic_read_write() {
        let (_dir, config) = create_config();

        let mut topic = Topic::load_from_disk(config, 0)
            .await
            .expect("Failed to create topic");

        let record = basic_record("foo", "bar");
        let offset = topic
            .append(0, record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 0);

        let read_record = topic.read_exact(0, 0).await.expect("Failed to read record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar");
        assert_eq!(read_record.offset, 0);
    }
}
