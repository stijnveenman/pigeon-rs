use std::{collections::BTreeMap, ops::Bound, path::Path};

use tokio::fs::create_dir_all;

use super::{error::Result, segment::Segment};
use crate::{config::Config, data::record::Record, dur::error::Error};

pub struct Partition {
    topic_id: u64,
    partition_id: u64,
    config: Config,

    next_offset: u64,
    segments: BTreeMap<u64, Segment>,
}

impl Partition {
    pub async fn load_from_disk(config: Config, topic_id: u64, partition_id: u64) -> Result<Self> {
        let partition_path = config.partition_path(topic_id, partition_id);

        create_dir_all(Path::new(&partition_path)).await?;

        // FIX: load_from_disk
        let start_segment = Segment::load_from_disk(&config, topic_id, partition_id, 0).await?;

        Ok(Self {
            partition_id,
            topic_id,
            config,

            // FIX: load_from_disk
            next_offset: 0,
            segments: BTreeMap::from([(0, start_segment)]),
        })
    }

    pub async fn read_exact(&self, offset: u64) -> Result<Record> {
        // We want to get the segment with the latest start offset before the offset

        let mut cursor = self.segments.lower_bound(Bound::Excluded(&offset));
        let segment = cursor.prev().ok_or(Error::OffsetNotFound)?.1;

        segment.read_exact(offset).await
    }

    pub async fn append(&mut self, mut record: Record) -> Result<u64> {
        if self
            .segments
            .last_entry()
            .expect("A partition should always have at least 1 segment")
            .get()
            .is_full()
        {
            self.segments.insert(
                self.next_offset,
                Segment::load_from_disk(
                    &self.config,
                    self.topic_id,
                    self.partition_id,
                    self.next_offset,
                )
                .await?,
            );
        }

        record.offset = self.next_offset;
        self.next_offset += 1;

        self.segments
            .last_entry()
            .unwrap()
            .get_mut()
            .append(&record)
            .await?;

        Ok(record.offset)
    }
}

#[cfg(test)]
mod test {
    use crate::dur::partition::Partition;
    use std::{fs::create_dir_all, path::Path};

    use tempfile::tempdir;

    use crate::{
        config::{self, Config},
        data::{record::Record, timestamp::Timestamp},
    };

    fn basic_record(key: &str, value: &str) -> Record {
        Record {
            headers: vec![],
            offset: 0,
            value: value.to_string().into(),
            key: key.to_string().into(),
            timestamp: Timestamp::now(),
        }
    }

    fn create_config() -> (tempfile::TempDir, config::Config) {
        let dir = tempdir().expect("failed to create tempdir");

        let config = Config {
            path: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let partition_path = config.partition_path(0, 0);

        create_dir_all(Path::new(&partition_path)).expect("failed to create partition_path");

        (dir, config)
    }

    #[tokio::test]
    async fn partition_basic_read_write() {
        let (_dir, config) = create_config();

        let mut partition = Partition::load_from_disk(config, 0, 0)
            .await
            .expect("Failed to load partition");

        let record = basic_record("foo", "bar");
        let offset = partition
            .append(record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 0);

        let record = basic_record("foo", "bar2");
        let offset = partition
            .append(record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 1);

        let read_record = partition
            .read_exact(1)
            .await
            .expect("Failed to read record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar2");
        assert_eq!(read_record.offset, 1);
    }
}
