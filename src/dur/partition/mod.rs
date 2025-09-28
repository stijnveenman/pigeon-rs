// TODO: remove
#![allow(unused)]
use std::{collections::BTreeMap, ops::Bound, path::Path, sync::Arc};

use tokio::fs::{self, create_dir_all};

use super::{error::Result, segment::Segment};
use crate::{config::Config, data::record::Record, dur::error::Error};

pub struct Partition {
    topic_id: u64,
    partition_id: u64,
    config: Arc<Config>,

    next_offset: u64,
    segments: BTreeMap<u64, Segment>,
}

async fn load_segments_form_disk(
    config: &Config,
    topic_id: u64,
    partition_id: u64,
    dir: &str,
) -> Result<BTreeMap<u64, Segment>> {
    let mut btree = BTreeMap::new();

    let mut stream = fs::read_dir(dir).await?;
    while let Some(entry) = stream.next_entry().await? {
        if entry.path().extension().is_none_or(|s| s != "log") {
            continue;
        }

        // TODO: error handling
        let start_offset = entry
            .path()
            .file_stem()
            .expect("Log file has invalid file name format")
            .to_str()
            .expect("start_offset string conversion invalid")
            .parse::<u64>()
            .expect("start_offset of log file is invalid");

        let segment = Segment::load_from_disk(config, topic_id, partition_id, start_offset).await?;

        btree.insert(start_offset, segment);
    }

    if btree.is_empty() {
        btree.insert(
            0,
            Segment::load_from_disk(config, topic_id, partition_id, 0).await?,
        );
    }

    Ok(btree)
}

impl Partition {
    pub async fn load_from_disk(
        config: Arc<Config>,
        topic_id: u64,
        partition_id: u64,
    ) -> Result<Self> {
        let partition_path = config.partition_path(topic_id, partition_id);

        create_dir_all(Path::new(&partition_path)).await?;

        let segments =
            load_segments_form_disk(&config, topic_id, partition_id, &partition_path).await?;

        let mut next_offset = 0;
        let mut cursos = segments.upper_bound(Bound::Unbounded);
        while let Some(segment) = cursos.prev() {
            if let Some(offset) = segment.1.max_offset() {
                next_offset = offset + 1;
                break;
            }
        }

        Ok(Self {
            partition_id,
            topic_id,
            config,

            next_offset,
            segments,
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
    use crate::dur::{partition::Partition, segment};
    use std::{fs::create_dir_all, path::Path, sync::Arc};

    use tempfile::tempdir;

    use crate::{
        config::{self, Config},
        data::{record::Record, timestamp::Timestamp},
    };

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
        let config = Arc::new(config);

        let mut partition = Partition::load_from_disk(config, 0, 0)
            .await
            .expect("Failed to load partition");

        let record = Record::basic("foo", "bar");
        let offset = partition
            .append(record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 0);

        let record = Record::basic("foo", "bar2");
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

    #[tokio::test]
    async fn partition_ocntinue_on_existing() {
        let (_dir, config) = create_config();
        let config = Arc::new(config);

        let mut partition = Partition::load_from_disk(config.clone(), 0, 0)
            .await
            .expect("Failed to load partition");

        let record = Record::basic("foo", "bar");
        let offset = partition
            .append(record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 0);

        drop(partition);

        let mut partition = Partition::load_from_disk(config, 0, 0)
            .await
            .expect("Failed to load partition");

        let record = Record::basic("foo", "bar2");
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

    #[tokio::test]
    async fn partition_multiple_segments() {
        let (_dir, mut config) = create_config();
        config.segment.size = 1;
        let config = Arc::new(config);

        let mut partition = Partition::load_from_disk(config.clone(), 0, 0)
            .await
            .expect("Failed to load partition");

        let record = Record::basic("foo", "bar");
        let offset = partition
            .append(record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 0);

        let record = Record::basic("foo", "bar2");
        let offset = partition
            .append(record)
            .await
            .expect("Failed to append record");
        assert_eq!(offset, 1);

        assert_eq!(partition.segments.len(), 2);

        let read_record = partition
            .read_exact(0)
            .await
            .expect("Failed to read record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar");
        assert_eq!(read_record.offset, 0);
    }
}
