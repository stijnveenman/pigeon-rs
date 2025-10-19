use std::{collections::BTreeMap, ops::Bound, path::Path, sync::Arc};

use bytes::Bytes;
use shared::{
    data::{offset_selection::OffsetSelection, timestamp::Timestamp},
    state::partition_state::PartitionState,
};
use tokio::fs::{self, create_dir_all, remove_dir};

use super::{error::Result, segment::Segment};
use crate::{
    config::Config,
    data::record::{Record, RecordHeader},
    dur::error::Error,
};

pub struct Partition {
    topic_id: u64,
    partition_id: u64,
    config: Arc<Config>,

    next_offset: u64,
    pub(super) segments: BTreeMap<u64, Segment>,
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

        let start_offset = entry
            .path()
            .file_stem()
            .ok_or(Error::InvalidLogFilename(entry.file_name()))?
            .to_str()
            .ok_or(Error::InvalidLogFilename(entry.file_name()))?
            .parse::<u64>()
            .map_err(|_| Error::InvalidLogFilename(entry.file_name()))?;

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

    pub fn min_offset(&self) -> Option<u64> {
        self.segments.iter().find_map(|e| e.1.min_offset())
    }

    pub fn max_offset(&self) -> Option<u64> {
        self.segments.iter().rev().find_map(|e| e.1.max_offset())
    }

    pub async fn read_exact(&self, offset: u64) -> Result<Option<Record>> {
        // We want to get the segment with the latest start offset before the offset
        let mut cursor = self.segments.lower_bound(Bound::Excluded(&offset));
        let Some((_, segment)) = cursor.prev() else {
            return Ok(None);
        };

        segment.read_exact(offset).await
    }

    // TODO: unit test
    pub async fn read(&self, offset: &OffsetSelection) -> Result<Option<Record>> {
        // We want to get the segment with the latest start offset before the offset
        let mut cursor = self.segments.upper_bound(Bound::Unbounded);

        while let Some((_, segment)) = cursor.prev() {
            let mut range = match offset {
                OffsetSelection::Exact(offset) => segment.index().range(offset..=offset),
                OffsetSelection::From(offset) => segment.index().range(offset..),
            };

            if let Some((offset, _)) = range.next() {
                return segment.read_exact(*offset).await;
            }
        }

        Ok(None)
    }

    pub async fn delete(self) -> Result<()> {
        for (_, segment) in self.segments.into_iter() {
            segment.delete().await?;
        }

        remove_dir(self.config.partition_path(self.topic_id, self.partition_id)).await?;

        Ok(())
    }

    pub fn state(&self) -> PartitionState {
        PartitionState {
            partition_id: self.partition_id,
            current_offset: self.next_offset,
            segment_count: self.segments.len(),
        }
    }

    pub async fn append(
        &mut self,
        key: Bytes,
        value: Bytes,
        headers: Vec<RecordHeader>,
    ) -> Result<Record> {
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

        let record = Record {
            timestamp: Timestamp::now(),
            key,
            value,
            headers,
            offset: self.next_offset,
        };

        self.next_offset += 1;

        self.segments
            .last_entry()
            .unwrap()
            .get_mut()
            .append(&record)
            .await?;

        Ok(record)
    }
}

#[cfg(test)]
mod test {
    use crate::dur::partition::Partition;
    use std::sync::Arc;

    use crate::config::Config;

    #[tokio::test]
    async fn partition_basic_read_write() {
        let config = Arc::new(Config::default());

        let mut partition = Partition::load_from_disk(config, 0, 0)
            .await
            .expect("Failed to load partition");

        let record = partition
            .append("foo".into(), "bar".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 0);

        let record = partition
            .append("foo".into(), "bar2".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 1);

        let read_record = partition
            .read_exact(1)
            .await
            .expect("Failed to read record")
            .expect("Did not recieve a record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar2");
        assert_eq!(read_record.offset, 1);
    }

    #[tokio::test]
    async fn partition_ocntinue_on_existing() {
        let config = Arc::new(Config::default());

        let mut partition = Partition::load_from_disk(config.clone(), 0, 0)
            .await
            .expect("Failed to load partition");

        let record = partition
            .append("foo".into(), "bar".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 0);

        drop(partition);

        let mut partition = Partition::load_from_disk(config, 0, 0)
            .await
            .expect("Failed to load partition");

        let record = partition
            .append("foo".into(), "bar2".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 1);

        let read_record = partition
            .read_exact(1)
            .await
            .expect("Failed to read record")
            .expect("Did not recieve a record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar2");
        assert_eq!(read_record.offset, 1);
    }

    #[tokio::test]
    async fn partition_multiple_segments() {
        let mut config = Config::default();
        config.segment.size = 1;
        let config = Arc::new(config);

        let mut partition = Partition::load_from_disk(config.clone(), 0, 0)
            .await
            .expect("Failed to load partition");

        let record = partition
            .append("foo".into(), "bar".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 0);

        let record = partition
            .append("foo".into(), "bar2".into(), vec![])
            .await
            .expect("Failed to append record");
        assert_eq!(record.offset, 1);

        assert_eq!(partition.segments.len(), 2);

        let read_record = partition
            .read_exact(0)
            .await
            .expect("Failed to read record")
            .expect("Did not recieve a record");
        assert_eq!(read_record.key, "foo");
        assert_eq!(read_record.value, "bar");
        assert_eq!(read_record.offset, 0);
    }
}
