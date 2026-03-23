use std::{
    collections::{BTreeSet, HashMap, HashSet},
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
};

use pigeon_core::{PError, record::Record};
use tokio::{
    fs::{create_dir, remove_dir_all},
    sync::{RwLock, RwLockReadGuard},
};
use tracing::warn;

use crate::{
    disk::read_topic_states,
    dur::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter},
    metadata::TopicMetadata,
};

pub struct TopicSystem {
    base_dir: PathBuf,
    segments: RwLock<HashMap<String, Vec<BTreeSet<u64>>>>,
    read_segments: RwLock<HashMap<(String, u64, u64), SegmentReader>>,
    active_segments: RwLock<HashMap<String, Vec<RwLock<SegmentWriter>>>>,
}

impl TopicSystem {
    async fn initialise_active_segments(
        base_dir: &str,
        segments: &HashMap<String, Vec<BTreeSet<u64>>>,
    ) -> HashMap<String, Vec<RwLock<SegmentWriter>>> {
        let mut topics = HashMap::new();

        for (topic, partitions) in segments {
            let mut writers = Vec::new();
            let base_dir = Path::new(base_dir).join(topic);

            for (index, partition) in partitions.iter().enumerate() {
                let base_dir = base_dir.join(index.to_string());
                let start_offset = partition.iter().max().cloned().unwrap_or_default();

                let writer = SegmentWriter::open(&base_dir, start_offset).await.unwrap();

                writers.push(RwLock::new(writer));
            }

            topics.insert(topic.to_string(), writers);
        }

        topics
    }

    pub async fn initialise(base_dir: &str) -> TopicSystem {
        let segments = read_topic_states(base_dir).expect("TopicSystem::initialise failed");
        let active_segments = Self::initialise_active_segments(base_dir, &segments).await;

        TopicSystem {
            base_dir: PathBuf::from(base_dir),
            segments: RwLock::new(segments),
            active_segments: RwLock::new(active_segments),
            read_segments: Default::default(),
        }
    }

    pub async fn sync(&mut self, topics: &HashMap<String, TopicMetadata>) {
        let segments = self.segments.read().await;
        let disk_topics = segments.keys().cloned().collect::<HashSet<_>>();
        drop(segments);

        for topic in topics.values() {
            if !disk_topics.contains(&topic.topic_name) {
                warn!("Topic {} missing from disk, creating", topic.topic_name);
                self.create_topic(&topic.topic_name, topic.num_partitions)
                    .await
                    .expect("failed to sync metadata topic on disk");
            }
        }
    }

    async fn get_reader(
        &self,
        topic_name: &str,
        partition: u64,
        start_offset: u64,
    ) -> Result<RwLockReadGuard<'_, SegmentReader>, PError> {
        let read = self.read_segments.read().await;

        if let Ok(reader) = RwLockReadGuard::try_map(read, |read| {
            read.get(&(topic_name.to_string(), partition, start_offset))
        }) {
            return Ok(reader);
        }

        let segment_dir = self.base_dir.join(topic_name).join(partition.to_string());
        let segment = SegmentReader::open(&segment_dir, start_offset).await?;

        let mut write = self.read_segments.write().await;
        write.insert((topic_name.to_string(), partition, start_offset), segment);

        Ok(RwLockReadGuard::map(write.downgrade(), |read| {
            // we have just inserted this key, and are still carying a lock. so this unwrap is safe
            read.get(&(topic_name.to_string(), partition, start_offset))
                .unwrap()
        }))
    }

    pub(super) async fn read_range<R>(
        &self,
        topic_name: &str,
        partition_id: u64,
        offsets: R,
    ) -> Result<Vec<Record>, PError>
    where
        R: RangeBounds<u64> + Clone,
    {
        let read = self.segments.read().await;

        let segments = read.get(topic_name).ok_or(PError::TopicNotFound)?;

        let segments = segments
            .get(partition_id as usize)
            .ok_or(PError::PartitionNotFound)?;

        // TODO: test reading from multiple segments

        let start_offset = match offsets.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => *start + 1,
            Bound::Unbounded => 0u64,
        };

        // get last segment with a offset before the target offset
        let mut segment_start_offsets = Vec::new();
        if let Some(previous) = segments.range(0..start_offset).next_back() {
            segment_start_offsets.push(previous);
        }
        segment_start_offsets.extend(segments.range(offsets.clone()));

        let mut records = Vec::new();
        for start_offset in segment_start_offsets {
            let reader = self
                .get_reader(topic_name, partition_id, *start_offset)
                .await?;

            records.extend(reader.read_range(offsets.clone()).await?);
        }

        Ok(records)
    }

    async fn create_topic(&self, topic_name: &str, num_partitions: u64) -> Result<(), PError> {
        let topic_dir = self.base_dir.join(topic_name);

        create_dir(&topic_dir)
            .await
            .map_err(|_| PError::CreateTopicFailed)?;

        let mut segments = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let partition_dir = topic_dir.join(i.to_string());
            create_dir(&partition_dir)
                .await
                .map_err(|_| PError::CreateTopicFailed)?;

            let segment = SegmentWriter::open(&partition_dir, 0).await?;
            segments.push(RwLock::new(segment));
        }

        let mut active_segments = self.active_segments.write().await;
        active_segments.insert(topic_name.to_string(), segments);

        let mut topic_segments = self.segments.write().await;
        topic_segments.insert(
            topic_name.to_string(),
            (0..num_partitions).map(|_| BTreeSet::from([0])).collect(),
        );

        Ok(())
    }

    async fn append_record(
        &self,
        topic_name: &str,
        partition: u64,
        record: Record,
    ) -> Result<u64, PError> {
        let active_segments = self.active_segments.read().await;

        let topic = active_segments
            .get(topic_name)
            .ok_or(PError::TopicNotFound)?;

        let mut current_segment = topic
            .get(partition as usize)
            .ok_or(PError::PartitionNotFound)?
            .write()
            .await;

        let current_segment_start_offset = current_segment.start_offset;
        let (offset, byte_offset) = current_segment.append_record(record).await?;

        let mut read_segments = self.read_segments.write().await;
        if let Some(current_read_segment) = read_segments.get_mut(&(
            topic_name.to_string(),
            partition,
            current_segment_start_offset,
        )) {
            current_read_segment.append(offset, byte_offset)?;
        }

        Ok(offset)
    }
}

#[cfg(test)]
mod test {
    use pigeon_core::record::Record;
    use tempfile::{TempDir, tempdir};

    use crate::systems::topic_system::TopicSystem;

    async fn system() -> (TempDir, TopicSystem) {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let system = TopicSystem::initialise(base_dir).await;

        (dir, system)
    }

    #[tokio::test]
    async fn basic_end_to_end() {
        let (_dir, system) = system().await;

        system.create_topic("test", 10).await.unwrap();

        system
            .append_record("test", 0, Record::new("key", "t1"))
            .await
            .unwrap();

        system
            .append_record("test", 1, Record::new("key", "t2"))
            .await
            .unwrap();

        system
            .append_record("test", 9, Record::new("key", "t3"))
            .await
            .unwrap();

        todo!();
        // let record = system.read_record("test", 0, 1).await.unwrap();
        // assert_eq!(&record.text().unwrap(), "t1");
        //
        // let record = system.read_record("test", 1, 1).await.unwrap();
        // assert_eq!(&record.text().unwrap(), "t2");
        //
        // let record = system.read_record("test", 9, 1).await.unwrap();
        // assert_eq!(&record.text().unwrap(), "t3");
    }

    #[tokio::test]
    async fn multiple_read_writes() {
        let (_dir, system) = system().await;

        system.create_topic("world", 1).await.unwrap();

        system
            .append_record("world", 0, Record::new("k1", "value1"))
            .await
            .unwrap();

        todo!();
        // let record = system.read_record("world", 0, 1).await.unwrap();
        // assert_eq!(&record.text().unwrap(), "value1");
        //
        // system
        //     .append_record("world", 0, Record::new("k2", "value2"))
        //     .await
        //     .unwrap();
        //
        // let record = system.read_record("world", 0, 2).await.unwrap();
        // assert_eq!(&record.text().unwrap(), "value2");
    }

    #[tokio::test]
    async fn read_in_middle() {
        let (_dir, system) = system().await;

        system.create_topic("test", 1).await.unwrap();

        for i in 0..10 {
            system
                .append_record("test", 0, Record::new(&i.to_string(), &i.to_string()))
                .await
                .unwrap();
        }

        todo!();
        // for i in 0..10 {
        //     let record = system.read_record("test", 0, i + 1).await.unwrap();
        //
        //     assert_eq!(
        //         record,
        //         Record::with_offset(i + 1, &i.to_string(), &i.to_string())
        //     );
        // }
    }

    #[tokio::test]
    async fn reinitialise_system() {
        let (dir, system) = system().await;

        system.create_topic("test", 5).await.unwrap();

        system
            .append_record("test", 2, Record::new("foo", "bar"))
            .await
            .unwrap();

        let base_dir = dir.path().to_str().unwrap();
        let system = TopicSystem::initialise(base_dir).await;

        todo!();
        // let record = system.read_record("test", 2, 1).await.unwrap();
        // assert_eq!(record, Record::with_offset(1, "foo", "bar"));
    }
}
