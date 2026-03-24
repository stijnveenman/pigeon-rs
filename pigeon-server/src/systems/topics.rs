use std::{
    collections::{BTreeSet, HashMap},
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

use pigeon_core::{PError, record::Record};
use tokio::{
    fs::{create_dir, remove_dir_all},
    sync::{RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard},
};
use tracing::info;

use crate::{
    disk::read_partition_states,
    dur::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter},
    metadata::entry::{create_topic::CreateTopicEntry, delete_topic::DeleteTopicEntry},
    systems::execution_context::ExecutionContext,
};

struct PartitionState {
    segments: BTreeSet<u64>,
    read_segments: HashMap<u64, SegmentReader>,
    active_segment: SegmentWriter,
}

impl Debug for PartitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionState")
            .field("segments", &self.segments)
            .field(
                "read_segments",
                &self.read_segments.iter().map(|v| v.0).collect::<Vec<_>>(),
            )
            // .field("active_segment", &self.active_segment)
            .finish()
    }
}

#[derive(Debug)]
pub struct TopicState {
    partitions: Vec<PartitionState>,
}

impl ExecutionContext {
    pub async fn create_topic(
        &self,
        topic_name: &str,
        num_partitions: Option<u64>,
    ) -> Result<(), PError> {
        self.can_write_topic(topic_name)?;

        let num_partitions = num_partitions.unwrap_or(self.system.config.topics.default_partitions);

        let topic_dir = self.config.data_dir.join(topic_name);
        create_dir(&topic_dir)
            .await
            .map_err(|_| PError::CreateTopicFailed)?;

        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let partition_dir = topic_dir.join(i.to_string());
            create_dir(&partition_dir)
                .await
                .map_err(|_| PError::CreateTopicFailed)?;

            let active_segment = SegmentWriter::open(&partition_dir, 0).await?;
            partitions.push(PartitionState {
                segments: BTreeSet::from([0]),
                read_segments: HashMap::new(),
                active_segment,
            });
        }

        let mut topic_states = self.system.topics.write().await;
        topic_states.insert(topic_name.to_string(), TopicState { partitions });
        drop(topic_states);

        self.apply_metadata(CreateTopicEntry {
            topic_name: topic_name.to_string(),
            num_partitions,
        })
        .await?;

        Ok(())
    }

    async fn open_topic(&self, topic_name: &str) -> Result<(), PError> {
        info!("Opening topic {topic_name}");

        let Ok(topics) = read_partition_states(self.config.data_dir.join(topic_name)) else {
            let metadata = self.system.metadata.read().await;

            return match metadata.topics.contains_key(topic_name) {
                true => Err(PError::OpenTopicFailed),
                false => Err(PError::TopicNotFound),
            };
        };

        let mut partitions = Vec::with_capacity(topics.len());

        for (index, segments) in topics.into_iter().enumerate() {
            let base_dir = self
                .config
                .data_dir
                .join(topic_name)
                .join(index.to_string());

            let start_offset = segments.last().cloned().unwrap_or(0);

            let writer = SegmentWriter::open(&base_dir, start_offset).await?;

            partitions.push(PartitionState {
                segments,
                read_segments: HashMap::new(),
                active_segment: writer,
            });
        }

        let mut topics = self.system.topics.write().await;
        topics.insert(topic_name.to_string(), TopicState { partitions });

        Ok(())
    }

    async fn get_topic(&self, topic_name: &str) -> Result<RwLockReadGuard<'_, TopicState>, PError> {
        self.can_read_topic(topic_name)?;

        if let Ok(topic) = RwLockReadGuard::try_map(self.system.topics.read().await, |topics| {
            topics.get(topic_name)
        }) {
            return Ok(topic);
        }

        self.open_topic(topic_name).await?;

        Ok(RwLockReadGuard::map(
            self.system.topics.read().await,
            |topics| {
                // SAFETY: as we have just succesfully open the tpoic, it should already be open
                topics.get(topic_name).expect("Expected topic to be open")
            },
        ))
    }

    async fn get_topic_mut(
        &self,
        topic_name: &str,
    ) -> Result<RwLockMappedWriteGuard<'_, TopicState>, PError> {
        self.can_write_topic(topic_name)?;

        if let Ok(topic) = RwLockWriteGuard::try_map(self.system.topics.write().await, |topics| {
            topics.get_mut(topic_name)
        }) {
            return Ok(topic);
        }

        self.open_topic(topic_name).await?;

        Ok(RwLockWriteGuard::map(
            self.system.topics.write().await,
            |topics| {
                // SAFETY: as we have just succesfully open the tpoic, it should already be open
                topics
                    .get_mut(topic_name)
                    .expect("Expected topic to be open")
            },
        ))
    }

    async fn get_partition_mut(
        &self,
        topic_name: &str,
        partition_id: u64,
    ) -> Result<RwLockMappedWriteGuard<'_, PartitionState>, PError> {
        let topic = self.get_topic_mut(topic_name).await?;

        RwLockMappedWriteGuard::try_map(topic, |topic| {
            topic.partitions.get_mut(partition_id as usize)
        })
        .map_err(|_| PError::PartitionNotFound)
    }

    async fn get_partition(
        &self,
        topic_name: &str,
        partition_id: u64,
    ) -> Result<RwLockReadGuard<'_, PartitionState>, PError> {
        let topic = self.get_topic(topic_name).await?;

        RwLockReadGuard::try_map(topic, |topic| topic.partitions.get(partition_id as usize))
            .map_err(|_| PError::PartitionNotFound)
    }

    pub async fn append_record(
        &self,
        topic_name: &str,
        partition_id: u64,
        record: Record,
    ) -> Result<u64, PError> {
        let mut partition = self.get_partition_mut(topic_name, partition_id).await?;

        let active_segment_start = partition.active_segment.start_offset;
        let (offset, byte_offset) = partition.active_segment.append_record(record).await?;

        if let Some(read_segment) = partition.read_segments.get_mut(&active_segment_start) {
            read_segment.append(offset, byte_offset)?;
        }

        Ok(offset)
    }

    async fn get_segment_reader(
        &self,
        topic_name: &str,
        partition_id: u64,
        start_offset: u64,
    ) -> Result<RwLockReadGuard<'_, SegmentReader>, PError> {
        let mut partition = self.get_partition(topic_name, partition_id).await?;

        if !partition.read_segments.contains_key(&start_offset) {
            drop(partition);

            let segment_dir = self
                .config
                .data_dir
                .join(topic_name)
                .join(partition_id.to_string());

            let segment = SegmentReader::open(&segment_dir, start_offset).await?;

            let mut partition_m = self.get_partition_mut(topic_name, partition_id).await?;
            partition_m.read_segments.insert(start_offset, segment);
            drop(partition_m);

            partition = self.get_partition(topic_name, partition_id).await?;
        }

        Ok(RwLockReadGuard::map(partition, |partition| {
            partition.read_segments.get(&start_offset).unwrap()
        }))
    }

    pub async fn read_record(
        &self,
        topic_name: &str,
        partition_id: u64,
        offset: u64,
    ) -> Result<Record, PError> {
        let partition = self.get_partition(topic_name, partition_id).await?;

        let segment_start_offset = *partition
            .segments
            .range(0..=offset)
            .next_back()
            .ok_or(PError::OffsetNotFound)?;

        drop(partition);

        let reader = self
            .get_segment_reader(topic_name, partition_id, segment_start_offset)
            .await?;

        reader.read_record(offset).await
    }

    pub async fn read_range<R>(
        &self,
        topic_name: &str,
        partition_id: u64,
        offsets: R,
    ) -> Result<Vec<Record>, PError>
    where
        R: RangeBounds<u64> + Clone,
    {
        let partition = self.get_partition(topic_name, partition_id).await?;

        let start_offset = match offsets.start_bound() {
            Bound::Included(start) => *start,
            Bound::Excluded(start) => *start + 1,
            Bound::Unbounded => 0u64,
        };

        // get last segment with a offset before the target offset
        let mut segment_start_offsets = Vec::new();
        if let Some(previous) = partition.segments.range(0..start_offset).next_back() {
            segment_start_offsets.push(*previous);
        }
        segment_start_offsets.extend(partition.segments.range(offsets.clone()));

        drop(partition);

        let mut records = Vec::new();
        for start_offset in segment_start_offsets {
            let reader = self
                .get_segment_reader(topic_name, partition_id, start_offset)
                .await?;

            records.extend(reader.read_range(offsets.clone()).await?);
        }

        Ok(records)
    }

    pub async fn delete_topic(&self, topic_name: &str) -> Result<(), PError> {
        self.can_write_topic(topic_name)?;

        self.apply_metadata(DeleteTopicEntry {
            topic_name: topic_name.to_string(),
        })
        .await?;

        let mut topics = self.system.topics.write().await;
        topics.remove(topic_name);

        let topic_dir = self.config.data_dir.join(topic_name);
        remove_dir_all(topic_dir)
            .await
            .map_err(|_| PError::DeleteTopicFailed)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use pigeon_core::{PError, record::Record};

    use crate::systems::{execution_context::ExecutionContext, test_system::TestSystem};

    #[tokio::test]
    async fn basic_end_to_end() {
        let system = TestSystem::create().await;

        system.create_topic("test", Some(10)).await.unwrap();

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

        let record = system.read_record("test", 0, 1).await.unwrap();
        assert_eq!(&record.text().unwrap(), "t1");

        let record = system.read_record("test", 1, 1).await.unwrap();
        assert_eq!(&record.text().unwrap(), "t2");

        let record = system.read_record("test", 9, 1).await.unwrap();
        assert_eq!(&record.text().unwrap(), "t3");
    }

    #[tokio::test]
    async fn multiple_read_writes() {
        let system = TestSystem::create().await;

        system.create_topic("world", Some(1)).await.unwrap();

        system
            .append_record("world", 0, Record::new("k1", "value1"))
            .await
            .unwrap();

        let record = system.read_record("world", 0, 1).await.unwrap();
        assert_eq!(&record.text().unwrap(), "value1");

        system
            .append_record("world", 0, Record::new("k2", "value2"))
            .await
            .unwrap();

        let record = system.read_record("world", 0, 2).await.unwrap();
        assert_eq!(&record.text().unwrap(), "value2");
    }

    #[tokio::test]
    async fn read_in_middle() {
        let system = TestSystem::create().await;

        system.create_topic("test", Some(1)).await.unwrap();

        for i in 0..10 {
            system
                .append_record("test", 0, Record::new(&i.to_string(), &i.to_string()))
                .await
                .unwrap();
        }

        for i in 0..10 {
            let record = system.read_record("test", 0, i + 1).await.unwrap();

            assert_eq!(
                record,
                Record::with_offset(i + 1, &i.to_string(), &i.to_string())
            );
        }
    }

    #[tokio::test]
    async fn reinitialise_system() {
        let system = TestSystem::create().await;

        system.create_topic("test", Some(5)).await.unwrap();

        system
            .append_record("test", 2, Record::new("foo", "bar"))
            .await
            .unwrap();

        let dir = system.close();

        let system = TestSystem::from(dir).await;

        let record = system.read_record("test", 2, 1).await.unwrap();
        assert_eq!(record, Record::with_offset(1, "foo", "bar"));
    }

    #[tokio::test]
    async fn users_cannot_write_to_metadata() {
        let system = TestSystem::create().await;

        let user = ExecutionContext::user(system.system.clone());

        let result = user
            .append_record(".metadata", 0, Record::new("foo", "bar"))
            .await;

        assert_eq!(result, Err(PError::Unauthorized))
    }

    #[tokio::test]
    async fn users_cannot_delete_metadata() {
        let system = TestSystem::create().await;

        let user = ExecutionContext::user(system.system.clone());

        let result = user.delete_topic(".metadata").await;

        assert_eq!(result, Err(PError::Unauthorized))
    }

    #[tokio::test]
    async fn users_cannot_create_internal_topics() {
        let system = TestSystem::create().await;

        let user = ExecutionContext::user(system.system.clone());

        let result = user.create_topic(".foobar", None).await;

        assert_eq!(result, Err(PError::Unauthorized))
    }

    #[tokio::test]
    async fn topic_delete_test() {
        let system = TestSystem::create().await;

        system.create_topic("foo", None).await.unwrap();

        system
            .append_record("foo", 0, Record::new("foo", "bar"))
            .await
            .unwrap();

        system.delete_topic("foo").await.unwrap();

        let result = system.read_record("foo", 0, 1).await;

        assert_eq!(result, Err(PError::TopicNotFound));

        system.create_topic("foo", None).await.unwrap();

        system
            .append_record("foo", 0, Record::new("hello", "world"))
            .await
            .unwrap();

        let record = system.read_record("foo", 0, 1).await;

        assert_eq!(
            record,
            Ok(Record {
                offset: 1,
                key: b"hello".into(),
                value: b"world".into(),
            })
        );
    }
}
