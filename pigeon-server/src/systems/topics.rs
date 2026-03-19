use std::collections::{BTreeSet, HashMap};

use pigeon_core::{PError, record::Record};
use tokio::{
    fs::{create_dir, remove_dir_all},
    sync::{RwLock, RwLockMappedWriteGuard, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{
    dur::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter},
    metadata::entry::create_topic::CreateTopicEntry,
    systems::execution_context::ExecutionContext,
};

struct PartitionState {
    segments: BTreeSet<u64>,
    read_segments: HashMap<u64, SegmentReader>,
    active_segment: SegmentWriter,
}

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

        self.system
            .apply_metadata(CreateTopicEntry {
                topic_name: topic_name.to_string(),
                num_partitions,
            })
            .await?;

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

        let mut topic_states = self.system.topic_states.write().await;

        topic_states.insert(topic_name.to_string(), TopicState { partitions });

        Ok(())
    }

    async fn get_topic(&self, topic_name: &str) -> Result<RwLockReadGuard<'_, TopicState>, PError> {
        self.can_read_topic(topic_name)?;

        let topics = self.system.topic_states.read().await;

        // TODO: open topic if not in memory
        RwLockReadGuard::try_map(topics, |topics| topics.get(topic_name))
            .map_err(|_| PError::TopicNotFound)
    }

    async fn get_topic_mut(
        &self,
        topic_name: &str,
    ) -> Result<RwLockMappedWriteGuard<'_, TopicState>, PError> {
        self.can_write_topic(topic_name)?;

        let topics = self.system.topic_states.write().await;

        // TODO: open topic if not in memory
        RwLockWriteGuard::try_map(topics, |topics| topics.get_mut(topic_name))
            .map_err(|_| PError::TopicNotFound)
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

    pub async fn read_record(
        &self,
        topic_name: &str,
        partition_id: u64,
        offset: u64,
    ) -> Result<Record, PError> {
        let mut partition = self.get_partition(topic_name, partition_id).await?;

        let segment_start_offset = *partition
            .segments
            .range(0..=offset)
            .next_back()
            .ok_or(PError::OffsetNotFound)?;

        let reader = match partition.read_segments.get(&segment_start_offset) {
            Some(reader) => reader,
            None => {
                let segment_dir = self
                    .config
                    .data_dir
                    .join(topic_name)
                    .join(partition_id.to_string());

                let segment = SegmentReader::open(&segment_dir, segment_start_offset).await?;

                drop(partition);

                let mut partition_m = self.get_partition_mut(topic_name, partition_id).await?;
                partition_m
                    .read_segments
                    .insert(segment_start_offset, segment);
                drop(partition_m);

                partition = self.get_partition(topic_name, partition_id).await?;
                partition.read_segments.get(&segment_start_offset).unwrap()
            }
        };

        reader.read_record(offset).await
    }

    pub async fn delete_topic(&self, topic_name: &str) -> Result<(), PError> {
        self.can_write_topic(topic_name)?;

        let mut topics = self.system.topic_states.write().await;
        topics.remove(topic_name);

        let topic_dir = self.config.data_dir.join(topic_name);
        remove_dir_all(topic_dir)
            .await
            .map_err(|_| PError::DeleteTopicFailed)?;

        Ok(())
    }
}
