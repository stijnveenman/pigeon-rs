use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
    sync::RwLock,
};

use pigeon_core::PError;
use tokio::fs::create_dir;

use crate::{
    dur::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter},
    metadata::entry::create_topic::CreateTopicEntry,
    systems::execution_context::ExecutionContext,
};

struct PartitionState {
    segments: BTreeSet<u64>,
    read_segments: HashMap<u64, SegmentReader>,
    active_segment: RwLock<SegmentWriter>,
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
                active_segment: RwLock::new(active_segment),
            });
        }

        let mut topic_states = self.system.topic_states.write().await;

        topic_states.insert(topic_name.to_string(), TopicState { partitions });

        Ok(())
    }
}
