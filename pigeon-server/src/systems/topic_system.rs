use std::{
    collections::{
        BTreeSet, HashMap,
        hash_map::Entry::{Occupied, Vacant},
    },
    path::PathBuf,
};

use parking_lot::{
    RawRwLock, RwLock, RwLockReadGuard,
    lock_api::{MappedRwLockReadGuard, RwLockWriteGuard},
};
use pigeon_core::{PError, record::Record};
use tokio::fs::create_dir;

use crate::dur::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter};

pub struct TopicSystem {
    base_dir: PathBuf,
    segments: RwLock<HashMap<String, Vec<BTreeSet<u64>>>>,
    read_segments: RwLock<HashMap<(String, u64, u64), SegmentReader>>,
    active_segments: RwLock<HashMap<String, Vec<RwLock<SegmentWriter>>>>,
}

impl TopicSystem {
    pub fn initialise(base_dir: &str) -> TopicSystem {
        TopicSystem {
            base_dir: PathBuf::from(base_dir),
            segments: Default::default(),
            active_segments: Default::default(),
            read_segments: Default::default(),
        }
    }

    async fn get_reader(
        &self,
        topic_name: &str,
        partition: u64,
        start_offset: u64,
    ) -> Result<MappedRwLockReadGuard<'_, RawRwLock, SegmentReader>, PError> {
        let read = self.read_segments.read();
        if let Ok(reader) = RwLockReadGuard::try_map(read, |read_segments| {
            read_segments.get(&(topic_name.to_string(), partition, start_offset))
        }) {
            return Ok(reader);
        }

        let segment_dir = self.base_dir.join(topic_name).join(partition.to_string());
        let segment = SegmentReader::open(&segment_dir, start_offset).await?;

        let mut write = self.read_segments.write();
        write.insert((topic_name.to_string(), partition, start_offset), segment);

        let read = RwLockWriteGuard::downgrade(write);
        Ok(RwLockReadGuard::map(read, |read| {
            // we have just inserted this key, and are still carying a lock. so this unwrap is safe
            read.get(&(topic_name.to_string(), partition, start_offset))
                .unwrap()
        }))
    }

    pub async fn read_record(
        &self,
        topic_name: &str,
        partition: u64,
        offset: u64,
    ) -> Result<Record, PError> {
        let read = self.segments.read();

        let segments = read.get(topic_name).ok_or(PError::TopicNotFound)?;

        let segments = segments
            .get(partition as usize)
            .ok_or(PError::PartitionNotFound)?;

        // get last segment with a offset before the target offset
        let segment_start_offset = segments
            .range(0..=offset)
            .next_back()
            .ok_or(PError::OffsetNotFound)?;

        // TODO:
        Ok(Record::new(0, "", ""))
    }

    pub async fn create_topic(&self, topic_name: &str, num_partitions: u64) -> Result<(), PError> {
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

            let segment = SegmentWriter::open(&partition_dir, i).await?;
            segments.push(RwLock::new(segment));
        }

        let mut active_segments = self.active_segments.write();
        active_segments.insert(topic_name.to_string(), segments);

        let mut topic_segments = self.segments.write();
        topic_segments.insert(
            topic_name.to_string(),
            (0..num_partitions).map(|_| BTreeSet::from([0])).collect(),
        );

        Ok(())
    }

    pub async fn append_record(
        &self,
        topic_name: &str,
        partition: u64,
        record: &Record,
    ) -> Result<u64, PError> {
        let active_segments = self.active_segments.read();

        let topic = active_segments
            .get(topic_name)
            .ok_or(PError::TopicNotFound)?;

        topic
            .get(partition as usize)
            .ok_or(PError::PartitionNotFound)?
            .write()
            .append_record(record)
            .await
    }
}

#[cfg(test)]
mod test {
    use pigeon_core::record::Record;
    use tempfile::tempdir;

    use crate::systems::topic_system::TopicSystem;

    #[tokio::test]
    async fn basic_end_to_end() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let system = TopicSystem::initialise(base_dir);
        system.create_topic("test", 10).await.unwrap();

        system
            .append_record("test", 0, &Record::new(0, "key", "value"))
            .await
            .unwrap();

        system
            .append_record("test", 1, &Record::new(0, "key", "value"))
            .await
            .unwrap();

        system
            .append_record("test", 9, &Record::new(0, "key", "value"))
            .await
            .unwrap();

        let record = system.read_record("test", 0, 0).await.unwrap();
        let record = system.read_record("test", 1, 0).await.unwrap();
        let record = system.read_record("test", 9, 0).await.unwrap();
    }
}
