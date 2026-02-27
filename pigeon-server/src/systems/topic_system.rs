use std::{collections::HashMap, path::PathBuf};

use pigeon_core::{PError, record::Record};
use tokio::{fs::create_dir, sync::RwLock};

use crate::dur::segment::segment_writer::SegmentWriter;

pub struct TopicSystem {
    base_dir: PathBuf,
    active_segments: RwLock<HashMap<String, Vec<RwLock<SegmentWriter>>>>,
}

impl TopicSystem {
    pub fn initialise(base_dir: &str) -> TopicSystem {
        TopicSystem {
            base_dir: PathBuf::from(base_dir),
            active_segments: RwLock::default(),
        }
    }

    pub async fn create_topic(&self, topic_name: &str, num_partitions: u64) {
        let topic_dir = self.base_dir.join(topic_name);

        // should not fail, otherwise topic already exists
        create_dir(&topic_dir).await.unwrap();

        let mut segments = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let partition_dir = topic_dir.join(i.to_string());
            create_dir(&partition_dir).await.unwrap();

            let segment = SegmentWriter::open(&partition_dir, i).await.unwrap();
            segments.push(RwLock::new(segment));
        }

        let mut active_segments = self.active_segments.write().await;
        active_segments.insert(topic_name.to_string(), segments);
    }

    pub async fn append_record(
        &self,
        topic_name: &str,
        partition: u64,
        record: &Record,
    ) -> Result<u64, PError> {
        let active_segments = self.active_segments.read().await;

        let topic = active_segments.get(topic_name).unwrap();
        topic
            .get(partition as usize)
            .unwrap()
            .write()
            .await
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
        system.create_topic("test", 10).await;

        system
            .append_record("test", 0, &Record::new(0, "key", "value"))
            .await
            .unwrap();

        system
            .append_record("test", 9, &Record::new(0, "key", "value"))
            .await
            .unwrap();
    }
}
