use std::{collections::HashMap, path::PathBuf, sync::RwLock};

use tokio::fs::create_dir;

use crate::dur::segment::segment_writer::SegmentWriter;

pub struct TopicSystem {
    base_dir: PathBuf,
    active_segments: RwLock<HashMap<String, Vec<SegmentWriter>>>,
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

            segments.push(SegmentWriter::open(&partition_dir, i).await.unwrap());
        }

        let mut active_segments = self.active_segments.write().unwrap();
        active_segments.insert(topic_name.to_string(), segments);
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::systems::topic_system::TopicSystem;

    #[tokio::test]
    async fn can_create_segments() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let system = TopicSystem::initialise(base_dir);
        system.create_topic("test", 10).await;
    }
}
