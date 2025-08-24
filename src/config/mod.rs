#[derive(Debug)]
pub struct Config {
    pub path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: "data".to_string(),
        }
    }
}

impl Config {
    pub fn base_path(&self) -> String {
        self.path.to_string()
    }

    pub fn topics_path(&self) -> String {
        format!("{}/topics", self.base_path())
    }

    pub fn topic_path(&self, topic_id: u64) -> String {
        format!("{}/{}", self.topics_path(), topic_id)
    }

    pub fn partitions_path(&self, topic_id: u64) -> String {
        format!("{}/partitions", self.topic_path(topic_id))
    }

    pub fn partition_path(&self, topic_id: u64, partition_id: u64) -> String {
        format!("{}/{}", self.partitions_path(topic_id), partition_id)
    }

    pub fn segment_path(&self, topic_id: u64, partition_id: u64, start_offset: u64) -> String {
        format!(
            "{}/{:0>10}",
            self.partition_path(topic_id, partition_id),
            start_offset
        )
    }

    pub fn log_path(&self, topic_id: u64, partition_id: u64, start_offset: u64) -> String {
        format!(
            "{}.log",
            self.segment_path(topic_id, partition_id, start_offset)
        )
    }

    pub fn index_path(&self, topic_id: u64, partition_id: u64, start_offset: u64) -> String {
        format!(
            "{}.index",
            self.segment_path(topic_id, partition_id, start_offset)
        )
    }
}
