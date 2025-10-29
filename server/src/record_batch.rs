use std::collections::HashMap;

use crate::dur::record::Record;

pub struct RecordBatch {
    /// A record batch should try to return at least this many bytes
    min_bytes: usize,
    /// A record batch will try up to this many bytes if available
    max_bytes: usize,
    /// How many bytes are currently in the batch
    total_bytes: usize,

    records: HashMap<u64, HashMap<u64, Vec<Record>>>,
}

impl RecordBatch {
    pub fn is_ready(&self) -> bool {
        self.total_bytes > self.min_bytes
    }

    pub fn is_full(&self) -> bool {
        self.total_bytes > self.max_bytes
    }

    pub fn push(&mut self, topic_id: u64, partition_id: u64, record: &Record) {
        let topic_records = self.records.entry(topic_id).or_default();
        let partition_records = topic_records.entry(partition_id).or_default();

        partition_records.push(record.clone());
        self.total_bytes += record.size();
    }
}
