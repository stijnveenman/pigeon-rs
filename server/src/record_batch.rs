use std::collections::HashMap;

use shared::{
    data::encoding::{self, Encoding},
    response::record_response::{FetchResponse, RecordResponse},
};

use crate::dur::record::Record;

pub struct RecordBatch {
    /// A record batch should try to return at least this many bytes
    min_bytes: usize,
    /// A record batch will try up to this many bytes if available
    max_bytes: Option<usize>,
    /// How many bytes are currently in the batch
    total_bytes: usize,

    records: HashMap<u64, HashMap<u64, Vec<Record>>>,
}

impl RecordBatch {
    pub fn new(min_bytes: usize, max_bytes: Option<usize>) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            min_bytes,
            records: HashMap::new(),
        }
    }

    pub fn is_ready(&self) -> bool {
        self.total_bytes > self.min_bytes
    }

    pub fn is_full(&self) -> bool {
        self.max_bytes.is_some_and(|max| self.total_bytes > max)
    }

    pub fn push(&mut self, topic_id: u64, partition_id: u64, record: Record) {
        let topic_records = self.records.entry(topic_id).or_default();
        let partition_records = topic_records.entry(partition_id).or_default();

        self.total_bytes += record.size();
        partition_records.push(record);
    }

    pub fn to_response(&self, encoding: Encoding) -> Result<FetchResponse, encoding::Error> {
        let mut records = Vec::new();
        for (topic_id, partition) in &self.records {
            for (partition_id, batch) in partition {
                records.append(
                    &mut batch
                        .iter()
                        .map(|r| r.to_response(&encoding, *topic_id, *partition_id))
                        .collect::<Result<Vec<RecordResponse>, encoding::Error>>()?,
                );
            }
        }

        Ok(FetchResponse {
            total_size: self.total_bytes,
            encoding,
            count: records.len(),
            records,
        })
    }
}
