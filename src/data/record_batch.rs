use super::record::Record;

#[derive(Debug)]
pub struct RecordBatch {
    total_size: usize,
    max_size: usize,
    records: Vec<Record>,
}

impl RecordBatch {
    pub fn new(max_size: usize) -> Self {
        RecordBatch {
            total_size: 0,
            max_size,
            records: Vec::new(),
        }
    }

    pub fn push(&mut self, record: Record) {
        self.total_size += record.size();
        self.records.push(record);
    }

    pub fn full(&self) -> bool {
        self.total_size >= self.max_size
    }

    pub fn get(self) -> Vec<Record> {
        self.records
    }
}
