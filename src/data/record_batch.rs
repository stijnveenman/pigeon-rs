use std::sync::Arc;

use crate::http::responses::record_response::{FetchResponse, RecordResponse};

use super::{
    encoding::{self, Encoding},
    record::Record,
};

#[derive(Debug)]
pub struct RecordBatch {
    total_size: usize,
    max_size: usize,
    records: Vec<Arc<Record>>,
}

impl RecordBatch {
    pub fn new(max_size: usize) -> Self {
        RecordBatch {
            total_size: 0,
            max_size,
            records: Vec::new(),
        }
    }

    pub fn push(&mut self, record: Arc<Record>) {
        self.total_size += record.size();
        self.records.push(record);
    }

    pub fn is_full(&self) -> bool {
        self.total_size >= self.max_size
    }

    pub fn into_response(self, encoding: &Encoding) -> Result<FetchResponse, encoding::Error> {
        let records = self
            .records
            .iter()
            .map(|record| RecordResponse::from(record, encoding))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(FetchResponse {
            total_size: self.total_size,
            records,
        })
    }
}
