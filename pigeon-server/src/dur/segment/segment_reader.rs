use std::{
    ops::{Range, RangeBounds},
    path::Path,
};

use pigeon_core::{PError, record::Record};

use crate::dur::{index::Index, log::log_reader::LogReader};

pub struct SegmentReader {
    log: LogReader,
    index: Index,
}

impl SegmentReader {
    pub async fn open(base_dir: &Path, start_offset: u64) -> Result<SegmentReader, PError> {
        let log = LogReader::open(base_dir, start_offset).await?;

        let index = Index::from_file(base_dir, start_offset).await?;

        Ok(SegmentReader { log, index })
    }

    pub async fn read_record(&self, offset: u64) -> Result<Record, PError> {
        let start_offset = self.index.get(offset).ok_or(PError::OffsetNotFound)?;
        let end_offset = self.index.range(offset + 1..).next().map(|v| v.1);

        let mut records = self
            .log
            .read_records(*start_offset, end_offset.copied())
            .await?;

        records.pop().ok_or(PError::OffsetNotFound)
    }

    pub async fn read_range<R>(&self, offsets: R) -> Result<Vec<Record>, PError>
    where
        R: RangeBounds<u64> + Clone,
    {
        let mut range = self.index.range(offsets);
        let Some(start_offset) = range.next() else {
            return Ok(Vec::new());
        };
        let end_offset = range.next_back().map(|i| *i.1);

        self.log.read_records(*start_offset.1, end_offset).await
    }

    pub fn append(&mut self, offset: u64, byte_offset: u64) -> Result<(), PError> {
        self.index.append(offset, byte_offset)
    }
}
