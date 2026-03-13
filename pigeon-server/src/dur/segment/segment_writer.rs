use std::path::Path;

use pigeon_core::{PError, record::Record, uncommited_record::UncommitedRecord};

use crate::dur::{index::index_writer::IndexWriter, log::log_writer::LogWriter};

pub struct SegmentWriter {
    pub start_offset: u64,
    current_offset: u64,
    log: LogWriter,
    index: IndexWriter,
}

impl SegmentWriter {
    pub async fn open(base_dir: &Path, start_offset: u64) -> Result<SegmentWriter, PError> {
        let log = LogWriter::open(base_dir, start_offset).await?;

        let index = IndexWriter::open(base_dir, start_offset).await?;

        Ok(SegmentWriter {
            log,
            index,
            start_offset,
            // TODO: read from disk
            current_offset: start_offset,
        })
    }

    pub async fn append_record(&mut self, record: UncommitedRecord) -> Result<(u64, u64), PError> {
        let record = record.to_record(self.current_offset);
        self.current_offset += 1;

        let byte_offset = self.log.append(&record).await?;

        self.index.append(record.offset, byte_offset).await?;

        Ok((record.offset, byte_offset))
    }
}
