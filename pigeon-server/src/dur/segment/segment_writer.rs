use std::path::Path;

use pigeon_core::{PError, record::Record};

use crate::dur::{index::index_writer::IndexWriter, log::log_writer::LogWriter};

pub struct SegmentWriter {
    log: LogWriter,
    index: IndexWriter,
}

impl SegmentWriter {
    pub async fn open(base_dir: &Path, start_offset: u64) -> Result<SegmentWriter, PError> {
        let log = LogWriter::open(base_dir, start_offset).await?;

        let index = IndexWriter::open(base_dir, start_offset).await?;

        Ok(SegmentWriter { log, index })
    }

    pub async fn append(&mut self, record: &Record) -> Result<u64, PError> {
        let byte_offset = self.log.append(record).await?;

        self.index.append(record.offset, byte_offset).await?;

        Ok(byte_offset)
    }
}
