use std::path::Path;

use pigeon_core::{PError, record::Record};

use crate::dur::{
    index::{Index, index_writer::IndexWriter},
    log::log_writer::LogWriter,
};

pub struct SegmentWriter {
    pub start_offset: u64,
    next_offset: u64,
    log: LogWriter,
    index: IndexWriter,
}

impl SegmentWriter {
    pub async fn open(base_dir: &Path, start_offset: u64) -> Result<SegmentWriter, PError> {
        let log = LogWriter::open(base_dir, start_offset).await?;

        let current_offset = match Index::from_file(base_dir, start_offset).await {
            Ok(index) => index.max().map(|offset| offset + 1).unwrap_or_default(),
            Err(PError::IndexNotFound) => start_offset.max(1),
            Err(e) => return Err(e),
        };

        let index = IndexWriter::open(base_dir, start_offset).await?;

        Ok(SegmentWriter {
            log,
            index,
            start_offset,
            next_offset: current_offset,
        })
    }

    pub async fn append_record(&mut self, mut record: Record) -> Result<(u64, u64), PError> {
        assert!(!record.is_commited());
        record.offset = self.next_offset;
        assert!(record.is_commited());

        self.next_offset += 1;

        let byte_offset = self.log.append(&record).await?;

        self.index.append(record.offset, byte_offset).await?;

        Ok((record.offset, byte_offset))
    }
}
