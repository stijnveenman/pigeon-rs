use pigeon_core::{PError, record::Record};

use crate::dur::{index::Index, log::log_reader::LogReader};

struct SegmentReader {
    log: LogReader,
    index: Index,
}

impl SegmentReader {
    pub async fn open(base_dir: &str, start_offset: u64) -> Result<SegmentReader, PError> {
        let log = LogReader::open(base_dir, start_offset).await?;

        let index = Index::from_file(base_dir, start_offset).await?;

        Ok(SegmentReader { log, index })
    }

    pub async fn read_record(&self, offset: u64) -> Result<Record, PError> {
        let start_offset = self.index.get(offset).ok_or(PError::OffsetNotFound)?;
        let end_offset = self.index.range(offset + 1..).next().map(|v| v.1);

        self.log
            .read_record(*start_offset, end_offset.copied())
            .await
    }
}
