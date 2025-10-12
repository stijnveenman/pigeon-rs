mod index;

use std::fmt::Display;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use index::Index;
use std::fs::File as StdFile;
use tokio::task::spawn_blocking;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::data::record::RecordHeader;
use crate::data::timestamp::Timestamp;
use crate::dur::error::Error;
use crate::{config::Config, data::record::Record};

use super::error::Result;

pub struct Segment {
    topic_id: u64,
    partition_id: u64,
    start_offset: u64,
    log_file_r: Arc<StdFile>,
    log_file_w: File,
    log_size: u64,
    index: Index,
    max_log_size: u64,
}

impl Segment {
    pub async fn load_from_disk(
        config: &Config,
        topic_id: u64,
        partition_id: u64,
        start_offset: u64,
    ) -> Result<Self> {
        let log_file_path = config.log_path(topic_id, partition_id, start_offset);
        let index_file_path = config.index_path(topic_id, partition_id, start_offset);

        // TODO: should we always open the write file? what if a segment is closed
        let log_file_write = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&log_file_path)
            .await?;

        let log_size = log_file_write.metadata().await?.size();

        let log_file_read = OpenOptions::new()
            .read(true)
            .open(&log_file_path)
            .await?
            .into_std()
            .await;

        Ok(Self {
            start_offset,
            topic_id,
            partition_id,

            index: Index::load_from_disk(&index_file_path).await?,
            log_file_w: log_file_write,
            log_file_r: Arc::new(log_file_read),
            log_size,
            max_log_size: config.segment.size,
        })
    }

    pub async fn append(&mut self, record: &Record) -> Result<()> {
        if self.is_full() {
            return Err(Error::SegmentFull);
        }

        let mut writer = BufWriter::new(&mut self.log_file_w);

        writer.write_u64(record.offset).await?;
        writer.write_u64(record.timestamp.as_micros()).await?;

        writer.write_u32(record.key.len() as u32).await?;
        writer.write_all(&record.key).await?;

        writer.write_u32(record.value.len() as u32).await?;
        writer.write_all(&record.value).await?;

        writer.write_u16(record.headers.len() as u16).await?;

        for header in &record.headers {
            writer.write_u32(header.key.len() as u32).await?;
            writer.write_all(header.key.as_bytes()).await?;

            writer.write_u32(header.value.len() as u32).await?;
            writer.write_all(&header.value).await?;
        }

        writer.flush().await?;

        // Save the size of the start of the mesasge, or, the log size before writing the message
        self.index.append(record.offset, self.log_size).await?;

        self.log_size = self.log_file_w.stream_position().await?;

        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.log_size >= self.max_log_size
    }

    pub async fn read_exact(&self, offset: u64) -> Result<Record> {
        let mut index_range = self.index.range(offset..);

        let record_file_offset = index_range.next().ok_or(Error::OffsetNotFound)?;
        if (*record_file_offset.0 != offset) {
            return Err(Error::OffsetNotFound);
        }

        let record_file_offset = *record_file_offset.1;

        // If we have a next entry in the index, we know how many bytes to read
        // Otherwise, we need to read until EOF
        let next_file_offset = index_range.next().map(|e| *e.1);
        let record_len = if let Some(next) = next_file_offset {
            next - record_file_offset
        } else {
            self.log_size - record_file_offset
        } as usize;

        let bytes = self.read_at(record_file_offset, record_len).await?;
        assert_eq!(bytes.len(), record_len);

        let mut bytes = Bytes::from(bytes);

        let offset = bytes.get_u64();
        let timestamp = Timestamp::from(bytes.get_u64());

        let key_len = bytes.get_u32();
        let key = bytes.copy_to_bytes(key_len as usize);

        let value_len = bytes.get_u32();
        let value = bytes.copy_to_bytes(value_len as usize);

        let header_len = bytes.get_u16();

        let headers = (0..header_len)
            .map(|_| {
                let key_len = bytes.get_u32();
                let key = bytes.copy_to_bytes(key_len as usize);
                let key = String::from_utf8(key.to_vec()).expect("failed to parse from_utf8");

                let value_len = bytes.get_u32();
                let value = bytes.copy_to_bytes(value_len as usize);

                RecordHeader { key, value }
            })
            .collect::<Vec<_>>();

        Ok(Record {
            offset,
            timestamp,
            key,
            value,
            headers,
        })
    }

    pub fn max_offset(&self) -> Option<u64> {
        self.index.max_offset()
    }

    pub fn min_offset(&self) -> Option<u64> {
        self.index.min_offset()
    }

    #[allow(clippy::uninit_vec)]
    async fn read_at(&self, file_offset: u64, length: usize) -> Result<Vec<u8>> {
        let file = self.log_file_r.clone();
        spawn_blocking(move || {
            let mut buf = Vec::with_capacity(length);
            unsafe {
                buf.set_len(length);
            }
            file.read_exact_at(&mut buf, file_offset)?;

            Ok(buf)
        })
        .await
        .expect("failed to join spawn_blocking handle")
    }
}

impl Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Segment({{ topic_id: {}, partition_id: {}, start_offset: {}, log_size: {} }})",
            self.topic_id, self.partition_id, self.start_offset, self.log_size
        )
    }
}

#[cfg(test)]
mod test {
    use std::{fs::create_dir_all, path::Path};

    use tempfile::tempdir;

    use super::Segment;
    use crate::{
        config::{self, Config},
        data::{record::Record, timestamp::Timestamp},
        dur::error::Error,
    };

    #[tokio::test]
    async fn segment_basic_read_write() {
        let config = Config::default();
        create_dir_all(config.partition_path(0, 0));

        let mut segment = Segment::load_from_disk(&config, 0, 0, 0)
            .await
            .expect("Failed to load segment");

        let record = Record::basic_with_offset(0, "Hello", "World");
        segment
            .append(&record)
            .await
            .expect("Failed to append record");

        println!("{}", segment);

        let read_record = segment
            .read_exact(record.offset)
            .await
            .expect("Read of record failed");

        assert_eq!(record, read_record);
    }

    #[tokio::test]
    async fn segment_continue_on_existing_segment() {
        let config = Config::default();
        create_dir_all(config.partition_path(0, 0));

        let mut segment = Segment::load_from_disk(&config, 0, 0, 0)
            .await
            .expect("Failed to load segment");

        let record = Record::basic_with_offset(0, "Hello", "World");
        segment
            .append(&record)
            .await
            .expect("Failed to append record");

        println!("{}", segment);
        drop(segment);

        let segment = Segment::load_from_disk(&config, 0, 0, 0)
            .await
            .expect("Failed to load segment");

        let read_record = segment
            .read_exact(record.offset)
            .await
            .expect("Read record failed");
        assert_eq!(record, read_record);
    }

    #[tokio::test]
    async fn segment_is_full() {
        let mut config = Config::default();
        create_dir_all(config.partition_path(0, 0));

        config.segment.size = 1;

        let mut segment = Segment::load_from_disk(&config, 0, 0, 0)
            .await
            .expect("Failed to load segment");

        assert!(!segment.is_full());

        let record = Record::basic_with_offset(0, "Hello", "World");
        segment
            .append(&record)
            .await
            .expect("Failed to append record");

        assert!(segment.is_full());

        let record = Record::basic_with_offset(1, "Hello", "World");
        let result = segment.append(&record).await;

        assert!(matches!(result, Err(Error::SegmentFull)))
    }
}
