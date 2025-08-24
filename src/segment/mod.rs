use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::{collections::BTreeMap, path::Path};

use bytes::{Buf, Bytes};
use std::fs::File as StdFile;
use tokio::task::spawn_blocking;
use tokio::{
    fs::{create_dir_all, File, OpenOptions},
    io::{self, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::data::record::RecordHeader;
use crate::data::timestamp::Timestamp;
use crate::{config::Config, data::record::Record};

pub struct Segment {
    start_offset: u64,

    /// A BTreeMap of Offset -> File location to index records.
    index: BTreeMap<u64, u64>,
    log_file_r: Arc<StdFile>,
    log_file_w: File,
    log_size: u64,
}

impl Segment {
    pub async fn load(config: &Config, start_offset: u64) -> Result<Self, io::Error> {
        let segment_path = config.segment_path(0, 0, start_offset);

        if !Path::new(&segment_path).exists() {
            create_dir_all(&segment_path).await?;
        }

        let logfile_path = config.log_path(0, 0, start_offset);
        // TODO: should we always open the write file? what if a segment is closed
        let log_file_write = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&logfile_path)
            .await?;

        let log_file_read = OpenOptions::new()
            .read(true)
            .open(&logfile_path)
            .await?
            .into_std()
            .await;

        Ok(Self {
            start_offset,
            // TODO: if a record exist, we should try loading this from disk
            index: BTreeMap::default(),
            log_file_w: log_file_write,
            log_file_r: Arc::new(log_file_read),
            // FIX: should come from file size when loading existing segment
            log_size: 0,
        })
    }

    pub async fn append(&mut self, record: &Record) -> io::Result<()> {
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
        self.index.insert(record.offset, self.log_size);

        self.log_size = self.log_file_w.stream_position().await?;

        Ok(())
    }

    // FIX: error handling
    pub async fn read(&self, offset: u64) -> Record {
        let _offset = self.index.get(&offset).expect("record offset not found");

        let mut index_range = self.index.range(offset..);
        let record_file_offset = index_range
            .next()
            .expect("expect to receive at least first record");
        // FIX: Expect offset to always be in the index for now
        assert_eq!(*record_file_offset.0, offset);

        let record_file_offset = *record_file_offset.1;

        // If we have a next entry in the index, we know how many bytes to read
        // Otherwise, we need to read until EOF
        let next_file_offset = index_range.next().map(|e| *e.1);

        let record_len = if let Some(next) = next_file_offset {
            next - record_file_offset
        } else {
            self.log_size - record_file_offset
        } as usize;

        let bytes = self
            .read_at(record_file_offset, record_len)
            .await
            .expect("failed to read from file");

        assert_eq!(bytes.len(), record_len);

        let mut bytes = Bytes::from(bytes);

        // FIX: handle not enough bytes
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

        Record {
            offset,
            timestamp,
            key,
            value,
            headers,
        }
    }

    #[allow(clippy::uninit_vec)]
    async fn read_at(&self, file_offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
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

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::Segment;
    use crate::{
        config::Config,
        data::{record::Record, timestamp::Timestamp},
    };

    fn basic_record(offset: u64, key: &str, value: &str) -> Record {
        Record {
            headers: vec![],
            offset: 0,
            value: value.to_string().into(),
            key: key.to_string().into(),
            timestamp: Timestamp::now(),
        }
    }

    #[tokio::test]
    async fn segment_basic_read_write() {
        let dir = tempdir().expect("failed to create tempdir");

        let config = Config {
            path: dir.path().to_str().unwrap().to_string(),
        };

        let mut segment = Segment::load(&config, 0)
            .await
            .expect("Failed to load segment");

        let record = basic_record(0, "Hello", "World");
        segment
            .append(&record)
            .await
            .expect("Failed to append record");

        let read_record = segment.read(record.offset).await;
        assert_eq!(record, read_record);
    }
}
