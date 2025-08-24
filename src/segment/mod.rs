use std::{collections::BTreeMap, io::Read, path::Path};

use tokio::{
    fs::{create_dir_all, File, OpenOptions},
    io::{self, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::{config::Config, data::record::Record};

pub struct Segment {
    start_offset: u64,

    /// A BTreeMap of Offset -> File location to index records.
    index: BTreeMap<u64, u64>,
    log_file: File,
}

impl Segment {
    pub async fn load(config: &Config, start_offset: u64) -> Result<Self, io::Error> {
        let segment_path = config.segment_path(0, 0, start_offset);

        if !Path::new(&segment_path).exists() {
            create_dir_all(&segment_path).await?;
        }

        let log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(config.log_path(0, 0, start_offset))
            .await?;

        Ok(Self {
            start_offset,
            // TODO: if a record exist, we should try loading this from disk
            index: BTreeMap::default(),
            log_file,
        })
    }

    pub async fn append(&mut self, record: &Record) -> io::Result<()> {
        let mut writer = BufWriter::new(&mut self.log_file);

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

        let position = self.log_file.stream_position().await?;

        self.index.insert(record.offset, position);

        Ok(())
    }

    pub async fn read(&self, offset: u64) {
        let _offset = self.index.get(&offset).expect("record offset not found");
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

        segment.read(record.offset).await;
    }
}
