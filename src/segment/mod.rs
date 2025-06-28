mod record_reader;
mod record_writer;

use std::{io, path::Path};

use record_reader::RecordReader;
use record_writer::RecordWriter;
use tokio::fs::create_dir_all;

use crate::{
    config::Config,
    data::{record::Record, timestamp::Timestamp},
};

pub struct Segment {
    start_offset: u64,
    record_log_path: String,
    record_writer: RecordWriter,
    record_reader: RecordReader,
}

impl Segment {
    pub async fn load(config: &Config, start_offset: u64) -> Result<Self, io::Error> {
        let segment_path = config.segment_path(0, 0, start_offset);
        if !Path::new(&segment_path).exists() {
            create_dir_all(&segment_path).await?;
        };

        let record_log_path = config.log_path(0, 0, start_offset);

        let record_writer = RecordWriter::new(&record_log_path).await?;
        let record_reader = RecordReader::new(&record_log_path).await?;

        Ok(Self {
            record_log_path,
            start_offset,
            record_writer,
            record_reader,
        })
    }

    async fn read_records_from_offset(
        &mut self,
        offset: u64,
        count: u64,
    ) -> Result<Vec<Record>, io::Error> {
        self.record_reader.load_messages_at(0, offset, count).await
    }

    async fn write_batch(
        &mut self,
        mut batch: Vec<Record>,
        current_offset: u64,
    ) -> Result<(), io::Error> {
        let mut next_offset = current_offset + 1;
        let timestamp = Timestamp::now();

        for record in batch.iter_mut() {
            record.offset = next_offset;
            record.timestamp = timestamp;

            next_offset += 1;
        }

        self.record_writer.append_record_set(&batch).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use tempfile::tempdir;

    use crate::{
        config::Config,
        data::{record::Record, timestamp::Timestamp},
    };

    use super::Segment;

    fn create_record(key: &str, value: &str) -> Record {
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
            .expect("Faileed to load segment");

        segment
            .write_batch(vec![create_record("hello", "world 1")], 0)
            .await
            .expect("Failed to write_batch");
        segment
            .write_batch(vec![create_record("hello", "world 2")], 0)
            .await
            .expect("Failed to write_batch");
        segment
            .write_batch(vec![create_record("hello", "world 3")], 0)
            .await
            .expect("Failed to write_batch");

        let returned_record = segment
            .read_records_from_offset(1, 1)
            .await
            .expect("Failed to read a record")
            .into_iter()
            .next()
            .unwrap();

        assert_eq!(&returned_record.key, "hello");
        assert_eq!(&returned_record.value, "world 1");
        assert_eq!(returned_record.offset, 1u64);
    }

    // TODO: add a test overlapping multiple batches
}
