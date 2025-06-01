mod record_reader;
mod record_set;
mod record_writer;

use std::io;

use record_reader::RecordReader;
use record_writer::RecordWriter;

use crate::data::{record::Record, timestamp::Timestamp};

pub struct Segment {
    start_offset: u64,
    record_log_path: String,
    record_writer: Option<RecordWriter>,
    record_reader: Option<RecordReader>,
}

fn get_path(base_dir: &str, start_offset: u64) -> String {
    // TODO: have a log config, and order topics etc in correct path

    format!("{}/{:0>10}.log", base_dir, start_offset)
}

impl Segment {
    pub fn new(base_dir: &str, start_offset: u64) -> Self {
        let record_log_path = get_path(base_dir, start_offset);

        Self {
            record_log_path,
            start_offset,
            record_writer: None,
            record_reader: None,
        }
    }

    async fn prepare_reading(&mut self) -> Result<(), io::Error> {
        let record_reader = RecordReader::new(&self.record_log_path).await?;

        self.record_reader = Some(record_reader);

        Ok(())
    }

    async fn prepare_writing(&mut self) -> Result<(), io::Error> {
        let record_writer = RecordWriter::new(&self.record_log_path).await?;

        self.record_writer = Some(record_writer);

        Ok(())
    }

    async fn prepare(&mut self) -> Result<(), io::Error> {
        self.prepare_writing().await?;
        self.prepare_reading().await?;

        Ok(())
    }

    async fn read_records_from_offset(
        &mut self,
        offset: u64,
        count: u64,
    ) -> Result<Vec<Record>, io::Error> {
        let reader = self
            .record_reader
            .as_mut()
            .expect("Reading has not been initialized");

        let mut messages = reader.read_records_at(0).await?;

        loop {
            if messages.is_empty() {
                return Ok(vec![]);
            }

            let from_idx = messages.iter().position(|r| r.offset >= offset);
            let Some(from_idx) = from_idx else {
                continue;
            };

            let to_idx = from_idx + count as usize;

            if to_idx > messages.len() {
                if let Ok(mut next_batch) = reader.read_next().await {
                    messages.append(&mut next_batch);
                }
            }

            return Ok(messages);
        }
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

        self.record_writer
            .as_mut()
            .expect("Record writer not prepared")
            .append_record_set(&batch)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use tempfile::tempdir;

    use crate::data::{record::Record, timestamp::Timestamp};

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
        let mut segment = Segment::new(dir.path().to_str().unwrap(), 0);

        segment.prepare().await.expect("Failed to prepare segment");

        segment
            .write_batch(vec![create_record("hello", "world")], 0)
            .await
            .expect("Failed to write_batch");
        segment
            .write_batch(vec![create_record("hello", "world")], 0)
            .await
            .expect("Failed to write_batch");
        segment
            .write_batch(vec![create_record("hello", "world")], 0)
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
        assert_eq!(&returned_record.value, "world");
        assert_eq!(returned_record.offset, 1u64);
    }
}
