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
    record_writer: RecordWriter,
    record_reader: RecordReader,
}

fn get_path(base_dir: &str, start_offset: u64) -> String {
    // TODO: have a log config, and order topics etc in correct path

    format!("{}/{:0>10}.log", base_dir, start_offset)
}

impl Segment {
    pub async fn load(base_dir: &str, start_offset: u64) -> Result<Self, io::Error> {
        let record_log_path = get_path(base_dir, start_offset);

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
        let reader = self
            .record_reader
            .as_mut()
            .expect("Reading has not been initialized");

        reader.load_messages_at(0, offset, count).await
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
        let mut segment = Segment::load(dir.path().to_str().unwrap(), 0)
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
