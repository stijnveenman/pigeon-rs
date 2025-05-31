use std::io;

use crate::data::{record::Record, timestamp::Timestamp};

use super::record_writer::RecordWriter;

pub struct Segment {
    start_offset: u64,
    record_log_path: String,
    record_writer: Option<RecordWriter>,
}

fn get_path(start_offset: u64) -> String {
    // TODO have a log config, and order topics etc in correct path

    format!("{:0>10}.log", start_offset)
}

impl Segment {
    pub fn create(start_offset: u64) -> Self {
        let record_log_path = get_path(start_offset);

        Self {
            record_log_path,
            start_offset,
            record_writer: None,
        }
    }

    async fn prepare_writing(&mut self) -> Result<(), io::Error> {
        let record_writer = RecordWriter::new(&self.record_log_path).await?;

        self.record_writer = Some(record_writer);

        Ok(())
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
