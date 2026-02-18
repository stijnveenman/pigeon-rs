mod log_reader;
mod log_writer;

pub const LOG_EXTENSION: &str = "log";

#[cfg(test)]
mod test {
    use pigeon_core::record::Record;
    use tempfile::tempdir;

    use crate::dur::log::{log_reader::LogReader, log_writer::LogWriter};

    #[tokio::test]
    async fn rw_log_records_with_no_end_offset() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::new(base_dir, 0);
        let record = Record {
            offset: 1,
            key: "hello".as_bytes().to_vec(),
            value: "world".as_bytes().to_vec(),
        };

        writer.append(&record).await.unwrap();
        drop(writer);

        let reader = LogReader::open(base_dir, 0).await.unwrap();
        let read_record = reader.read_record(0, None).await.unwrap();

        assert_eq!(record, read_record);
    }

    #[tokio::test]
    async fn rw_log_records_with_end_offset() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::new(base_dir, 0);
        let record = Record {
            offset: 1,
            key: "hello".as_bytes().to_vec(),
            value: "world".as_bytes().to_vec(),
        };

        let end_offset = writer.append(&record).await.unwrap();
        writer
            .append(&Record {
                offset: 2,
                value: Vec::new(),
                key: Vec::new(),
            })
            .await
            .unwrap();
        drop(writer);

        let reader = LogReader::open(base_dir, 0).await.unwrap();
        let read_record = reader.read_record(0, Some(end_offset)).await.unwrap();

        assert_eq!(record, read_record);
    }
}
