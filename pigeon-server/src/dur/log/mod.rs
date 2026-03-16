pub mod log_reader;
pub mod log_writer;

pub const LOG_EXTENSION: &str = "log";

#[cfg(test)]
mod test {
    use std::path::Path;

    use pigeon_core::record::Record;
    use tempfile::tempdir;

    use crate::dur::log::{log_reader::LogReader, log_writer::LogWriter};

    #[tokio::test]
    async fn rw_log_records_with_no_end_offset() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::open(Path::new(base_dir), 0).await.unwrap();
        let record = Record {
            offset: 1,
            key: "hello".as_bytes().to_vec(),
            value: "world".as_bytes().to_vec(),
        };

        writer.append(&record).await.unwrap();
        drop(writer);

        let reader = LogReader::open(Path::new(base_dir), 0).await.unwrap();
        let read_record = reader.read_records(0, None).await.unwrap();

        assert_eq!(vec![record], read_record);
    }

    #[tokio::test]
    async fn rw_log_records_with_end_offset() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::open(Path::new(base_dir), 0).await.unwrap();
        let record = Record {
            offset: 1,
            key: "hello".as_bytes().to_vec(),
            value: "world".as_bytes().to_vec(),
        };

        writer.append(&record).await.unwrap();
        let end_offset = writer
            .append(&Record {
                offset: 2,
                value: Vec::new(),
                key: Vec::new(),
            })
            .await
            .unwrap();
        drop(writer);

        let reader = LogReader::open(Path::new(base_dir), 0).await.unwrap();
        let read_record = reader.read_records(0, Some(end_offset)).await.unwrap();

        assert_eq!(vec![record], read_record);
    }

    #[tokio::test]
    async fn read_multiple() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut writer = LogWriter::open(Path::new(base_dir), 0).await.unwrap();

        let record1 = Record {
            offset: 1,
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };
        let record2 = Record {
            offset: 2,
            key: b"hello1".to_vec(),
            value: b"world1".to_vec(),
        };

        writer.append(&record1).await.unwrap();
        writer.append(&record2).await.unwrap();

        drop(writer);

        let reader = LogReader::open(Path::new(base_dir), 0).await.unwrap();

        let records = reader.read_records(0, None).await.unwrap();
        assert_eq!(vec![record1, record2], records);
    }
}
