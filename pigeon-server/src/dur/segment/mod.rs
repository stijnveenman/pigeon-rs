pub mod segment_reader;
pub mod segment_writer;

#[cfg(test)]
mod test {
    use std::path::Path;

    use pigeon_core::{record::Record, uncommited_record::UncommitedRecord};
    use tempfile::tempdir;

    use crate::dur::segment::{segment_reader::SegmentReader, segment_writer::SegmentWriter};

    #[tokio::test]
    async fn segment_rw() {
        let dir = tempdir().unwrap();
        let base_dir = dir.path().to_str().unwrap();

        let mut segment_writer = SegmentWriter::open(Path::new(base_dir), 0).await.unwrap();
        segment_writer
            .append_record(UncommitedRecord::new("key", "value"))
            .await
            .unwrap();
        segment_writer
            .append_record(UncommitedRecord::new("hello", "world"))
            .await
            .unwrap();
        drop(segment_writer);

        let segment_reader = SegmentReader::open(Path::new(base_dir), 0).await.unwrap();

        let record = segment_reader.read_record(0).await;
        assert_eq!(record, Ok(Record::new(0, "key", "value")));

        let record = segment_reader.read_record(1).await;
        assert_eq!(record, Ok(Record::new(1, "hello", "world")));
    }
}
