use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};

use crate::{
    bin_ser::{BinaryDeserialize, BinarySerialize, StaticBinarySize},
    data::{record::Record, record_set_header::RecordSetHeader},
};

#[derive(Debug)]
pub struct RecordSet {
    header: RecordSetHeader,
    records: Vec<Record>,
}

impl RecordSet {
    pub async fn write_to_buf<T: AsyncWrite + Unpin>(
        records: &[Record],
        writer: &mut BufWriter<T>,
    ) -> std::io::Result<()> {
        let header = RecordSetHeader::for_records(records);

        let mut buf = BytesMut::new();
        header.serialize(&mut buf);

        writer.write_all(&buf).await?;

        for record in records {
            buf.clear();
            record.serialize(&mut buf);
            writer.write_all(&buf).await?;
        }

        writer.flush().await?;

        Ok(())
    }

    pub async fn read_from<T: AsyncRead + Unpin>(
        reader: &mut BufReader<T>,
    ) -> std::io::Result<RecordSet> {
        let mut buf = Vec::with_capacity(RecordSetHeader::binary_size());
        reader
            .take(RecordSetHeader::binary_size() as u64)
            .read_to_end(buf.as_mut())
            .await?;

        // TODO handle deserialize errors
        let header = RecordSetHeader::deserialize(&mut Bytes::from(buf))
            .expect("Failed to deserialize RecordSetHeader");

        let mut buf = Vec::with_capacity(header.length as usize);
        reader
            .take(header.length.into())
            .read_to_end(buf.as_mut())
            .await?;
        let mut reader = Bytes::from(buf);

        let records = (0..header.record_count)
            .map(|_| Record::deserialize(&mut reader).expect("Failed to deserialize Record"))
            .collect();

        Ok(RecordSet { header, records })
    }
}

#[cfg(test)]
mod test {
    use tokio::io::{BufReader, BufWriter};

    use crate::{
        bin_ser::DynamicBinarySize,
        data::{record::Record, record_set_header::RecordSetHeader, timestamp::Timestamp},
    };

    use super::RecordSet;

    #[tokio::test]
    async fn test_write_to_buf_empty() {
        let mut buf = BufWriter::new(Vec::new());
        RecordSet::write_to_buf(&[], &mut BufWriter::new(&mut buf))
            .await
            .expect("write_to_buf failed");

        assert_eq!(buf.get_ref().len(), 28);
    }

    #[tokio::test]
    async fn test_write_and_read_set() {
        let mut buf = Vec::new();
        let records = vec![Record {
            offset: 1,
            timestamp: Timestamp::now(),
            key: "foo".into(),
            value: "bar".into(),
            headers: vec![],
        }];

        RecordSet::write_to_buf(&records, &mut BufWriter::new(&mut buf))
            .await
            .expect("failed to write RecordSet");

        let record_set = RecordSet::read_from(&mut BufReader::new(buf.as_slice()))
            .await
            .expect("failed to read RecordSet");

        assert_eq!(
            RecordSetHeader {
                length: records.first().unwrap().binary_size() as u32,
                start_offset: 1,
                end_offset: 1,
                crc: 0,
                record_count: 1
            },
            record_set.header
        );
        dbg!(&record_set);
        assert_eq!(records, record_set.records);
    }
}
