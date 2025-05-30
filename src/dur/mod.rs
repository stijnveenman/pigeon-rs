use std::io::Write;

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};

use crate::{
    bin_ser::BinarySerialize,
    data::{record::Record, record_set_header::RecordSetHeader},
};

pub struct RecordSet {}

impl RecordSet {
    pub async fn write_to_buf<T: AsyncWrite + Unpin>(
        records: &[Record],
        writer: &mut BufWriter<T>,
    ) -> std::io::Result<()> {
        let header = RecordSetHeader::empty();

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
}

#[cfg(test)]
mod test {
    use tokio::io::BufWriter;

    use super::RecordSet;

    #[tokio::test]
    async fn test_write_to_buf_empty() {
        let mut buf = BufWriter::new(Vec::new());
        RecordSet::write_to_buf(&[], &mut BufWriter::new(&mut buf))
            .await
            .expect("write_to_buf failed");

        assert_eq!(buf.get_ref().len(), 28);
    }
}
