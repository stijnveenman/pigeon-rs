use bytes::{Bytes, BytesMut};
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader,
    BufWriter,
};

use crate::{
    bin_ser::{BinaryDeserialize, BinarySerialize, StaticBinarySize},
    data::{record::Record, record_set_header::RecordSetHeader},
};

#[derive(Debug)]
pub struct RecordSet<T> {
    header: RecordSetHeader,
    reader: T,
}

impl<T: AsyncRead + Unpin + AsyncSeek> RecordSet<T> {
    async fn read_header(reader: &mut BufReader<T>) -> std::io::Result<RecordSetHeader> {
        let mut buf = Vec::with_capacity(RecordSetHeader::binary_size());
        reader
            .take(RecordSetHeader::binary_size() as u64)
            .read_to_end(buf.as_mut())
            .await?;

        // TODO: handle deserialize errors
        let header = RecordSetHeader::deserialize(&mut Bytes::from(buf))
            .expect("Failed to deserialize RecordSetHeader");

        Ok(header)
    }

    pub async fn read_from(mut reader: BufReader<T>) -> std::io::Result<RecordSet<BufReader<T>>> {
        let header = Self::read_header(&mut reader).await?;

        Ok(RecordSet { header, reader })
    }

    pub async fn records(&mut self) -> std::io::Result<Vec<Record>> {
        let mut buf = Vec::with_capacity(self.header.length as usize);
        (&mut self.reader)
            .take(self.header.length.into())
            .read_to_end(buf.as_mut())
            .await?;
        let mut reader = Bytes::from(buf);

        let records = (0..self.header.record_count)
            .map(|_| Record::deserialize(&mut reader).expect("Failed to deserialize Record"))
            .collect();

        Ok(records)
    }

    pub async fn skip(&mut self) -> std::io::Result<()> {
        self.reader
            .seek(std::io::SeekFrom::Current(self.header.length.into()))
            .await?;

        Ok(())
    }

    pub fn remaining_buf(self) -> T {
        self.reader
    }
}
