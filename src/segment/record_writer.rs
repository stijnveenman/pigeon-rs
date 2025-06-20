use bytes::BytesMut;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
};

use crate::{
    bin_ser::{BinarySerialize, DynamicBinarySize},
    data::{record::Record, record_set_header::RecordSetHeader},
};

pub struct RecordWriter {
    file: File,
}

impl RecordWriter {
    pub async fn new(file_path: &str) -> Result<Self, tokio::io::Error> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .await?;

        Ok(Self { file })
    }

    pub async fn append_record_set(&mut self, set: &[Record]) -> Result<(), tokio::io::Error> {
        let mut writer = BufWriter::new(&mut self.file);

        let header = RecordSetHeader::for_records(set);

        let mut buf = BytesMut::with_capacity(header.binary_size());
        header.serialize(&mut buf);

        writer.write_all(&buf).await?;

        for record in set {
            buf.clear();
            record.serialize(&mut buf);
            writer.write_all(&buf).await?;
        }

        writer.flush().await?;
        self.file.sync_all().await?;

        Ok(())
    }
}
