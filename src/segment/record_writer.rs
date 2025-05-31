use tokio::{
    fs::{File, OpenOptions},
    io::BufWriter,
};

use crate::data::record::Record;

use super::record_set::RecordSet;

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

        RecordSet::write_to_buf(set, &mut writer).await?;

        self.file.sync_all().await?;

        Ok(())
    }
}
