use tokio::fs::{File, OpenOptions};

pub struct RecordReader {
    file: std::fs::File,
}

impl RecordReader {
    pub async fn new(file_path: &str) -> Result<Self, tokio::io::Error> {
        let file = OpenOptions::new().read(true).open(file_path).await?;

        Ok(Self {
            file: file.into_std().await,
        })
    }

    pub async fn read_records_at(&self, file_offset: u32) {
        todo!()
    }
}
