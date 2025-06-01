use std::io::Seek;

use anyhow::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncSeekExt, BufReader},
    task::spawn_blocking,
};

use crate::data::record::Record;

use super::record_set::RecordSet;

pub struct RecordReader {
    file: File,
}

impl RecordReader {
    pub async fn new(file_path: &str) -> Result<Self, tokio::io::Error> {
        let file = OpenOptions::new().read(true).open(file_path).await?;

        Ok(Self { file })
    }

    pub async fn read_next(&mut self) -> Result<Vec<Record>, tokio::io::Error> {
        let mut set = RecordSet::read_from(BufReader::new(&mut self.file)).await?;

        set.records().await
    }

    pub async fn read_records_at(
        &mut self,
        file_offset: u32,
    ) -> Result<Vec<Record>, tokio::io::Error> {
        self.file
            .seek(std::io::SeekFrom::Start(file_offset as u64))
            .await?;

        self.read_next().await
    }
}
