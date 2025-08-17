use tokio::{
    fs::{File, OpenOptions},
    io::{self, BufWriter},
};

use crate::{config::Config, data::record::Record};

pub struct Segment {
    start_offset: u64,

    log_file: File,
}

impl Segment {
    pub async fn load(config: &Config, start_offset: u64) -> Result<Self, io::Error> {
        let path = config.segment_path(0, 0, start_offset);

        let log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .await?;

        Ok(Self {
            start_offset,
            log_file,
        })
    }

    pub async fn append(&mut self, record: &Record) -> io::Result<()> {
        let mut writer = BufWriter::new(&mut self.log_file);

        Ok(())
    }
}
