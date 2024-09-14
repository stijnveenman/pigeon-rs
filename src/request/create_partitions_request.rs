use std::io::Cursor;

use tokio::io::AsyncWriteExt;

use crate::{
    protocol::{get_i32, get_u32, Error},
    ApiKey,
};

use super::Framing;

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub num_partitions: isize,
}

#[derive(Debug)]
pub struct CreatePartitionsRequest {
    pub topics: Vec<Topic>,
}

impl Framing for CreatePartitionsRequest {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error> {
        let len = get_u32(src)?;

        for _ in 0..len {
            Topic::parse(src, api_version)?;
        }

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let len = get_u32(src)?;

        let mut topics = Vec::with_capacity(len);

        for _ in 0..len {
            topics.push(Topic::parse(src, api_version)?);
        }

        Ok(CreatePartitionsRequest { topics })
    }

    async fn write_to(
        &self,
        dst: &mut tokio::io::BufWriter<tokio::net::TcpStream>,
        api_version: i16,
    ) -> std::io::Result<()> {
        dst.write_i16(ApiKey::CreatePartition as i16).await?;
        dst.write_i16(api_version).await?;

        dst.write_u32(self.topics.len() as u32).await?;

        for topic in &self.topics {
            topic.write_to(dst, api_version).await?;
        }

        Ok(())
    }
}

impl Framing for Topic {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error> {
        String::check(src, api_version)?;
        get_i32(src)?;

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(Topic {
            name: String::parse(src, api_version)?,
            num_partitions: get_i32(src)?,
        })
    }

    async fn write_to(
        &self,
        dst: &mut tokio::io::BufWriter<tokio::net::TcpStream>,
        api_version: i16,
    ) -> std::io::Result<()> {
        self.name.write_to(dst, api_version).await?;
        dst.write_i32(self.num_partitions as i32).await?;

        Ok(())
    }
}
