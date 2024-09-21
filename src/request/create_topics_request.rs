use std::io::Cursor;

use tokio::io::AsyncWriteExt;

use crate::{
    connection::Connection,
    protocol::{get_i32, Error, Framing},
    response::create_topics_response::CreateTopicResponse,
    ApiKey,
};

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub num_partitions: isize,
}

#[derive(Debug)]
pub struct CreateTopicsRequest {
    pub topics: Vec<Topic>,
}

impl CreateTopicsRequest {
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(CreateTopicResponse { topics: vec![] })
            .await?;

        Ok(())
    }
}

impl Framing for CreateTopicsRequest {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error> {
        Vec::<Topic>::parse(src, api_version)?;

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(CreateTopicsRequest {
            topics: Vec::<Topic>::parse(src, api_version)?,
        })
    }

    async fn write_to(
        &self,
        dst: &mut tokio::io::BufWriter<tokio::net::TcpStream>,
        api_version: i16,
    ) -> std::io::Result<()> {
        dst.write_i16(ApiKey::CreateTopics as i16).await?;
        dst.write_i16(api_version).await?;

        self.topics.write_to(dst, api_version).await?;

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
