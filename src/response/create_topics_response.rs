use tokio::io::AsyncWriteExt;

use crate::protocol::{get_i16, Framing};

#[derive(Debug)]
pub struct TopicResponse {
    pub name: String,
    pub error_code: i16,
}

#[derive(Debug)]
pub struct CreateTopicResponse{
    pub topics: Vec<TopicResponse>,
}

impl Framing for CreateTopicResponse{
    fn check(
        src: &mut std::io::Cursor<&[u8]>,
        api_version: i16,
    ) -> Result<(), crate::protocol::Error> {
        Vec::<TopicResponse>::parse(src, api_version)?;

        Ok(())
    }

    fn parse(
        src: &mut std::io::Cursor<&[u8]>,
        api_version: i16,
    ) -> Result<Self, crate::protocol::Error>
    where
        Self: Sized,
    {
        Ok(CreateTopicResponse{
            topics: Vec::<TopicResponse>::parse(src, api_version)?,
        })
    }

    async fn write_to(
        &self,
        dst: &mut tokio::io::BufWriter<tokio::net::TcpStream>,
        api_version: i16,
    ) -> tokio::io::Result<()> {
        self.topics.write_to(dst, api_version).await?;

        Ok(())
    }
}

impl Framing for TopicResponse {
    fn check(
        src: &mut std::io::Cursor<&[u8]>,
        api_version: i16,
    ) -> Result<(), crate::protocol::Error> {
        String::check(src, api_version)?;
        get_i16(src)?;

        Ok(())
    }

    fn parse(
        src: &mut std::io::Cursor<&[u8]>,
        api_version: i16,
    ) -> Result<Self, crate::protocol::Error>
    where
        Self: Sized,
    {
        Ok(TopicResponse {
            name: String::parse(src, api_version)?,
            error_code: get_i16(src)?,
        })
    }

    async fn write_to(
        &self,
        dst: &mut tokio::io::BufWriter<tokio::net::TcpStream>,
        api_version: i16,
    ) -> tokio::io::Result<()> {
        self.name.write_to(dst, api_version).await?;
        dst.write_i16(self.error_code).await?;

        Ok(())
    }
}
