pub mod create_topics_request;

use create_topics_request::CreateTopicsRequest;
use tokio::{io::BufWriter, net::TcpStream};

use std::io::{self, Cursor};

use crate::{
    connection::Connection,
    protocol::{get_i16, Error, Framing},
    ApiKey,
};

#[derive(Debug)]
pub enum Request {
    CreateTopicsRequest(CreateTopicsRequest),
}

impl Request {
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        match self {
            Request::CreateTopicsRequest(request) => request.apply(dst).await,
        }
    }
}

impl Framing for Request {
    fn check(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<(), Error> {
        let api_key = get_i16(src)?;
        let api_key = ApiKey::from_repr(api_key).ok_or(Error::from("invalid api key"))?;
        let api_version = get_i16(src)?;

        match api_key {
            ApiKey::CreateTopics => CreateTopicsRequest::check(src, api_version),
        }
    }

    fn parse(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<Request, Error> {
        let api_key = get_i16(src)?;
        let api_key = ApiKey::from_repr(api_key).ok_or(Error::from("invalid api key"))?;
        let api_version = get_i16(src)?;

        match api_key {
            ApiKey::CreateTopics => Ok(Request::CreateTopicsRequest(CreateTopicsRequest::parse(
                src,
                api_version,
            )?)),
        }
    }

    async fn write_to(&self, dst: &mut BufWriter<TcpStream>, api_version: i16) -> io::Result<()> {
        match self {
            Request::CreateTopicsRequest(request) => request.write_to(dst, api_version),
        }
        .await
    }
}
