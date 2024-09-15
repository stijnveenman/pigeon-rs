pub mod create_partitions_request;

use create_partitions_request::CreatePartitionsRequest;
use tokio::{io::BufWriter, net::TcpStream};

use std::io::{self, Cursor};

use crate::{
    protocol::{get_i16, Error, Framing},
    ApiKey,
};

#[derive(Debug)]
pub enum Request {
    CreatePartitionRequest(CreatePartitionsRequest),
}

impl Framing for Request {
    fn check(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<(), Error> {
        let api_key = get_i16(src)?;
        let api_key = ApiKey::from_repr(api_key).ok_or(Error::from("invalid api key"))?;
        let api_version = get_i16(src)?;

        match api_key {
            ApiKey::CreatePartition => CreatePartitionsRequest::check(src, api_version),
        }
    }

    fn parse(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<Request, Error> {
        let api_key = get_i16(src)?;
        let api_key = ApiKey::from_repr(api_key).ok_or(Error::from("invalid api key"))?;
        let api_version = get_i16(src)?;

        match api_key {
            ApiKey::CreatePartition => Ok(Request::CreatePartitionRequest(
                CreatePartitionsRequest::parse(src, api_version)?,
            )),
        }
    }

    async fn write_to(&self, dst: &mut BufWriter<TcpStream>, api_version: i16) -> io::Result<()> {
        match self {
            Request::CreatePartitionRequest(request) => request.write_to(dst, api_version),
        }
        .await
    }
}
