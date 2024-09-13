mod create_partitions_request;

use strum_macros::FromRepr;

use clap::error::Result;
use create_partitions_request::CreatePartitionsRequest;

use std::io::Cursor;

use crate::cursor::{get_i16, Error};

pub trait FromFrame {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error>;
    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized;
}

pub enum Request {
    CreatePartitionRequest(CreatePartitionsRequest),
}

#[derive(FromRepr, Debug, PartialEq)]
#[repr(i16)]
enum ApiKey {
    CreatePartition = 1,
}

impl Request {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        let api_key = get_i16(src)?;
        let api_key = ApiKey::from_repr(api_key).ok_or(Error::from("invalid api key"))?;
        let api_version = get_i16(src)?;

        match api_key {
            ApiKey::CreatePartition => CreatePartitionsRequest::check(src, api_version),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Request, Error> {
        let api_key = get_i16(src)?;
        let api_key = ApiKey::from_repr(api_key).ok_or(Error::from("invalid api key"))?;
        let api_version = get_i16(src)?;

        match api_key {
            ApiKey::CreatePartition => Ok(Request::CreatePartitionRequest(
                CreatePartitionsRequest::parse(src, api_version)?,
            )),
        }
    }
}
