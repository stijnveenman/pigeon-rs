use std::io::Cursor;

use crate::protocol::{get_i32, get_string, get_u32, Error};

use super::FromFrame;

#[derive(Debug)]
pub struct Topic {
    name: String,
    num_partitions: isize,
}

#[derive(Debug)]
pub struct CreatePartitionsRequest {
    topics: Vec<Topic>,
}

impl FromFrame for CreatePartitionsRequest {
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
}

impl FromFrame for Topic {
    fn check(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<(), Error> {
        get_string(src)?;
        get_i32(src)?;

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Ok(Topic {
            name: get_string(src)?,
            num_partitions: get_i32(src)?,
        })
    }
}
