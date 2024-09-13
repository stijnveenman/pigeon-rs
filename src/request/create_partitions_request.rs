use std::io::Cursor;

use crate::cursor::{get_usize, Error};

use super::FromFrame;

#[derive(Debug)]
pub struct Topic {
    name: String,
    num_partitions: i32,
}

#[derive(Debug)]
pub struct CreatePartitionsRequest {
    topics: Vec<Topic>,
}

impl FromFrame for CreatePartitionsRequest {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<(), Error> {
        let len = get_usize(src)?;

        for _ in 0..len {
            Topic::parse(src, api_version)?;
        }

        Ok(())
    }

    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let len = get_usize(src)?;

        let mut topics = Vec::with_capacity(len);

        for _ in 0..len {
            topics.push(Topic::parse(src, api_version)?);
        }

        Ok(CreatePartitionsRequest { topics })
    }
}

impl FromFrame for Topic {
    fn check(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<(), Error> {
        todo!()
    }

    fn parse(src: &mut Cursor<&[u8]>, _api_version: i16) -> Result<Self, Error>
    where
        Self: Sized,
    {
        todo!()
    }
}
