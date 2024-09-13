use std::io::Cursor;

use crate::cursor::Error;

use super::FromFrame;

pub struct Topic {
    name: String,
    num_partitions: i32,
}

pub struct CreatePartitionsRequest {
    topic: Vec<Topic>,
}

impl FromFrame for CreatePartitionsRequest {
    fn check(src: &mut Cursor<&[u8]>, api_version: i16) -> clap::error::Result<(), Error> {
        todo!()
    }

    fn parse(src: &mut Cursor<&[u8]>, api_version: i16) -> clap::error::Result<Self, Error>
    where
        Self: Sized,
    {
        todo!()
    }
}
