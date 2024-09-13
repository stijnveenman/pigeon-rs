use clap::error::Result;

use std::io::Cursor;

use crate::cursor::{get_u8, peek_u8, Error};

pub enum Request {}

impl Request {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        peek_u8(src)?;
        peek_u8(src)?;

        Ok(())
    }

    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
        get_u8(src)?;
        let api_key = get_u8(src)?;

        Ok(api_key)
    }
}
