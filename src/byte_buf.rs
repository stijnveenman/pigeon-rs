use std::{
    fmt::Debug,
    ops::{Index, Range, RangeFull},
};

use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
pub struct ByteBuf(Vec<u8>);

impl ByteBuf {
    pub fn new() -> ByteBuf {
        ByteBuf(vec![])
    }
}

impl Debug for ByteBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(string) = std::str::from_utf8(&self.0) {
            write!(f, "\"{}\"", string)
        } else {
            write!(f, "{:?}", self.0)
        }
    }
}

impl Index<Range<usize>> for ByteBuf {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        &self.0[index]
    }
}

impl Index<RangeFull> for ByteBuf {
    type Output = [u8];

    fn index(&self, index: RangeFull) -> &Self::Output {
        &self.0[index]
    }
}

impl<T> From<T> for ByteBuf
where
    T: Into<Vec<u8>>,
{
    fn from(value: T) -> Self {
        ByteBuf(value.into())
    }
}
