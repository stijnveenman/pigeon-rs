use bytes::Buf;
use std::io::Cursor;
use tracing::info;

use bytes::BytesMut;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{cursor::Error, request::Request};

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn write_request(&mut self, api_key: u8) -> io::Result<()> {
        self.stream.write_u8(0).await?;
        self.stream.write_u8(api_key).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<u8>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> crate::Result<Option<u8>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Request::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Request::parse(&mut buf)?;

                self.buffer.advance(len);

                Ok(Some(frame))
            }

            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
