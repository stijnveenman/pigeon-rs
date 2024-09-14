use bytes::Buf;
use std::io::Cursor;

use bytes::BytesMut;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    protocol::Error,
    request::{Framing, Request},
};

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

    pub async fn write_frame<T: Framing>(&mut self, request: T) -> io::Result<()> {
        request.write_to(&mut self.stream, 0).await?;

        self.stream.flush().await?;

        Ok(())
    }

    pub async fn read_frame<T: Framing + Sized>(&mut self) -> crate::Result<Option<T>> {
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

    fn parse_frame<T: Framing + Sized>(&mut self) -> crate::Result<Option<T>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match T::check(&mut buf, -1) {
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = T::parse(&mut buf, -1)?;

                self.buffer.advance(len);

                Ok(Some(frame))
            }

            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
