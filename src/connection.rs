use bson::Document;
use bytes::Buf;
use serde::Serialize;
use std::io::Cursor;

use bytes::BytesMut;
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::cmd::{Command, Response};

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

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Document>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
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

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> crate::Result<Option<Document>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        let doc = Document::from_reader(&mut buf);

        match doc {
            Err(bson::de::Error::EndOfStream) => Ok(None),
            Err(bson::de::Error::Io(inner))
                if inner.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
            Ok(document) => {
                self.buffer.advance(buf.position() as usize);
                Ok(Some(document))
            }
        }
    }

    pub async fn write_frame<T: Serialize>(&mut self, frame: &T) -> crate::Result<()> {
        let bytes = bson::to_vec(frame)?;

        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        Ok(())
    }
}
