mod ping;
use std::net::Shutdown;

pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{parse::Parse, Connection, Frame};

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;

        Ok(command)
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
