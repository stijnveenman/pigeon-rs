mod ping;
pub use ping::Ping;

pub use create_topic::CreateTopic;
mod create_topic;

mod produce;
pub use produce::Produce;

mod unknown;
pub use unknown::Unknown;

mod fetch;
pub use fetch::Fetch;

use crate::{db::Db, parse::Parse, Connection, Frame};

#[derive(Debug)]
pub enum Command {
    CreateTopic(CreateTopic),
    Produce(Produce),
    Fetch(Fetch),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ctopic" => Command::CreateTopic(CreateTopic::parse_frames(&mut parse)?),
            "produce" => Command::Produce(Produce::parse_frames(&mut parse)?),
            "fetch" => Command::Fetch(Fetch::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;

        Ok(command)
    }

    pub(crate) async fn apply(self, db: &mut Db, dst: &mut Connection) -> crate::Result<()> {
        use Command::*;

        match self {
            CreateTopic(cmd) => cmd.apply(db, dst).await,
            Produce(cmd) => cmd.apply(db, dst).await,
            Fetch(cmd) => cmd.apply(db, dst).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }
}
