mod ping;
pub use ping::Ping;

use crate::{db::Db, shutdown::Shutdown, Connection};

#[derive(Debug)]
pub enum Response {
    Success,
}

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
}

impl Command {
    pub(crate) async fn apply(
        self,
        _db: &mut Db,
        dst: &mut Connection,
        _shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Ping(cmd) => cmd.apply(dst).await,
        }
    }
}
