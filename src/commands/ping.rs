use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "PING",
    arity: -1,
    handler: ping,
};

fn ping(_db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    Ok(match args.get(1) {
        Some(message) => Frame::Bulk(message.clone()),
        None => Frame::Simple("PONG".into()),
    })
}
