use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "LLEN",
    arity: -2,
    handler: llen,
};

fn llen(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    Ok(Frame::Integer(db.llen(&args[1])? as i64))
}
