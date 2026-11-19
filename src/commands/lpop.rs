use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "LPOP",
    arity: -2,
    handler: lpop,
};

fn lpop(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    Ok(Frame::Integer(db.lpop(&args[1])? as i64))
}
