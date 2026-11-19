use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "LPUSH",
    arity: -3,
    handler: lpush,
};

fn lpush(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    Ok(Frame::Integer(
        db.lpush(&args[1], (&args[2..]).to_vec())? as i64
    ))
}
