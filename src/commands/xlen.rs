use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "XLEN",
    arity: -2,
    handler: xlen,
};

fn xlen(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let length = db.xlen(&args[1]).map_err(|error| match error {
        crate::db::WrongType => CommandError::WrongType,
    })?;

    Ok(Frame::Integer(length as i64))
}
