use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "GET",
    arity: 2,
    handler: get,
};

fn get(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    Ok(db.get(&args[1]).map_or(Frame::Null, Frame::Bulk))
}
