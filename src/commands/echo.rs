use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "ECHO",
    arity: 2,
    handler: echo,
};

fn echo(_db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    Ok(Frame::Bulk(args[1].clone()))
}
