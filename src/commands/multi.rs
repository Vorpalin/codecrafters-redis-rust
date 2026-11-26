use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "MULTI",
    arity: 1,
    handler: multi,
};

pub fn multi(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let multi = db.multi();
    Ok(Frame::Simple("OK".into()))
}
