use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "WATCH",
    arity: 2,
    handler: watch,
};

pub fn watch(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let key = &args[1];

    db.watch(&[key.clone()]);

    Ok(Frame::Simple("OK".into()))
}
