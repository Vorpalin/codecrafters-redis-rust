use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "INCR",
    arity: 2,
    handler: incr,
};

pub fn incr(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let key = &args[1];

    let new_value = db.incr(key).map_err(|error| match error {
        crate::db::IncrError::WrongType => CommandError::WrongType,
        crate::db::IncrError::InvalidInteger => CommandError::NotInteger,
    })?;

    Ok(Frame::Integer(new_value))
}
