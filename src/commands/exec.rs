use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "EXEC",
    arity: 1,
    handler: exec,
};

pub fn exec(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let multi = db.multi();
    let res = db.exec(multi).map_err(|error| match error {
        crate::db::ExecError::WrongType => CommandError::WrongType,
    })?;
    Ok(Frame::Array(
        res.into_iter()
            .map(|r| match r {
                Ok(frame) => frame,
                Err(e) => Frame::Error(e.to_string()),
            })
            .collect(),
    ))
}
