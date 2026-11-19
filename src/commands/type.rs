use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "TYPE",
    arity: -2,
    handler: r#type,
};

fn r#type(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let key = &args[1];
    let type_name = db.r#type(key)?;
    Ok(Frame::Simple(type_name))
}
