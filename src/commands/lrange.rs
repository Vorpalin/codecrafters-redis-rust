use super::{Command, CommandError};
use crate::commands::parse_i64;
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "LRANGE",
    arity: 4,
    handler: lrange,
};

fn lrange(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let start = parse_i64(&args[2])?;
    let stop = parse_i64(&args[3])?;

    let items = db.lrange(&args[1], start, stop)?;

    Ok(Frame::Array(items.into_iter().map(Frame::Bulk).collect()))
}
