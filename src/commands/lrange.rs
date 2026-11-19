
use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;
use crate::commands::parse_u64;

pub static COMMAND: Command = Command {
    name: "LRANGE",
    arity: 4,
    handler: lrange,
};

fn lrange(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let start = parse_u64(&args[2])? as i64;
    let stop = parse_u64(&args[3])? as i64;

    let items = db.lrange(&args[1], start, stop)?;

    Ok(Frame::Array(
        items.into_iter().map(Frame::Bulk).collect()
    ))
}
