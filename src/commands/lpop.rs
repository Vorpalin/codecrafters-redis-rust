use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

use crate::commands::parse_u64;

pub static COMMAND: Command = Command {
    name: "LPOP",
    arity: -2,
    handler: lpop,
};

fn lpop(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let count = if args.len() == 3 {
        Some(parse_u64(&args[2])? as usize)
    } else {
        None
    };

    match db.lpop(&args[1], count)? {
        None => Ok(Frame::Null),
        Some(items) if count.is_none() => Ok(Frame::Bulk(items.into_iter().next().unwrap())),
        Some(items) => Ok(Frame::Array(items.into_iter().map(Frame::Bulk).collect())),
    }
}
