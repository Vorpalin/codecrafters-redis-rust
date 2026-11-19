use std::time::Duration;

use super::{Command, CommandError};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "BLPOP",
    arity: -3,
    handler: blpop,
};

fn blpop(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let timeout = std::str::from_utf8(args.last().unwrap())
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|timeout| timeout.is_finite() && *timeout >= 0.0)
        .and_then(|timeout| Duration::try_from_secs_f64(timeout).ok())
        .ok_or(CommandError::NotInteger)?;

    let keys = &args[1..args.len() - 1];

    match db.blpop(keys, timeout)? {
        Some((key, value)) => Ok(Frame::Array(vec![Frame::Bulk(key), Frame::Bulk(value)])),
        None => Ok(Frame::Null),
    }
}
