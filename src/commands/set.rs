use std::time::Duration;

use super::{Command, CommandError, parse_u64};
use crate::db::Db;
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "SET",
    arity: -3,
    handler: set,
};

fn set(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let mut ttl = None;

    let mut options = args[3..].iter();
    while let Some(option) = options.next() {
        match option.to_ascii_uppercase().as_slice() {
            b"PX" => {
                let ms = parse_u64(options.next().ok_or(CommandError::Syntax)?)?;
                ttl = Some(Duration::from_millis(ms));
            }
            b"EX" => {
                let secs = parse_u64(options.next().ok_or(CommandError::Syntax)?)?;
                ttl = Some(Duration::from_secs(secs));
            }
            _ => return Err(CommandError::Syntax),
        }
    }

    db.set(args[1].clone(), args[2].clone(), ttl);
    Ok(Frame::ok())
}
