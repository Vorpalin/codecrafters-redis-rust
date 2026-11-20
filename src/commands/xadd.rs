use super::{Command, CommandError};
use crate::db::{Db};
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "XADD",
    arity: -4,
    handler: xadd,
};

fn xadd(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let key = &args[1];
    let id = args.get(2).map(|s| StreamId {
        milliseconds: s.parse().unwrap_or(0),
        sequence: 0,
    });
    let fields = args[3..]
        .chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();
    let new_id = db.xadd(key, id, fields)?;
    Ok(Frame::Simple(new_id.to_string()))
}
