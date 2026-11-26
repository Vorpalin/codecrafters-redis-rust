use super::{Command, CommandError};
use crate::db::{Db, StreamId, WrongType};
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "XRANGE",
    arity: -4,
    handler: xrange,
};

fn xrange(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let start = StreamId::parse(&args[2]).map_err(|_| CommandError::InvalidStreamId)?;
    let end = StreamId::parse(&args[3]).map_err(|_| CommandError::InvalidStreamId)?;

    let entries = db
        .xrange(&args[1], start, end)
        .map_err(|error| match error {
            WrongType => CommandError::WrongType,
        })?;

    let mut response = Vec::with_capacity(entries.len());
    for (id, fields) in entries {
        let mut entry = Vec::with_capacity(2);
        entry.push(Frame::Bulk(id.to_string().into_bytes()));
        let mut field_list = Vec::with_capacity(fields.len() * 2);
        for (field, value) in fields {
            field_list.push(Frame::Bulk(field));
            field_list.push(Frame::Bulk(value));
        }
        entry.push(Frame::Array(field_list));
        response.push(Frame::Array(entry));
    }

    Ok(Frame::Array(response))
}
