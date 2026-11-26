use super::{Command, CommandError};
use crate::db::{Db, StreamId, WrongType};
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "XRANGE",
    arity: -4,
    handler: xrange,
};

fn parse_range_bound(bytes: &[u8], is_start: bool) -> Result<StreamId, CommandError> {
    match bytes {
        b"-" if is_start => Ok(StreamId::MIN),
        b"+" if !is_start => Ok(StreamId::MAX),
        b"-" | b"+" => Err(CommandError::InvalidStreamId),
        _ => {
            let text = std::str::from_utf8(bytes).map_err(|_| CommandError::InvalidStreamId)?;

            if !text.contains('-') {
                let milliseconds = text
                    .parse::<u64>()
                    .map_err(|_| CommandError::InvalidStreamId)?;

                return Ok(StreamId {
                    milliseconds,
                    sequence: if is_start { 0 } else { u64::MAX },
                });
            }

            StreamId::parse(bytes).map_err(|_| CommandError::InvalidStreamId)
        }
    }
}

fn xrange(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let start = parse_range_bound(&args[2], true)?;
    let end = parse_range_bound(&args[3], false)?;

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
