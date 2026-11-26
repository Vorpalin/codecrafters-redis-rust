use super::{Command, CommandError, parse_u64};
use crate::db::{Db, StreamId};
use crate::resp::Frame;

use std::time::Duration;

pub static COMMAND: Command = Command {
    name: "XREAD",
    arity: -4,
    handler: xread,
};

fn xread(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let mut count = None;
    let mut block = None;

    let mut index = 1;

    while index < args.len() {
        if args[index].eq_ignore_ascii_case(b"STREAMS") {
            break;
        }

        if index + 1 >= args.len() {
            return Err(CommandError::InvalidStreamId);
        }

        if args[index].eq_ignore_ascii_case(b"COUNT") {
            let value = parse_u64(&args[index + 1])?;
            count = Some(usize::try_from(value).map_err(|_| CommandError::InvalidStreamId)?);
        } else if args[index].eq_ignore_ascii_case(b"BLOCK") {
            let milliseconds = parse_u64(&args[index + 1])?;
            block = Some(Duration::from_millis(milliseconds));
        } else {
            return Err(CommandError::InvalidStreamId);
        }

        index += 2;
    }

    if index >= args.len() {
        return Err(CommandError::InvalidStreamId);
    }

    let stream_args = &args[index + 1..];

    if stream_args.is_empty() || stream_args.len() % 2 != 0 {
        return Err(CommandError::InvalidStreamId);
    }

    let stream_count = stream_args.len() / 2;

    let keys = &stream_args[..stream_count];
    let id_args = &stream_args[stream_count..];

    let ids: Vec<StreamId> = if id_args.iter().any(|id| id == b"$") {
        // Si au moins un ID est $, on résout les IDs individuellement.
        let mut ids = Vec::with_capacity(id_args.len());

        for (key, id) in keys.iter().zip(id_args.iter()) {
            if id == b"$" {
                let last_ids = db
                    .xread_last_ids(std::slice::from_ref(key))
                    .map_err(|_| CommandError::WrongType)?;

                ids.push(last_ids[0]);
            } else {
                ids.push(StreamId::parse(id).map_err(|_| CommandError::InvalidStreamId)?);
            }
        }

        ids
    } else {
        id_args
            .iter()
            .map(|id| StreamId::parse(id).map_err(|_| CommandError::InvalidStreamId))
            .collect::<Result<_, _>>()?
    };

    let results = db
        .xread(keys, &ids, count, block)
        .map_err(|error| match error {
            crate::db::WrongType => CommandError::WrongType,
        })?;

    if results.is_empty() {
        return Ok(Frame::NullArray);
    }

    Ok(Frame::Array(
        results
            .into_iter()
            .map(|(stream_key, messages)| {
                Frame::Array(vec![
                    Frame::Bulk(stream_key),
                    Frame::Array(
                        messages
                            .into_iter()
                            .map(|(id, fields)| {
                                let mut field_list = Vec::with_capacity(fields.len() * 2);

                                for (field, value) in fields {
                                    field_list.push(Frame::Bulk(field));
                                    field_list.push(Frame::Bulk(value));
                                }

                                Frame::Array(vec![
                                    Frame::Bulk(id.to_string().into_bytes()),
                                    Frame::Array(field_list),
                                ])
                            })
                            .collect(),
                    ),
                ])
            })
            .collect(),
    ))
}
