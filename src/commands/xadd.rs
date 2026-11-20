use super::{Command, CommandError};
use crate::db::{Db, StreamId, XAddError};
use crate::resp::Frame;

pub static COMMAND: Command = Command {
    name: "XADD",
    // command, key, id, then one or more field/value pairs
    arity: -5,
    handler: xadd,
};

fn xadd(db: &Db, args: &[Vec<u8>]) -> Result<Frame, CommandError> {
    let field_args = &args[3..];
    if field_args.is_empty() || field_args.len() % 2 != 0 {
        return Err(CommandError::Syntax);
    }

    let id = if args[2].as_slice() == b"*" {
        None
    } else {
        Some(StreamId::parse(&args[2]).map_err(|_| CommandError::InvalidStreamId)?)
    };

    let fields = field_args
        .chunks_exact(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect();

    let id = db.xadd(&args[1], id, fields).map_err(|error| match error {
        XAddError::WrongType => CommandError::WrongType,
        XAddError::InvalidId => CommandError::InvalidStreamId,
        XAddError::IdNotGreater => CommandError::StreamIdNotGreater,
        XAddError::ZeroId => CommandError::ZeroStreamId,
    })?;

    Ok(Frame::Bulk(id.to_string().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &Db, args: &[&str]) -> Frame {
        let args = args
            .iter()
            .map(|arg| arg.as_bytes().to_vec())
            .collect::<Vec<_>>();
        super::super::dispatch(db, &args)
    }

    #[test]
    fn xadd_auto_generates_id() {
        let db = Db::default();
        let response = run(&db, &["XADD", "events", "*", "name", "Alice"]);
        assert!(matches!(response, Frame::Bulk(_)));
        assert_eq!(db.r#type(b"events").unwrap(), "stream");
    }

    #[test]
    fn xadd_validates_pairs_and_ids() {
        let db = Db::default();
        assert_eq!(
            run(&db, &["XADD", "s", "*", "field"]),
            Frame::Error("ERR syntax error".into())
        );
        assert_eq!(
            run(&db, &["XADD", "s", "not-an-id", "f", "v"]),
            Frame::Error("ERR Invalid stream ID specified as stream command argument".into())
        );
    }

    #[test]
    fn xadd_explicit_ids_must_increase() {
        let db = Db::default();
        assert!(matches!(
            run(&db, &["XADD", "s", "10-0", "f", "v"]),
            Frame::Bulk(_)
        ));
        assert_eq!(
            run(&db, &["XADD", "s", "10-0", "f", "v"]),
            Frame::Error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .into()
            )
        );
    }
}
