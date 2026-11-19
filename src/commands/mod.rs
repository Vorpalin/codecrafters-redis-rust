use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use crate::db::{Db, WrongType};
use crate::resp::Frame;

pub mod echo;
pub mod get;
pub mod llen;
pub mod lpush;
pub mod lrange;
pub mod ping;
pub mod rpush;
pub mod set;

/// Every command has this shape. `args[0]` is the command name itself.
/// Arity has already been validated when the handler runs.
pub type Handler = fn(&Db, &[Vec<u8>]) -> Result<Frame, CommandError>;

pub struct Command {
    pub name: &'static str,
    /// Redis convention, counting the command name itself:
    /// `n >= 0` means exactly `n` args, `n < 0` means at least `|n|` args.
    pub arity: i32,
    pub handler: Handler,
}

impl Command {
    fn arity_ok(&self, argc: usize) -> bool {
        if self.arity >= 0 {
            argc == self.arity as usize
        } else {
            argc >= self.arity.unsigned_abs() as usize
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandError {
    Syntax,
    NotInteger,
    WrongType,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax => write!(f, "ERR syntax error"),
            Self::NotInteger => write!(f, "ERR value is not an integer or out of range"),
            Self::WrongType => write!(
                f,
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
        }
    }
}

impl std::error::Error for CommandError {}

impl From<WrongType> for CommandError {
    fn from(_: WrongType) -> Self {
        CommandError::WrongType
    }
}

/// To add a command: create `commands/<name>.rs` exporting a `COMMAND`,
/// then list it here. Nothing else needs to change.
static REGISTRY: LazyLock<HashMap<&'static str, &'static Command>> = LazyLock::new(|| {
    [
        &ping::COMMAND,
        &echo::COMMAND,
        &get::COMMAND,
        &set::COMMAND,
        &rpush::COMMAND,
        &lrange::COMMAND,
        &lpush::COMMAND,
        &llen::COMMAND,
    ]
    .into_iter()
    .map(|c| (c.name, c))
    .collect()
});

/// Looks up the command, validates arity, runs it, and turns errors into RESP errors.
/// `args` must not be empty.
pub fn dispatch(db: &Db, args: &[Vec<u8>]) -> Frame {
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();

    let Some(cmd) = REGISTRY.get(name.as_str()) else {
        return Frame::Error(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(&args[0])
        ));
    };

    if !cmd.arity_ok(args.len()) {
        return Frame::Error(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd.name.to_ascii_lowercase()
        ));
    }

    match (cmd.handler)(db, args) {
        Ok(frame) => frame,
        Err(e) => Frame::Error(e.to_string()),
    }
}

/// Shared argument helper.
pub fn parse_u64(arg: &[u8]) -> Result<u64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

fn parse_i64(arg: &[u8]) -> Result<i64, CommandError> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::NotInteger)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn run(db: &Db, parts: &[&str]) -> Frame {
        let args: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        dispatch(db, &args)
    }

    fn err(msg: &str) -> Frame {
        Frame::Error(msg.into())
    }

    #[test]
    fn ping_and_echo() {
        let db = Db::default();
        assert_eq!(run(&db, &["ping"]), Frame::Simple("PONG".into()));
        assert_eq!(run(&db, &["PING", "yo"]), Frame::Bulk(b"yo".to_vec()));
        assert_eq!(run(&db, &["ECHO", "hey"]), Frame::Bulk(b"hey".to_vec()));
    }

    #[test]
    fn set_and_get() {
        let db = Db::default();
        assert_eq!(run(&db, &["GET", "k"]), Frame::Null);
        assert_eq!(run(&db, &["SET", "k", "v"]), Frame::ok());
        assert_eq!(run(&db, &["get", "k"]), Frame::Bulk(b"v".to_vec()));
    }

    #[test]
    fn set_with_px_expires() {
        let db = Db::default();
        assert_eq!(run(&db, &["SET", "k", "v", "px", "20"]), Frame::ok());
        assert_eq!(run(&db, &["GET", "k"]), Frame::Bulk(b"v".to_vec()));
        std::thread::sleep(std::time::Duration::from_millis(40));
        assert_eq!(run(&db, &["GET", "k"]), Frame::Null);
    }

    #[test]
    fn get_on_a_list_is_wrongtype() {
        let db = Db::default();
        db.rpush(b"l", vec![b"a".to_vec()]).unwrap();
        assert_eq!(
            run(&db, &["GET", "l"]),
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        );
    }

    #[test]
    fn errors() {
        let db = Db::default();
        assert_eq!(run(&db, &["NOPE"]), err("ERR unknown command 'NOPE'"));
        assert_eq!(
            run(&db, &["GET"]),
            err("ERR wrong number of arguments for 'get' command")
        );
        assert_eq!(
            run(&db, &["SET", "k"]),
            err("ERR wrong number of arguments for 'set' command")
        );
        assert_eq!(run(&db, &["SET", "k", "v", "PX"]), err("ERR syntax error"));
        assert_eq!(
            run(&db, &["SET", "k", "v", "BOGUS"]),
            err("ERR syntax error")
        );
        assert_eq!(
            run(&db, &["SET", "k", "v", "PX", "abc"]),
            err("ERR value is not an integer or out of range")
        );
    }
}
