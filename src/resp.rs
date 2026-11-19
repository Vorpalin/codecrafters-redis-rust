use std::io;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Max size of a single bulk string (same limit as Redis: 512 MB).
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Max number of arguments in a single command.
const MAX_ARGS: usize = 1024 * 1024;

/// A RESP value that can be sent back to a client.
// `Integer` and `Array` aren't used by the current commands but are part of RESP.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    pub fn ok() -> Self {
        Frame::Simple("OK".into())
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Frame::Simple(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Frame::Integer(n) => out.extend_from_slice(format!(":{n}\r\n").as_bytes()),
            Frame::Bulk(b) => {
                out.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Frame::Null => out.extend_from_slice(b"$-1\r\n"),
            Frame::Array(items) => {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                for item in items {
                    item.encode(out);
                }
            }
        }
    }
}

fn invalid(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn parse_header(line: &str, prefix: char) -> io::Result<usize> {
    line.strip_prefix(prefix)
        .and_then(|s| s.trim_end().parse().ok())
        .ok_or_else(|| invalid("bad header"))
}

/// Reads one command (an array of bulk strings).
///
/// `Ok(None)` means the peer closed the connection cleanly.
/// `ErrorKind::InvalidData` means a protocol error.
pub async fn read_command<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<Vec<Vec<u8>>>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }

    let count = parse_header(&line, '*')?;
    if count > MAX_ARGS {
        return Err(invalid("too many arguments"));
    }

    let mut args = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        let len = parse_header(&line, '$')?;
        if len > MAX_BULK_LEN {
            return Err(invalid("bulk string too large"));
        }

        // `take` avoids pre-allocating `len` bytes based on an untrusted header.
        let mut buf = Vec::new();
        (&mut *reader)
            .take(len as u64 + 2)
            .read_to_end(&mut buf)
            .await?;
        if buf.len() != len + 2 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf.truncate(len);
        args.push(buf);
    }

    Ok(Some(args))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(frame: Frame) -> Vec<u8> {
        let mut out = Vec::new();
        frame.encode(&mut out);
        out
    }

    #[test]
    fn encodes_frames() {
        assert_eq!(encoded(Frame::ok()), b"+OK\r\n");
        assert_eq!(encoded(Frame::Error("ERR x".into())), b"-ERR x\r\n");
        assert_eq!(encoded(Frame::Integer(-3)), b":-3\r\n");
        assert_eq!(encoded(Frame::Bulk(b"hey".to_vec())), b"$3\r\nhey\r\n");
        assert_eq!(encoded(Frame::Null), b"$-1\r\n");
        assert_eq!(
            encoded(Frame::Array(vec![Frame::Integer(1), Frame::Null])),
            b"*2\r\n:1\r\n$-1\r\n"
        );
    }

    #[tokio::test]
    async fn parses_command() {
        let mut input: &[u8] = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let args = read_command(&mut input).await.unwrap().unwrap();
        assert_eq!(args, vec![b"ECHO".to_vec(), b"hey".to_vec()]);
        assert!(read_command(&mut input).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_garbage() {
        let mut input: &[u8] = b"PING\r\n";
        let err = read_command(&mut input).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
