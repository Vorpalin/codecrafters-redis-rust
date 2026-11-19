use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use crate::commands::{echo, get, ping, set};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Max size of a single bulk string (same limit as Redis: 512 MB).
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;

// A plain std Mutex is fine here as long as the guard is never held across an `.await`.
// The command functions lock internally and return, so that holds.
static STORE: LazyLock<Mutex<HashMap<String, (String, i128)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn parse_header(line: &str, prefix: char) -> Option<usize> {
    line.strip_prefix(prefix)?.trim_end().parse().ok()
}

fn arg_str(arg: &[u8]) -> String {
    String::from_utf8_lossy(arg).into_owned()
}

fn wrong_args(cmd: &str) -> Vec<u8> {
    format!("-ERR wrong number of arguments for '{cmd}' command\r\n").into_bytes()
}

pub async fn handle_client(stream: TcpStream) -> Result<(), Error> {
    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        // Read the array header, e.g. "*1\r\n"
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(()); // client closed the connection
        }

        let Some(count) = parse_header(&line, '*') else {
            writer.write_all(b"-ERR protocol error\r\n").await?;
            return Ok(());
        };

        // Read each bulk string in the array
        let mut args: Vec<Vec<u8>> = Vec::new();
        for _ in 0..count {
            line.clear();
            if reader.read_line(&mut line).await? == 0 {
                return Ok(());
            }

            let len = match parse_header(&line, '$') {
                Some(len) if len <= MAX_BULK_LEN => len,
                _ => {
                    writer.write_all(b"-ERR protocol error\r\n").await?;
                    return Ok(());
                }
            };

            // Read exactly `len` bytes plus the trailing \r\n
            let mut buf = vec![0u8; len + 2];
            reader.read_exact(&mut buf).await?;
            buf.truncate(len);
            args.push(buf);
        }

        let Some(name) = args.first() else {
            continue;
        };

        match name.to_ascii_uppercase().as_slice() {
            b"PING" => writer.write_all(ping::ping()).await?,
            b"ECHO" => {
                if let Some(message) = args.get(1) {
                    writer.write_all(&echo::echo(&arg_str(message))).await?;
                } else {
                    writer.write_all(&wrong_args("echo")).await?;
                }
            }
            b"SET" => {
                if args.len() < 3 {
                    writer.write_all(&wrong_args("set")).await?;
                } else {
                    // args[1] is the key, args[2] is the value
                    let key = arg_str(&args[1]);
                    let value = arg_str(&args[2]);
                    let mut arguments = Vec::new();
                    for arg in args.iter().skip(3) {
                        arguments.push(arg_str(arg));
                    }
                    writer
                        .write_all(&set::set(&key, &value, arguments, &STORE))
                        .await?;
                }
            }
            b"GET" => {
                if args.len() < 2 {
                    writer.write_all(&wrong_args("get")).await?;
                } else {
                    let key = arg_str(&args[1]);
                    writer.write_all(&get::get(&key, &STORE)).await?;
                }
            }
            _ => writer.write_all(b"-ERR unknown command\r\n").await?,
        }
    }
}
