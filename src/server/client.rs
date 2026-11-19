use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::commands::{ping, echo};

type Error = Box<dyn std::error::Error + Send + Sync>;


fn parse_header(line: &str, prefix: char) -> Option<usize> {
    line.strip_prefix(prefix)?.trim_end().parse().ok()
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
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(count);
        for _ in 0..count {
            line.clear();
            if reader.read_line(&mut line).await? == 0 {
                return Ok(());
            }

            let Some(len) = parse_header(&line, '$') else {
                writer.write_all(b"-ERR protocol error\r\n").await?;
                return Ok(());
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
                    writer.write_all(&echo::echo(&String::from_utf8_lossy(message))).await?;
                } else {
                    writer.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n").await?;
                }
            }
            _ => writer.write_all(b"-ERR unknown command\r\n").await?,
        }
    }
}
