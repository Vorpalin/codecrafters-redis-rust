use std::io;

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::commands;
use crate::db::Db;
use crate::resp;

type Error = Box<dyn std::error::Error + Send + Sync>;

pub async fn handle_client(stream: TcpStream, db: Db) -> Result<(), Error> {
    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut out = Vec::new();

    loop {
        let args = match resp::read_command(&mut reader).await {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()), // client closed the connection
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                writer.write_all(b"-ERR protocol error\r\n").await?;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        if args.is_empty() {
            continue;
        }

        out.clear();
        commands::dispatch(&db, &args).encode(&mut out);
        writer.write_all(&out).await?;
    }
}
