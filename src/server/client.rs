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

    let mut transaction = None;

    loop {
        let args = match resp::read_command(&mut reader).await {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
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

        let db = db.clone();

        let (frame, updated_transaction) = tokio::task::spawn_blocking(move || {
            let frame = commands::dispatch(&db, &args, &mut transaction);
            (frame, transaction)
        })
        .await?;

        transaction = updated_transaction;

        frame.encode(&mut out);
        writer.write_all(&out).await?;
    }
}
