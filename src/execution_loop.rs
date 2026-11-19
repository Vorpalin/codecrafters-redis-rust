use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use crate::commands::ping;

pub fn exec_loop(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(stream.try_clone()?);

    loop {
        let mut line = String::new();

        // Read the array header
        if reader.read_line(&mut line)? == 0 {
            break Ok(());
        }

        if !line.starts_with('*') {
            break Ok(());
        }

        // Read the bulk string header
        let mut length = String::new();
        reader.read_line(&mut length)?;

        // Read the command
        let mut command = String::new();
        reader.read_line(&mut command)?;

        match command.trim() {
            "PING" => {
                stream.write_all(ping::ping())?;
            }
            _ => {
                stream.write_all(b"-ERR unknown command\r\n")?;
            }
        }
    }
}
