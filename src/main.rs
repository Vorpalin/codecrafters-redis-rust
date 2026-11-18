
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

mod commands {
    pub mod ping;
}

use commands::ping::ping;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");

                let mut reader = BufReader::new(stream.try_clone()?);

                let mut line = String::new();
                reader.read_line(&mut line)?;

                if line.starts_with('*') {
                    let mut length = String::new();
                    reader.read_line(&mut length)?;

                    let mut command = String::new();
                    reader.read_line(&mut command)?;

                    if command.trim() == "PING" {
                        stream.write_all(ping())?;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
