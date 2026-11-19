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

                loop {
                    let mut line = String::new();

                    // Read the array header
                    if reader.read_line(&mut line)? == 0 {
                        break;
                    }

                    if !line.starts_with('*') {
                        break;
                    }

                    // Read the bulk string header
                    let mut length = String::new();
                    reader.read_line(&mut length)?;

                    // Read the command
                    let mut command = String::new();
                    reader.read_line(&mut command)?;

                    match command.trim() {
                        "PING" => {
                            stream.write_all(b"+PONG\r\n")?;
                        }
                        _ => {
                            stream.write_all(b"-ERR unknown command\r\n")?;
                        }
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
