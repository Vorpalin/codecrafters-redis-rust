#![allow(unused_imports)]
use std::net::TcpListener;

mod commands {
    pub mod ping;
}

use commands::ping::ping;
use std::io::Write;
use std::io::Read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(ref _stream) => {
                println!("accepted new connection");

                let mut response = String::new();
                stream.as_ref().expect("REASON").read_to_string(&mut response)?;

                if response == "PING" {
                    stream.expect("REASON").write_all(ping()).unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
