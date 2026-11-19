use std::net::TcpListener;

mod commands;
mod execution_loop;

use execution_loop::exec_loop;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");

                let _ = exec_loop(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
