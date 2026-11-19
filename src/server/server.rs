use tokio::net::TcpListener;

use crate::server::client::handle_client;

pub async fn run(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let (socket, address) = listener.accept().await?;

        println!("New client: {address}");

        tokio::spawn(async move {
            if let Err(error) = handle_client(socket).await {
                eprintln!("Client error: {error}");
            }
        });
    }
}
