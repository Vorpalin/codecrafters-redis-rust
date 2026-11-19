use tokio::net::TcpListener;

use crate::db::Db;
use crate::server::client::handle_client;

pub async fn run(address: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;
    let db = Db::default();

    loop {
        let (socket, address) = listener.accept().await?;

        println!("New client: {address}");

        let db = db.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_client(socket, db).await {
                eprintln!("Client error: {error}");
            }
        });
    }
}
