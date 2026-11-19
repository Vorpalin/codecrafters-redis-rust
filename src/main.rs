mod commands;
mod server;

use crate::server::server::run;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run("127.0.0.1:6379").await?;

    Ok(())
}
