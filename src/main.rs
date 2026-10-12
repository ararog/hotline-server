use std::error::Error;

use tokio::{signal, sync::watch};

use crate::server::Server;

mod broker;
mod reader;
mod server;
mod subscription;
mod worker;
mod writer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (signal_send, signal_recv) = watch::channel(false);
    let mut server = Server::new();
    let mut shut_signal = signal_recv.clone();
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = shut_signal.changed() => {
                println!("Server stopping")
            }
            res = server.run() => {
                match res {
                    Ok(_) => {
                        println!("Server stopped")
                    }
                    Err(e) => {
                        println!("Server stopped with error: {}", e.to_string())
                    }
                }
            }
        }
    });

    signal::ctrl_c().await?;

    signal_send.send(true)?;

    handle.await?;

    Ok(())
}
