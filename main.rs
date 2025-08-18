#![allow(unused_imports)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // Set listener to port 6379
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    
    loop {
        // Accept client (blocking)
        let (mut socket, _) = listener.accept().await.unwrap();

        // Tokio runtime read-write function
        tokio::spawn(async move {
            let mut buf = vec![0; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(_n) => socket.write_all(b"+PONG\r\n").await.unwrap(),
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
