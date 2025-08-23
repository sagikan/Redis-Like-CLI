mod db;
mod commands;

use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use crate::db::*;
use crate::commands::Command;

fn parse_resp(to_parse: &[u8]) -> Option<Command> {
    let unparsed_str = std::str::from_utf8(to_parse).unwrap();
    let mut lines = unparsed_str.split("\r\n");

    lines.next(); // Skip array header

    let mut parsed = Vec::new();
    while let Some(curr_line) = lines.next() {
        // Skip non-bulk-string-length lines
        if !curr_line.starts_with('$') { continue; }
        // Get length and add next line to parsed Vec
        let len = curr_line[1..].parse().unwrap();
        if let Some(val) = lines.next() {
            parsed.push(val[..len].to_string());
        }
    }

    let args = match parsed.len() {
        0 => { return None; } // Empty command
        1 => None,
        _ => Some(Vec::from(parsed[1..].to_vec()))
    };

    Some(Command {
        name: parsed[0].clone(),
        args
    })
}

async fn process_cmd(cmd: &[u8], client: &Client, db: Database,
                     blocked_clients: BlockedClients) {
    let mut cmd: Command = match parse_resp(cmd) {
        Some(cmd) => cmd,
        None => { // Empty command
            client.tx.send(b"-ERR unknown command ''".to_vec()).unwrap();
            return;
        }
    };

    cmd.execute(&client, db, blocked_clients).await; 
}

#[tokio::main]
async fn main() {
    // Set listener to port 6379 + maps
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Database::default();
    let blocked_clients = BlockedClients::default();
    
    // Event loop
    loop {
        // Accept client
        let (socket, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = unbounded_channel();
        let client = Client {
            id: get_next_id(),
            tx,
            in_transaction: Arc::new(Mutex::new(false)),
            queued_commands: Arc::new(Mutex::new(Vec::new()))
        };

        // Tokio-runtime write task
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                // Write + check for disconnection
                if writer.write_all(&msg).await.is_err() { break; }
            }
        });

        // Tokio-runtime read task
        let tokio_db = db.clone();
        let tokio_blocked_clients = blocked_clients.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 1024];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => process_cmd(&buf[0..n], &client, tokio_db.clone(),
                                         tokio_blocked_clients.clone()).await,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
