mod db;
mod config;
mod client;
mod commands;

use std::env;
use std::error::Error;
use std::sync::Arc;
use std::io::{Read, Write};
use std::net::TcpStream;
use tokio::sync::{mpsc::unbounded_channel, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use crate::db::*;
use crate::config::*;
use crate::client::{get_next_id, Client, Response};
use crate::commands::Command;

fn send_and_verify(
    stream: &mut TcpStream, to_write: Vec<u8>, expected: Vec<u8>, error: &str
)-> Result<(), Box<dyn Error>> {
    let mut buf = [0; 256];

    // Write/read to/from stream + verify reponse
    stream.write_all(&to_write)?;
    let bytes_read = stream.read(&mut buf)?;
    if buf[..bytes_read] != expected[..] && !buf[..bytes_read].starts_with(&expected) {
        return Err(error.into());
    }

    Ok(())
}

fn send_handshake(
    master_addr: &String, master_port: u16, port: u16
) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(format!("{master_addr}:{master_port}"))?;

    // (1) PING
    send_and_verify(
        &mut stream,
        Response::Ping.into(),
        Response::Pong.into(),
        "'PING' -> Master"
    )?;
    // (2) REPLCONF listening-port <PORT>
    send_and_verify(
        &mut stream,
        format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port}\r\n"
        ).into_bytes(),
        Response::Ok.into(),
        "'REPLCONF listening-port' -> Master"
    )?;
    // (3) REPLCONF capa psync2
    send_and_verify(
        &mut stream,
        format!(
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        ).into_bytes(),
        Response::Ok.into(),
        "'REPLCONF capa' -> Master"
    )?;
    // (4) PSYNC ? -1
    send_and_verify(
        &mut stream,
        format!(
            "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        ).into_bytes(),
        "+FULLRESYNC".to_string().into_bytes(),
        "'PSYNC' -> Master"
    )?;
    
    Ok(())
}

async fn process_cmd(cmd: &[u8], client: &Client, config: Config, repl_state: ReplState,
                     db: Database, blocked_clients: BlockedClients) {
    let mut cmd: Command = match Command::from(cmd) {
        Some(cmd) => cmd,
        None => { // Empty command
            client.tx.send(Response::ErrEmptyCommand.into()).unwrap();
            return;
        }
    };

    cmd.execute(&client, config, repl_state, db, blocked_clients).await; 
}

#[tokio::main]
async fn main() {
    // Set server configuration + DBs
    let config = Arc::new(Config_::from(env::args().skip(1).collect()));
    let repl_state = ReplState::default();
    let db = Database::default();
    let blocked_clients = BlockedClients::default();

    if config.is_master {
        // Set a Replicas object in ReplState
        let mut replicas = repl_state.replicas.lock().await;
        *replicas = Some(Vec::new());
    } else { // Replica
        // Initiate handshake with master
        if let Err(e) = send_handshake(
            config.master_addr.as_ref().unwrap(),
            config.master_port.unwrap(),
            config.port
        ) {
            eprintln!("Error: {}", e);
            return;
        }
    }

    // Set listener
    let listener = TcpListener::bind(format!(
        "{}:{}", config.bind_addr, config.port
    )).await.unwrap();

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
        let tokio_config = config.clone();
        let tokio_repl_state = repl_state.clone();
        let tokio_db = db.clone();
        let tokio_blocked_clients = blocked_clients.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 1024];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => process_cmd(
                        &buf[0..n],
                        &client,
                        tokio_config.clone(),
                        tokio_repl_state.clone(),
                        tokio_db.clone(),
                        tokio_blocked_clients.clone()
                    ).await,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
