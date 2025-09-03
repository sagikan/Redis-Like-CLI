mod db;
mod config;
mod bundle;
mod rdb;
mod client;
#[path = "commands/__command__.rs"]
mod commands;

use std::{env, str, error::Error, sync::Arc};
use tokio::{sync::{mpsc::unbounded_channel, Mutex}, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use crate::db::Database;
use crate::config::{Config, ReplState};
use crate::bundle::Bundle;
use crate::rdb::RDBFile;
use crate::client::{get_next_id, Client_, Client, Response};
use crate::commands::Command;

static SML_BUFSIZE: usize = 256;
static BIG_BUFSIZE: usize = 1024;

async fn send_and_verify(
    stream: &mut TcpStream, to_write: Vec<u8>, expected: Vec<u8>, error: &str
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; SML_BUFSIZE];

    // Write/read to/from stream + verify response
    stream.write_all(&to_write).await?;
    let bytes_read = stream.read(&mut buf).await?;
    if buf[..bytes_read] != expected[..] {
        return Err(error.into());
    }

    Ok(())
}

async fn send_and_process_psync(
    stream: &mut TcpStream, repl_state: ReplState, db: Database
) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
    let mut buf = vec![0; BIG_BUFSIZE];

    // Write/read to/from stream + verify reponse
    stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    let mut n = stream.read(&mut buf).await?;
    if !buf[..n].starts_with(b"+FULLRESYNC") {
        return Err("'PSYNC' -> Master".into());
    }

    // Process FULLRESYNC
    let resync_end = match buf[..n].windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => { return Err("Resync failed".into()); }
    };
    let split: Vec<&str> = str::from_utf8(&buf[..resync_end])?
        .trim() // Remove \r\n
        .split(' ')
        .collect();
    let (master_replid, master_repl_offset) = match (split.get(1), split.get(2)) {
        (Some(id), Some(offset)) => (id.to_string(), offset.parse::<usize>()?),
        _ => { return Err("Resync failed".into()); }
    };

    { // Update replica's ReplState
        let mut state_guard = repl_state.lock().await;
        state_guard.replid = master_replid.to_string();
        state_guard.repl_offset = master_repl_offset;
    }
    
    // Process RDB file
    let rdb_start = resync_end + 2;
    let rdb_end = loop { // Try processing
        match process_rdb(&buf[rdb_start..n], db.clone()).await {
            Ok(rel_end) => break rdb_start + rel_end, // Absolute end value
            _ => { // Read more data
                buf.resize(n + BIG_BUFSIZE, 0);
                match stream.read(&mut buf[n..]).await {
                    Ok(0) => return Err("Resync failed".into()),
                    Ok(m) => n += m, // Will now process more data
                    Err(e) => return Err(Box::new(e))
                }
            }
        }
    };

    // Return attached data (if exists)
    let attached = if rdb_end < n {
        Some(buf[rdb_end..n].to_vec())
    } else {
        None
    };

    Ok(attached)
}

async fn send_handshake(
    config: Config, repl_state: ReplState, db: Database
) -> Result<(TcpStream, Option<Vec<u8>>), Box<dyn Error + Send + Sync>> {
    let master_addr = config.master_addr.as_ref().unwrap();
    let master_port = config.master_port.unwrap();
    let port = config.port;

    let mut stream = TcpStream::connect(
        format!("{master_addr}:{master_port}")
    ).await?;

    // (1) PING
    send_and_verify(
        &mut stream,
        Response::Ping.into(),
        Response::Pong.into(),
        "'PING' -> Master"
    ).await?;

    // (2) REPLCONF listening-port <PORT>
    send_and_verify(
        &mut stream,
        format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port}\r\n"
        ).into_bytes(),
        Response::Ok.into(),
        "'REPLCONF listening-port' -> Master"
    ).await?;

    // (3) REPLCONF capa psync2
    send_and_verify(
        &mut stream,
        format!(
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        ).into_bytes(),
        Response::Ok.into(),
        "'REPLCONF capa' -> Master"
    ).await?;

    // (4) PSYNC ? -1
    let attached_data = send_and_process_psync(&mut stream, repl_state, db).await?;

    Ok((stream, attached_data))
}

async fn process_rdb(
    rdb: &[u8], db: Database
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    let n = rdb.len();
    if let Some(d_start) = rdb[..n].iter().position(|&c| c == b'$') {
        if let Some(d_end) = rdb[d_start+1..n].windows(2).position(|w| w == b"\r\n") {
            // Extract length of RDB file
            let rdb_len: usize = str::from_utf8(
                &rdb[d_start+1..d_start+1+d_end]
            ).unwrap().parse().unwrap();

            let rdb_start = d_start + d_end + 3; // '$' + header + '\r\n'
            let rdb_end = rdb_start + rdb_len;
            if n < rdb_end {
                return Err("Buffer too small".into());
            }

            let rdb_file = RDBFile::from_vec(rdb[rdb_start..rdb_end].to_vec())?;
            // Insert RDB entries into the existing DB
            db.lock().await.extend(
                Database::from(rdb_file).await?.inner().lock().await.iter().map(
                    |(k, v)| (k.clone(), v.clone())
                )
            );

            return Ok(rdb_end);
        }
    }

    Err("Resync failed".into())
}

async fn process_cmd(
    cmd: &[u8], is_propagated: bool, client: &Client, bundle: Bundle
) {
    // Parse
    let mut cmd: Command = match Command::from(cmd, is_propagated) {
        Some(cmd) => cmd,
        None => { // Empty command
            client.tx.send(Response::ErrEmptyCommand.into()).unwrap();
            return;
        }
    };

    cmd.execute(&client, bundle).await; 
}

async fn process_cmd_block(start: usize, end: usize, buf: &[u8], client: &Client, bundle: Bundle) {
    let mut cmd_start = start;

    while cmd_start < end {
        let cmd_len = match buf[cmd_start..end].windows(4).position(
            |w| w[..3].to_vec() == b"\r\n*" && w[3].is_ascii_digit()
        ) {
            Some(pos) => pos + 2, // Count \r\n too
            None => end - cmd_start // Relative end of buffer
        };

        let cmd = &buf[cmd_start..cmd_start+cmd_len];
        process_cmd(cmd, true, client, bundle.clone()).await;

        cmd_start += cmd_len;
    }
}

async fn run_server(listener: TcpListener, bundle: Bundle) {
    loop {
        // Accept client
        let (socket, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let client = Arc::new(Client_ {
            id: get_next_id(),
            tx,
            in_transaction: Some(Arc::new(Mutex::new(false))),
            in_sub_mode: Some(Arc::new(Mutex::new(false))),
            queued_commands: Some(Arc::new(Mutex::new(Vec::new()))),
            subs: Some(Arc::new(Mutex::new(Vec::new())))
        });

        // Tokio-runtime write task
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                // Write + check for disconnection
                if writer.write_all(&msg).await.is_err() { break; }
            }
        });

        // Tokio-runtime read task
        let tokio_bundle = bundle.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; BIG_BUFSIZE];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => process_cmd(&buf[..n], false, &client, tokio_bundle.clone()).await,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        return;
                    }
                }
            }
        });
    }
}

fn run_replica(master_stream: TcpStream, attached_data: Option<Vec<u8>>, bundle: Bundle) {
    let (mut reader, mut writer) = master_stream.into_split();
    let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
    let client = Arc::new(Client_ { // Dummy
        id: 0,
        tx,
        in_transaction: None,
        in_sub_mode: None,
        queued_commands: None,
        subs: None
    });

    // Tokio-runtime write task (for ACKs)
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            // Write to master + check for disconnection
            if writer.write_all(&msg).await.is_err() { break; }
        }
    });

    // Tokio-runtime read task
    tokio::spawn(async move {
        let mut buf = vec![0; BIG_BUFSIZE];

        // Process propagated commands received during handshake
        if let Some(cmd_block) = attached_data {
            if let Some(star) = cmd_block.iter().position(|&c| c == b'*') {
                process_cmd_block(
                    star, cmd_block.len(), // Start + End
                    &cmd_block, &client, bundle.clone()
                ).await;
            }
        }

        loop {
            // Process propagated commands
            match reader.read(&mut buf).await {
                Ok(0) => return, // Connection closed
                Ok(n) => process_cmd_block(
                    0, n, // Start + End
                    &buf, &client, bundle.clone()
                ).await, Err(e) => {
                    eprintln!("Error: {e}");
                    return;
                }
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Set server configuration + DBs
    let bundle = Bundle::from(env::args()).await?;

    // Set listener
    let listener = TcpListener::bind(format!(
        "{}:{}", bundle.config.bind_addr, bundle.config.port
    )).await?;

    // Run server-side
    let tokio_bundle = bundle.clone();
    let server_handler = tokio::spawn(async move {
        run_server(listener, tokio_bundle).await;
    });

    // Run replica-side
    if !bundle.config.is_master {
        // Initiate handshake with master
        match send_handshake(
            bundle.config.clone(),
            bundle.repl_state.clone(),
            bundle.db.clone()
        ).await {
            Ok((stream, attached_data)) => run_replica(stream, attached_data, bundle),
            Err(e) => {
                eprintln!("Error: {e}");
            }
        }
    }

    server_handler.await?; // Keep-Alive
    
    unreachable!()
}
