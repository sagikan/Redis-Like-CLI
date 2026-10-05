use std::{error::Error, str};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc::unbounded_channel};
use crate::{core::{bundle::Bundle, client::{Client_, Response}, config::{Config, ReplState}, db::Database}, runtime::server::process_cmd_block, persistence::rdb::RDBFile};
use crate::runtime::protocol::{read_bulk_response, BIG_BUFSIZE, SML_BUFSIZE};

async fn send_and_verify(
    stream: &mut TcpStream, to_write: Vec<u8>, expected: Vec<u8>, error: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut buf = [0; SML_BUFSIZE];
    stream.write_all(&to_write).await?;
    let bytes_read = tokio::io::AsyncReadExt::read(stream, &mut buf).await?;
    if buf[..bytes_read] != expected[..] { return Err(error.into()); }
    Ok(())
}

async fn process_rdb(rdb: &[u8], db: Database) -> Result<usize, Box<dyn Error + Send + Sync>> {
    let n = rdb.len();
    if let Some(d_start) = rdb[..n].iter().position(|&c| c == b'$') {
        if let Some(d_end) = rdb[d_start + 1..n].windows(2).position(|w| w == b"\r\n") {
            let rdb_len: usize = str::from_utf8(&rdb[d_start + 1..d_start + 1 + d_end])?.parse()?;
            let rdb_start = d_start + d_end + 3;
            let rdb_end = rdb_start + rdb_len;
            if n < rdb_end { return Err("Buffer too small".into()); }
            let rdb_file = RDBFile::from_vec(rdb[rdb_start..rdb_end].to_vec())?;
            db.lock().await.extend(Database::from(rdb_file).await?.inner().lock().await.iter()
                .map(|(k, v)| (k.clone(), v.clone())));
            return Ok(rdb_end);
        }
    }
    Err("Resync failed".into())
}

async fn send_and_process_psync(
    stream: &mut TcpStream, repl_state: ReplState, db: Database,
) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
    let mut buf = vec![0; BIG_BUFSIZE];
    stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    let mut n = tokio::io::AsyncReadExt::read(stream, &mut buf).await?;
    if !buf[..n].starts_with(b"+FULLRESYNC") { return Err("'PSYNC' -> Master".into()); }
    let resync_end = buf[..n].windows(2).position(|w| w == b"\r\n").ok_or("Resync failed")?;
    let split: Vec<&str> = str::from_utf8(&buf[..resync_end])?.trim().split(' ').collect();
    let (master_replid, master_repl_offset) = match (split.get(1), split.get(2)) {
        (Some(id), Some(offset)) => (id.to_string(), offset.parse::<usize>()?),
        _ => return Err("Resync failed".into()),
    };
    {
        let mut state_guard = repl_state.lock().await;
        state_guard.replid = master_replid;
        state_guard.repl_offset = master_repl_offset;
    }
    let rdb_start = resync_end + 2;
    let rdb_end = loop {
        match process_rdb(&buf[rdb_start..n], db.clone()).await {
            Ok(rel_end) => break rdb_start + rel_end,
            Err(_) => {
                buf.resize(n + BIG_BUFSIZE, 0);
                match tokio::io::AsyncReadExt::read(stream, &mut buf[n..]).await {
                    Ok(0) => return Err("Resync failed".into()),
                    Ok(m) => n += m,
                    Err(e) => return Err(Box::new(e)),
                }
            }
        }
    };
    Ok((rdb_end < n).then(|| buf[rdb_end..n].to_vec()))
}

pub async fn send_handshake(
    config: Config, repl_state: ReplState, db: Database,
) -> Result<(TcpStream, Option<Vec<u8>>), Box<dyn Error + Send + Sync>> {
    let master_addr = config.master_addr.as_ref().unwrap();
    let master_port = config.master_port.unwrap();
    let mut stream = TcpStream::connect(format!("{master_addr}:{master_port}")).await?;
    send_and_verify(&mut stream, Response::Ping.into(), Response::Pong.into(), "'PING' -> Master").await?;
    send_and_verify(&mut stream, format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{}\r\n", config.port).into_bytes(), Response::Ok.into(), "'REPLCONF listening-port' -> Master").await?;
    send_and_verify(&mut stream, b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".to_vec(), Response::Ok.into(), "'REPLCONF capa' -> Master").await?;
    let attached_data = send_and_process_psync(&mut stream, repl_state, db).await?;
    Ok((stream, attached_data))
}

pub async fn run(mut master_stream: TcpStream, attached_data: Option<Vec<u8>>, bundle: Bundle) {
    let (tx, _rx) = unbounded_channel::<Vec<u8>>();
    let client = std::sync::Arc::new(Client_ { id: 0, tx, in_transaction: None, in_sub_mode: None, queued_commands: None, subs: None });
    if let Some(cmd_block) = attached_data {
        if let Some(star) = cmd_block.iter().position(|&c| c == b'*') {
            process_cmd_block(star, cmd_block.len(), &cmd_block, true, &client, bundle.clone()).await;
        }
    }
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let offset = bundle.repl_state.lock().await.repl_offset;
        let request = format!("*3\r\n$8\r\nREPLCONF\r\n$9\r\nGETSTREAM\r\n${}\r\n{}\r\n", offset.to_string().len(), offset);
        if let Err(e) = master_stream.write_all(request.as_bytes()).await { eprintln!("Replication stream write error: {e}"); return; }
        match read_bulk_response(&mut master_stream).await {
            Ok(payload) if !payload.is_empty() => { process_cmd_block(0, payload.len(), &payload, true, &client, bundle.clone()).await; },
            Ok(_) => (),
            Err(e) => { eprintln!("Replication stream read error: {e}"); return; }
        }
    }
}


