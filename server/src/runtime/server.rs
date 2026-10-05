use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc::unbounded_channel, Mutex},
};

use crate::{
    commands::Command,
    core::{
        bundle::Bundle,
        client::{get_next_id, Client, Client_, Response},
    },
    runtime::protocol::{resp_frame_end, BIG_BUFSIZE},
};

pub async fn process_cmd(
    cmd: &[u8],
    is_propagated: bool,
    client: &Client,
    bundle: Bundle,
) {
    let mut cmd = match Command::from(cmd, is_propagated) {
        Some(cmd) => cmd,
        None => {
            client.tx.send(Response::ErrEmptyCommand.into()).unwrap();
            return;
        }
    };

    cmd.execute(client, bundle).await;
}

pub async fn process_cmd_block(
    start: usize,
    end: usize,
    buf: &[u8],
    is_propagated: bool,
    client: &Client,
    bundle: Bundle,
) -> usize {
    let mut cmd_start = start;

    while cmd_start < end {
        while cmd_start < end && (buf[cmd_start] == b'\r' || buf[cmd_start] == b'\n') {
            cmd_start += 1;
        }
        if cmd_start == end {
            break;
        }

        let cmd_end = match resp_frame_end(&buf[..end], cmd_start) {
            Some(end) => end,
            None => break,
        };
        process_cmd(
            &buf[cmd_start..cmd_end],
            is_propagated,
            client,
            bundle.clone(),
        )
        .await;
        cmd_start = cmd_end;
    }

    cmd_start - start
}

pub async fn run(listener: TcpListener, bundle: Bundle) {
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                eprintln!("Accept error: {error}");
                continue;
            }
        };
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = unbounded_channel::<Vec<u8>>();
        let client = Arc::new(Client_ {
            id: get_next_id(),
            tx,
            in_transaction: Some(Arc::new(Mutex::new(false))),
            in_sub_mode: Some(Arc::new(Mutex::new(false))),
            queued_commands: Some(Arc::new(Mutex::new(Vec::new()))),
            subs: Some(Arc::new(Mutex::new(Vec::new()))),
        });

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                if writer.write_all(&message).await.is_err() {
                    break;
                }
            }
        });

        let tokio_bundle = bundle.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; BIG_BUFSIZE];
            let mut input = Vec::new();
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(n) => {
                        input.extend_from_slice(&buf[..n]);
                        let consumed = process_cmd_block(
                            0,
                            input.len(),
                            &input,
                            false,
                            &client,
                            tokio_bundle.clone(),
                        )
                        .await;
                        if consumed > 0 {
                            input.drain(..consumed);
                        }
                    }
                    Err(error) => {
                        eprintln!("Error: {error}");
                        return;
                    }
                }
            }
        });
    }
}
