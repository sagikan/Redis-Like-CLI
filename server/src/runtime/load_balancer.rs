use std::{
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::{
    commands::Command,
    core::{client::Response, config::Config_},
    runtime::protocol::{read_resp_frame, resp_frame_end, BIG_BUFSIZE},
};

async fn proxy_stateful(
    client_stream: TcpStream,
    backend: TcpStream,
    mut input: Vec<u8>,
) {
    let (mut client_reader, mut client_writer) = client_stream.into_split();
    let (mut backend_reader, mut backend_writer) = backend.into_split();
    let mut client_buf = [0; BIG_BUFSIZE];
    let mut backend_buf = [0; BIG_BUFSIZE];
    let mut backend_input = Vec::new();

    loop {
        tokio::select! {
            result = client_reader.read(&mut client_buf) => {
                let n = match result {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                input.extend_from_slice(&client_buf[..n]);
                while let Some(end) = resp_frame_end(&input, 0) {
                    if backend_writer.write_all(&input[..end]).await.is_err() {
                        return;
                    }
                    input.drain(..end);
                }
            }
            result = backend_reader.read(&mut backend_buf) => {
                let n = match result {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                backend_input.extend_from_slice(&backend_buf[..n]);
                while let Some(end) = resp_frame_end(&backend_input, 0) {
                    if client_writer.write_all(&backend_input[..end]).await.is_err() {
                        return;
                    }
                    backend_input.drain(..end);
                }
            }
        }
    }
}

async fn proxy_client(
    mut client_stream: TcpStream,
    config: Arc<Config_>,
    read_cursor: Arc<AtomicUsize>,
) {
    let master = match config.lb_master.clone() {
        Some(master) => master,
        None => {
            let _ = client_stream
                .write_all(b"-ERR load balancer master is not configured\r\n")
                .await;
            return;
        }
    };
    let mut read_backends = vec![master.clone()];
    read_backends.extend(config.lb_replicas.iter().cloned());
    let mut input = Vec::new();
    let mut stateful_backend: Option<TcpStream> = None;

    loop {
        let frame = loop {
            if let Some(end) = resp_frame_end(&input, 0) {
                break input.drain(..end).collect::<Vec<_>>();
            }

            let mut chunk = [0; BIG_BUFSIZE];
            match client_stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => input.extend_from_slice(&chunk[..n]),
            }
        };

        let command = match Command::from(&frame, false) {
            Some(command) => command,
            None => {
                let response: Vec<u8> = Response::ErrEmptyCommand.into();
                let _ = client_stream.write_all(&response).await;
                continue;
            }
        };
        let name = command.name.to_uppercase();
        let starts_stateful = matches!(
            name.as_str(),
            "MULTI" | "SUBSCRIBE" | "PSUBSCRIBE" | "BLPOP" | "BRPOP"
        ) || (name == "XREAD"
            && command.args.as_ref().is_some_and(|args| {
                args.iter().any(|arg| arg.eq_ignore_ascii_case("BLOCK"))
            }));
        let ends_transaction = matches!(name.as_str(), "EXEC" | "DISCARD");
        let was_stateful = stateful_backend.is_some();
        let to_master = stateful_backend.is_some()
            || command.is_write()
            || matches!(name.as_str(), "PUBLISH" | "SUBSCRIBE" | "UNSUBSCRIBE");
        let endpoint = if to_master {
            master.clone()
        } else {
            let index = read_cursor.fetch_add(1, Ordering::Relaxed) % read_backends.len();
            read_backends[index].clone()
        };

        let mut backend = match stateful_backend.take() {
            Some(stream) => stream,
            None => match TcpStream::connect(&endpoint).await {
                Ok(stream) => stream,
                Err(_) => {
                    let _ = client_stream
                        .write_all(b"-ERR backend unavailable\r\n")
                        .await;
                    continue;
                }
            },
        };

        if backend.write_all(&frame).await.is_err() {
            let _ = client_stream
                .write_all(b"-ERR backend unavailable\r\n")
                .await;
            continue;
        }
        let response = match read_resp_frame(&mut backend).await {
            Ok(response) => response,
            Err(_) => {
                let _ = client_stream
                    .write_all(b"-ERR backend did not reply\r\n")
                    .await;
                continue;
            }
        };
        if client_stream.write_all(&response).await.is_err() {
            return;
        }

        if name == "SUBSCRIBE" || name == "PSUBSCRIBE" {
            proxy_stateful(client_stream, backend, input).await;
            return;
        }
        if (starts_stateful || was_stateful) && !ends_transaction {
            stateful_backend = Some(backend);
        }
    }
}

pub async fn run(config: Config_) -> Result<(), Box<dyn Error + Send + Sync>> {
    if config.lb_master.is_none() {
        return Err("--load-balancer requires --master <host:port>".into());
    }
    let listener = TcpListener::bind(format!("{}:{}", config.bind_addr, config.port)).await?;
    let config = Arc::new(config);
    let read_cursor = Arc::new(AtomicUsize::new(0));

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(proxy_client(stream, config.clone(), read_cursor.clone()));
    }
}
