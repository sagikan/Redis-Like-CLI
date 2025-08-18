#![allow(unused_imports)]
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn cmd_echo(parsed_cmd: &Vec<String>, socket: &mut TcpStream) {
    if let Some(_val) = parsed_cmd.get(1) {
        let to_write = format!("+{}\r\n", parsed_cmd[1..].join(" "));
        socket.write_all(to_write.as_bytes()).await.unwrap();
    } else {
        socket.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n").await.unwrap();
    }
}

async fn cmd_set(parsed_cmd: &Vec<String>, socket: &mut TcpStream, pairs: &mut Arc<Mutex<HashMap<String, String>>>) {
    if parsed_cmd.len() != 3 {
        socket.write_all(b"-ERR wrong number of arguments for 'set' command\r\n").await.unwrap();
        return;
    }

    // Insert key-value pair
    pairs.lock().await.insert(parsed_cmd[1].clone(), parsed_cmd[2].clone());
    socket.write_all(b"+OK\r\n").await.unwrap();
}

async fn cmd_get(parsed_cmd: &Vec<String>, socket: &mut TcpStream, pairs: &mut Arc<Mutex<HashMap<String, String>>>) {
    if parsed_cmd.len() != 2 {
        socket.write_all(b"-ERR wrong number of arguments for 'get' command\r\n").await.unwrap();
        return;
    }

    let key = &parsed_cmd[1];
    // Return (nil) if no such key is found
    if !pairs.lock().await.contains_key(key) {
        socket.write_all(b"$-1\r\n").await.unwrap();
    }

    // Extract + emit value
    let val = pairs.lock().await.get(key).unwrap().clone();
    socket.write_all(format!("+{}\r\n", val).as_bytes()).await.unwrap();
}

async fn cmd_other(parsed_cmd: &Vec<String>, socket: &mut TcpStream) {
    // Emit error string
    let mut err_str = format!("-ERR unknown command `{}`, with args beginning with: ", parsed_cmd[0]);
    for arg in &parsed_cmd[1..] {
        err_str.push_str(&format!("`{}`, ", arg));
    }
    err_str.push_str("\r\n");
    socket.write_all(err_str.as_bytes()).await.unwrap();
}

async fn process_cmd(cmd: &[u8], socket: &mut TcpStream, pairs: &mut Arc<Mutex<HashMap<String, String>>>) {
    let parsed_cmd: Vec<String> = resp_parse(cmd).await;

    // Handle by command
    match parsed_cmd[0].to_uppercase().as_str() {
        "PING" => socket.write_all(b"+PONG\r\n").await.unwrap(),
        "ECHO" => cmd_echo(&parsed_cmd, socket).await,
        "SET" => cmd_set(&parsed_cmd, socket, pairs).await,
        "GET" => cmd_get(&parsed_cmd, socket, pairs).await,
        _ => cmd_other(&parsed_cmd, socket).await
    }
}

async fn resp_parse(to_parse: &[u8]) -> Vec<String> {
    let unparsed_str = std::str::from_utf8(to_parse).unwrap();
    let mut lines = unparsed_str.split("\r\n");

    // Skip array header
    lines.next();

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

    parsed
}

#[tokio::main]
async fn main() {
    // Set listener to port 6379
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    
    // Event loop
    loop {
        // Accept client (blocking)
        let (mut socket, _) = listener.accept().await.unwrap();

        // Tokio runtime read-write function
        tokio::spawn(async move {
            let mut pairs: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
            let mut buf = vec![0; 1024];

            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => process_cmd(&buf[0..n], &mut socket, &mut pairs).await,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
