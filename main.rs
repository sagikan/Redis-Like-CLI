#![allow(unused_imports)]
use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};

#[derive(Clone)]
enum Value {
    ValString(String),
    ValStringList(VecDeque<String>)
}

fn extract_str(val: &Value) -> Option<String> {
    match val {
        Value::ValString(str) => Some(str.clone()),
        _ => None
    }
}

async fn set_pair(key: String, val: Value, socket: &mut TcpStream, mut guard: MutexGuard<'_, HashMap<String, Value>>) {
    guard.insert(key, val);
    socket.write_all(b"+OK\r\n").await.unwrap();
}

async fn cmd_echo(parsed_cmd: &Vec<String>, socket: &mut TcpStream) {
    if let Some(_val) = parsed_cmd.get(1) {
        let to_write = format!("+{}\r\n", parsed_cmd[1..].join(" "));
        socket.write_all(to_write.as_bytes()).await.unwrap();
    } else {
        socket.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n").await.unwrap();
    }
}

async fn cmd_set(parsed_cmd: &Vec<String>, socket: &mut TcpStream, pairs: &Arc<Mutex<HashMap<String, Value>>>) {
    let guard = pairs.lock().await;

    match parsed_cmd.len() {
        3 => { // SET [Key] [Value]
            set_pair(parsed_cmd[1].clone(), Value::ValString(parsed_cmd[2].clone()), socket, guard).await;
        }, 4 => { // SET [Key] [Value] [NX / XX / Wrong Syntax]
            match parsed_cmd[3].to_uppercase().as_str() {
                "NX" => {
                    // Set only if key does not already exist
                    match guard.contains_key(&parsed_cmd[1]) {
                        true => socket.write_all(b"$-1\r\n").await.unwrap(),
                        false => set_pair(parsed_cmd[1].clone(), Value::ValString(parsed_cmd[2].clone()), socket, guard).await
                    }
                }, "XX" => {
                    // Set only if key already exists
                    match guard.contains_key(&parsed_cmd[1]) {
                        true => set_pair(parsed_cmd[1].clone(), Value::ValString(parsed_cmd[2].clone()), socket, guard).await,
                        false => socket.write_all(b"$-1\r\n").await.unwrap()
                    }
                }, _ => socket.write_all(b"-ERR syntax error\r\n").await.unwrap()
            }
        }, 5 => { // SET [Key] [Value] [EX / PX / Wrong Syntax] [Stringified Number / Wrong Syntax]
            match parsed_cmd[3].to_uppercase().as_str() {
                arg @ ("EX" | "PX") => {
                    // Extract expiry (+ parse to milliseconds)
                    let expiry = match parsed_cmd[4].parse::<i32>() {
                        Ok(val) => if arg == "EX" { val * 1000 } else { val },
                        Err(_) => {
                            socket.write_all(b"-ERR value is not an integer or out of range\r\n").await.unwrap();
                            return;
                        }
                    };
                    
                    // Insert and remove after [expiry] ms via Tokio runtime
                    set_pair(parsed_cmd[1].clone(), Value::ValString(parsed_cmd[2].clone()), socket, guard).await;
                    let tokio_pairs = Arc::clone(&pairs);
                    let tokio_key = parsed_cmd[1].clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(expiry as u64)).await;
                        tokio_pairs.lock().await.remove(&tokio_key);
                    });
                }, _ => socket.write_all(b"-ERR syntax error\r\n").await.unwrap()
            }
        }, _ => socket.write_all(b"-ERR wrong number of arguments for 'set' command\r\n").await.unwrap()
    }
}

async fn cmd_get(parsed_cmd: &Vec<String>, socket: &mut TcpStream, pairs: &Arc<Mutex<HashMap<String, Value>>>) {
    if parsed_cmd.len() != 2 {
        socket.write_all(b"-ERR wrong number of arguments for 'get' command\r\n").await.unwrap();
        return;
    }

    let guard = pairs.lock().await;
    let key = &parsed_cmd[1];

    // Return (nil) if no such key is found
    if !guard.contains_key(key) {
        socket.write_all(b"$-1\r\n").await.unwrap();
        return;
    }

    // Extract + emit value
    let raw_val: Value = guard.get(key).unwrap().clone();
    let val: String = match extract_str(&raw_val) {
        Some(str_val) => str_val,
        None => {
            eprintln!("Extraction Error");
            return;
        }
    };
    socket.write_all(format!("+{}\r\n", val).as_bytes()).await.unwrap();
}

async fn cmd_rpush(parsed_cmd: &Vec<String>, socket: &mut TcpStream, pairs: &Arc<Mutex<HashMap<String, Value>>>) {
    if parsed_cmd.len() < 3 {
        socket.write_all(b"-ERR wrong number of arguments for 'rpush' command\r\n").await.unwrap();
        return;
    }

    let mut guard = pairs.lock().await;
    let key = &parsed_cmd[1];

    // Create and insert string list if no such key is found
    let val_list_len = if !guard.contains_key(key) {
        let val_list: VecDeque<String> = parsed_cmd[2..].iter().cloned().collect();
        let val_list_len = val_list.len(); // Avoids moving & cloning val_list
        guard.insert(key.clone(), Value::ValStringList(val_list));
        val_list_len
    } // Insert values to an existing string list if found
    else if let Some(Value::ValStringList(val_list)) = guard.get_mut(key) {
        val_list.extend(parsed_cmd[2..].iter().cloned());
        val_list.len()
    } // Emit error message if value is of the wrong type
    else {
        socket.write_all(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n").await.unwrap();
        return;
    };

    // Emit the list's length
    socket.write_all(format!(":{}\r\n", val_list_len).as_bytes()).await.unwrap();
}

async fn cmd_other(parsed_cmd: &Vec<String>, socket: &mut TcpStream) {
    // Build error string
    let mut err_str = format!("-ERR unknown command `{}`, with args beginning with: ", parsed_cmd[0]);
    for arg in &parsed_cmd[1..] {
        err_str.push_str(&format!("`{}`, ", arg));
    }
    err_str.push_str("\r\n");

    socket.write_all(err_str.as_bytes()).await.unwrap();
}

async fn process_cmd(cmd: &[u8], socket: &mut TcpStream, pairs: &Arc<Mutex<HashMap<String, Value>>>) {
    let parsed_cmd: Vec<String> = resp_parse(cmd).await;

    // Handle by command
    match parsed_cmd[0].to_uppercase().as_str() {
        "PING" => socket.write_all(b"+PONG\r\n").await.unwrap(),
        "ECHO" => cmd_echo(&parsed_cmd, socket).await,
        "SET" => cmd_set(&parsed_cmd, socket, pairs).await,
        "GET" => cmd_get(&parsed_cmd, socket, pairs).await,
        "RPUSH" => cmd_rpush(&parsed_cmd, socket, pairs).await,
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
            let pairs: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
            let mut buf = vec![0; 1024];

            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => process_cmd(&buf[0..n], &mut socket, &pairs).await,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
