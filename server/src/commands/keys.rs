use crate::db::{Database};
use crate::client::{Client, Response};
use crate::commands::_helpers_::glob;

pub async fn cmd_keys(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let pattern = args[0].trim_matches('"'); // Remove "s
    
    let mut matching: Vec<&String> = Vec::new();
    let mut bulk_str = String::new();
    let guard = db.lock().await;
    // Match key to pattern + build bulk string
    for key in guard.keys() {
        if glob(pattern.as_bytes(), key.as_bytes()) {
            matching.push(&key);
            bulk_str.push_str(&format!("${}\r\n{key}\r\n", key.len()));
        }
    }

    // Emit final bulk string
    bulk_str = format!("*{}\r\n{bulk_str}", matching.len());
    client.tx.send(bulk_str.into_bytes()).unwrap();
}
