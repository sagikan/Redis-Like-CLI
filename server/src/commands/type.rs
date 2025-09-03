use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_type(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }
    
    let val_type = match db.lock().await.get(&args[0]) {
        Some(value) => match &value.val {
            ValueType::String(_) => "string",
            ValueType::StringList(_) => "list",
            ValueType::Stream(_) => "stream"
        }, None => "none"
    };

    client.tx.send(format!("+{val_type}\r\n").into_bytes()).unwrap();
}
