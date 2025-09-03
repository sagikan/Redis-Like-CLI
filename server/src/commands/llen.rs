use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_llen(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let val_list_len = match db.lock().await.get_mut(&args[0]) {
        Some(value) => match &mut value.val {
            // An existing list is found
            ValueType::StringList(val_list) => val_list.len(),
            _ => { // Value is of the wrong type
                client.tx.send(Response::WrongType.into()).unwrap();
                return;
            }
        }, None => 0 // Key doesn't exist
    };

    // Emit the list's length
    client.tx.send(format!(":{val_list_len}\r\n").into_bytes()).unwrap();
}
