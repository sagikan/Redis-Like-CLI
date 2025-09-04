use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_zcard(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Get + emit # of members in set
    let num_members = match db.lock().await.get(&args[0]) {
        Some(value) => match &value.val {
            ValueType::SortedSet(set) => set.len(), // An existing set is found
            _ => { // Value is of the wrong type
                client.tx.send(Response::WrongType.into()).unwrap();
                return;
            }
        }, None => 0 // Set not found
    };

    client.tx.send(format!(":{num_members}\r\n").into_bytes()).unwrap();
}
