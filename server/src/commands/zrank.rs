use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_zrank(args: &[String], client: &Client, db: Database) {
    if args.len() != 2 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract set key and member name
    let set_key = &args[0];
    let member = &args[1];

    // Get + emit set member's rank
    let res = match db.lock().await.get(set_key) {
        Some(value) => match &value.val {
            ValueType::SortedSet(set) => { // An existing set is found
                match set.iter().position(|s| s.member == *member) { // Member's rank
                    Some(rank) => format!(":{rank}\r\n").into_bytes(),
                    None => Response::Nil.into() // Member not found
                }
            }, _ => Response::Nil.into() // Value is of the wrong type
        }, None => Response::Nil.into() // Set not found
    };

    client.tx.send(res).unwrap();
}
