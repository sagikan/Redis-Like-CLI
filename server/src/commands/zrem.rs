use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_zrem(to_send: bool, args: &[String], client: &Client, db: Database) {
    if args.len() != 2 {
        client.send_if(to_send, Response::ErrArgCount);
        return;
    }

    // Extract set key and member name
    let set_key = &args[0];
    let member = args[1].trim_matches('"');

    // Get + emit # of removed set members
    let num_rem_members = match db.lock().await.get_mut(set_key) {
        Some(value) => match &mut value.val {
            ValueType::SortedSet(set) => { // An existing set is found
                let prev_set_len = set.len();
                
                // Remove (if exists) set member
                set.retain(|s| s.member != *member);
                
                prev_set_len - set.len()
            }, _ => { // Value is of the wrong type
                client.send_if(to_send, Response::WrongType);
                return;
            }
        }, None => 0 // Set not found
    };

    client.send_if(to_send, format!(":{num_rem_members}\r\n").as_bytes());
}
