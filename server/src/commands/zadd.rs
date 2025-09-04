use std::collections::BTreeSet;
use crate::db::{Database, SetMember, Value, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_zadd(args: &[String], client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract set key, score, and member name
    let set_key = &args[0];
    let score = match args[1].parse::<f64>() {
        Ok(s) => s,
        _ => {
            client.tx.send(Response::ErrNotFloat.into()).unwrap();
            return;
        }
    };
    let member = &args[2];

    // Create set member
    let set_member = SetMember {
        member: member.clone(),
        score
    };

    let mut guard = db.lock().await;
    let num_new_members = match guard.get_mut(set_key) {
        Some(value) => match &mut value.val {
            ValueType::SortedSet(set) => { // An existing set is found
                let prev_set_len = set.len();
                
                // Insert (update if already exists) set member
                set.retain(|s| s.member != *member);
                set.insert(set_member);
                
                set.len() - prev_set_len
            }, _ => { // Value is of the wrong type
                client.tx.send(Response::WrongType.into()).unwrap();
                return;
            }
        }, None => { // Set not found
            let mut set_val: BTreeSet<SetMember> = BTreeSet::new();
            // Insert set member + create set
            set_val.insert(set_member);
            let set = Value {
                val: ValueType::SortedSet(set_val),
                exp: None
            };
            // Insert set
            guard.insert(set_key.clone(), set);

            1
        }
    };

    // Emit # of new members in set
    client.tx.send(format!(":{num_new_members}\r\n").into_bytes()).unwrap();
}
