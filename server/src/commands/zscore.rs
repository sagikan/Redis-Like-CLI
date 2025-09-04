use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_zscore(args: &[String], client: &Client, db: Database) {
    if args.len() != 2 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract set key and member name
    let set_key = &args[0];
    let member = args[1].trim_matches('"');

    // Get + emit set member's score as bulk string
    let res = match db.lock().await.get(set_key) {
        Some(value) => match &value.val {
            ValueType::SortedSet(set) => { // An existing set is found
                match set.iter().find(|s| s.member == *member) {
                    Some(set_member) => format!(
                        "${}\r\n{}\r\n",
                        set_member.score.to_string().len(), set_member.score
                    ).into_bytes(),
                    None => Response::Nil.into() // Member not found
                }
            }, _ => Response::WrongType.into() // Value is of the wrong type
        }, None => Response::Nil.into() // Set not found
    };

    client.tx.send(res).unwrap();
}
