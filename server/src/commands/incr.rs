use crate::db::{Database, Value, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_incr(to_send: bool, args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.send_if(to_send, Response::ErrArgCount);
        return;
    }

    let key = &args[0];

    let mut guard = db.lock().await;
    let incr_val: String = match guard.get_mut(key) {
        Some(value) => match &value.val {
            ValueType::String(val) => match val.parse::<i32>() {
                Ok(parsed_val) => {
                    // Increase + set new value
                    let res = (parsed_val + 1).to_string();
                    value.val = ValueType::String(res.clone());
                    
                    res
                }, _ => {
                   client.send_if(to_send, Response::ErrNotInteger);
                    return;
                }
            }, _ => { // Value is of the wrong type
                client.send_if(to_send, Response::WrongType);
                return;
            }
        }, None => { // Key doesn't exist
            // Set value to 1 + insert pair
            let res = "1".to_string();
            let value = Value {
                val: ValueType::String(res.clone()),
                exp: None
            };
            guard.insert(key.clone(), value);
            
            res
        }
    };

    // Emit new value
    client.send_if(to_send, format!(":{incr_val}\r\n").into_bytes());
}
