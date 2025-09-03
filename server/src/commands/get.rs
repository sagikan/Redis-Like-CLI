use crate::db::{Database, ValueType, ExpiryType};
use crate::client::{Client, Response};
use crate::commands::_helpers_::get_unix_time;

pub async fn cmd_get(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let key = &args[0];

    // Extract + emit value
    let mut guard = db.lock().await;
    let val = match guard.get(key) {
        Some(value) => {
            let mut expired = false;
            // Check expiry
            if let Some(exp) = &value.exp {
                // Get current UNIX time + expiry time
                let (now, exp) = match exp {
                    ExpiryType::Milliseconds(exp) => (get_unix_time(true), *exp),
                    ExpiryType::Seconds(exp) => (get_unix_time(false), *exp)
                };

                expired = now > exp;
            }

            if !expired {
                match &value.val {
                    ValueType::String(val) => Some(val),
                    _ => { panic!("Extraction Error"); }
                }
            } else { None }
        }, None => {
            client.tx.send(Response::Nil.into()).unwrap();
            return;
        }
    };

    if val.is_none() { // Expired value
        guard.remove(key); // Remove entry
        client.tx.send(Response::Nil.into()).unwrap();
        return;
    }

    client.tx.send(format!("+{}\r\n", val.unwrap()).into_bytes()).unwrap();
}
