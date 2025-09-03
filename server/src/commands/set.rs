use std::collections::HashMap;
use tokio::{sync::MutexGuard, time::{sleep, Duration}};
use crate::db::{Database, Value, ValueType, ExpiryType};
use crate::client::{Client, Response};
use crate::commands::_helpers_::get_unix_time;

fn set_pair(key: String, value: Value, client: &Client, to_send: bool,
            mut guard: MutexGuard<'_, HashMap<String, Value>>) {
    guard.insert(key, value);
    client.send_if(to_send, Response::Ok);
}

pub async fn cmd_set(to_send: bool, args: &[String], client: &Client, db: Database) {
    let guard = db.lock().await;

    match args.len() {
        2 => { // [Key] [Value]
            let key = args[0].clone();
            let value = Value {
                val: ValueType::String(args[1].clone()),
                exp: None
            };
            set_pair(key, value, &client, to_send, guard);
        }, 3 => { // [Key] [Value] [NX / XX]
            let key = args[0].clone();
            let value = Value {
                val: ValueType::String(args[1].clone()),
                exp: None
            };
            // Set only if NX + key doesn't exist OR if XX + key exists
            match (
                args[2].to_uppercase().as_str(), guard.contains_key(&key)
            ) {
                ("NX", false) | ("XX", true) => set_pair(key, value, &client, to_send, guard),
                ("NX", true) | ("XX", false) => client.send_if(to_send, Response::Nil),
                _ => client.send_if(to_send, Response::ErrSyntax)
            }
        }, 4 => { // [Key] [Value] [EX / PX] [Stringified Number]
            match args[2].to_uppercase().as_str() {
                arg @ ("EX" | "PX") => {
                    // Extract expiry time (+ parse to milliseconds, check validity)
                    let timeout = match args[3].parse::<usize>() {
                        Ok(t) if t > 0 => if arg == "EX" { t * 1000 } else { t },
                        _ => {
                            client.send_if(to_send, Response::ErrSetExpireTime);
                            return;
                        }
                    };

                    let key = args[0].clone();
                    let value = Value {
                        val: ValueType::String(args[1].clone()),
                        exp: Some(match arg {
                            "EX" => ExpiryType::Seconds(get_unix_time(false) + timeout),
                            "PX" => ExpiryType::Milliseconds(get_unix_time(true) + timeout),
                            _ => unreachable!()
                        })
                    };

                    // Insert + employ active expiration via Tokio-runtime
                    set_pair(key.clone(), value, &client, to_send, guard);
                    let tokio_db = db.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(timeout as u64)).await;
                        tokio_db.lock().await.remove(&key);
                    });
                }, _ => client.send_if(to_send, Response::ErrSyntax)
            }
        }, _ => client.send_if(to_send, Response::ErrArgCount)
    }
}
