use std::{cmp::min, collections::VecDeque};
use crate::db::{Database, BlockedClients, Value, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_push(from_right: bool, to_send: bool, args: &[String],
                      client: &Client, db: Database, blocked_clients: BlockedClients) {
    if args.len() < 2 {
        client.send_if(to_send, Response::ErrArgCount);
        return;
    }

    // Extract key and values
    let key = &args[0];
    let mut val_list: VecDeque<String> = args[1..].iter().cloned().collect();
    let val_list_len = val_list.len();

    {
        let mut to_unblock = Vec::new();
        let mut blocked_clients_guard = blocked_clients.lock().await;
        // Look for blocked clients waiting for PUSH
        if let Some(blocked_list) = blocked_clients_guard.get_mut(key) {
            let pop_num = min(val_list.len(), blocked_list.len());
            'outer: for _ in 0..pop_num {
                // Pop from front a non-expired blocked client
                let front_client = loop {
                    match blocked_list.pop_front() {
                        Some(bc) if !bc.expired => break bc,
                        Some(_) => continue, // Expired
                        None => break 'outer // Empty list
                    }
                };
                // Pop value based on [R\L]PUSH and front client's B[R\L]POP
                let front_client_from_right = match front_client.from_right {
                    Some(f_r) => f_r,
                    None => { break; } // Not blocked by BPOP operation
                };
                let val = match (from_right, front_client_from_right) {
                    (true, true) | (false, false) => val_list.pop_back().unwrap(),
                    (true, false) | (false, true) => val_list.pop_front().unwrap()
                };

                // Emit key + popped value
                front_client.client.send_if(to_send, format!(
                    "*2\r\n${}\r\n{key}\r\n${}\r\n{val}\r\n", key.len(), val.len()
                ).into_bytes());
                
                to_unblock.push(front_client);
            }
        }

        // Unblock clients from all keys attached to their BPOP command
        for blocked_client in to_unblock {
            for key in &blocked_client.blocked_by {
                if let Some(blocked_list) = blocked_clients_guard.get_mut(key) {
                    blocked_list.retain(
                        |bc| bc.client.id != blocked_client.client.id
                    );
                }
            }
        }
    }
    
    let mut guard = db.lock().await;
    // Create (if needed), insert, and calculate new list length
    let stored_val_list_len = match guard.get_mut(key) {
        Some(value) => match &mut value.val {
            ValueType::StringList(stored_val_list) => { // An existing list is found
                // Insert if there are values to push
                if val_list.len() > 0 {
                    match from_right {
                        true => stored_val_list.extend(val_list),
                        false => while let Some(val) = val_list.pop_front() {
                            stored_val_list.push_front(val);
                        }
                    }
                }

                stored_val_list.len()
            }, _ => { // Value is of the wrong type
                client.send_if(to_send, Response::WrongType);
                return;
            }
        }, None => {
            // Create and insert list if there are values to push
            if val_list.len() > 0 {
                let value = Value {
                    val: ValueType::StringList(val_list),
                    exp: None
                };
                guard.insert(key.clone(), value);
            }

            val_list_len // OG length
        }
    };
    
    // Emit the list's length
    client.send_if(to_send, format!(":{stored_val_list_len}\r\n").into_bytes());
}
