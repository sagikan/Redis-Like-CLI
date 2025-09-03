use std::{cmp::min, collections::VecDeque};
use tokio::time::{sleep, Duration};
use crate::db::{Database, BlockedClients, ValueType};
use crate::client::{Client, BlockedClient, Response};

pub async fn cmd_pop(from_right: bool, to_send: bool, args: &[String],
                     client: &Client, db: Database) {
    let args_len = args.len();
    if args_len != 1 && args_len != 2 {
        client.send_if(to_send, Response::ErrArgCount);
        return;
    }

    let key = &args[0];
    // Get # of pops based on args length
    let pop_num = match args_len {
        1 => 1,
        2 => match args[1].parse::<usize>() {
            Ok(v) => v,
            _ => {
                client.send_if(to_send, Response::ErrOutOfRange);
                return;
            }
        }, _ => 0
    };
    
    let mut guard = db.lock().await;
    match guard.get_mut(key) {
        Some(value) => match &mut value.val {
            ValueType::StringList(val_list) => { // An existing list is found
                // Clamp # of pops and set L/R functionality
                let val_list_len = val_list.len();
                let pop_num_usize: usize = min(pop_num as usize, val_list_len);
                let mut my_pop = || if from_right { val_list.pop_back() }
                                    else { val_list.pop_front() };

                // Pop while building bulk string
                let bulk_str = match pop_num_usize {
                    1 => match my_pop() {
                        Some(v) => format!("+{v}\r\n"),
                        None => return
                    }, _ => format!(
                        "*{pop_num_usize}\r\n{}",
                        (0..pop_num_usize) // For [pop_num_usize] times
                            .filter_map(|_| my_pop()) // Pop -> filter -> map popped value
                            .map(|val| format!("${}\r\n{val}\r\n", val.len())) // Stringify
                            .collect::<String>() // Unify to one string
                    ) 
                };

                client.send_if(to_send, bulk_str.into_bytes());

                // Remove key if popped all values
                if pop_num_usize == val_list_len {
                    guard.remove(key);
                }
            }, // Value is of the wrong type
            _ => client.send_if(to_send, Response::WrongType)
        }, None => client.send_if(to_send, Response::Nil)
    }    
}

pub async fn cmd_bpop(from_right: bool, to_send: bool, args: &[String],
                      client: &Client, db: Database, blocked_clients: BlockedClients) {
    let args_len = args.len();
    if args_len < 2 {
        client.send_if(to_send, Response::ErrArgCount);
        return;
    }

    // Extract keys
    let keys: Vec<String> = args[0..args_len-1].to_vec();

    { // Try immediate pop
        let mut guard = db.lock().await;
        for key in &keys {
            let val = match guard.get_mut(key) {
                Some(value) => match &mut value.val {
                     // An existing list is found
                    ValueType::StringList(val_list) => match from_right {
                        true => val_list.pop_back().unwrap(),
                        false => val_list.pop_front().unwrap()
                    }, _ => { // Value is of the wrong type
                        client.send_if(to_send, Response::WrongType);
                        return;
                    }
                }, None => { continue; } // Skip
            };

            // Emit key + popped value array
            client.send_if(to_send, format!(
                "*2\r\n${}\r\n{key}\r\n${}\r\n{val}\r\n", key.len(), val.len()
            ).into_bytes());
            return;
        }
    }

    { // All keys are empty => Add client to every blocked list
        let mut blocked_clients_guard = blocked_clients.lock().await;
        for key in &keys {
            blocked_clients_guard
                .entry(key.clone()) // Get entry of key if exists
                .or_insert_with(VecDeque::new) // Or create an empty list
                .push_back(BlockedClient { // Push blocked client to end of list
                    client: client.clone(),
                    blocked_by: keys.clone(),
                    expired: false,
                    from_right: Some(from_right),
                    from_entry_id: None, // <- Nonfactor
                    count: None // <- Nonfactor
                });
        }
    }

    // Extract non-negative timeout
    let block = match args[args_len-1].parse::<f64>() {
        Ok(v) if v >= 0.0 => v,
        _ => {
            client.send_if(to_send, Response::ErrNegativeTimeout);
            return;
        }
    };

    // Handle non-zero block via Tokio-runtime
    if block > 0.0 {
        let tokio_client = client.clone();
        let tokio_blocked_clients = blocked_clients.clone();
        let tokio_keys = keys.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs_f64(block)).await;

            let mut blocked_clients_guard = tokio_blocked_clients.lock().await;
            let mut still_blocked = false;
            for key in &tokio_keys {
                if let Some(blocked_list) = blocked_clients_guard.get_mut(key) {
                    for blocked_client in blocked_list.iter_mut().filter(
                        |bc| bc.client.id == tokio_client.id
                    ) { // Client still blocked by some list (=> haven't popped yet)
                        still_blocked = true;
                        blocked_client.expired = true; // Update expiry
                    }
                }
            }

            if still_blocked {
                tokio_client.send_if(to_send, Response::NilArray);
            }
        });
    }
}
