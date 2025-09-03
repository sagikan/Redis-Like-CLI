use std::collections::VecDeque;
use tokio::time::{sleep, Duration};
use crate::db::{Database, BlockedClients, ValueType, Entry, EntryIdType};
use crate::client::{Client, BlockedClient, Response};
use crate::commands::_helpers_::{get_last_entry, validate_read_entry_id};

pub async fn cmd_xread(args: &[String], client: &Client, db: Database,
                   blocked_clients: BlockedClients) {
    let args_len = args.len();
    let wrong_args_len = || {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    };
    if args_len < 3 || args_len % 2 == 0 { wrong_args_len(); }

    // Extract max count, block time, stream keys, and entry IDs
    let arg_1 = args[0].to_uppercase();
    let arg_3 = args[2].to_uppercase();
    let arg_5 = args.get(4)
                    .map(|s| s.to_uppercase())
                    .unwrap_or_default(); // Safe indexing
    let (count, block, mut stream_keys, mut entry_ids) = match (
        arg_1.as_str(), arg_3.as_str(), arg_5.as_str()
    ) {
        ("COUNT", "BLOCK", "STREAMS") => {
            if args_len < 7 { wrong_args_len(); }

            let (c, b) = match (
                args[1].parse::<usize>(),
                args[3].parse::<usize>()
            ) {
                (Ok(c), Ok(b)) => (c, b),
                _ => {
                    client.tx.send(Response::ErrNotInteger.into()).unwrap();
                    return;
                }
            };
            let key_num = (args_len - 5) / 2;
            let s_k: Vec<String> = args[5..5+key_num].to_vec();
            let e_i: Vec<String> = args[5+key_num..args_len].to_vec();

            (Some(c), Some(b), s_k, e_i)
        }, arg_names @ (("COUNT", "STREAMS", _) | ("BLOCK", "STREAMS", _)) => {
            if args_len < 5 { wrong_args_len(); }

            let x = match args[1].parse::<usize>() { // Value of count / block
                Ok(x) => x,
                _ => {
                    client.tx.send(Response::ErrNotInteger.into()).unwrap();
                    return;
                }
            };
            let key_num = (args_len - 3) / 2;
            let s_k: Vec<String> = args[3..3+key_num].to_vec();
            let e_i: Vec<String> = args[3+key_num..args_len].to_vec();

            match arg_names.0 {
                "COUNT" => (Some(x), None, s_k, e_i),
                "BLOCK" => (None, Some(x), s_k, e_i),
                _ => unreachable!() // By definition of args
            }
        }, ("STREAMS", _, _) => {
            let key_num = (args_len - 1) / 2;
            let s_k: Vec<String> = args[1..1+key_num].to_vec();
            let e_i: Vec<String> = args[1+key_num..args_len].to_vec();

            (None, None, s_k, e_i)
        }, _ => {
            client.tx.send(Response::ErrSyntax.into()).unwrap();
            return;
        }
    };

    let to_block = block.is_some();
    if !to_block {
        // Clean up non-existing stream keys
        let guard = db.lock().await;
        let mut i = 0;
        stream_keys.retain(|key| {
            let exists = guard.get(key).is_some();
            match exists {
                true => i += 1, // Continue indexing
                false => { entry_ids.remove(i); } // Remove respective entry ID
            }

            exists
        });
    }

    // Parse entry IDs
    let mut parsed_entry_ids = Vec::new();
    for (i, entry_id) in entry_ids.iter().enumerate() {
        let (ms_time, seq_num) = match validate_read_entry_id(
            |e_id| to_block && e_id == "$", &entry_id, &client
        ).await {
            Some(EntryIdType::Full(_)) => { // Entry ID is '$'
                if let Some(last_entry) = get_last_entry(
                    db.clone(), &stream_keys[i]
                ).await {
                    (last_entry.ms_time, last_entry.seq_num)
                } else { // No entries
                    (0, 0)
                }
            },
            Some(EntryIdType::Partial(ms)) => (ms, 0),
            Some(EntryIdType::Explicit((ms, seq))) => (ms, seq),
            None => return
        };

        parsed_entry_ids.push((ms_time, seq_num));
    }
    
    let mut bulk_str = String::new();
    let mut no_following_entries = 0; // # of streams w/o following entries
    { // Try reading synchronously
        let mut guard = db.lock().await;
        // Iterate over streams
        for (i, stream_key) in stream_keys.iter().enumerate() {
            // Get the following entries of the specified entry
            let following_entries: Vec<Entry> = match guard.get_mut(stream_key) {
                Some(value) => match &value.val {
                    ValueType::Stream(stream) => { // An existing stream is found
                        let ms_time = parsed_entry_ids[i].0;
                        let seq_num = parsed_entry_ids[i].1;
                        let f_e: Vec<Entry> = stream.entries.lock().await.iter().filter(
                            |e| ms_time < e.ms_time
                                || (ms_time == e.ms_time && seq_num < e.seq_num)
                        ).take(
                            count.map(|c| c as usize).unwrap_or(usize::MAX)
                        ).cloned().collect();

                        if f_e.is_empty() {
                            no_following_entries += 1;
                            continue;
                        }

                        f_e
                    }, _ => { // Value is of the wrong type
                        client.tx.send(Response::WrongType.into()).unwrap();
                        return;
                    }
                }, None => { // Key doesn't exist
                    no_following_entries += 1;
                    continue;
                }
            };
            bulk_str.push_str(&format!(
                "*2\r\n${}\r\n{stream_key}\r\n*1\r\n", stream_key.len()
            ));
            // Iterate over entries
            for entry in following_entries {
                let entry_id = format!("{}-{}", entry.ms_time, entry.seq_num);
                bulk_str.push_str(&format!(
                    "*2\r\n${}\r\n{entry_id}\r\n*{}\r\n",
                    entry_id.len(), entry.fields.len() * 2
                ));
                // Iterate over entry's fields
                for (key, val) in entry.fields.iter() {
                    bulk_str.push_str(&format!(
                        "${}\r\n{key}\r\n${}\r\n{val}\r\n", key.len(), val.len()
                    ));
                }
            }
        }
    }
    
    if to_block && no_following_entries == stream_keys.len() { // Blocking
        { // Add client to every blocked list
            let mut blocked_clients_guard = blocked_clients.lock().await;
            for (i, stream_key) in stream_keys.iter().enumerate() {
                blocked_clients_guard
                    .entry(stream_key.clone()) // Get entry of key if exists
                    .or_insert_with(VecDeque::new) // Or create an empty list
                    .push_back(BlockedClient { // Push blocked client to end of list
                        client: client.clone(),
                        blocked_by: stream_keys.clone(),
                        expired: false,
                        from_right: None, // <- Nonfactor
                        from_entry_id: Some(parsed_entry_ids[i].clone()),
                        count
                    });
            }
        }

        // Handle non-zero block via Tokio-runtime
        if block.unwrap() > 0 {
            let tokio_client = client.clone();
            let tokio_blocked_clients = blocked_clients.clone();
            let tokio_stream_keys = stream_keys.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(block.unwrap() as u64)).await;

                let mut still_blocked = false;
                let mut blocked_clients_guard = tokio_blocked_clients.lock().await;
                for stream_key in &tokio_stream_keys {
                    if let Some(blocked_list) = blocked_clients_guard.get_mut(stream_key) {
                        for blocked_client in blocked_list.iter_mut().filter(
                            |bc| bc.client.id == tokio_client.id
                        ) {
                            still_blocked = true;
                            blocked_client.expired = true; // Update expiry
                        }
                    }
                }

                if still_blocked {
                    tokio_client.tx.send(Response::NilArray.into()).unwrap();
                }
            });
        }
    } else { // Synchronous
        bulk_str = format!("*{}\r\n{bulk_str}", stream_keys.len() - no_following_entries);
        client.tx.send(bulk_str.into_bytes()).unwrap();
    }
}
