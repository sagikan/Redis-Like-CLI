use std::{collections::HashMap, sync::Arc};
use crate::db::*;
use crate::client::{Client, Response};
use crate::commands::_helpers_::{get_unix_time, get_last_entry};

async fn validate_added_entry_id(
    db: Database, stream_key: &String, entry_id: &String, client: &Client
) -> Option<EntryIdType> {
    if entry_id == "*" { // Full
        return Some(EntryIdType::Full(()));
    }
    
    let invalid_id = |response: Response| {
        client.tx.send(response.into()).unwrap();
        None
    };

    // Parse + validate
    match entry_id.split_once('-') {
        Some((ms_time, seq_num)) => {
            match (ms_time.parse::<usize>(), seq_num.parse::<usize>()) {
                (Ok(ms_time), Ok(seq_num)) => { // Explicit
                    // Invalidate 0-0
                    if (ms_time, seq_num) == (0, 0) {
                        invalid_id(Response::ErrEntryIdZero)
                    } else if let Some(last_entry) = get_last_entry(
                        db.clone(), &stream_key
                    ).await {
                        // Invalidate if not sequential to last entry
                        if ms_time < last_entry.ms_time
                           || (ms_time == last_entry.ms_time
                               && seq_num <= last_entry.seq_num) {
                            invalid_id(Response::ErrEntryIdEqualOrSmall)
                        } else { Some(EntryIdType::Explicit((ms_time, seq_num))) }
                    } else { Some(EntryIdType::Explicit((ms_time, seq_num))) }
                }, (Ok(ms_time), _) if seq_num == "*" => { // Partial
                    // Invalidate if ms_time is not sequential to last entry
                    if let Some(last_entry) = get_last_entry(
                        db.clone(), &stream_key
                    ).await {
                        if ms_time < last_entry.ms_time {
                            invalid_id(Response::ErrEntryIdEqualOrSmall)
                        } else { Some(EntryIdType::Partial(ms_time)) }
                    } else { Some(EntryIdType::Partial(ms_time)) }
                }, _ => invalid_id(Response::ErrEntryIdInvalid)
            }
        }, None => invalid_id(Response::ErrEntryIdInvalid)
    }
}

async fn gen_seq(db: Database, stream_key: &String, ms_time: usize) -> usize {
    match get_last_entry(db.clone(), &stream_key).await {
        Some(last_entry) => {
            if ms_time == last_entry.ms_time { // Same ms_time
                last_entry.seq_num + 1
            } else { // Fresh ms_time (> last entry's as previously validated)
                0
            }
        },
        None if ms_time > 0 => 0,
        None => 1 // ms_time = 0
    }
}

pub async fn cmd_xadd(to_send: bool, args: &[String], client: &Client,
                      db: Database, blocked_clients: BlockedClients) {
    let args_len = args.len();
    if args_len < 4 || args_len % 2 == 1 {
        client.send_if(to_send, Response::ErrArgCount);
        return;
    }

    // Extract stream key, entry ID (+ validate, automate), and fields
    let stream_key = &args[0];
    let (ms_time, seq_num) = match validate_added_entry_id(
        db.clone(), &stream_key, &args[1], &client
    ).await {
        Some(EntryIdType::Full(_)) => {
            let ms = get_unix_time(true); // Current UNIX time in ms
            let seq = gen_seq(db.clone(), &stream_key, ms).await;
            (ms, seq)
        }, Some(EntryIdType::Partial(ms)) => {
            let seq = gen_seq(db.clone(), &stream_key, ms).await;
            (ms, seq)
        }, Some(EntryIdType::Explicit((ms, seq))) => (ms, seq),
        None => return
    };
    let mut map = HashMap::new();
    for i in (2..args_len).step_by(2) {
        map.insert(args[i].clone(), args[i + 1].clone());
    }
    let fields: Fields = Arc::new(map);

    // Create entry
    let entry = Entry {
        ms_time: ms_time.clone(),
        seq_num: seq_num.clone(),
        fields
    };
    
    {
        let mut guard = db.lock().await;
        // Insert entry to stream
        match guard.get_mut(stream_key) {
            Some(value) => match &value.val {
                // An existing stream is found
                ValueType::Stream(stream) => stream.entries.lock().await.push_back(entry),
                _ => { // Value is of the wrong type
                    client.send_if(to_send, Response::WrongType);
                    return;
                }
            }, None => {
                let entries = Entries::default();
                // Insert entry + create stream
                entries.lock().await.push_back(entry);
                let stream = Value {
                    val: ValueType::Stream(Stream { entries }),
                    exp: None
                };
                // Insert stream
                guard.insert(stream_key.clone(), stream);
            }
        }
    }

    {
        let mut to_unblock = Vec::new();
        let mut blocked_clients_guard = blocked_clients.lock().await;
        // Look for blocked clients waiting for XADD
        if let Some(blocked_list) = blocked_clients_guard.get_mut(stream_key) {
            let guard = db.lock().await;
            // Build + emit bulk string for each client
            'outer: for blocked_client in blocked_list {
                let mut bulk_str = String::new();
                let mut with_following_entries = 0; // # of streams w/ following entries
                // Iterate over streams
                for key in &blocked_client.blocked_by {
                    // Get the following entries of the client's specified entry
                    let following_entries: Vec<Entry> = match guard.get(key) {
                        Some(value) => match &value.val {
                            ValueType::Stream(stream) => { // An existing stream is found
                                let (ms_time_, seq_num_) = match blocked_client.from_entry_id {
                                    Some((ms, seq)) => (ms, seq),
                                    None => { break 'outer; } // Not blocked by XREAD
                                };
                                let f_e: Vec<Entry> = stream.entries.lock().await.iter()
                                    .filter(
                                        |e| ms_time_ < e.ms_time
                                            || (ms_time_ == e.ms_time && seq_num_ < e.seq_num)
                                    ).take(
                                        blocked_client.count
                                            .map(|c| c as usize)
                                            .unwrap_or(usize::MAX) // Take all if no count
                                    ).cloned().collect();

                                if f_e.is_empty() { continue; } // No following entries
                                with_following_entries += 1;

                                f_e
                            }, _ => continue // Value is of the wrong type
                        }, None => { continue; } // Key doesn't exist
                    };
                    bulk_str.push_str(&format!("*2\r\n${}\r\n{key}\r\n*1\r\n", key.len()));
                    // Iterate over entries
                    for entry in following_entries {
                        let entry_id = format!("{}-{}", entry.ms_time, entry.seq_num);
                        bulk_str.push_str(&format!(
                            "*2\r\n${}\r\n{entry_id}\r\n*{}\r\n",
                            entry_id.len(), entry.fields.len() * 2
                        ));
                        // Iterate over entry's fields
                        for (f_key, f_val) in entry.fields.iter() {
                            bulk_str.push_str(&format!(
                                "${}\r\n{f_key}\r\n${}\r\n{f_val}\r\n",
                                f_key.len(), f_val.len()
                            ));
                        }
                    }
                }
                bulk_str = format!("*{with_following_entries}\r\n{bulk_str}");
                blocked_client.client.send_if(to_send, bulk_str.into_bytes());

                to_unblock.push(blocked_client.clone());
            }
        }

        // Unblock clients from all keys attached to their XREAD command
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

    // Emit the entry ID
    let entry_id = format!("{ms_time}-{seq_num}");
    client.send_if(to_send, format!(
        "${}\r\n{entry_id}\r\n", entry_id.len()
    ).into_bytes());
}
