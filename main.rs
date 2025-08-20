#![allow(unused_imports)]
mod db;
use std::collections::{VecDeque, HashMap};
use std::cmp::min;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::MutexGuard;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};
use crate::db::*;

fn resp_parse(to_parse: &[u8]) -> Vec<String> {
    let unparsed_str = std::str::from_utf8(to_parse).unwrap();
    let mut lines = unparsed_str.split("\r\n");

    // Skip array header
    lines.next();

    let mut parsed = Vec::new();
    while let Some(curr_line) = lines.next() {
        // Skip non-bulk-string-length lines
        if !curr_line.starts_with('$') { continue; }
        // Get length and add next line to parsed Vec
        let len = curr_line[1..].parse().unwrap();
        if let Some(val) = lines.next() {
            parsed.push(val[..len].to_string());
        }
    }

    parsed
}

fn str_to_i32<FOk: Fn(i32) -> i32>(to_parse: &String, ok_logic: FOk) -> Option<i32> {
    match to_parse.parse::<i32>() {
        Ok(val) => Some(ok_logic(val)),
        Err(_) => None
    }
}

fn str_to_f64<FOk: Fn(f64) -> f64>(to_parse: &String, ok_logic: FOk) -> Option<f64> {
    match to_parse.parse::<f64>() {
        Ok(val) => Some(ok_logic(val)),
        Err(_) => None
    }
}

fn set_pair(key: String, value: Value, client: &Client,
                  mut guard: MutexGuard<'_, HashMap<String, Value>>) {
    guard.insert(key, value);
    client.tx.send(b"+OK\r\n".to_vec()).unwrap();
}

async fn validate_entry_id(db: Database, stream_key: &String, entry_id: &String,
                           client: &Client) -> Option<EntryIdType> {
    if entry_id == "*" { // Full auto-gen
        return Some(EntryIdType::Full(()));
    }
    
    let invalid_id = |msg: &[u8]| {
        client.tx.send(msg.to_vec()).unwrap();
        None
    };

    match entry_id.split_once('-') {
        Some((ms_time, seq_num)) => {
            if let Ok(ms_time) = ms_time.parse::<u64>() {
                if let Ok(seq_num) = seq_num.parse::<u64>() { // Explicit
                    // Invalidate if 0-0 / not sequential to last entry
                    if (ms_time, seq_num) == (0, 0) {
                        invalid_id(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                    } else if let Some(last_entry) = get_last_entry(db.clone(), &stream_key).await {
                        if ms_time < last_entry.ms_time || (ms_time == last_entry.ms_time && seq_num <= last_entry.seq_num) {
                            invalid_id(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                        } else {
                            Some(EntryIdType::Explicit((ms_time, seq_num)))
                        }
                    } else {
                        Some(EntryIdType::Explicit((ms_time, seq_num)))
                    }
                } else if seq_num == "*" { // Partial auto-gen
                    // Invalidate if ms_time is not sequential to last entry
                    if let Some(last_entry) = get_last_entry(db.clone(), &stream_key).await {
                        if ms_time < last_entry.ms_time {
                            invalid_id(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                        } else {
                            Some(EntryIdType::Partial(ms_time))
                        }
                    } else {
                        Some(EntryIdType::Partial(ms_time))
                    }
                } else {
                    invalid_id(b"-ERR Invalid stream ID specified as stream command argument\r\n")
                }
            } else {
                invalid_id(b"-ERR Invalid stream ID specified as stream command argument\r\n")
            }
        }, None => invalid_id(b"-ERR Invalid stream ID specified as stream command argument\r\n")
    }
}

async fn get_last_entry(db: Database, stream_key: &String) -> Option<Entry> {
    if let Some(value) = db.lock().await.get_mut(stream_key) {
        if let ValueType::Stream(stream) = &value.val {
            if let Some(last_entry) = stream.entries.lock().await.back() {
                return Some(last_entry.clone());
            }
        }
    }

    None
}

fn gen_ms() -> u64 {
    // Return the current UNIX time in ms
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => t.as_millis() as u64,
        Err(_) => panic!("You're a time traveller, Harry!")
    }
}

async fn gen_seq(db: Database, stream_key: &String, ms_time: u64) -> u64 {
    if let Some(last_entry) = get_last_entry(db.clone(), &stream_key).await {
        // Same ms_time => Inc. seq_num
        if ms_time == last_entry.ms_time { last_entry.seq_num + 1 }
        // New ms_time (> last entry's as previously validated) => New sequence
        else { 0 }
    } else if ms_time > 0 { // Empty stream, non-zero ms_time => New sequence
        0
    } else { // Empty stream, ms_time = 0 => New sequence starting at 1
        1
    }
}

fn cmd_echo(parsed_cmd: &Vec<String>, client: &Client) {
    if let Some(_val) = parsed_cmd.get(1) {
        let to_write = format!("+{}\r\n", parsed_cmd[1..].join(" "));
        client.tx.send(to_write.as_bytes().to_vec()).unwrap();
    } else {
        client.tx.send(b"-ERR wrong number of arguments for 'echo' command\r\n".to_vec()).unwrap();
    }
}

async fn cmd_set(parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    let guard = db.lock().await;

    match parsed_cmd.len() {
        3 => { // SET [Key] [Value]
            let key = parsed_cmd[1].clone();
            let value = Value {
                val: ValueType::String(parsed_cmd[2].clone()),
                expiry: None
            };
            set_pair(key, value, &client, guard);
        }, 4 => { // SET [Key] [Value] [NX / XX / Wrong Syntax]
            let key = parsed_cmd[1].clone();
            let value = Value {
                val: ValueType::String(parsed_cmd[2].clone()),
                expiry: None
            };
            // Set only if NX + key doesn't exist OR if XX + key exists
            match (parsed_cmd[3].to_uppercase().as_str(), guard.contains_key(&key)) {
                ("NX", false) | ("XX", true) => set_pair(key, value, &client, guard),
                ("NX", true) | ("XX", false) => client.tx.send(b"$-1\r\n".to_vec()).unwrap(),
                _ => client.tx.send(b"-ERR syntax error\r\n".to_vec()).unwrap()
            }
        }, 5 => { // SET [Key] [Value] [EX / PX / Wrong Syntax] [Stringified Number / Wrong Syntax]
            match parsed_cmd[3].to_uppercase().as_str() {
                arg @ ("EX" | "PX") => {
                    // Extract expiry time (+ parse to milliseconds, check validity)
                    let timeout = match str_to_i32(&parsed_cmd[4], |val| if arg == "EX" { val * 1000 } else { val }) {
                        Some(v) if v > 0 => v,
                        _ => {
                            client.tx.send(b"-ERR invalid expire time in 'set' command\r\n".to_vec()).unwrap();
                            return;
                        }
                    };

                    let key = parsed_cmd[1].clone();
                    let value = Value {
                        val: ValueType::String(parsed_cmd[2].clone()),
                        expiry: Some(Instant::now() + Duration::from_millis(timeout as u64))
                    };

                    // Insert + employ active expiration via Tokio-runtime
                    set_pair(key.clone(), value, &client, guard);
                    let tokio_db = db.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(timeout as u64)).await;
                        tokio_db.lock().await.remove(&key);
                    });
                }, _ => client.tx.send(b"-ERR syntax error\r\n".to_vec()).unwrap()
            }
        }, _ => client.tx.send(b"-ERR wrong number of arguments for 'set' command\r\n".to_vec()).unwrap()
    }
}

async fn cmd_get(parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    if parsed_cmd.len() != 2 {
        client.tx.send(b"-ERR wrong number of arguments for 'get' command\r\n".to_vec()).unwrap();
        return;
    }

    let key = &parsed_cmd[1];

    let guard = db.lock().await;
    // Return (nil) if no such key is found
    if !guard.contains_key(key) {
        client.tx.send(b"$-1\r\n".to_vec()).unwrap();
        return;
    }

    // Extract + emit value
    let value: Value = guard.get(key).unwrap().clone();
    let val = match &value.val {
        ValueType::String(val) => val,
        _ => {
            eprintln!("Extraction Error");
            return;
        }
    };

    client.tx.send(format!("+{}\r\n", val).as_bytes().to_vec()).unwrap();
}

async fn cmd_push(from_right: bool, parsed_cmd: &Vec<String>, client: &Client,
                  db: Database, blocked_clients: BlockedClients) {
    if parsed_cmd.len() < 3 {
        let err_str = format!("-ERR wrong number of arguments for '{}' command\r\n", parsed_cmd[0].to_lowercase());
        client.tx.send(err_str.as_bytes().to_vec()).unwrap();
        return;
    }

    // Extract key and values
    let key = &parsed_cmd[1];
    let mut val_list: VecDeque<String> = parsed_cmd[2..].iter().cloned().collect();
    let val_list_len = val_list.len();

    {
        let mut blocked_clients_guard = blocked_clients.lock().await;
        let mut to_unblock = Vec::new();
        // Look for blocked clients waiting for push
        if let Some(blocked_list) = blocked_clients_guard.get_mut(key) {
            let pop_num = min(val_list.len(), blocked_list.len());
            for _ in 0..pop_num {
                let front_client = blocked_list.pop_front().unwrap();
                // Extract value based on [R\L]PUSH and client's B[R\L]POP
                let val = match (from_right, front_client.from_right) {
                    (true, true) | (false, false) => val_list.pop_back().unwrap(),
                    (true, false) | (false, true) => val_list.pop_front().unwrap()
                };

                // Emit to front_client an array of key + popped value
                front_client.client.tx.send(format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                                    key.len(), key, val.len(), val).as_bytes().to_vec()).unwrap();
                
                to_unblock.push(front_client);
            }
        }

        // Unblock clients from all keys attached to their BPOP command
        for blocked_client in to_unblock {
            for key in &blocked_client.blocked_by {
                if let Some(blocked_list) = blocked_clients_guard.get_mut(key) {
                    blocked_list.retain(|bc| bc.client.id != blocked_client.client.id);
                }
            }
        }
    }

    // Get updated number of values to push
    let upd_val_list_len = val_list.len();
    
    let mut guard = db.lock().await;
    // Create and insert string list if no such key is found
    let stored_val_list_len = if !guard.contains_key(key) {
        // Insert if there are still values to push
        if upd_val_list_len > 0 {
            let value = Value {
                val: ValueType::StringList(val_list),
                expiry: None
            };
            guard.insert(key.clone(), value);
        }

        val_list_len // Return OG length
    } else {
        let value = guard.get_mut(key).unwrap();
        match &mut value.val {
            // An existing string list is found
            ValueType::StringList(stored_val_list) => {
                // Insert if there are still values to push
                if val_list_len > 0 {
                    match from_right {
                        true => stored_val_list.extend(val_list),
                        false => while let Some(val) = val_list.pop_front() {
                            stored_val_list.push_front(val);
                        }
                    }
                }

                stored_val_list.len()
            }, // Value is of the wrong type
            _ => {
                client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                return;
            }
        }
    };
    
    // Emit the list's length
    client.tx.send(format!(":{}\r\n", stored_val_list_len).as_bytes().to_vec()).unwrap();
}

async fn cmd_pop(from_right: bool, parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    let args_len = parsed_cmd.len();
    if args_len != 2 && args_len != 3 {
        let err_str = format!("-ERR wrong number of arguments for '{}' command\r\n", parsed_cmd[0].to_lowercase());
        client.tx.send(err_str.as_bytes().to_vec()).unwrap();
        return;
    }

    let key = &parsed_cmd[1];
    // Get number of pops based on args length
    let pop_num = match args_len {
        2 => 1,
        3 => match str_to_i32(&parsed_cmd[2], |val| val) {
                Some(v) if v >= 0 => v, // 0 or positive number
                _ => { // Negative number / not parseable
                    client.tx.send(b"-ERR value is out of range, must be positive\r\n".to_vec()).unwrap();
                    return;
                }
            },
        _ => 0
    };
    
    let mut guard = db.lock().await;
    // Return (nil) if no such key is found
    if !guard.contains_key(key) {
        client.tx.send(b"$-1\r\n".to_vec()).unwrap();
        return;
    }
    
    let value = guard.get_mut(key).unwrap();
    match &mut value.val {
        // An existing string list is found
        ValueType::StringList(val_list) => {
            // Clamp number of pops and set L/R functionality
            let val_list_len = val_list.len();
            let pop_num_usize: usize = min(pop_num as usize, val_list_len);
            let mut pop = || if from_right { val_list.pop_back() } else { val_list.pop_front() };

            // Pop & build bulk string
            let bulk_str = match pop_num_usize {
                1 => match pop() {
                        Some(v) => format!("+{}\r\n", v),
                        None => return
                    },
                _ => format!("*{}\r\n{}", pop_num_usize,
                        (0..pop_num_usize) // For [pop_num_usize] times
                            .filter_map(|_| pop()) // Pop, filter & map popped values
                            .map(|val| format!("${}\r\n{}\r\n", val.len(), val)) // Stringify each value
                            .collect::<String>()) // Unify to one string
            };

            client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();

            // Remove key if popped all values
            if pop_num_usize == val_list_len { guard.remove(key); }
        }, // Value is of the wrong type
        _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
    }
}

async fn cmd_bpop(from_right: bool, parsed_cmd: &Vec<String>, client: &Client,
                  db: Database, blocked_clients: BlockedClients) {
    let args_len = parsed_cmd.len();
    if args_len < 3 {
        let err_str = format!("-ERR wrong number of arguments for '{}' command\r\n", parsed_cmd[0].to_lowercase());
        client.tx.send(err_str.as_bytes().to_vec()).unwrap();
        return;
    }

    // Extract keys
    let keys: Vec<String> = parsed_cmd[1..args_len-1].to_vec();

    // Try immediate pop
    {
        let mut guard = db.lock().await;
        for key in &keys {
            // Skip 
            if !guard.contains_key(key) { continue; }

            let value = guard.get_mut(key).unwrap();
            let val: String = match &mut value.val {
                // An existing string list is found
                ValueType::StringList(val_list) =>
                    match from_right {
                        true => val_list.pop_back().unwrap(),
                        false => val_list.pop_front().unwrap()
                    },
                // Value is of the wrong type
                _ => {
                    client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                    return;
                }
            };

            // Emit key + popped value array
            client.tx.send(format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                   key.len(), key, val.len(), val)
                           .as_bytes().to_vec()).unwrap();
            return;
        }
    }

    // All keys are empty => add client to every blocked list
    {
        let mut blocked_clients_guard = blocked_clients.lock().await;
        for key in &keys {
            blocked_clients_guard
                .entry(key.clone()) // Get entry of key if exists
                .or_insert_with(VecDeque::new) // If not, create an empty list
                .push_back(BlockedClient { // Push client to end of list
                    client: client.clone(),
                    from_right,
                    blocked_by: keys.clone()
                });
        }
    }

    // Extract timeout
    let timeout = match str_to_f64(&parsed_cmd[args_len-1], |val| val) {
        Some(v) if v >= 0.0 => v, // 0 or positive number
        _ => { // Negative number / not parseable
            client.tx.send(b"-ERR timeout is negative\r\n".to_vec()).unwrap();
            return;
        }
    };

    // Handle non-zero timeout via Tokio-runtime
    if timeout > 0.0 {
        let tokio_client = client.clone();
        let tokio_blocked_clients = blocked_clients.clone();
        let tokio_keys = keys.clone();
        tokio::spawn(async move {
            sleep(Duration::from_secs_f64(timeout)).await;

            let mut blocked_clients_guard = tokio_blocked_clients.lock().await;
            // Client is still blocked by some list (=> haven't popped yet)
            if tokio_keys.iter().any(|key|
                if let Some(blocked_list) = blocked_clients_guard.get_mut(key) {
                    blocked_list.iter().any(|bc| bc.client.id == tokio_client.id)
                } else { false }
            ) {
                // Emit (nil)
                tokio_client.tx.send(b"$-1\r\n".to_vec()).unwrap();
            }
        });
    }
}

async fn cmd_lrange(parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    if parsed_cmd.len() != 4 {
        client.tx.send(b"-ERR wrong number of arguments for 'lrange' command\r\n".to_vec()).unwrap();
        return;
    }

    // Extract key, start and stop args
    let key = &parsed_cmd[1];
    let mut start = match str_to_i32(&parsed_cmd[2], |val| val) {
        Some(v) => v,
        None => {
            client.tx.send(b"-ERR value is not an integer or out of range\r\n".to_vec()).unwrap();
            return;
        }
    };
    let mut stop = match str_to_i32(&parsed_cmd[3], |val| val) {
        Some(v) => v,
        None => {
            client.tx.send(b"-ERR value is not an integer or out of range\r\n".to_vec()).unwrap();
            return;
        }
    };
    
    let mut guard = db.lock().await;
    // Emit an empty array if no such key is found
    if !guard.contains_key(key) {
        client.tx.send(b"*0\r\n".to_vec()).unwrap();
        return;
    }

    let value = guard.get_mut(key).unwrap();
    match &mut value.val {
        // An existing string list is found
        ValueType::StringList(val_list) => {
            // Adjust and clamp start and stop
            let val_list_len: i32 = val_list.len() as i32;
            let adjust = |x: i32| if x < 0 { val_list_len + x } else { x };
            start = adjust(start);
            stop = adjust(stop);
            if start < 0 { start = 0; }
            if stop < 0 { stop = -1; }
            // + Clamp stop to list range
            stop = min(stop, val_list_len - 1);

            // Emit an empty array if not valid post-adjustment
            if start > stop {
                client.tx.send(b"*0\r\n".to_vec()).unwrap();
                return;
            }

            // Build bulk string
            let range_size = stop - start + 1;
            let mut bulk_str = format!("*{}\r\n", range_size);
            for val in val_list.iter().skip(start as usize).take(range_size as usize) {
                bulk_str.push_str(&format!("${}\r\n{}\r\n", val.len(), val));
            }

            client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();
        }, // Value is of the wrong type
        _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
    }
}

async fn cmd_llen(parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    if parsed_cmd.len() != 2 {
        client.tx.send(b"-ERR wrong number of arguments for 'llen' command".to_vec()).unwrap();
        return;
    }

    let val_list_len = match db.lock().await.get_mut(&parsed_cmd[1]) {
        None => 0, // No such key is found (=> no string list)
        Some(value) => match &mut value.val {
            ValueType::StringList(val_list) => val_list.len(),
            _ => { // Value is of the wrong type
                client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                return;
            }
        }
    };

    // Emit the list's length
    client.tx.send(format!(":{}\r\n", val_list_len).as_bytes().to_vec()).unwrap();
}

async fn cmd_type(parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    if parsed_cmd.len() != 2 {
        client.tx.send(b"-ERR wrong number of arguments for 'type' command\r\n".to_vec()).unwrap();
        return;
    }
    
    let val_type = match db.lock().await.get(&parsed_cmd[1]) {
        Some(value) => match &value.val {
            ValueType::String(_) => "string",
            ValueType::StringList(_) => "list",
            ValueType::Stream(_) => "stream",
            _ => "none"
        },
        None => "none"
    };

    client.tx.send(format!("+{}\r\n", val_type).as_bytes().to_vec()).unwrap();
}

async fn cmd_xadd(parsed_cmd: &Vec<String>, client: &Client, db: Database) {
    let args_len = parsed_cmd.len();
    if args_len < 5 || args_len % 2 == 0 {
        client.tx.send(b"-ERR wrong number of arguments for 'xadd' command\r\n".to_vec()).unwrap();
        return;
    }

    // Extract stream key, entry ID (+ validate, automate), and fields
    let stream_key = &parsed_cmd[1];
    let (ms_time, seq_num) = match validate_entry_id(db.clone(), &stream_key,
                                                     &parsed_cmd[2], &client).await
    {
        Some(EntryIdType::Full(_)) => {
            let ms = gen_ms();
            let seq = gen_seq(db.clone(), &stream_key, ms).await;
            (ms, seq)
        }, Some(EntryIdType::Partial(ms)) => {
            let seq = gen_seq(db.clone(), &stream_key, ms).await;
            (ms, seq)
        }, Some(EntryIdType::Explicit((ms, seq))) => (ms, seq),
        None => return
    };
    let mut map = HashMap::new();
    for i in (3..args_len).step_by(2) {
        map.insert(parsed_cmd[i].clone(), parsed_cmd[i+1].clone());
    }
    let fields: Fields = Arc::new(map);

    // Create entry
    let entry = Entry {
        ms_time: ms_time.clone(),
        seq_num: seq_num.clone(),
        fields
    };

    let mut guard = db.lock().await;
    // Insert entry to stream
    if let Some(value) = guard.get_mut(stream_key) {
        match &value.val {
            // An existing stream is found
            ValueType::Stream(stream) => stream.entries.lock().await.push_back(entry),
            // Value is of the wrong type
            _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
        }
    } else {
        let entries = Entries::default();
        // Insert entry + create stream
        entries.lock().await.push_back(entry);
        let stream = Value {
            val: ValueType::Stream(Stream { entries }),
            expiry: None
        };
        // Insert stream
        guard.insert(stream_key.clone(), stream);
    }

    // Emit the entry ID
    let entry_id = format!("{ms_time}-{seq_num}");
    client.tx.send(format!("${}\r\n{}\r\n", entry_id.len(), entry_id).as_bytes().to_vec()).unwrap();
}

fn cmd_other(parsed_cmd: &Vec<String>, client: &Client) {
    // Build error string
    let mut err_str = format!("-ERR unknown command `{}`, with args beginning with: ", parsed_cmd[0]);
    for arg in &parsed_cmd[1..] {
        err_str.push_str(&format!("`{}`, ", arg));
    }
    err_str.push_str("\r\n");

    // Emit the string
    client.tx.send(err_str.as_bytes().to_vec()).unwrap();
}

async fn process_cmd(cmd: &[u8], client: &Client,
                     db: Database, blocked_clients: BlockedClients) {
    let parsed_cmd: Vec<String> = resp_parse(cmd);

    // Handle by command
    match parsed_cmd[0].to_uppercase().as_str() {
        "PING"   => client.tx.send(b"+PONG\r\n".to_vec()).unwrap(),
        "ECHO"   => cmd_echo(&parsed_cmd, &client),
        "SET"    => cmd_set(&parsed_cmd, &client, db).await,
        "GET"    => cmd_get(&parsed_cmd, &client, db).await,
        "RPUSH"  => cmd_push(true, &parsed_cmd, &client, db, blocked_clients).await,
        "LPUSH"  => cmd_push(false, &parsed_cmd, &client, db, blocked_clients).await,
        "RPOP"   => cmd_pop(true, &parsed_cmd, &client, db).await,
        "LPOP"   => cmd_pop(false, &parsed_cmd, &client, db).await,
        "BRPOP"  => cmd_bpop(true, &parsed_cmd, &client, db, blocked_clients).await,
        "BLPOP"  => cmd_bpop(false, &parsed_cmd, &client, db, blocked_clients).await,
        "LRANGE" => cmd_lrange(&parsed_cmd, &client, db).await,
        "LLEN"   => cmd_llen(&parsed_cmd, &client, db).await,
        "TYPE"   => cmd_type(&parsed_cmd, &client, db).await,
        "XADD"   => cmd_xadd(&parsed_cmd, &client, db).await,
        _        => cmd_other(&parsed_cmd, &client)
    }
}

#[tokio::main]
async fn main() {
    // Set listener to port 6379 + maps
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Database::default();
    let blocked_clients = BlockedClients::default();
    
    // Event loop
    loop {
        // Accept client
        let (socket, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = unbounded_channel();
        let client = Client { id: get_next_id(), tx };

        // Tokio-runtime write task
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                // Write + check for disconnection
                if writer.write_all(&msg).await.is_err() { break; }
            }
        });

        // Tokio-runtime read task
        let tokio_db = db.clone();
        let tokio_blocked_clients = blocked_clients.clone();
        tokio::spawn(async move {
            let mut buf = vec![0; 1024];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => process_cmd(&buf[0..n], &client, tokio_db.clone(),
                                         tokio_blocked_clients.clone()).await,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                }
            }
        });
    }
}
