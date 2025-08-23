use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::cmp::min;
use tokio::sync::MutexGuard;
use tokio::time::{sleep, Duration};
use crate::db::*;

fn set_pair(key: String, value: Value, client: &Client,
            mut guard: MutexGuard<'_, HashMap<String, Value>>) {
    guard.insert(key, value);
    client.tx.send(b"+OK\r\n".to_vec()).unwrap();
}

async fn validate_added_entry_id(
    db: Database, stream_key: &String, entry_id: &String, client: &Client
) -> Option<EntryIdType> {
    if entry_id == "*" { // Full
        return Some(EntryIdType::Full(()));
    }
    
    let invalid_id = |msg: &[u8]| {
        client.tx.send(msg.to_vec()).unwrap();
        None
    };

    // Parse + validate
    match entry_id.split_once('-') {
        Some((ms_time, seq_num)) => {
            match (ms_time.parse::<u64>(), seq_num.parse::<u64>()) {
                (Ok(ms_time), Ok(seq_num)) => { // Explicit
                    // Invalidate 0-0
                    if (ms_time, seq_num) == (0, 0) {
                        invalid_id(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                    } else if let Some(last_entry) = get_last_entry(
                        db.clone(), &stream_key
                    ).await {
                        // Invalidate if not sequential to last entry
                        if ms_time < last_entry.ms_time
                           || (ms_time == last_entry.ms_time
                               && seq_num <= last_entry.seq_num) {
                            invalid_id(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                        } else { Some(EntryIdType::Explicit((ms_time, seq_num))) }
                    } else { Some(EntryIdType::Explicit((ms_time, seq_num))) }
                }, (Ok(ms_time), _) if seq_num == "*" => { // Partial
                    // Invalidate if ms_time is not sequential to last entry
                    if let Some(last_entry) = get_last_entry(
                        db.clone(), &stream_key
                    ).await {
                        if ms_time < last_entry.ms_time {
                            invalid_id(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                        } else { Some(EntryIdType::Partial(ms_time)) }
                    } else { Some(EntryIdType::Partial(ms_time)) }
                }, _ => invalid_id(b"-ERR Invalid stream ID specified as stream command argument\r\n")
            }
        }, None => invalid_id(b"-ERR Invalid stream ID specified as stream command argument\r\n")
    }
}

async fn validate_read_entry_id<F: Fn(&String) -> bool>(
    full_check: F, entry_id: &String, client: &Client
) -> Option<EntryIdType> {
    if full_check(entry_id) { // Full
        return Some(EntryIdType::Full(()));
    }

    let invalid_id = || {
        client.tx.send(b"-ERR Invalid stream ID specified as stream command argument\r\n".to_vec()).unwrap();
        None
    };

    // Parse + validate
    match entry_id.split_once('-') {
        Some((ms_time, seq_num)) => { // Explicit
            match (ms_time.parse::<u64>(), seq_num.parse::<u64>()) {
                (Ok(ms_time), Ok(seq_num)) =>
                    Some(EntryIdType::Explicit((ms_time, seq_num))),
                _ => invalid_id()
            }
        }, None => { // Partial
            match entry_id.parse::<u64>() {
                Ok(ms_time) => Some(EntryIdType::Partial(ms_time)),
                _ => invalid_id()
            }
        }
    }
}

async fn get_first_entry(db: Database, stream_key: &String) -> Option<Entry> {
    if let Some(value) = db.lock().await.get_mut(stream_key) {
        if let ValueType::Stream(stream) = &value.val {
            if let Some(first_entry) = stream.entries.lock().await.front() {
                return Some(first_entry.clone());
            }
        }
    }

    None
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

async fn get_last_seq_entry(db: Database, stream_key: &String,
                            ms_time: u64) -> Option<Entry> {
    if let Some(value) = db.lock().await.get_mut(stream_key) {
        if let ValueType::Stream(stream) = &value.val {
            // Iterate from the right to assure max sequence
            if let Some(entry) = stream.entries.lock().await.iter()
                                               .rfind(|e| e.ms_time == ms_time) {
                return Some(entry.clone());
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

fn cmd_echo(args: Vec<String>, client: &Client) {
    if let Some(_val) = args.get(0) {
        let to_write = format!("+{}\r\n", args[0..].join(" "));
        client.tx.send(to_write.as_bytes().to_vec()).unwrap();
    } else {
        client.tx.send(b"-ERR wrong number of arguments for 'echo' command\r\n".to_vec()).unwrap();
    }
}

async fn cmd_set(args: Vec<String>, client: &Client, db: Database) {
    let guard = db.lock().await;

    match args.len() {
        2 => { // [Key] [Value]
            let key = args[0].clone();
            let value = Value { val: ValueType::String(args[1].clone()) };
            set_pair(key, value, &client, guard);
        }, 3 => { // [Key] [Value] [NX / XX]
            let key = args[0].clone();
            let value = Value { val: ValueType::String(args[1].clone()) };
            // Set only if NX + key doesn't exist OR if XX + key exists
            match (
                args[2].to_uppercase().as_str(), guard.contains_key(&key)
            ) {
                ("NX", false) | ("XX", true) => set_pair(key, value, &client, guard),
                ("NX", true) | ("XX", false) => client.tx.send(b"$-1\r\n".to_vec()).unwrap(),
                _ => client.tx.send(b"-ERR syntax error\r\n".to_vec()).unwrap()
            }
        }, 4 => { // [Key] [Value] [EX / PX] [Stringified Number]
            match args[2].to_uppercase().as_str() {
                arg @ ("EX" | "PX") => {
                    // Extract expiry time (+ parse to milliseconds, check validity)
                    let timeout = match args[3].parse::<u64>() {
                        Ok(t) if t > 0 => if arg == "EX" { t * 1000 } else { t },
                        _ => {
                            client.tx.send(b"-ERR invalid expire time in 'set' command\r\n".to_vec()).unwrap();
                            return;
                        }
                    };

                    let key = args[0].clone();
                    let value = Value { val: ValueType::String(args[1].clone()) };

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

async fn cmd_get(args: Vec<String>, client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(b"-ERR wrong number of arguments for 'get' command\r\n".to_vec()).unwrap();
        return;
    }

    // Extract + emit value
    let val = match db.lock().await.get(&args[0]) {
        Some(value) => match &value.val {
            ValueType::String(val) => val.clone(),
            _ => { panic!("Extraction Error"); }
        }, None => {
            client.tx.send(b"$-1\r\n".to_vec()).unwrap(); // (nil)
            return;
        }
    };

    client.tx.send(format!("+{val}\r\n").as_bytes().to_vec()).unwrap();
}

async fn cmd_push(from_right: bool, args: Vec<String>, client: &Client,
                  db: Database, blocked_clients: BlockedClients) {
    if args.len() < 2 {
        client.tx.send(b"-ERR wrong number of arguments for command\r\n".to_vec()).unwrap();
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
                front_client.client.tx.send(format!("*2\r\n${}\r\n{key}\r\n${}\r\n{val}\r\n",
                                                    key.len(), val.len())
                                            .as_bytes().to_vec()).unwrap();
                
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
                client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                return;
            }
        }, None => {
            // Create and insert list if there are values to push
            if val_list.len() > 0 {
                let value = Value { val: ValueType::StringList(val_list) };
                guard.insert(key.clone(), value);
            }

            val_list_len // OG length
        }
    };
    
    // Emit the list's length
    client.tx.send(format!(":{stored_val_list_len}\r\n").as_bytes().to_vec()).unwrap();
}

async fn cmd_pop(from_right: bool, args: Vec<String>, client: &Client,
                 db: Database) {
    let args_len = args.len();
    if args_len != 1 && args_len != 2 {
        client.tx.send(b"-ERR wrong number of arguments for command\r\n".to_vec()).unwrap();
        return;
    }

    let key = &args[0];
    // Get number of pops based on args length
    let pop_num = match args_len {
        1 => 1,
        2 => match args[1].parse::<u64>() {
            Ok(v) => v,
            _ => {
                client.tx.send(b"-ERR value is out of range, must be positive\r\n".to_vec()).unwrap();
                return;
            }
        }, _ => 0
    };
    
    let mut guard = db.lock().await;
    match guard.get_mut(key) {
        Some(value) => match &mut value.val {
            ValueType::StringList(val_list) => { // An existing list is found
                // Clamp number of pops and set L/R functionality
                let val_list_len = val_list.len();
                let pop_num_usize: usize = min(pop_num as usize, val_list_len);
                let mut my_pop = || if from_right { val_list.pop_back() }
                                    else { val_list.pop_front() };

                // Pop while building bulk string
                let bulk_str = match pop_num_usize {
                    1 => match my_pop() {
                        Some(v) => format!("+{v}\r\n"),
                        None => return
                    }, _ => format!("*{pop_num_usize}\r\n{}",
                                (0..pop_num_usize) // For [pop_num_usize] times
                                    .filter_map(|_| my_pop()) // Pop -> filter -> map popped values
                                    .map(|val| format!("${}\r\n{val}\r\n", val.len())) // Stringify
                                    .collect::<String>()) // Unify to one string
                };

                client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();

                // Remove key if popped all values
                if pop_num_usize == val_list_len {
                    guard.remove(key);
                }
            }, // Value is of the wrong type
            _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
        }, None => client.tx.send(b"$-1\r\n".to_vec()).unwrap()
    }    
}

async fn cmd_bpop(from_right: bool, args: Vec<String>, client: &Client,
                  db: Database, blocked_clients: BlockedClients) {
    let args_len = args.len();
    if args_len < 2 {
        client.tx.send(b"-ERR wrong number of arguments for command\r\n".to_vec()).unwrap();
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
                        client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                        return;
                    }
                }, None => { continue; } // Skip
            };

            // Emit key + popped value array
            client.tx.send(format!("*2\r\n${}\r\n{key}\r\n${}\r\n{val}\r\n",
                                   key.len(), val.len()).as_bytes().to_vec()).unwrap();
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
            client.tx.send(b"-ERR timeout is negative\r\n".to_vec()).unwrap();
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
                tokio_client.tx.send(b"$-1\r\n".to_vec()).unwrap(); // (nil)
            }
        });
    }
}

async fn cmd_lrange(args: Vec<String>, client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(b"-ERR wrong number of arguments for 'lrange' command\r\n".to_vec()).unwrap();
        return;
    }

    // Extract key, start and stop args
    let key = &args[0];
    let (mut start, mut stop) = match (
        args[1].parse::<i32>(),
        args[2].parse::<i32>()
    ) {
        (Ok(start), Ok(stop)) => (start, stop),
        _ => {
            client.tx.send(b"-ERR value is not an integer or out of range\r\n".to_vec()).unwrap();
            return;
        }
    };

    match db.lock().await.get_mut(key) {
        Some(value) => match &mut value.val {
            ValueType::StringList(val_list) => { // An existing list is found
                // Adjust + clamp start and stop
                let val_list_len: i32 = val_list.len() as i32;
                let adjust = |x: i32| if x < 0 { val_list_len + x } else { x };
                start = adjust(start);
                stop = adjust(stop);
                if start < 0 { start = 0; }
                if stop < 0 { stop = -1; }
                // + Clamp stop to list range
                stop = min(stop, val_list_len - 1);

                if start > stop { // Invalid post-adjustment
                    client.tx.send(b"*0\r\n".to_vec()).unwrap(); // Empty array
                    return;
                }

                // Build bulk string
                let range_size = stop - start + 1;
                let mut bulk_str = format!("*{range_size}\r\n");
                for val in val_list.iter().skip(start as usize)
                                   .take(range_size as usize) {
                    bulk_str.push_str(&format!("${}\r\n{val}\r\n", val.len()));
                }

                client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();
            }, // Value is of the wrong type
            _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
        }, None => client.tx.send(b"*0\r\n".to_vec()).unwrap() // Empty array
    }
}

async fn cmd_llen(args: Vec<String>, client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(b"-ERR wrong number of arguments for 'llen' command".to_vec()).unwrap();
        return;
    }

    let val_list_len = match db.lock().await.get_mut(&args[0]) {
        Some(value) => match &mut value.val {
            // An existing list is found
            ValueType::StringList(val_list) => val_list.len(),
            _ => { // Value is of the wrong type
                client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                return;
            }
        }, None => 0 // Key doesn't exist (and hence a list)
    };

    // Emit the list's length
    client.tx.send(format!(":{val_list_len}\r\n").as_bytes().to_vec()).unwrap();
}

async fn cmd_type(args: Vec<String>, client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(b"-ERR wrong number of arguments for 'type' command\r\n".to_vec()).unwrap();
        return;
    }
    
    let val_type = match db.lock().await.get(&args[0]) {
        Some(value) => match &value.val {
            ValueType::String(_) => "string",
            ValueType::StringList(_) => "list",
            ValueType::Stream(_) => "stream"
        }, None => "none"
    };

    client.tx.send(format!("+{val_type}\r\n").as_bytes().to_vec()).unwrap();
}

async fn cmd_xadd(args: Vec<String>, client: &Client, db: Database,
                  blocked_clients: BlockedClients) {
    let args_len = args.len();
    if args_len < 4 || args_len % 2 == 1 {
        client.tx.send(b"-ERR wrong number of arguments for 'xadd' command\r\n".to_vec()).unwrap();
        return;
    }

    // Extract stream key, entry ID (+ validate, automate), and fields
    let stream_key = &args[0];
    let (ms_time, seq_num) = match validate_added_entry_id(
        db.clone(), &stream_key, &args[1], &client
    ).await {
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
                // Value is of the wrong type
                _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
            }, None => {
                let entries = Entries::default();
                // Insert entry + create stream
                entries.lock().await.push_back(entry);
                let stream = Value { val: ValueType::Stream(Stream { entries }) };
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
                                    None => { break 'outer; } // Not blocked by XREAD operation
                                };
                                let f_e: Vec<Entry> = stream.entries.lock().await.iter().filter(
                                    |e| ms_time_ < e.ms_time
                                        || (ms_time_ == e.ms_time && seq_num_ < e.seq_num)
                                ).take(
                                    blocked_client.count.map(|c| c as usize).unwrap_or(usize::MAX)
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
                        bulk_str.push_str(&format!("*2\r\n${}\r\n{entry_id}\r\n*{}\r\n",
                                                   entry_id.len(), entry.fields.len() * 2));
                        // Iterate over entry's fields
                        for (f_key, f_val) in entry.fields.iter() {
                            bulk_str.push_str(&format!("${}\r\n{f_key}\r\n${}\r\n{f_val}\r\n",
                                                       f_key.len(), f_val.len()));
                        }
                    }
                }
                bulk_str = format!("*{with_following_entries}\r\n{bulk_str}");
                blocked_client.client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();

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
    client.tx.send(format!("${}\r\n{entry_id}\r\n", entry_id.len())
                   .as_bytes().to_vec()).unwrap();
}

async fn cmd_xrange(args: Vec<String>, client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(b"-ERR wrong number of arguments for 'xrange' command\r\n".to_vec()).unwrap();
        return;
    }

    // Extract key + start, end entry IDs
    let stream_key = &args[0];
    let (s_ms_time, s_seq_num) = match validate_read_entry_id(
        |e_id| e_id == "-", &args[1], &client
    ).await {
        Some(EntryIdType::Full(_)) => { // Entry ID is '-'
            match get_first_entry(db.clone(), &stream_key).await {
                Some(e) => (e.ms_time, e.seq_num),
                None => { // No entries
                    client.tx.send(b"*0\r\n".to_vec()).unwrap(); // Empty array
                    return;
                }
            }
        },
        Some(EntryIdType::Partial(ms)) => (ms, 0),
        Some(EntryIdType::Explicit((ms, seq))) => (ms, seq),
        None => return
    };
    let (e_ms_time, e_seq_num) = match validate_read_entry_id(
        |e_id| e_id == "+", &args[2], &client
    ).await {
        Some(EntryIdType::Full(_)) => { // Entry ID is '+'
            match get_last_entry(db.clone(), &stream_key).await {
                Some(e) => (e.ms_time, e.seq_num),
                None => { // No entries
                    client.tx.send(b"*0\r\n".to_vec()).unwrap(); // Empty array
                    return;
                }
            }
        },
        Some(EntryIdType::Partial(ms)) => {
            // Set with the max seq_num of [ms]
            let seq = match get_last_seq_entry(db.clone(), &stream_key, ms).await {
                Some(e) => e.seq_num,
                None if ms > 0 => 0,
                _ => 1
            };
            (ms, seq)
        },
        Some(EntryIdType::Explicit((ms, seq))) => (ms, seq),
        None => return
    };

    // Range not sequential
    if s_ms_time > e_ms_time || (s_ms_time == e_ms_time && s_seq_num > e_seq_num) {
        client.tx.send(b"*0\r\n".to_vec()).unwrap(); // Empty array
        return;
    }

    if let Some(value) = db.lock().await.get_mut(stream_key) {
        match &value.val {
            ValueType::Stream(stream) => { // An existing stream is found
                // Filter entries based on range
                let ranged: Vec<Entry> = stream.entries.lock().await.iter().filter(
                    |e| s_ms_time <= e.ms_time && e_ms_time >= e.ms_time
                        && s_seq_num <= e.seq_num && e_seq_num >= e.seq_num
                ).cloned().collect();
                
                // Build + emit bulk string
                let mut bulk_str = String::new();
                bulk_str.push_str(&format!("*{}\r\n", ranged.len()));
                // Iterate over entries
                for entry in ranged {
                    let e_id = format!("{}-{}", entry.ms_time, entry.seq_num);
                    bulk_str.push_str(&format!("*2\r\n${}\r\n{e_id}\r\n*{}\r\n",
                                               e_id.len(), entry.fields.len() * 2));
                    // Iterate over entry's fields
                    for (key, val) in entry.fields.iter() {
                        bulk_str.push_str(&format!("${}\r\n{key}\r\n${}\r\n{val}\r\n",
                                                   key.len(), val.len()));
                    }
                }

                client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();
            }, // Value is of the wrong type
            _ => client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap()
        }
    }
}

async fn cmd_xread(args: Vec<String>, client: &Client, db: Database,
                   blocked_clients: BlockedClients) {
    let args_len = args.len();
    let wrong_args_len = || {
        client.tx.send(b"-ERR wrong number of arguments for 'xread' command\r\n".to_vec()).unwrap();
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
                args[1].parse::<u64>(),
                args[3].parse::<u64>()
            ) {
                (Ok(c), Ok(b)) => (c, b),
                _ => {
                    client.tx.send(b"-ERR value is not an integer or out of range\r\n".to_vec()).unwrap();
                    return;
                }
            };
            let key_num = (args_len - 5) / 2;
            let s_k: Vec<String> = args[5..5+key_num].to_vec();
            let e_i: Vec<String> = args[5+key_num..args_len].to_vec();

            (Some(c), Some(b), s_k, e_i)
        }, arg_names @ (("COUNT", "STREAMS", _) | ("BLOCK", "STREAMS", _)) => {
            if args_len < 5 { wrong_args_len(); }

            let x = match args[1].parse::<u64>() { // Value of count / block
                Ok(x) => x,
                _ => {
                    client.tx.send(b"-ERR value is not an integer or out of range\r\n".to_vec()).unwrap();
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
            client.tx.send(b"-ERR syntax error".to_vec()).unwrap();
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
                        client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                        return;
                    }
                }, None => { // Key doesn't exist
                    no_following_entries += 1;
                    continue;
                }
            };
            bulk_str.push_str(&format!("*2\r\n${}\r\n{stream_key}\r\n*1\r\n",
                                       stream_key.len()));
            // Iterate over entries
            for entry in following_entries {
                let entry_id = format!("{}-{}", entry.ms_time, entry.seq_num);
                bulk_str.push_str(&format!("*2\r\n${}\r\n{entry_id}\r\n*{}\r\n",
                                           entry_id.len(), entry.fields.len() * 2));
                // Iterate over entry's fields
                for (key, val) in entry.fields.iter() {
                    bulk_str.push_str(&format!("${}\r\n{key}\r\n${}\r\n{val}\r\n",
                                               key.len(), val.len()));
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
                sleep(Duration::from_millis(block.unwrap())).await;

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
                    tokio_client.tx.send(b"$-1\r\n".to_vec()).unwrap(); // (nil)
                }
            });
        }
    } else { // Synchronous
        bulk_str = format!("*{}\r\n{bulk_str}", stream_keys.len() - no_following_entries);
        client.tx.send(bulk_str.as_bytes().to_vec()).unwrap();
    }
}

async fn cmd_incr(args: Vec<String>, client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(b"-ERR wrong number of arguments for 'incr' command\r\n".to_vec()).unwrap();
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
                }, Err(_) => { // Not an integer
                    client.tx.send(b"-ERR value is not an integer or out of range\r\n".to_vec()).unwrap();
                    return;
                }
            }, _ => { // Value is of the wrong type
                client.tx.send(b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n".to_vec()).unwrap();
                return;
            }
        }, None => { // Key doesn't exist
            // Set value to 1 + insert pair
            let res = "1".to_string();
            let value = Value { val: ValueType::String(res.clone()) };
            guard.insert(key.clone(), value);
            
            res
        }
    };

    // Emit new value
    client.tx.send(format!(":{incr_val}\r\n").as_bytes().to_vec()).unwrap();
}

async fn cmd_multi(client: &Client) {
    let mut in_transaction = client.in_transaction.lock().await;
    if *in_transaction {
        client.tx.send(b"-ERR MULTI calls can not be nested\r\n".to_vec()).unwrap();
        return;
    }
    *in_transaction = true; // Start transaction

    client.tx.send(b"+OK\r\n".to_vec()).unwrap();
}

async fn cmd_exec(client: &Client, db: Database, blocked_clients: BlockedClients) {
    {
        let mut in_transaction = client.in_transaction.lock().await;
        if !*in_transaction {
            client.tx.send(b"-ERR EXEC without MULTI\r\n".to_vec()).unwrap();
            return;
        }
        *in_transaction = false; // End transaction
    }

    let queue: Vec<Command> = client.queued_commands.lock().await
                                                    .drain(..).collect();
    // Emit array length
    client.tx.send(format!("*{}\r\n", queue.len()).as_bytes().to_vec()).unwrap();
    // Execute queued commands
    for mut cmd in queue {
        // Box::pin for recursion warning
        Box::pin(cmd.execute(&client, db.clone(), blocked_clients.clone())).await;
    }
}

fn cmd_other(cmd_name: &String, args: Vec<String>, client: &Client) {
    // Build + emit error string
    let mut err_str = format!("-ERR unknown command '{cmd_name}', with args beginning with: ");
    for arg in &args[0..] {
        err_str.push_str(&format!("'{arg}', "));
    }
    err_str.push_str("\r\n");

    client.tx.send(err_str.as_bytes().to_vec()).unwrap();
}

#[derive(Clone)]
pub struct Command {
    pub name: String,
    pub args: Option<Vec<String>>
}

impl Command {
    fn get_args(&mut self) -> Vec<String> {
        self.args.take().unwrap_or_else(Vec::new)
    }

    pub async fn execute(&mut self, client: &Client, db: Database,
                         blocked_clients: BlockedClients) {
        let uc_name = self.name.to_uppercase(); // Extract uppercased name

        // Queue a non-EXEC command if in transaction mode
        if *client.in_transaction.lock().await&& uc_name.as_str() != "EXEC" {
            client.queued_commands.lock().await.push(self.clone());
            client.tx.send(b"+QUEUED\r\n".to_vec()).unwrap();
            return;
        }
        
        let args = self.get_args(); // Extract arguments

        match uc_name.as_str() {
            "PING"   => client.tx.send(b"+PONG\r\n".to_vec()).unwrap(),
            "ECHO"   => cmd_echo(args, &client),
            "SET"    => cmd_set(args, &client, db).await,
            "GET"    => cmd_get(args, &client, db).await,
            "RPUSH"  => cmd_push(true, args, &client, db, blocked_clients).await,
            "LPUSH"  => cmd_push(false, args, &client, db, blocked_clients).await,
            "RPOP"   => cmd_pop(true, args, &client, db).await,
            "LPOP"   => cmd_pop(false, args, &client, db).await,
            "BRPOP"  => cmd_bpop(true, args, &client, db, blocked_clients).await,
            "BLPOP"  => cmd_bpop(false, args, &client, db, blocked_clients).await,
            "LRANGE" => cmd_lrange(args, &client, db).await,
            "LLEN"   => cmd_llen(args, &client, db).await,
            "TYPE"   => cmd_type(args, &client, db).await,
            "XADD"   => cmd_xadd(args, &client, db, blocked_clients).await,
            "XRANGE" => cmd_xrange(args, &client, db).await,
            "XREAD"  => cmd_xread(args, &client, db, blocked_clients).await,
            "INCR"   => cmd_incr(args, &client, db).await,
            "MULTI"  => cmd_multi(&client).await,
            "EXEC"   => cmd_exec(&client, db, blocked_clients).await,
            _        => cmd_other(&self.name, args, &client)
        }
    }
}
