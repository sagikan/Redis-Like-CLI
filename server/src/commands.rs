use std::str;
use std::cmp::min;
use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::MutexGuard;
use tokio::time::{sleep, Duration};
use crate::db::*;
use crate::config::{Config, ReplState};
use crate::client::{Client, BlockedClient, ReplicaClient, Response};
use crate::rdb::RDBFile;

fn set_pair(key: String, value: Value, client: &Client, to_send: bool,
            mut guard: MutexGuard<'_, HashMap<String, Value>>) {
    guard.insert(key, value);
    client.send_if(to_send, Response::Ok);
}

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

async fn validate_read_entry_id<F: Fn(&String) -> bool>(
    full_check: F, entry_id: &String, client: &Client
) -> Option<EntryIdType> {
    if full_check(entry_id) { // Full
        return Some(EntryIdType::Full(()));
    }

    let invalid_id = || {
        client.tx.send(Response::ErrEntryIdInvalid.into()).unwrap();
        None
    };

    // Parse + validate
    match entry_id.split_once('-') {
        Some((ms_time, seq_num)) => { // Explicit
            match (ms_time.parse::<usize>(), seq_num.parse::<usize>()) {
                (Ok(ms_time), Ok(seq_num)) =>
                    Some(EntryIdType::Explicit((ms_time, seq_num))),
                _ => invalid_id()
            }
        }, None => { // Partial
            match entry_id.parse::<usize>() {
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
                            ms_time: usize) -> Option<Entry> {
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

fn get_unix_time(in_ms: bool) -> usize {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("You're a time traveller, Harry!");

    if in_ms { // Milliseconds
        now.as_millis() as usize
    } else { // Seconds
        now.as_secs() as usize
    }
}

fn glob(pattern: &[u8], key: &[u8]) -> bool {
    let (mut pat_i, mut key_i) = (0, 0);
    let (mut star_pat, mut star_key) = (None, 0);
    let (pat_len, key_len) = (pattern.len(), key.len());

    while key_i < key_len {
        if pat_i < pat_len {
            match pattern[pat_i] {
                b'*' => {
                    // Collapse chained *s
                    while pat_i < pat_len && pattern[pat_i] == b'*' { pat_i += 1; }
                    if pat_i == pat_len { // Trailing *
                        return true;
                    }
                    // Record positions
                    star_pat = Some(pat_i);
                    star_key = key_i;
                    continue;
                }, c if c.is_ascii_alphanumeric() => {
                    if c == key[key_i] { // Matching
                        // Advance
                        key_i += 1;
                        pat_i += 1;
                        continue;
                    }
                }, _ => todo!()
            }
        }

        // Mismatch
        if let Some(pat_j) = star_pat {
            star_key += 1; // Absorb 1 character in key
            // Fallback
            key_i = star_key;
            pat_i = pat_j;
        } else { // No fallback option
            return false;
        }
    }

    pat_i == pat_len
}

async fn cmd_ping(to_send: bool, client: &Client) {
    let pong: Vec<u8> = match client.in_sub_mode().await {
        true => Response::SubPong,
        false => Response::Pong
    }.into();

    client.send_if(to_send, pong);
}

fn cmd_echo(args: &[String], client: &Client) {
    if let Some(_val) = args.get(0) {
        let to_write = format!("+{}\r\n", args[0..].join(" "));
        client.tx.send(to_write.into_bytes()).unwrap();
    } else {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
    }
}

async fn cmd_set(to_send: bool, args: &[String], client: &Client, db: Database) {
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

async fn cmd_get(args: &[String], client: &Client, db: Database) {
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

async fn cmd_push(from_right: bool, to_send: bool, args: &[String],
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

async fn cmd_pop(from_right: bool, to_send: bool, args: &[String],
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

async fn cmd_bpop(from_right: bool, to_send: bool, args: &[String],
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

async fn cmd_lrange(args: &[String], client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
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
            client.tx.send(Response::ErrNotInteger.into()).unwrap();
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
                    client.tx.send(Response::EmptyArray.into()).unwrap();
                    return;
                }

                // Build bulk string
                let range_size = stop - start + 1;
                let mut bulk_str = format!("*{range_size}\r\n");
                for val in val_list.iter().skip(start as usize)
                                   .take(range_size as usize) {
                    bulk_str.push_str(&format!("${}\r\n{val}\r\n", val.len()));
                }

                client.tx.send(bulk_str.into_bytes()).unwrap();
            }, // Value is of the wrong type
            _ => client.tx.send(Response::WrongType.into()).unwrap()
        }, None => client.tx.send(Response::EmptyArray.into()).unwrap()
    }
}

async fn cmd_llen(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let val_list_len = match db.lock().await.get_mut(&args[0]) {
        Some(value) => match &mut value.val {
            // An existing list is found
            ValueType::StringList(val_list) => val_list.len(),
            _ => { // Value is of the wrong type
                client.tx.send(Response::WrongType.into()).unwrap();
                return;
            }
        }, None => 0 // Key doesn't exist
    };

    // Emit the list's length
    client.tx.send(format!(":{val_list_len}\r\n").into_bytes()).unwrap();
}

async fn cmd_type(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }
    
    let val_type = match db.lock().await.get(&args[0]) {
        Some(value) => match &value.val {
            ValueType::String(_) => "string",
            ValueType::StringList(_) => "list",
            ValueType::Stream(_) => "stream"
        }, None => "none"
    };

    client.tx.send(format!("+{val_type}\r\n").into_bytes()).unwrap();
}

async fn cmd_xadd(to_send: bool, args: &[String], client: &Client, db: Database,
                  blocked_clients: BlockedClients) {
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
                // Value is of the wrong type
                _ => client.send_if(to_send, Response::WrongType)
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

async fn cmd_xrange(args: &[String], client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
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
                    client.tx.send(Response::EmptyArray.into()).unwrap();
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
                    client.tx.send(Response::EmptyArray.into()).unwrap();
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
        client.tx.send(Response::EmptyArray.into()).unwrap();
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
                    bulk_str.push_str(&format!(
                        "*2\r\n${}\r\n{e_id}\r\n*{}\r\n",
                        e_id.len(), entry.fields.len() * 2
                    ));
                    // Iterate over entry's fields
                    for (key, val) in entry.fields.iter() {
                        bulk_str.push_str(&format!(
                            "${}\r\n{key}\r\n${}\r\n{val}\r\n", key.len(), val.len()
                        ));
                    }
                }

                client.tx.send(bulk_str.into_bytes()).unwrap();
            }, // Value is of the wrong type
            _ => client.tx.send(Response::WrongType.into()).unwrap()
        }
    }
}

async fn cmd_xread(args: &[String], client: &Client, db: Database,
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

async fn cmd_incr(to_send: bool, args: &[String], client: &Client, db: Database) {
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

async fn cmd_multi(to_send: bool, client: &Client) {
    if client.in_transaction().await {
        client.send_if(to_send, Response::ErrNestedMulti);
        return;
    }
    client.set_transaction(true).await; // Start transaction

    client.send_if(to_send, Response::Ok);
}

async fn cmd_exec(to_send: bool, client: &Client, config: Config, repl_state: ReplState,
                  db: Database, blocked_clients: BlockedClients, subs: Subscriptions) {
    if !client.in_transaction().await {
        client.send_if(to_send, Response::ErrExecWithoutMulti);
        return;
    }
    client.set_transaction(false).await; // End transaction

    let queued: Vec<Command> = client
        .drain_queued().await
        .unwrap_or_else(Vec::new);
    // Emit array length
    client.send_if(to_send, format!("*{}\r\n", queued.len()).into_bytes());
    // Execute queued commands
    for mut cmd in queued {
        // Box::pin for recursion warning
        Box::pin(cmd.execute(
            &client, config.clone(), repl_state.clone(), db.clone(),
            blocked_clients.clone(), subs.clone()
        )).await;
    }
}

async fn cmd_discard(to_send: bool, client: &Client) {
    if !client.in_transaction().await {
        client.send_if(to_send, Response::ErrDiscardWithoutMulti);
        return;
    }
    client.set_transaction(false).await; // End transaction

    // Empty command queue
    client.drain_queued().await;

    client.send_if(to_send, Response::Ok);
}

async fn cmd_info(args: &[String], client: &Client, config: Config,
                  repl_state: ReplState) {
    let info_str: Vec<u8> = match args.len() {
        0 => { todo!(); }, // Default
        1 => { // [Section]
            match args[0].to_uppercase().as_str() {
                "REPLICATION" => config.get_replication(repl_state).await,
                _ => todo!()
            }
        }, _ => Response::WrongType.into()
    };

    client.tx.send(info_str).unwrap();
}

async fn cmd_replconf(args: &[String], client: &Client, config: Config,
                      repl_state: ReplState) {
    // Handle by role + subcommand
    let bulk_str = match args.len() {
        2 => match (
            config.is_master,
            args[0].to_uppercase().as_str(),
            args[1].as_str(),
        ) {
            (true, "ACK", offset_str) => match offset_str.parse::<usize>() {
                Ok(offset) => {
                    // Update replica's offset in list
                    if let Some(replica_list) = &mut repl_state.lock().await.replicas {
                        if let Some(replica) = replica_list.iter_mut().find(
                            |r| r.client.tx.same_channel(&client.tx) && r.handshaked
                        ) {
                            replica.ack_offset = offset;
                        }
                    }

                    return; // No reply
                }, Err(_) => Response::ErrSyntax.into()
            },
            (true, _, _) => Response::Ok.into(), // listening-port / capa
            (false, "GETACK", "*") => {
                let offset: usize = repl_state.lock().await.repl_offset;

                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{offset}\r\n",
                    offset.to_string().len()
                ).into_bytes()
            }, _ => Response::ErrSyntax.into()
        }, _ => Response::ErrArgCount.into()
    };

    client.tx.send(bulk_str).unwrap();
}

async fn cmd_psync(client: &Client, repl_state: ReplState) {
    let empty_rdb: Vec<u8> = RDBFile::default().to_vec();

    let mut state_guard = repl_state.lock().await;
    // Store replica info before ACK
    if let Some(replica_list) = &mut state_guard.replicas {
        replica_list.push(ReplicaClient {
            client: client.clone(),
            handshaked: false,
            ack_offset: 0
        });
    }

    // Emit resync to replica
    let mut bulk_str = Vec::new();
    bulk_str.extend(format!(
        "+FULLRESYNC {} {}\r\n", state_guard.replid, state_guard.repl_offset
    ).as_bytes());
    bulk_str.extend(format!("${}\r\n", empty_rdb.len()).as_bytes());
    bulk_str.extend(&empty_rdb);

    client.tx.send(bulk_str).unwrap();
}

async fn cmd_wait(args: &[String], client: &Client, repl_state: ReplState) {
    {
        let state_guard = repl_state.lock().await;
        let replicas = state_guard.replicas.as_ref();
        if replicas.is_none() || replicas.as_ref().unwrap().is_empty() {
            // No connected replicas
            client.tx.send(b":0\r\n".to_vec()).unwrap();
            return;
        }
    }
    
    if args.len() != 2 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let last_write = { repl_state.lock().await.repl_offset };
    // Handle by # of replicas + timeout
    let acknowledged = match (args[0].parse::<usize>(), args[1].parse::<usize>()) {
        (Ok(0), Ok(_)) => { // Immediate
            let state_guard = repl_state.lock().await;
            let replicas = state_guard.replicas.as_ref().unwrap();

            replicas.iter().filter(
                |repl| repl.ack_offset >= last_write // Acknowledgement check
            ).collect::<Vec<&ReplicaClient>>().len()
        }, (Ok(num_repl), Ok(timeout)) => { // Blocking
            let sleeper = tokio::spawn(async move {
                sleep(Duration::from_millis(timeout as u64)).await;
            });

            let mut ack = 0;
            // Block until replica number reached / (if > 0) timeout expires
            while ack < num_repl && (timeout == 0 || !sleeper.is_finished()) {
                ack = 0; // Reset

                // Get snapshot of replica list
                let replicas = {
                    let state_guard = repl_state.lock().await;
                    state_guard.replicas.as_ref().unwrap().clone()
                };

                let mut sent_getack = false;
                for repl in replicas {
                    if repl.ack_offset >= last_write { // Acknowledgement check
                        ack += 1;
                    } else if ack < num_repl {
                        // Dispatch GETACK to replica for offset update
                        repl.client.tx.send(
                            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                        .to_vec()).unwrap();
                        sent_getack = true;
                    }
                }
                
                if sent_getack { // Allow catch-up
                    sleep(Duration::from_millis(100)).await; 
                }
            }

            ack
        }, _ => {
            client.tx.send(Response::ErrNotInteger.into()).unwrap();
            return;
        }
    };

    // Emit # of up-to-last-write replicas
    client.tx.send(format!(":{acknowledged}\r\n").into_bytes()).unwrap();
}

fn cmd_config_get(args: &[String], client: &Client, config: Config) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract + emit key and value
    let key = &args[0];
    let val = match key.to_uppercase().as_str() {
        "DIR" => &config.dir,
        "DBFILENAME" => &config.dbfilename,
        _ => {
            client.tx.send(Response::NilArray.into()).unwrap();
            return;
        }
    };

    client.tx.send(format!(
        "*2\r\n${}\r\n{key}\r\n${}\r\n{val}\r\n", key.len(), val.len()
    ).into_bytes()).unwrap();
}

fn grp_config(args: &[String], client: &Client, config: Config) {
    if args.is_empty() { // No subcommand
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    match args[0].to_uppercase().as_str() {
        "GET" => cmd_config_get(&args[1..], &client, config.clone()),
        _ => todo!()
    }
}

async fn cmd_keys(args: &[String], client: &Client, db: Database) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let pattern = args[0].trim_matches('"'); // Remove "s
    
    let mut matching: Vec<&String> = Vec::new();
    let mut bulk_str = String::new();
    let guard = db.lock().await;
    // Match key to pattern + build bulk string
    for key in guard.keys() {
        if glob(pattern.as_bytes(), key.as_bytes()) {
            matching.push(&key);
            bulk_str.push_str(&format!("${}\r\n{key}\r\n", key.len()));
        }
    }

    // Emit final bulk string
    bulk_str = format!("*{}\r\n{bulk_str}", matching.len());
    client.tx.send(bulk_str.into_bytes()).unwrap();
}

async fn cmd_subscribe(args: &[String], client: &Client, subs: Subscriptions) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let channel = &args[0];
    // Enter sub mode
    client.set_sub_mode(true).await;
    // Subscribe (double-edged) + get updated # of channels client is subbed to
    let sub_count = client.sub(channel).await;
    {
        let mut subs_guard = subs.lock().await;
        if let Some(sub_list) = subs_guard.get_mut(channel.as_str()) {
            sub_list.push(client.clone());
        } else {
            // Create sub list
            let mut new_list = Vec::new();
            new_list.push(client.clone());
            subs_guard.insert(channel.clone(), new_list);
        }
    }
    
    // Emit array with sub info
    client.tx.send(format!(
        "*3\r\n$9\r\nsubscribe\r\n${}\r\n{channel}\r\n:{sub_count}\r\n",
        channel.len()
    ).into_bytes()).unwrap();
}

async fn cmd_unsubscribe(args: &[String], client: &Client, subs: Subscriptions) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let channel = &args[0];
    // Unsubscribe (double-edged) + get updated # of channels client is subbed to
    let sub_count = client.unsub(channel).await;
    {
        let mut subs_guard = subs.lock().await;
        if let Some(sub_list) = subs_guard.get_mut(channel.as_str()) {
            sub_list.retain(|c| c.id != client.id);
        }
    }

    // Emit array with unsub info
    client.tx.send(format!(
        "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{channel}\r\n:{sub_count}\r\n",
        channel.len()
    ).into_bytes()).unwrap();
}

async fn cmd_publish(args: &[String], client: &Client, subs: Subscriptions) {
    if args.len() != 2 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract channel + message (without ")
    let channel = &args[0];
    let msg = &args[1].trim_matches('"');

    // Send message to all subs
    let subs_guard = subs.lock().await;
    let subs_list = match subs_guard.get(channel.as_str()) {
        Some(list) => list,
        None => &Vec::new()
    };
    for sub in subs_list {
        sub.tx.send(format!(
            "*3\r\n$7\r\nmessage\r\n${}\r\n{channel}\r\n${}\r\n{msg}\r\n",
            channel.len(), msg.len()
        ).into_bytes()).unwrap();
    }

    // Emit # of subs
    client.tx.send(format!(":{}\r\n", subs_list.len()).into_bytes()).unwrap();
}

fn cmd_other(cmd_name: &String, args: &[String], client: &Client) {
    // Build + emit error string
    let mut err_str = format!("-ERR unknown command '{cmd_name}', with args beginning with: ");
    for arg in &args[0..] {
        err_str.push_str(&format!("'{arg}', "));
    }
    err_str.push_str("\r\n");

    client.tx.send(err_str.into_bytes()).unwrap();
}

#[derive(Clone)]
pub struct Command {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub is_propagated: bool,
    pub resp_len: usize
}

impl Command {
    pub fn from(resp_str: &[u8], is_propagated: bool) -> Option<Self> {
        let unparsed_str = str::from_utf8(resp_str).unwrap();
        let mut lines = unparsed_str.split("\r\n");
        lines.next(); // Skip array header

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

        let args = match parsed.len() {
            0 => { return None; } // Empty command
            1 => None,
            _ => Some(Vec::from(parsed[1..].to_vec()))
        };

        Some(Self {
            name: parsed[0].clone(),
            args,
            is_propagated,
            resp_len: resp_str.len()
        })
    }

    pub async fn execute(
        &mut self, client: &Client, config: Config, repl_state: ReplState,
        db: Database, blocked_clients: BlockedClients, subs: Subscriptions
    ) {
        if config.is_master && self.is_write() {
            // Propagate command to replicas
            if let Some(replica_list) = &repl_state.lock().await.replicas {
                let cmd = self.to_resp_array();
                for repl in replica_list {
                    repl.client.tx.send(cmd.clone()).unwrap();
                }
            }
        }
        
        let uc_name = self.name.to_uppercase();
        let transactional_cmds = ["MULTI", "EXEC", "DISCARD"];
        let sub_cmds = ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"];
        // Queue non-EXEC/DISCARD command if in transaction mode
        if client.in_transaction().await && !transactional_cmds.contains(&uc_name.as_str()) {
            client.push_queued(self.clone()).await;
            client.tx.send(Response::Queued.into()).unwrap();
            return;
        } // Reject invalid commands if in subscribed mode
        else if client.in_sub_mode().await && !sub_cmds.contains(&uc_name.as_str()) {
            client.tx.send(format!(
                "-ERR Can't execute '{uc_name}' in this context\r\n"
            ).into_bytes()).unwrap();
            return;
        }

        let to_send = !self.is_propagated; // Propagated => Executable not replying
        let args = self.args.as_deref().unwrap_or(&[]);

        // Handle command
        match uc_name.as_str() {
            "PING"        => cmd_ping(to_send, &client).await,
            "ECHO"        => cmd_echo(args, &client),
            "SET"         => cmd_set(to_send, args, &client, db).await,
            "GET"         => cmd_get(args, &client, db).await,
            "RPUSH"       => cmd_push(true, to_send, args, &client, db, blocked_clients).await,
            "LPUSH"       => cmd_push(false, to_send, args, &client, db, blocked_clients).await,
            "RPOP"        => cmd_pop(true, to_send, args, &client, db).await,
            "LPOP"        => cmd_pop(false, to_send, args, &client, db).await,
            "BRPOP"       => cmd_bpop(true, to_send, args, &client, db, blocked_clients).await,
            "BLPOP"       => cmd_bpop(false, to_send, args, &client, db, blocked_clients).await,
            "LRANGE"      => cmd_lrange(args, &client, db).await,
            "LLEN"        => cmd_llen(args, &client, db).await,
            "TYPE"        => cmd_type(args, &client, db).await,
            "XADD"        => cmd_xadd(to_send, args, &client, db, blocked_clients).await,
            "XRANGE"      => cmd_xrange(args, &client, db).await,
            "XREAD"       => cmd_xread(args, &client, db, blocked_clients).await,
            "INCR"        => cmd_incr(to_send, args, &client, db).await,
            "MULTI"       => cmd_multi(to_send, &client).await,
            "EXEC"        => cmd_exec(to_send, &client, config.clone(), repl_state.clone(), db, blocked_clients, subs).await,
            "DISCARD"     => cmd_discard(to_send, &client).await,
            "INFO"        => cmd_info(args, &client, config.clone(), repl_state.clone()).await,
            "REPLCONF"    => cmd_replconf(args, &client, config.clone(), repl_state.clone()).await,
            "PSYNC"       => cmd_psync(&client, repl_state.clone()).await,
            "WAIT"        => cmd_wait(args, &client, repl_state.clone()).await,
            "CONFIG"      => grp_config(args, &client, config.clone()),
            "KEYS"        => cmd_keys(args, &client, db).await,
            "SUBSCRIBE"   => cmd_subscribe(args, &client, subs).await,
            "UNSUBSCRIBE" => cmd_unsubscribe(args, &client, subs).await,
            "PUBLISH"     => cmd_publish(args, &client, subs).await,
            _             => cmd_other(&self.name, args, &client)
        }

        let mut state_guard = repl_state.lock().await;
        // Update offset
        if (config.is_master && self.is_write()) || self.is_propagated {
            state_guard.repl_offset += self.resp_len;
        }

        // Update handshake state
        if uc_name.as_str() == "PSYNC" {
            if let Some(replica_list) = &mut state_guard.replicas {
                if let Some(replica) = replica_list.iter_mut().find(
                    |r| r.client.tx.same_channel(&client.tx)
                ) {
                    replica.handshaked = true;
                }
            }
        }
    }

    fn is_write(&self) -> bool {
        matches!(
            self.name.to_uppercase().as_str(),
            "SET" | "INCR" | // Keyspace
            "RPUSH" | "LPUSH" | "RPOP" | "LPOP" | // Lists
            "XADD" | // Streams
            "MULTI" | "EXEC" | "DISCARD" // Transactions
        )
    }

    fn to_resp_array(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::from(format!(
            "*{}\r\n${}\r\n{}\r\n",
            1 + self.args.as_ref().unwrap_or(&Vec::new()).len(),
            self.name.len(),
            self.name
        ).into_bytes());
        
        if let Some(args) = self.args.as_ref() {
            for arg in args {
                res.extend(format!(
                    "${}\r\n{arg}\r\n", arg.len()
                ).into_bytes());
            }
        }

        res
    }
}
