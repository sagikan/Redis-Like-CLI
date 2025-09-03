use crate::db::{Database, ValueType, Entry, EntryIdType};
use crate::client::{Client, Response};
use crate::commands::_helpers_::{get_first_entry, get_last_entry, get_last_seq_entry, validate_read_entry_id};

pub async fn cmd_xrange(args: &[String], client: &Client, db: Database) {
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
