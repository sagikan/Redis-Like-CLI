use std::time::{SystemTime, UNIX_EPOCH};
use crate::db::{Database, ValueType, Entry, EntryIdType};
use crate::client::{Client, Response};

pub fn get_unix_time(in_ms: bool) -> usize {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("You're a time traveller, Harry!");

    if in_ms { // Milliseconds
        now.as_millis() as usize
    } else { // Seconds
        now.as_secs() as usize
    }
}

pub async fn get_first_entry(db: Database, stream_key: &String) -> Option<Entry> {
    if let Some(value) = db.lock().await.get_mut(stream_key) {
        if let ValueType::Stream(stream) = &value.val {
            if let Some(first_entry) = stream.entries.lock().await.front() {
                return Some(first_entry.clone());
            }
        }
    }

    None
}

pub async fn get_last_entry(db: Database, stream_key: &String) -> Option<Entry> {
    if let Some(value) = db.lock().await.get_mut(stream_key) {
        if let ValueType::Stream(stream) = &value.val {
            if let Some(last_entry) = stream.entries.lock().await.back() {
                return Some(last_entry.clone());
            }
        }
    }

    None
}

pub async fn get_last_seq_entry(db: Database, stream_key: &String,
                            ms_time: usize) -> Option<Entry> {
    if let Some(value) = db.lock().await.get_mut(stream_key) {
        if let ValueType::Stream(stream) = &value.val {
            // Iterate from the right to assure max sequence
            if let Some(entry) = stream.entries.lock().await.iter().rfind(
                |e| e.ms_time == ms_time
            ) {
                return Some(entry.clone());
            }
        }
    }

    None
}

pub async fn validate_read_entry_id<F: Fn(&String) -> bool>(
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

pub fn glob(pattern: &[u8], key: &[u8]) -> bool {
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
