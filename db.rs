#![allow(unused_imports)]
use std::collections::{VecDeque, HashMap};
use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

static CLIENT_ID: AtomicU64 = AtomicU64::new(1);

pub fn get_next_id() -> u64 {
    CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone)]
pub enum ValueType {
    String(String),
    StringList(VecDeque<String>)
}

#[derive(Clone)]
pub struct Client {
    pub id: u64,
    pub tx: UnboundedSender<Vec<u8>>
}

pub struct BlockedClient {
    pub client: Client,
    pub from_right: bool,
    pub blocked_by: Vec<String>
}

#[derive(Clone)]
pub struct Value {
    pub val: ValueType,
    pub expiry: Option<Instant>
}

// pub fn is_expired(value: &Value) -> bool {
//     value.expiry.map_or(false, |e| Instant::now() >= e)
// }

pub type Database = Arc<Mutex<HashMap<String, Value>>>;

pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedClient>>>>;
