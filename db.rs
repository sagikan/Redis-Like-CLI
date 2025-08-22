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

pub type Fields = Arc<HashMap<String, String>>;

pub type Entries = Arc<Mutex<VecDeque<Entry>>>;

pub type Database = Arc<Mutex<HashMap<String, Value>>>;

pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedClient>>>>;

#[derive(Clone)]
pub enum ValueType {
    String(String),
    StringList(VecDeque<String>),
    Stream(Stream)
}

#[derive(Clone)]
pub enum EntryIdType {
    Full(()),
    Partial(u64),
    Explicit((u64, u64))
}

#[derive(Clone)]
pub struct Entry {
    pub ms_time: u64,
    pub seq_num: u64,
    pub fields: Fields
}

#[derive(Clone)]
pub struct Stream {
    pub entries: Entries
}

#[derive(Clone)]
pub struct Client {
    pub id: u64,
    pub tx: UnboundedSender<Vec<u8>>
}

#[derive(Clone)]
pub struct BlockedClient {
    pub client: Client,
    pub blocked_by: Vec<String>,
    pub expired: bool,
    pub from_right: Option<bool>, // BPOP
    pub from_entry_id: Option<(u64, u64)>, // XREAD
    pub count: Option<u64> // XREAD
}

#[derive(Clone)]
pub struct Value {
    pub val: ValueType
}
