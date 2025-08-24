use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::client::{BlockedClient};

pub type Fields = Arc<HashMap<String, String>>;

pub type Entries = Arc<Mutex<VecDeque<Entry>>>;

pub type Database = Arc<Mutex<HashMap<String, Value>>>;

pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedClient>>>>;

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
pub struct Value {
    pub val: ValueType
}

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
