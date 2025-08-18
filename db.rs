#![allow(unused_imports)]
use std::collections::{VecDeque, HashMap};
use std::time::Instant;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone)]
pub enum ValueType {
    String(String),
    StringList(VecDeque<String>)
}

pub struct Client {
    pub tx: UnboundedSender<Vec<u8>>
}

#[derive(Clone)]
pub struct Value {
    pub val: ValueType,
    pub expiry: Option<Instant>
}

pub type Database = Arc<Mutex<HashMap<String, Value>>>;

pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<Client>>>>;
