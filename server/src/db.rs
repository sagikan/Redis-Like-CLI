use std::error::Error;
use std::collections::{VecDeque, HashMap};
use std::sync::Arc;
use std::future::Future;
use tokio::sync::{Mutex, MutexGuard};
use crate::client::BlockedClient;
use crate::rdb::RDBFile;

pub type Fields = Arc<HashMap<String, String>>;

pub type Entries = Arc<Mutex<VecDeque<Entry>>>;

pub type BlockedClients = Arc<Mutex<HashMap<String, VecDeque<BlockedClient>>>>;

#[derive(Clone)]
pub struct Entry {
    pub ms_time: usize,
    pub seq_num: usize,
    pub fields: Fields
}

#[derive(Clone)]
pub enum EntryIdType {
    Full(()),
    Partial(usize),
    Explicit((usize, usize))
}

#[derive(Clone)]
pub struct Stream {
    pub entries: Entries
}

#[derive(Clone)]
pub struct Value {
    pub val: ValueType,
    pub exp: Option<ExpiryType>
}

#[derive(Clone)]
pub enum ValueType {
    String(String),
    StringList(VecDeque<String>),
    Stream(Stream)
}

#[derive(Clone)]
pub enum ExpiryType {
    Milliseconds(usize),
    Seconds(usize)
}

pub trait ExpiryLen {
    fn elen(&self) -> usize;
}

impl ExpiryLen for HashMap<String, Value> {
    fn elen(&self) -> usize {
        self.values().filter(|val| val.exp.is_some()).count()
    }
}

#[derive(Clone)]
pub struct Database(pub Arc<Mutex<HashMap<String, Value>>>);

impl Default for Database {
    fn default() -> Self {
        Database(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl Database {
    pub async fn from(rdb: RDBFile) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Get DBs list + handle empty / singular DB
        let dbs = rdb.to_dbs().await?;
        if dbs.is_empty() {
            return Ok(Database::default());
        } else if dbs.len() == 1 {
            return Ok(dbs.into_iter().next().unwrap());
        }

        // Merge DBs
        let merged = Database::default();
        {
            let mut merged_guard = merged.lock().await;
            for db in dbs {
                let guard = db.lock().await;
                for (key, val) in guard.iter() {
                    merged_guard.insert(key.clone(), val.clone());
                }
            }
        }

        Ok(merged)
    }

    pub fn inner(&self) -> &Arc<Mutex<HashMap<String, Value>>> {
        &self.0
    }

    pub fn lock(&self) -> impl Future<Output = MutexGuard<'_, HashMap<String, Value>>> {
        self.0.lock()
    }
}
