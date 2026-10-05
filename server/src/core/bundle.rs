use std::{error::Error, sync::Arc};
use crate::core::config::{Config, Config_, ReplState};
use crate::core::db::{Database, BlockedClients, Subscriptions};
use crate::persistence::rdb::RDBFile;

pub struct Bundle {
    pub config: Config,
    pub repl_state: ReplState,
    pub db: Database,
    pub blocked_clients: BlockedClients,
    pub subs: Subscriptions
}

impl Bundle {
    pub async fn from_config(config: Config_) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let config = Arc::new(config);
        let repl_state = if config.is_master {
            let repl_state = ReplState::default();
            // Initialize replica list
            repl_state.lock().await.replicas = Some(Vec::new());

            repl_state
        } else { ReplState::default() };
        let db = if config.is_master {
            Database::from(RDBFile::from(&config.dir, &config.dbfilename)?).await?
        } else { Database::default() };
        let blocked_clients = BlockedClients::default();
        let subs = Subscriptions::default();

        Ok(Self {
            config,
            repl_state,
            db,
            blocked_clients,
            subs
        })
    }

    pub fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            repl_state: self.repl_state.clone(),
            db: self.db.clone(),
            blocked_clients: self.blocked_clients.clone(),
            subs: self.subs.clone()
        }
    }
}


