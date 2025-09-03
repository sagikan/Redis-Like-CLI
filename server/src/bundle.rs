use std::{env::Args, error::Error, sync::Arc};
use crate::config::{Config, Config_, ReplState};
use crate::db::{Database, BlockedClients, Subscriptions};
use crate::rdb::RDBFile;

pub struct Bundle {
    pub config: Config,
    pub repl_state: ReplState,
    pub db: Database,
    pub blocked_clients: BlockedClients,
    pub subs: Subscriptions
}

impl Bundle {
    pub async fn from(args: Args) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let config = Arc::new(Config_::from(args.skip(1).collect()));
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
