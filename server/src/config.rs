use std::sync::Arc;
use rand::{rng, Rng, distr::Alphanumeric};
use tokio::sync::Mutex;
use crate::client::ReplicaClient;

static DEF_BIND: &str = "127.0.0.1";
static DEF_PORT: u16 = 6379;
pub static DEF_DB_DIR: &str = ".";
pub static DEF_DB_FILE: &str = "dump.rdb";

pub type Config = Arc<Config_>;

pub type ReplState = Arc<Mutex<ReplState_>>;

fn gen_rand_str() -> String {
    // Generate a pseudo-random alphanumeric string of 40 characters
    rng()
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect()
}

#[derive(Clone)]
pub struct Config_ {
    pub bind_addr: String,
    pub port: u16,
    pub is_master: bool,
    pub master_addr: Option<String>,
    pub master_port: Option<u16>,
    pub dir: String,
    pub dbfilename: String
}

impl Default for Config_ {
    fn default() -> Self {
        Self {
            bind_addr: DEF_BIND.to_string(),
            port: DEF_PORT,
            is_master: true,
            master_addr: None,
            master_port: None,
            dir: DEF_DB_DIR.to_string(),
            dbfilename: DEF_DB_FILE.to_string()
        }
    }
}

impl Config_ {
    // Config builder
    pub fn from(args: Vec<String>) -> Self {
        let mut config = Config_::default();
        
        let args_len = args.len();
        let mut i = 0;
        while i < args_len {
            // Extract argument(s) after a command option
            match args[i].as_str() {
                "--bind" if i + 1 < args_len => {
                    config.bind_addr = args[i + 1].clone();
                    i += 2; // Skip
                }, "--port" if i + 1 < args_len => {
                    config.port = args[i + 1].parse().unwrap_or(DEF_PORT);
                    i += 2; // Skip
                }, "--replicaof" if i + 1 < args_len => {
                    let split: Vec<&str> = args[i + 1].split_whitespace().collect();
                    if split.len() == 2 {
                        config.is_master = false;
                        config.master_addr = Some(split[0].to_string());
                        config.master_port = Some(split[1].parse().unwrap_or(DEF_PORT));
                    }
                    i += 2; // Skip
                }, "--dir" if i + 1 < args_len => {
                    config.dir = args[i + 1].clone();
                    i += 2; // Skip
                }, "--dbfilename" if i + 1 < args_len => {
                    config.dbfilename = args[i + 1].clone();
                    i += 2; // Skip
                }, _ => i += 1
            }
        }

        config
    }

    pub async fn get_replication(&self, repl_state: ReplState) -> Vec<u8> {
        let role = if self.is_master { "master" } else { "slave" };
        let repl_state_guard = repl_state.lock().await;
        let master_replid = &repl_state_guard.replid;
        let master_repl_offset = &repl_state_guard.repl_offset;

        let content = format!("\
            # Replication\r\n\
            role:{role}\r\n\
            master_replid:{master_replid}\r\n\
            master_repl_offset:{master_repl_offset}"
        );
        
        format!("${}\r\n{content}\r\n", content.len()).into_bytes()
    }
}

#[derive(Clone)]
pub struct ReplState_ {
    pub replid: String,
    pub repl_offset: usize,
    pub replicas: Option<Vec<ReplicaClient>>
}

impl Default for ReplState_ {
    fn default() -> Self {
        Self {
            replid: gen_rand_str(),
            repl_offset: 0,
            replicas: None
        }
    }
}
