use std::sync::Arc;
use rand::{rng, Rng};
use rand::distr::Alphanumeric;

static DEF_PORT: u16 = 6379;

pub type Config = Arc<Config_>;
pub type ReplState = Arc<ReplState_>;

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
    pub master_port: Option<u16>
}

impl Default for Config_ {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1".to_string(),
            port: DEF_PORT,
            is_master: true,
            master_addr: None,
            master_port: None
        }
    }
}

impl Config_ {
    // Config builder
    pub fn from(args: Vec<String>) -> Self {
        let mut config = Config_::default();
        
        let mut i = 0;
        while i < args.len() {
            // Extract argument(s) after a command option
            match args[i].as_str() {
                "--bind" if i + 1 < args.len() => {
                    config.bind_addr = args[i + 1].clone();
                    i += 2; // Skip
                }, "--port" if i + 1 < args.len() => {
                    config.port = args[i + 1].parse().unwrap_or(DEF_PORT);
                    i += 2; // Skip
                }, "--replicaof" if i + 1 < args.len() => {
                    let split: Vec<&str> = args[i + 1].split_whitespace().collect();
                    if split.len() == 2 {
                        config.is_master = false;
                        config.master_addr = Some(split[0].to_string());
                        config.master_port = Some(split[1].parse().unwrap_or(DEF_PORT));
                    }
                    i += 2; // Skip
                }, _ => i += 1
            }
        }

        config
    }

    pub fn get_replication(&self, repl_state: ReplState) -> Vec<u8> {
        let role = if self.is_master { "master" } else { "slave" };
        let master_replid = &repl_state.replid;
        let master_repl_offset = &repl_state.repl_offset;

        let content = format!("\
            # Replication\r\n\
            role:{role}\r\n\
            master_replid:{master_replid}\r\n\
            master_repl_offset:{master_repl_offset}\r\n"
        );
        
        format!("${}\r\n{content}\r\n", content.len()).into_bytes()
    }
}

#[derive(Clone)]
pub struct ReplState_ {
    pub replid: String,
    pub repl_offset: u64
}

impl Default for ReplState_ {
    fn default() -> Self {
        Self {
            replid: gen_rand_str(),
            repl_offset: 0
        }
    }
}
