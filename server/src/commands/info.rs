use crate::config::{Config, ReplState};
use crate::client::{Client, Response};

pub async fn cmd_info(args: &[String], client: &Client, config: Config,
                      repl_state: ReplState) {
    let info_str: Vec<u8> = match args.len() {
        0 => { todo!(); }, // Default
        1 => { // [Section]
            match args[0].to_uppercase().as_str() {
                "REPLICATION" => config.get_replication(repl_state).await,
                _ => todo!()
            }
        }, _ => Response::WrongType.into()
    };

    client.tx.send(info_str).unwrap();
}
