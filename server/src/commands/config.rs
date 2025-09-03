use crate::config::Config;
use crate::client::{Client, Response};

pub fn grp_config(args: &[String], client: &Client, config: Config) {
    if args.is_empty() { // No subcommand
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    match args[0].to_uppercase().as_str() {
        "GET" => cmd_config_get(&args[1..], &client, config.clone()),
        _ => todo!()
    }
}

pub fn cmd_config_get(args: &[String], client: &Client, config: Config) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract + emit key and value
    let key = &args[0];
    let val = match key.to_uppercase().as_str() {
        "DIR" => &config.dir,
        "DBFILENAME" => &config.dbfilename,
        _ => {
            client.tx.send(Response::NilArray.into()).unwrap();
            return;
        }
    };

    client.tx.send(format!(
        "*2\r\n${}\r\n{key}\r\n${}\r\n{val}\r\n", key.len(), val.len()
    ).into_bytes()).unwrap();
}
