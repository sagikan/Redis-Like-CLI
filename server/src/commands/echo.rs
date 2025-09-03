use crate::client::{Client, Response};

pub fn cmd_echo(args: &[String], client: &Client) {
    if let Some(_val) = args.get(0) {
        let to_write = format!("+{}\r\n", args[0..].join(" "));
        client.tx.send(to_write.into_bytes()).unwrap();
    } else {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
    }
}
