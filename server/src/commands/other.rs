use crate::client::Client;

pub fn cmd_other(cmd_name: &String, args: &[String], client: &Client) {
    // Build + emit error string
    let mut err_str = format!("-ERR unknown command '{cmd_name}', with args beginning with: ");
    for arg in &args[0..] {
        err_str.push_str(&format!("'{arg}', "));
    }
    err_str.push_str("\r\n");

    client.tx.send(err_str.into_bytes()).unwrap();
}
