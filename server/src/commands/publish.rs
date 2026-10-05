use crate::db::Subscriptions;
use crate::client::{Client, Response};

pub async fn cmd_publish(args: &[String], client: &Client, subs: Subscriptions) {
    if args.len() != 2 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract channel + message (without ")
    let channel = &args[0];
    let msg = &args[1].trim_matches('"');

    // Send message to all subs
    let subs_guard = subs.lock().await;
    if let Some(subs_list) = subs_guard.get(channel.as_str()) {
        for sub in subs_list {
            sub.tx.send(format!(
                "*3\r\n$7\r\nmessage\r\n${}\r\n{channel}\r\n${}\r\n{msg}\r\n",
                channel.len(), msg.len()
            ).into_bytes()).unwrap();
        }
    }

    // Emit # of subs
    let sub_count = subs_guard.get(channel.as_str()).map_or(0, Vec::len);
    client.tx.send(format!(":{sub_count}\r\n").into_bytes()).unwrap();
}
