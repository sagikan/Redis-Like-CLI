use crate::db::Subscriptions;
use crate::client::{Client, Response};

pub async fn cmd_unsubscribe(args: &[String], client: &Client, subs: Subscriptions) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let channel = &args[0];
    // Unsubscribe (double-edged) + get updated # of channels client is subbed to
    let sub_count = client.unsub(channel).await;
    {
        let mut subs_guard = subs.lock().await;
        if let Some(sub_list) = subs_guard.get_mut(channel.as_str()) {
            sub_list.retain(|c| c.id != client.id);
        }
    }

    // Emit array with unsub info
    client.tx.send(format!(
        "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{channel}\r\n:{sub_count}\r\n",
        channel.len()
    ).into_bytes()).unwrap();
}
