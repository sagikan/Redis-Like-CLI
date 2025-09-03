use crate::db::Subscriptions;
use crate::client::{Client, Response};

pub async fn cmd_subscribe(args: &[String], client: &Client, subs: Subscriptions) {
    if args.len() != 1 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let channel = &args[0];
    // Enter sub mode
    client.set_sub_mode(true).await;
    // Subscribe (double-edged) + get updated # of channels client is subbed to
    let sub_count = client.sub(channel).await;
    {
        let mut subs_guard = subs.lock().await;
        if let Some(sub_list) = subs_guard.get_mut(channel.as_str()) {
            sub_list.push(client.clone());
        } else {
            // Create sub list
            let mut new_list = Vec::new();
            new_list.push(client.clone());
            subs_guard.insert(channel.clone(), new_list);
        }
    }
    
    // Emit array with sub info
    client.tx.send(format!(
        "*3\r\n$9\r\nsubscribe\r\n${}\r\n{channel}\r\n:{sub_count}\r\n",
        channel.len()
    ).into_bytes()).unwrap();
}
