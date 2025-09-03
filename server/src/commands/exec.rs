use crate::bundle::Bundle;
use crate::client::{Client, Response};
use crate::commands::Command;

pub async fn cmd_exec(to_send: bool, client: &Client, bundle: Bundle) {
    if !client.in_transaction().await {
        client.send_if(to_send, Response::ErrExecWithoutMulti);
        return;
    }
    client.set_transaction(false).await; // End transaction

    let queued: Vec<Command> = client
        .drain_queued().await
        .unwrap_or_else(Vec::new);
    // Emit array length
    client.send_if(to_send, format!("*{}\r\n", queued.len()).into_bytes());
    // Execute queued commands
    for mut cmd in queued {
        // Box::pin for recursion warning
        Box::pin(cmd.execute(&client, bundle.clone())).await;
    }
}
