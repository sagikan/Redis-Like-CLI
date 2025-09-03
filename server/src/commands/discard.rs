use crate::client::{Client, Response};

pub async fn cmd_discard(to_send: bool, client: &Client) {
    if !client.in_transaction().await {
        client.send_if(to_send, Response::ErrDiscardWithoutMulti);
        return;
    }
    client.set_transaction(false).await; // End transaction

    // Empty command queue
    client.drain_queued().await;

    client.send_if(to_send, Response::Ok);
}
