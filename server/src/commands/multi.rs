use crate::client::{Client, Response};

pub async fn cmd_multi(to_send: bool, client: &Client) {
    if client.in_transaction().await {
        client.send_if(to_send, Response::ErrNestedMulti);
        return;
    }
    client.set_transaction(true).await; // Start transaction

    client.send_if(to_send, Response::Ok);
}
