use crate::client::{Client, Response};

pub async fn cmd_ping(to_send: bool, client: &Client) {
    let pong: Vec<u8> = match client.in_sub_mode().await {
        true => Response::SubPong,
        false => Response::Pong
    }.into();

    client.send_if(to_send, pong);
}
