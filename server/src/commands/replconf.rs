use crate::config::{Config, ReplState};
use crate::client::{Client, Response};

pub async fn cmd_replconf(args: &[String], client: &Client,
                          config: Config, repl_state: ReplState) {
    // Handle by role + subcommand
    let bulk_str = match args.len() {
        2 => match (
            config.is_master,
            args[0].to_uppercase().as_str(),
            args[1].as_str(),
        ) {
            (true, "GETSTREAM", offset_str) => match offset_str.parse::<usize>() {
                Ok(offset) => {
                    let mut state_guard = repl_state.lock().await;
                    let stream = state_guard.replicas.as_mut()
                        .and_then(|replicas| replicas.iter_mut()
                            .find(|replica| replica.client.tx.same_channel(&client.tx))
                            .map(|replica| {
                                replica.ack_offset = offset;
                                replica.stream.clone()
                            }));
                    drop(state_guard);

                    match stream {
                        Some(stream) => {
                            let mut stream = stream.lock().await;
                            let payload: Vec<u8> = stream.drain(..).flatten().collect();
                            let mut response = format!("${}\r\n", payload.len()).into_bytes();
                            response.extend(payload);
                            response.extend_from_slice(b"\r\n");
                            response
                        },
                        None => Response::Nil.into()
                    }
                },
                Err(_) => Response::ErrSyntax.into()
            },
            (true, "ACK", offset_str) => match offset_str.parse::<usize>() {
                Ok(offset) => {
                    // Update replica's offset in list
                    if let Some(replica_list) = &mut repl_state.lock().await.replicas {
                        if let Some(replica) = replica_list.iter_mut().find(
                            |r| r.client.tx.same_channel(&client.tx) && r.handshaked
                        ) {
                            replica.ack_offset = offset;
                        }
                    }

                    return; // No reply
                }, Err(_) => Response::ErrSyntax.into()
            },
            (true, _, _) => Response::Ok.into(), // listening-port / capa
            (false, "GETACK", "*") => {
                let offset: usize = repl_state.lock().await.repl_offset;

                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{offset}\r\n",
                    offset.to_string().len()
                ).into_bytes()
            }, _ => Response::ErrSyntax.into()
        },
        _ => Response::ErrArgCount.into()
    };

    client.tx.send(bulk_str).unwrap();
}
