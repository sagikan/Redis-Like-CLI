use tokio::time::{sleep, Duration};
use crate::config::ReplState;
use crate::client::{Client, ReplicaClient, Response};

pub async fn cmd_wait(args: &[String], client: &Client, repl_state: ReplState) {
    {
        let state_guard = repl_state.lock().await;
        let replicas = state_guard.replicas.as_ref();
        if replicas.is_none() || replicas.as_ref().unwrap().is_empty() {
            // No connected replicas
            client.tx.send(b":0\r\n".to_vec()).unwrap();
            return;
        }
    }
    
    if args.len() != 2 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let last_write = { repl_state.lock().await.repl_offset };
    // Handle by # of replicas + timeout
    let acknowledged = match (args[0].parse::<usize>(), args[1].parse::<usize>()) {
        (Ok(0), Ok(_)) => { // Immediate
            let state_guard = repl_state.lock().await;
            let replicas = state_guard.replicas.as_ref().unwrap();

            replicas.iter().filter(
                |repl| repl.ack_offset >= last_write // Acknowledgement check
            ).collect::<Vec<&ReplicaClient>>().len()
        }, (Ok(num_repl), Ok(timeout)) => { // Blocking
            let sleeper = tokio::spawn(async move {
                sleep(Duration::from_millis(timeout as u64)).await;
            });

            let mut ack = 0;
            // Block until replica number reached / (if > 0) timeout expires
            while ack < num_repl && (timeout == 0 || !sleeper.is_finished()) {
                ack = 0; // Reset

                // Get snapshot of replica list
                let replicas = {
                    let state_guard = repl_state.lock().await;
                    state_guard.replicas.as_ref().unwrap().clone()
                };

                let mut sent_getack = false;
                for repl in replicas {
                    if repl.ack_offset >= last_write { // Acknowledgement check
                        ack += 1;
                    } else if ack < num_repl {
                        // Dispatch GETACK to replica for offset update
                        repl.client.tx.send(
                            b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                        .to_vec()).unwrap();
                        sent_getack = true;
                    }
                }
                
                if sent_getack { // Allow catch-up
                    sleep(Duration::from_millis(100)).await; 
                }
            }

            ack
        }, _ => {
            client.tx.send(Response::ErrNotInteger.into()).unwrap();
            return;
        }
    };

    // Emit # of up-to-last-write replicas
    client.tx.send(format!(":{acknowledged}\r\n").into_bytes()).unwrap();
}
