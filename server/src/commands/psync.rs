use crate::config::ReplState;
use crate::rdb::RDBFile;
use crate::client::{Client, ReplicaClient};

pub async fn cmd_psync(client: &Client, repl_state: ReplState) {
    let empty_rdb: Vec<u8> = RDBFile::default().to_vec();

    let mut state_guard = repl_state.lock().await;
    // Store replica info before ACK
    if let Some(replica_list) = &mut state_guard.replicas {
        replica_list.push(ReplicaClient {
            client: client.clone(),
            handshaked: false,
            ack_offset: 0
        });
    }

    // Emit resync to replica
    let mut bulk_str = Vec::new();
    bulk_str.extend(format!(
        "+FULLRESYNC {} {}\r\n", state_guard.replid, state_guard.repl_offset
    ).as_bytes());
    bulk_str.extend(format!("${}\r\n", empty_rdb.len()).as_bytes());
    bulk_str.extend(&empty_rdb);

    client.tx.send(bulk_str).unwrap();
}
