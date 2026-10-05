use crate::core::config::ReplState;
use crate::core::db::Database;
use crate::persistence::rdb::RDBFile;
use crate::core::client::{Client, ReplicaClient};
use tokio::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};

pub async fn cmd_psync(client: &Client, repl_state: ReplState, db: Database) {
    let mut rdb = RDBFile::default();
    rdb._gen_database(Some(vec![db])).await;
    rdb.gen_eof();
    let snapshot = rdb.to_vec();

    let mut state_guard = repl_state.lock().await;
    // Store replica info before ACK
    if let Some(replica_list) = &mut state_guard.replicas {
        replica_list.push(ReplicaClient {
            client: client.clone(),
            handshaked: false,
            ack_offset: 0,
            stream: Arc::new(Mutex::new(VecDeque::new()))
        });
    }

    // Emit resync to replica
    let mut bulk_str = Vec::new();
    bulk_str.extend(format!(
        "+FULLRESYNC {} {}\r\n", state_guard.replid, state_guard.repl_offset
    ).as_bytes());
    bulk_str.extend(format!("${}\r\n", snapshot.len()).as_bytes());
    bulk_str.extend(snapshot);

    client.tx.send(bulk_str).unwrap();
}


