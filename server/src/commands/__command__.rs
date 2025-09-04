macro_rules! declare {
    ($($name:ident),* $(,)?) => {
        $(pub mod $name;)*
        $(pub use $name::*;)*
    }
}

mod _helpers_;

use std::str;
use crate::bundle::Bundle;
use crate::client::{Client, Response};

declare!(ping, echo, r#type,
         set, get, incr, keys, // Strings
         push, pop, lrange, llen, // Lists
         xadd, xrange, xread, // Streams
         zadd, zrank, zrange, // Sorted Sets
         multi, exec, discard, // Transactions
         subscribe, unsubscribe, publish, // SUB/PUB
         info, config, replconf, psync, wait, // Server functionality
         other);

#[derive(Clone)]
pub struct Command {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub is_propagated: bool,
    pub resp_len: usize
}

impl Command {
    pub async fn execute(&mut self, client: &Client, bundle: Bundle) {
        if bundle.config.is_master && self.is_write() {
            // Propagate command to replicas
            if let Some(replica_list) = &bundle.repl_state.lock().await.replicas {
                let cmd = self.to_resp_array();
                for repl in replica_list {
                    repl.client.tx.send(cmd.clone()).unwrap();
                }
            }
        }
        
        let uc_name = self.name.to_uppercase();
        // Queue non-transactional command if in transaction mode
        if client.in_transaction().await && !self.is_transactional() {
            client.push_queued(self.clone()).await;
            client.tx.send(Response::Queued.into()).unwrap();
            return;
        } // Reject invalid commands if in subscribed mode
        else if client.in_sub_mode().await && !self.is_sub_related() {
            client.tx.send(format!(
                "-ERR Can't execute '{uc_name}' in this context\r\n"
            ).into_bytes()).unwrap();
            return;
        }

        let to_send = !self.is_propagated; // Propagated => no reply
        let args = self.args.as_deref().unwrap_or(&[]);
        // Handle command
        match uc_name.as_str() {
            "PING"        => cmd_ping(to_send, &client).await,
            "ECHO"        => cmd_echo(args, &client),
            "SET"         => cmd_set(to_send, args, &client, bundle.db).await,
            "GET"         => cmd_get(args, &client, bundle.db).await,
            "RPUSH"       => cmd_push(true, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "LPUSH"       => cmd_push(false, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "RPOP"        => cmd_pop(true, to_send, args, &client, bundle.db).await,
            "LPOP"        => cmd_pop(false, to_send, args, &client, bundle.db).await,
            "BRPOP"       => cmd_bpop(true, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "BLPOP"       => cmd_bpop(false, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "LRANGE"      => cmd_lrange(args, &client, bundle.db).await,
            "LLEN"        => cmd_llen(args, &client, bundle.db).await,
            "TYPE"        => cmd_type(args, &client, bundle.db).await,
            "XADD"        => cmd_xadd(to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "XRANGE"      => cmd_xrange(args, &client, bundle.db).await,
            "XREAD"       => cmd_xread(args, &client, bundle.db, bundle.blocked_clients).await,
            "ZADD"        => cmd_zadd(args, &client, bundle.db).await,
            "ZRANK"       => cmd_zrank(args, &client, bundle.db).await,
            "ZRANGE"      => cmd_zrange(args, &client, bundle.db).await,
            "INCR"        => cmd_incr(to_send, args, &client, bundle.db).await,
            "MULTI"       => cmd_multi(to_send, &client).await,
            "EXEC"        => cmd_exec(to_send, &client, bundle.clone()).await,
            "DISCARD"     => cmd_discard(to_send, &client).await,
            "INFO"        => cmd_info(args, &client, bundle.config.clone(), bundle.repl_state.clone()).await,
            "REPLCONF"    => cmd_replconf(args, &client, bundle.config.clone(), bundle.repl_state.clone()).await,
            "PSYNC"       => cmd_psync(&client, bundle.repl_state.clone()).await,
            "WAIT"        => cmd_wait(args, &client, bundle.repl_state.clone()).await,
            "CONFIG"      => grp_config(args, &client, bundle.config.clone()),
            "KEYS"        => cmd_keys(args, &client, bundle.db).await,
            "SUBSCRIBE"   => cmd_subscribe(args, &client, bundle.subs).await,
            "UNSUBSCRIBE" => cmd_unsubscribe(args, &client, bundle.subs).await,
            "PUBLISH"     => cmd_publish(args, &client, bundle.subs).await,
            _             => cmd_other(&self.name, args, &client)
        }

        let mut state_guard = bundle.repl_state.lock().await;
        // Update offset
        if (bundle.config.is_master && self.is_write()) || self.is_propagated {
            state_guard.repl_offset += self.resp_len;
        }

        // Update handshake state
        if uc_name.as_str() == "PSYNC" {
            if let Some(replica_list) = &mut state_guard.replicas {
                if let Some(replica) = replica_list.iter_mut().find(
                    |r| r.client.tx.same_channel(&client.tx)
                ) {
                    replica.handshaked = true;
                }
            }
        }
    }

    pub fn from(resp_str: &[u8], is_propagated: bool) -> Option<Self> {
        let unparsed_str = str::from_utf8(resp_str).unwrap();
        let mut lines = unparsed_str.split("\r\n");
        lines.next(); // Skip array header

        let mut parsed = Vec::new();
        while let Some(curr_line) = lines.next() {
            // Skip non-bulk-string-length lines
            if !curr_line.starts_with('$') { continue; }
            // Get length and add next line to parsed Vec
            let len = curr_line[1..].parse().unwrap();
            if let Some(val) = lines.next() {
                parsed.push(val[..len].to_string());
            }
        }

        let args = match parsed.len() {
            0 => { return None; } // Empty command
            1 => None,
            _ => Some(Vec::from(parsed[1..].to_vec()))
        };

        Some(Self {
            name: parsed[0].clone(),
            args,
            is_propagated,
            resp_len: resp_str.len()
        })
    }

    fn to_resp_array(&self) -> Vec<u8> {
        let mut res: Vec<u8> = Vec::from(format!(
            "*{}\r\n${}\r\n{}\r\n",
            1 + self.args.as_ref().unwrap_or(&Vec::new()).len(),
            self.name.len(),
            self.name
        ).into_bytes());
        
        if let Some(args) = self.args.as_ref() {
            for arg in args {
                res.extend(format!(
                    "${}\r\n{arg}\r\n", arg.len()
                ).into_bytes());
            }
        }

        res
    }

    fn is_write(&self) -> bool {
        matches!(
            self.name.to_uppercase().as_str(),
            "SET" | "INCR" | // Keyspace
            "RPUSH" | "LPUSH" | "RPOP" | "LPOP" | // Lists
            "XADD" | // Streams
            "MULTI" | "EXEC" | "DISCARD" // Transactions
        )
    }

    fn is_transactional(&self) -> bool {
        matches!(
            self.name.to_uppercase().as_str(),
            "MULTI" | "EXEC" | "DISCARD"
        )
    }

    fn is_sub_related(&self) -> bool {
        matches!(
            self.name.to_uppercase().as_str(),
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PING" | "QUIT" | "RESET"
        )
    }
}
