mod helpers;

pub mod strings;
pub mod lists;
pub mod streams;
pub mod sorted_sets;
pub mod transactions;
pub mod pubsub;
pub mod server;

pub use strings::*;
pub use lists::*;
pub use streams::*;
pub use sorted_sets::*;
pub use transactions::*;
pub use pubsub::*;
pub use server::*;

mod echo;
mod keys;
mod r#type;
pub use echo::*;
pub use keys::*;
pub use r#type::*;

use std::str;
use crate::core::{bundle::Bundle, client::{Client, Response}};

#[derive(Clone)]
pub struct Command {
    pub name: String,
    pub args: Option<Vec<String>>,
    pub is_propagated: bool,
    pub resp_len: usize
}

impl Command {
    #[async_recursion::async_recursion]
    pub async fn execute(&mut self, client: &Client, bundle: Bundle) {
        if bundle.config.is_master && self.is_write() {
            let streams = {
                let state_guard = bundle.repl_state.lock().await;
                state_guard.replicas.as_ref().map(
                    |replicas| replicas.iter().map(
                        |replica| replica.stream.clone()
                    ).collect::<Vec<_>>()
                ).unwrap_or_default()
            };
            let cmd = self.to_resp_array();
            for stream in streams {
                stream.lock().await.push_back(cmd.clone());
            }
        }
        let uc_name = self.name.to_uppercase();
        if client.in_transaction().await && !self.is_transactional() {
            client.push_queued(self.clone()).await;
            client.tx.send(Response::Queued.into()).unwrap();
            return;
        } else if client.in_sub_mode().await && !self.is_sub_related() {
            client.tx.send(format!("-ERR Can't execute '{uc_name}' in this context\r\n").into_bytes()).unwrap();
            return;
        }
        let to_send = !self.is_propagated;
        let args = self.args.as_deref().unwrap_or(&[]);
        match uc_name.as_str() {
            "PING" => cmd_ping(to_send, &client).await,
            "ECHO" => cmd_echo(args, &client),
            "SET" => cmd_set(to_send, args, &client, bundle.db).await,
            "GET" => cmd_get(args, &client, bundle.db).await,
            "RPUSH" => cmd_push(true, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "LPUSH" => cmd_push(false, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "RPOP" => cmd_pop(true, to_send, args, &client, bundle.db).await,
            "LPOP" => cmd_pop(false, to_send, args, &client, bundle.db).await,
            "BRPOP" => cmd_bpop(true, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "BLPOP" => cmd_bpop(false, to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "LRANGE" => cmd_lrange(args, &client, bundle.db).await,
            "LLEN" => cmd_llen(args, &client, bundle.db).await,
            "TYPE" => cmd_type(args, &client, bundle.db).await,
            "XADD" => cmd_xadd(to_send, args, &client, bundle.db, bundle.blocked_clients).await,
            "XRANGE" => cmd_xrange(args, &client, bundle.db).await,
            "XREAD" => cmd_xread(args, &client, bundle.db, bundle.blocked_clients).await,
            "ZADD" => cmd_zadd(to_send, args, &client, bundle.db).await,
            "ZRANK" => cmd_zrank(args, &client, bundle.db).await,
            "ZRANGE" => cmd_zrange(args, &client, bundle.db).await,
            "ZCARD" => cmd_zcard(args, &client, bundle.db).await,
            "ZSCORE" => cmd_zscore(args, &client, bundle.db).await,
            "ZREM" => cmd_zrem(to_send, args, &client, bundle.db).await,
            "INCR" => cmd_incr(to_send, args, &client, bundle.db).await,
            "MULTI" => cmd_multi(to_send, &client).await,
            "EXEC" => cmd_exec(to_send, &client, bundle.clone()).await,
            "DISCARD" => cmd_discard(to_send, &client).await,
            "INFO" => cmd_info(args, &client, bundle.config.clone(), bundle.repl_state.clone()).await,
            "REPLCONF" => cmd_replconf(args, &client, bundle.config.clone(), bundle.repl_state.clone()).await,
            "PSYNC" => cmd_psync(&client, bundle.repl_state.clone(), bundle.db).await,
            "WAIT" => cmd_wait(args, &client, bundle.repl_state.clone()).await,
            "SAVE" => cmd_save(args, &client, bundle.config.clone(), bundle.db).await,
            "CONFIG" => grp_config(args, &client, bundle.config.clone()),
            "KEYS" => cmd_keys(args, &client, bundle.db).await,
            "SUBSCRIBE" => cmd_subscribe(args, &client, bundle.subs).await,
            "UNSUBSCRIBE" => cmd_unsubscribe(args, &client, bundle.subs).await,
            "PUBLISH" => cmd_publish(args, &client, bundle.subs).await,
            _ => cmd_other(&self.name, args, &client)
        }
        let mut state_guard = bundle.repl_state.lock().await;
        if (bundle.config.is_master && self.is_write()) || self.is_propagated { state_guard.repl_offset += self.resp_len; }
        if uc_name.as_str() == "PSYNC" {
            if let Some(replica_list) = &mut state_guard.replicas {
                if let Some(replica) = replica_list.iter_mut().find(|r| r.client.tx.same_channel(&client.tx)) { replica.handshaked = true; }
            }
        }
    }

    pub fn from(resp_str: &[u8], is_propagated: bool) -> Option<Self> {
        let unparsed_str = str::from_utf8(resp_str).ok()?;
        let mut lines = unparsed_str.split("\r\n"); lines.next();
        let mut parsed = Vec::new();
        while let Some(curr_line) = lines.next() {
            if !curr_line.starts_with('$') { continue; }
            let len = curr_line[1..].parse::<usize>().ok()?;
            if let Some(val) = lines.next() { if val.len() < len { return None; } parsed.push(val[..len].to_string()); }
        }
        let args = match parsed.len() { 0 => return None, 1 => None, _ => Some(Vec::from(parsed[1..].to_vec())) };
        Some(Self { name: parsed[0].clone(), args, is_propagated, resp_len: resp_str.len() })
    }

    fn to_resp_array(&self) -> Vec<u8> {
        let mut res = Vec::from(format!("*{}\r\n${}\r\n{}\r\n", 1 + self.args.as_ref().unwrap_or(&Vec::new()).len(), self.name.len(), self.name).into_bytes());
        if let Some(args) = self.args.as_ref() { for arg in args { res.extend(format!("${}\r\n{arg}\r\n", arg.len()).into_bytes()); } }
        res
    }

    pub fn is_write(&self) -> bool { matches!(self.name.to_uppercase().as_str(), "SET" | "INCR" | "RPUSH" | "LPUSH" | "RPOP" | "LPOP" | "XADD" | "ZADD" | "ZREM" | "MULTI" | "EXEC" | "DISCARD") }
    fn is_transactional(&self) -> bool { matches!(self.name.to_uppercase().as_str(), "MULTI" | "EXEC" | "DISCARD") }
    fn is_sub_related(&self) -> bool { matches!(self.name.to_uppercase().as_str(), "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "PUBLISH" | "PING" | "QUIT" | "RESET") }
}

