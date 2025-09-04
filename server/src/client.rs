use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::{Mutex, mpsc::UnboundedSender};
use crate::commands::Command;

static CLIENT_ID: AtomicUsize = AtomicUsize::new(1);

pub type Client = Arc<Client_>;

pub fn get_next_id() -> usize {
    CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone)]
pub struct Client_ {
    pub id: usize,
    pub tx: UnboundedSender<Vec<u8>>,
    pub in_transaction: Option<Arc<Mutex<bool>>>,
    pub in_sub_mode: Option<Arc<Mutex<bool>>>,
    pub queued_commands: Option<Arc<Mutex<Vec<Command>>>>,
    pub subs: Option<Arc<Mutex<Vec<String>>>>
}

impl Client_ {
    pub fn send_if(&self, to_send: bool, msg: impl Into<Vec<u8>>) {
        if to_send {
            self.tx.send(msg.into()).unwrap();
        }
    }

    pub async fn in_transaction(&self) -> bool {
        match &self.in_transaction {
            Some(in_tr) => *in_tr.lock().await,
            None => false
        }
    }

    pub async fn set_transaction(&self, v: bool) {
        match &self.in_transaction {
            Some(in_tr) => { *in_tr.lock().await = v; },
            None => ()
        }
    }

    pub async fn in_sub_mode(&self) -> bool {
        match &self.in_sub_mode {
            Some(in_sub) => *in_sub.lock().await,
            None => false
        }
    }

    pub async fn set_sub_mode(&self, v: bool) {
        match &self.in_sub_mode {
            Some(in_sub) => { *in_sub.lock().await = v; },
            None => ()
        }
    }

    pub async fn sub(&self, channel: &String) -> usize {
        let mut upd_count = 0;

        if let Some(subs) = &self.subs {
            let mut subs_guard = subs.lock().await;
            // Insert channel + get updated count
            subs_guard.push(channel.clone());
            upd_count = subs_guard.len();
        }

        upd_count
    }

    pub async fn unsub(&self, channel: &String) -> usize {
        let mut upd_count = 0;

        if let Some(subs) = &self.subs {
            let mut subs_guard = subs.lock().await;
            // Remove channel + get updated count
            subs_guard.retain(|c| c != channel);
            upd_count = subs_guard.len();
        }

        upd_count
    }

    pub async fn push_queued(&self, cmd: Command) {
        if let Some(queued) = &self.queued_commands {
            queued.lock().await.push(cmd);
        }
    }

    pub async fn drain_queued(&self) -> Option<Vec<Command>> {
        match &self.queued_commands {
            Some(q_cmds) => Some(q_cmds.lock().await.drain(..).collect()),
            None => None
        }
    }
}

#[derive(Clone)]
pub struct BlockedClient {
    pub client: Client,
    pub blocked_by: Vec<String>,
    pub expired: bool,
    pub from_right: Option<bool>, // BPOP
    pub from_entry_id: Option<(usize, usize)>, // XREAD
    pub count: Option<usize> // XREAD
}

#[derive(Clone)]
pub struct ReplicaClient {
    pub client: Client,
    pub handshaked: bool,
    pub ack_offset: usize
}

#[derive(Clone)]
pub enum Response {
    Ok,
    Ping,
    Pong,
    SubPong,
    Queued,
    ErrEmptyCommand,
    ErrArgCount,
    ErrSyntax,
    ErrOutOfRange,
    ErrNotInteger,
    ErrNotFloat,
    ErrNegativeTimeout,
    ErrSetExpireTime,
    ErrNestedMulti,
    ErrExecWithoutMulti,
    ErrDiscardWithoutMulti,
    ErrEntryIdZero,
    ErrEntryIdEqualOrSmall,
    ErrEntryIdInvalid,
    WrongType,
    EmptyArray,
    NilArray,
    Nil
}

impl From<Response> for Vec<u8> {
    fn from(response: Response) -> Self {
        match response {
            Response::Ok => &b"+OK\r\n"[..],
            Response::Ping => &b"*1\r\n$4\r\nPING\r\n"[..],
            Response::Pong => &b"+PONG\r\n"[..],
            Response::SubPong => &b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"[..],
            Response::Queued => &b"+QUEUED\r\n"[..],
            Response::ErrEmptyCommand => &b"-ERR unknown command ''\r\n"[..],
            Response::ErrArgCount => &b"-ERR wrong number of arguments for command\r\n"[..],
            Response::ErrSyntax => &b"-ERR syntax error\r\n"[..],
            Response::ErrOutOfRange => &b"-ERR value is out of range, must be positive\r\n"[..],
            Response::ErrNotInteger => &b"-ERR value is not an integer or out of range\r\n"[..],
            Response::ErrNotFloat => &b"-ERR value is not a valid float\r\n"[..],
            Response::ErrNegativeTimeout => &b"-ERR timeout is negative\r\n"[..],
            Response::ErrSetExpireTime => &b"-ERR invalid expire time in 'set' command\r\n"[..],
            Response::ErrNestedMulti => &b"-ERR MULTI calls can not be nested\r\n"[..],
            Response::ErrExecWithoutMulti => &b"-ERR EXEC without MULTI\r\n"[..],
            Response::ErrDiscardWithoutMulti => &b"-ERR DISCARD without MULTI\r\n"[..],
            Response::ErrEntryIdZero => &b"-ERR The ID specified in XADD must be greater than 0-0\r\n"[..],
            Response::ErrEntryIdEqualOrSmall => &b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"[..],
            Response::ErrEntryIdInvalid => &b"-ERR Invalid stream ID specified as stream command argument\r\n"[..],
            Response::WrongType => &b"-WRONGTYPE Operation against a key holding a wrong kind of value\r\n"[..],
            Response::EmptyArray => &b"*0\r\n"[..],
            Response::NilArray => &b"*-1\r\n"[..],
            Response::Nil => &b"$-1\r\n"[..]
        }.to_vec()
    }
}
