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
    pub in_transaction: Arc<Mutex<bool>>,
    pub queued_commands: Arc<Mutex<Vec<Command>>>
}

impl Client_ {
    pub fn send_if(&self, to_send: bool, msg: impl Into<Vec<u8>>) {
        if to_send {
            self.tx.send(msg.into()).unwrap();
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
    Queued,
    ErrEmptyCommand,
    ErrArgCount,
    ErrSyntax,
    ErrOutOfRange,
    ErrNotInteger,
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
            Response::Queued => &b"+QUEUED\r\n"[..],
            Response::ErrEmptyCommand => &b"-ERR unknown command ''\r\n"[..],
            Response::ErrArgCount => &b"-ERR wrong number of arguments for command\r\n"[..],
            Response::ErrSyntax => &b"-ERR syntax error\r\n"[..],
            Response::ErrOutOfRange => &b"-ERR value is out of range, must be positive\r\n"[..],
            Response::ErrNotInteger => &b"-ERR value is not an integer or out of range\r\n"[..],
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
