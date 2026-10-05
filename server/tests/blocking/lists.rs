use std::thread;
use crate::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn blocking_list_client_is_released_by_a_push() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let blocked = wait_for_server(port);
    let mut producer = wait_for_server(port);

    // Keep the blocking client open while another client produces a value
    let reader = thread::spawn(move || {
        let mut blocked = blocked;
        command(&mut blocked, &["BLPOP", "queue", "2"])
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(command(&mut producer, &["RPUSH", "queue", "value"]), ":1\r\n");
    assert!(reader.join().unwrap().contains("value"));
}

#[test]
fn blocking_list_timeout_returns_nil_array() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // A timeout with no producer returns a nil array
    assert_eq!(command(&mut client, &["BLPOP", "missing", "0.1"]), "*-1\r\n");
}

#[test]
fn blocking_rpop_client_is_released_by_a_push() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let blocked = wait_for_server(port);
    let mut producer = wait_for_server(port);

    // BRPOP waits on the right side and returns the pushed value
    let reader = thread::spawn(move || {
        let mut blocked = blocked;
        command(&mut blocked, &["BRPOP", "queue", "2"])
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(command(&mut producer, &["LPUSH", "queue", "value"]), ":1\r\n");
    assert!(reader.join().unwrap().contains("value"));
}
