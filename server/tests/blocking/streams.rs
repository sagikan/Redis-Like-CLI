use std::thread;
use crate::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn blocking_stream_reader_is_released_by_xadd() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let blocked = wait_for_server(port);
    let mut producer = wait_for_server(port);

    // XREAD BLOCK waits for a new entry on the stream
    let reader = thread::spawn(move || {
        let mut blocked = blocked;
        command(&mut blocked, &["XREAD", "BLOCK", "1000", "STREAMS", "events", "$"])
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(command(&mut producer, &["XADD", "events", "*", "field", "value"]).starts_with('$'));
    assert!(reader.join().unwrap().contains("value"));
}
