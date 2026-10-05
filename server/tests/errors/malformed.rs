use std::io::Write;
use super::support::{command, free_port, read_frame, server_args, wait_for_server, RunningServer};

#[test]
fn malformed_utf8_is_rejected_without_killing_the_connection() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // A malformed bulk value returns an error instead of panicking the server
    client.write_all(b"*2\r\n$4\r\nECHO\r\n$1\r\n\xff\r\n").unwrap();
    assert!(read_frame(&mut client).starts_with(b"-"));
    assert_eq!(command(&mut client, &["PING"]), "+PONG\r\n");
}

#[test]
fn unknown_commands_in_a_pipeline_do_not_drop_following_frames() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);
    let mut pipeline = b"*1\r\n$7\r\nUNKNOWN\r\n".to_vec();
    pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    client.write_all(&pipeline).unwrap();

    // Each complete frame receives its own response
    assert!(read_frame(&mut client).starts_with(b"-"));
    assert_eq!(read_frame(&mut client), b"+PONG\r\n");
}
