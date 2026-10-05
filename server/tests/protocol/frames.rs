use std::io::Write;
use crate::support::{
    command, encode_command, free_port, read_frame, server_args, wait_for_server, RunningServer,
};

#[test]
fn multiple_resp_commands_can_be_pipelined() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // A single write can contain multiple complete RESP commands
    let mut pipeline = encode_command(&["SET", "pipelined", "yes"]);
    pipeline.extend(encode_command(&["GET", "pipelined"]));
    client.write_all(&pipeline).unwrap();
    assert_eq!(read_frame(&mut client), b"+OK\r\n");
    assert_eq!(read_frame(&mut client), b"+yes\r\n");

    // A normal command remains usable after a pipeline
    assert_eq!(command(&mut client, &["GET", "pipelined"]), "+yes\r\n");
}

#[test]
fn fragmented_resp_command_is_buffered_until_complete() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);
    let frame = encode_command(&["SET", "fragmented", "value"]);

    // A command split across TCP writes must be parsed as one frame
    client.write_all(&frame[..10]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(25));
    client.write_all(&frame[10..]).unwrap();
    assert_eq!(read_frame(&mut client), b"+OK\r\n");
    assert_eq!(command(&mut client, &["GET", "fragmented"]), "+value\r\n");
}
