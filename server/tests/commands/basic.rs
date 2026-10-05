use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn basic_commands_return_expected_responses() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Basic connection and echo commands
    assert_eq!(command(&mut client, &["PING"]), "+PONG\r\n");
    assert_eq!(command(&mut client, &["ECHO", "hello"]), "+hello\r\n");
}
