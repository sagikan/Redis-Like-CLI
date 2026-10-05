use crate::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn invalid_commands_return_explicit_errors() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Validate argument counts, unknown commands, types, and numeric input
    assert!(command(&mut client, &["GET"]).contains("wrong number"));
    assert!(command(&mut client, &["UNKNOWN"]).contains("unknown command"));
    assert_eq!(command(&mut client, &["SET", "number", "not-an-int"]), "+OK\r\n");
    assert!(command(&mut client, &["INCR", "number"]).contains("not an integer"));
    assert!(command(&mut client, &["ZADD", "scores", "not-a-float", "member"]).contains("valid float"));

    // Validate transaction state errors
    assert!(command(&mut client, &["EXEC"]).contains("without MULTI"));
    assert!(command(&mut client, &["DISCARD"]).contains("without MULTI"));
    assert_eq!(command(&mut client, &["MULTI"]), "+OK\r\n");
    assert!(command(&mut client, &["MULTI"]).contains("can not be nested"));
    assert_eq!(command(&mut client, &["DISCARD"]), "+OK\r\n");
}
