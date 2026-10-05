use super::support::{command, free_port, server_args, sleep_ms, wait_for_server, RunningServer};

#[test]
fn string_commands_support_values_increments_and_expiry() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // SET, GET, conditional writes, and integer increments
    assert_eq!(command(&mut client, &["SET", "greeting", "hello"]), "+OK\r\n");
    assert_eq!(command(&mut client, &["GET", "greeting"]), "+hello\r\n");
    assert_eq!(command(&mut client, &["SET", "greeting", "new", "NX"]), "$-1\r\n");
    assert_eq!(command(&mut client, &["INCR", "counter"]), ":1\r\n");

    // Expiring keys disappear after their configured lifetime
    assert_eq!(command(&mut client, &["SET", "temporary", "value", "PX", "50"]), "+OK\r\n");
    sleep_ms(100);
    assert_eq!(command(&mut client, &["GET", "temporary"]), "$-1\r\n");
}
