use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn missing_keys_return_nil_or_empty_results() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Missing keys use the RESP nil and empty collection forms
    assert_eq!(command(&mut client, &["GET", "missing"]), "$-1\r\n");
    assert_eq!(command(&mut client, &["TYPE", "missing"]), "+none\r\n");
    assert_eq!(command(&mut client, &["LRANGE", "missing", "0", "-1"]), "*0\r\n");
    assert_eq!(command(&mut client, &["ZRANK", "missing", "member"]), "$-1\r\n");
}

#[test]
fn unsubscribe_exits_subscription_mode() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Unsubscribing restores normal command execution on the same connection
    assert!(command(&mut client, &["SUBSCRIBE", "events"]).contains("subscribe"));
    assert!(command(&mut client, &["UNSUBSCRIBE", "events"]).contains("unsubscribe"));
    assert_eq!(command(&mut client, &["PING"]), "+PONG\r\n");
}
