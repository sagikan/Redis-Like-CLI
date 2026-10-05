use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn transactions_queue_commands_and_return_exec_results() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // MULTI queues a write; EXEC applies it and returns its result
    assert_eq!(command(&mut client, &["MULTI"]), "+OK\r\n");
    assert_eq!(command(&mut client, &["SET", "transaction-key", "value"]), "+QUEUED\r\n");
    assert_eq!(command(&mut client, &["EXEC"]), "*1\r\n+OK\r\n");
    assert_eq!(command(&mut client, &["GET", "transaction-key"]), "+value\r\n");
}
