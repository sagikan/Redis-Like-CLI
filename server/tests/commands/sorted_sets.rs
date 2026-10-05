use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn sorted_set_commands_manage_scores_and_members() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Add a scored member, inspect it, and remove it
    assert_eq!(command(&mut client, &["ZADD", "scores", "10", "alice"]), ":1\r\n");
    assert_eq!(command(&mut client, &["ZCARD", "scores"]), ":1\r\n");
    assert!(command(&mut client, &["ZSCORE", "scores", "alice"]).contains("10"));
    assert!(command(&mut client, &["ZRANGE", "scores", "0", "-1"]).contains("alice"));
    assert_eq!(command(&mut client, &["ZREM", "scores", "alice"]), ":1\r\n");
}

#[test]
fn sorted_sets_report_member_rank() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // ZRANK returns zero-based rank in score order
    command(&mut client, &["ZADD", "scores", "2", "second"]);
    command(&mut client, &["ZADD", "scores", "1", "first"]);
    assert_eq!(command(&mut client, &["ZRANK", "scores", "first"]), ":0\r\n");
    assert_eq!(command(&mut client, &["ZRANK", "scores", "missing"]), "$-1\r\n");
}
