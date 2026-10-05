use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn list_commands_mutate_and_inspect_lists() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Push values, inspect their range, pop one value, and check the length
    assert_eq!(command(&mut client, &["RPUSH", "list", "one", "two"]), ":2\r\n");
    assert_eq!(command(&mut client, &["LRANGE", "list", "0", "-1"]), "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n");
    assert_eq!(command(&mut client, &["LPOP", "list"]), "+one\r\n");
    assert_eq!(command(&mut client, &["LLEN", "list"]), ":1\r\n");
}

#[test]
fn list_commands_support_both_push_and_pop_directions() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // LPUSH and RPOP operate on opposite ends of the same list
    assert_eq!(command(&mut client, &["LPUSH", "deque", "left"]), ":1\r\n");
    assert_eq!(command(&mut client, &["RPUSH", "deque", "right"]), ":2\r\n");
    assert_eq!(command(&mut client, &["RPOP", "deque"]), "+right\r\n");
    assert_eq!(command(&mut client, &["LPOP", "deque"]), "+left\r\n");
    assert_eq!(command(&mut client, &["LLEN", "deque"]), ":0\r\n");
}
