use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn server_commands_inspect_keys_configuration_and_role() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // Create a typed value before inspecting keys and its type
    assert_eq!(command(&mut client, &["RPUSH", "list", "value"]), ":1\r\n");
    assert!(command(&mut client, &["TYPE", "list"]).contains("list"));
    assert!(command(&mut client, &["KEYS", "*"]).starts_with('*'));

    // CONFIG and INFO expose server configuration and replication role
    assert!(command(&mut client, &["CONFIG", "GET", "DIR"]).contains("redis-like-cli-test"));
    assert!(command(&mut client, &["INFO", "REPLICATION"]).contains("role:master"));
}

#[test]
fn wait_returns_zero_when_no_replicas_are_connected() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // WAIT has an immediate zero result when the master has no replicas
    assert_eq!(command(&mut client, &["WAIT", "1", "10"]), ":0\r\n");
}

#[test]
fn save_persists_string_values_for_restart() {
    let port = free_port();
    let directory = std::env::temp_dir().join(format!("redis-like-save-test-{port}"));
    let mut args = server_args(port);
    args.extend(["--dir".into(), directory.to_string_lossy().into_owned()]);
    let server = RunningServer::start(&args);
    let mut client = wait_for_server(port);

    // SAVE writes the current string database to the configured RDB path
    assert_eq!(command(&mut client, &["SET", "persistent", "value"]), "+OK\r\n");
    assert_eq!(command(&mut client, &["SAVE"]), "+OK\r\n");
    server.stop();

    // A new process loads the value from the saved RDB
    let restarted = RunningServer::start(&args);
    let mut client = wait_for_server(port);
    assert_eq!(command(&mut client, &["GET", "persistent"]), "+value\r\n");
    restarted.stop();
}
