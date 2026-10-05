use std::fs;
use crate::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn startup_creates_the_configured_rdb_path() {
    let port = free_port();
    let directory = std::env::temp_dir().join(format!("redis-like-rdb-test-{port}"));
    let mut args = server_args(port);
    args.extend(["--dir".into(), directory.to_string_lossy().into_owned()]);
    let _server = RunningServer::start(&args);
    let mut client = wait_for_server(port);

    // Startup creates an empty RDB file when the configured file is absent
    assert_eq!(command(&mut client, &["PING"]), "+PONG\r\n");
    assert!(fs::metadata(directory.join("dump.rdb")).is_ok());
}
