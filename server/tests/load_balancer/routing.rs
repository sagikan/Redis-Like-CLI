use std::thread;
use crate::support::{command, free_port, read_frame, server_args, wait_for_server, RunningServer};

#[test]
fn load_balancer_routes_reads_from_the_master_without_replicas() {
    let master_port = free_port();
    let balancer_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let mut master = wait_for_server(master_port);
    assert_eq!(command(&mut master, &["SET", "key", "value"]), "+OK\r\n");

    // An empty replica list still permits reads from the master
    let mut args = server_args(balancer_port);
    args.extend(["--load-balancer".into(), "--master".into(), format!("127.0.0.1:{master_port}")]);
    let _balancer = RunningServer::start(&args);
    let mut client = wait_for_server(balancer_port);
    assert_eq!(command(&mut client, &["GET", "key"]), "+value\r\n");
}

#[test]
fn load_balancer_keeps_transactions_on_one_backend_connection() {
    let master_port = free_port();
    let replica_port = free_port();
    let balancer_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let mut replica_args = server_args(replica_port);
    replica_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let _replica = RunningServer::start(&replica_args);

    let mut balancer_args = server_args(balancer_port);
    balancer_args.extend([
        "--load-balancer".into(),
        "--master".into(),
        format!("127.0.0.1:{master_port}"),
        "--replicas".into(),
        format!("127.0.0.1:{replica_port}"),
    ]);
    let _balancer = RunningServer::start(&balancer_args);
    let mut client = wait_for_server(balancer_port);

    // MULTI and EXEC must use the same master-side client connection
    assert_eq!(command(&mut client, &["MULTI"]), "+OK\r\n");
    assert_eq!(command(&mut client, &["SET", "sticky", "value"]), "+QUEUED\r\n");
    assert_eq!(command(&mut client, &["EXEC"]), "*1\r\n+OK\r\n");
    assert_eq!(command(&mut client, &["GET", "sticky"]), "+value\r\n");
}

#[test]
fn load_balancer_keeps_blocking_reads_on_the_master() {
    let master_port = free_port();
    let balancer_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let mut balancer_args = server_args(balancer_port);
    balancer_args.extend([
        "--load-balancer".into(),
        "--master".into(),
        format!("127.0.0.1:{master_port}"),
    ]);
    let _balancer = RunningServer::start(&balancer_args);
    let blocked = wait_for_server(balancer_port);
    let mut producer = wait_for_server(master_port);

    // Blocking reads through the proxy must wait on the same master as writes
    let reader = thread::spawn(move || {
        let mut blocked = blocked;
        command(&mut blocked, &["BLPOP", "queue", "2"])
    });
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(command(&mut producer, &["RPUSH", "queue", "value"]), ":1\r\n");
    assert!(reader.join().unwrap().contains("value"));
}

#[test]
fn load_balancer_forwards_pubsub_messages_on_a_sticky_connection() {
    let master_port = free_port();
    let balancer_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let mut balancer_args = server_args(balancer_port);
    balancer_args.extend([
        "--load-balancer".into(),
        "--master".into(),
        format!("127.0.0.1:{master_port}"),
    ]);
    let _balancer = RunningServer::start(&balancer_args);
    let mut subscriber = wait_for_server(balancer_port);
    let mut publisher = wait_for_server(balancer_port);

    // Pub/sub messages must arrive through the subscriber's persistent backend connection
    assert!(command(&mut subscriber, &["SUBSCRIBE", "events"]).contains("subscribe"));
    assert_eq!(command(&mut publisher, &["PUBLISH", "events", "message"]), ":1\r\n");
    assert!(read_frame(&mut subscriber).starts_with(b"*3\r\n$7\r\nmessage"));
}

#[test]
fn load_balancer_reports_unavailable_backends() {
    let balancer_port = free_port();
    let missing_master = free_port();
    let mut balancer_args = server_args(balancer_port);
    balancer_args.extend([
        "--load-balancer".into(),
        "--master".into(),
        format!("127.0.0.1:{missing_master}"),
    ]);
    let _balancer = RunningServer::start(&balancer_args);
    let mut client = wait_for_server(balancer_port);

    // A missing backend produces an explicit proxy error
    assert_eq!(command(&mut client, &["GET", "missing"]), "-ERR backend unavailable\r\n");
}
