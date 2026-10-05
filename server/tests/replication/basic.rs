use std::time::{Duration, Instant};
use crate::support::{command, free_port, server_args, sleep_ms, wait_for_server, RunningServer};

#[test]
fn load_balancer_routes_writes_and_replicates_to_read_backends() {
    let master_port = free_port();
    let replica_one_port = free_port();
    let replica_two_port = free_port();
    let load_balancer_port = free_port();

    // Start the master before its replicas so the replication handshake succeeds
    let _master = RunningServer::start(&server_args(master_port));
    let _master_probe = wait_for_server(master_port);

    // Start two replicas that poll their independent master-side streams
    let mut replica_one_args = server_args(replica_one_port);
    replica_one_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let _replica_one = RunningServer::start(&replica_one_args);
    let mut replica_two_args = server_args(replica_two_port);
    replica_two_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let _replica_two = RunningServer::start(&replica_two_args);
    sleep_ms(500);

    // Load balancer sends the write to the master backend
    let mut load_balancer_args = server_args(load_balancer_port);
    load_balancer_args.extend([
        "--load-balancer".into(),
        "--master".into(),
        format!("127.0.0.1:{master_port}"),
        "--replicas".into(),
        format!("127.0.0.1:{replica_one_port},127.0.0.1:{replica_two_port}"),
    ]);
    let _load_balancer = RunningServer::start(&load_balancer_args);
    let mut load_balancer = wait_for_server(load_balancer_port);
    assert_eq!(command(&mut load_balancer, &["SET", "replicated", "yes"]), "+OK\r\n");

    // Replication is async, so wait until both replicas observe the write
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut first = wait_for_server(replica_one_port);
        let mut second = wait_for_server(replica_two_port);
        if command(&mut first, &["GET", "replicated"]) == "+yes\r\n"
            && command(&mut second, &["GET", "replicated"]) == "+yes\r\n"
        {
            break;
        }
        assert!(Instant::now() < deadline, "replicas did not apply the write");
        sleep_ms(100);
    }

    // Reads are round-robin across the master and configured replicas
    for _ in 0..4 {
        assert_eq!(command(&mut load_balancer, &["GET", "replicated"]), "+yes\r\n");
    }
}

#[test]
fn wait_counts_a_connected_replica_after_replication() {
    let master_port = free_port();
    let replica_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let _master_probe = wait_for_server(master_port);
    let mut replica_args = server_args(replica_port);
    replica_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let _replica = RunningServer::start(&replica_args);
    sleep_ms(500);

    let mut master = wait_for_server(master_port);
    sleep_ms(1000);
    assert_eq!(command(&mut master, &["SET", "waited", "yes"]), "+OK\r\n");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // WAIT should observe the replica's polling acknowledgement
        let response = command(&mut master, &["WAIT", "1", "1000"]);
        if response == ":1\r\n" {
            break;
        }

        assert!(Instant::now() < deadline, "replica did not acknowledge WAIT");
        sleep_ms(100);
    }

}

#[test]
fn one_replica_can_continue_when_another_replica_stops() {
    let master_port = free_port();
    let stopped_port = free_port();
    let healthy_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let _master_probe = wait_for_server(master_port);
    let mut stopped_args = server_args(stopped_port);
    stopped_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let stopped = RunningServer::start(&stopped_args);
    let mut healthy_args = server_args(healthy_port);
    healthy_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let _healthy = RunningServer::start(&healthy_args);
    sleep_ms(500);

    let mut master = wait_for_server(master_port);
    sleep_ms(1000);
    stopped.stop();
    assert_eq!(command(&mut master, &["SET", "independent", "yes"]), "+OK\r\n");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut healthy = wait_for_server(healthy_port);
        if command(&mut healthy, &["GET", "independent"]) == "+yes\r\n" {
            break;
        }

        assert!(Instant::now() < deadline, "healthy replica did not receive write");
        sleep_ms(100);
    }

}

#[test]
fn replica_loads_existing_string_data_during_initial_sync() {
    let master_port = free_port();
    let replica_port = free_port();
    let _master = RunningServer::start(&server_args(master_port));
    let _master_probe = wait_for_server(master_port);

    // Write data before the replica performs its initial PSYNC
    let mut master = wait_for_server(master_port);
    assert_eq!(command(&mut master, &["SET", "before-sync", "value"]), "+OK\r\n");

    let mut replica_args = server_args(replica_port);
    replica_args.extend(["--replicaof".into(), format!("127.0.0.1 {master_port}")]);
    let _replica = RunningServer::start(&replica_args);

    // The snapshot should make pre-existing data immediately available
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut replica = wait_for_server(replica_port);
        if command(&mut replica, &["GET", "before-sync"]) == "+value\r\n" {
            break;
        }
        assert!(Instant::now() < deadline, "replica did not load initial snapshot");
        sleep_ms(100);
    }
}
