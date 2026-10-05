use super::support::{command, free_port, read_frame, server_args, wait_for_server, RunningServer};

#[test]
fn pubsub_delivers_messages_between_connections() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut publisher = wait_for_server(port);
    let mut subscriber = wait_for_server(port);

    // Subscription state belongs to the subscriber connection
    assert!(command(&mut subscriber, &["SUBSCRIBE", "events"]).contains("subscribe"));

    // PUBLISH sends a message to the subscribed connection
    assert_eq!(command(&mut publisher, &["PUBLISH", "events", "message"]), ":1\r\n");
    assert!(read_frame(&mut subscriber).starts_with(b"*3\r\n$7\r\nmessage"));
}
