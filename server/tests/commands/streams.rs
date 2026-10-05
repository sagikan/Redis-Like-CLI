use super::support::{command, free_port, server_args, wait_for_server, RunningServer};

#[test]
fn stream_commands_append_and_retrieve_entries() {
    let port = free_port();
    let _server = RunningServer::start(&server_args(port));
    let mut client = wait_for_server(port);

    // XADD creates a stream entry and XRANGE reads it back
    let stream_entry = command(&mut client, &["XADD", "events", "*", "kind", "created"]);
    assert!(stream_entry.starts_with('$'));
    assert!(command(&mut client, &["XRANGE", "events", "-", "+"]).contains("created"));
    assert!(command(&mut client, &["XREAD", "STREAMS", "events", "0-0"]).contains("created"));
}
