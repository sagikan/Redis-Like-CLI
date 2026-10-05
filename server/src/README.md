# Structure

## Top-level entry point

- [`main.rs`](./main.rs) parses configuration and starts the selected runtime

## Core state

The [`core/`](./core) folder contains shared server state and application context:

- [`bundle.rs`](./core/bundle.rs) groups configuration, database, replication, blocking-client, and subscription state
- [`client.rs`](./core/client.rs) defines client state, responses, blocked clients, and replica clients
- [`config.rs`](./core/config.rs) parses server options and stores replication state
- [`db.rs`](./core/db.rs) defines the in-memory database and supported value types

## Runtime services

The [`runtime/`](./runtime) folder contains the server execution modes and networking helpers:

- [`server.rs`](./runtime/server.rs) accepts normal client connections and executes commands
- [`replica.rs`](./runtime/replica.rs) performs the replica handshake, initial synchronization, and replication polling
- [`load_balancer.rs`](./runtime/load_balancer.rs) routes client commands to the master and replicas
- [`protocol.rs`](./runtime/protocol.rs) provides RESP frame parsing and network response helpers

## Persistence

The [`persistence/`](./persistence) folder contains RDB support:

- [`rdb.rs`](./persistence/rdb.rs) loads, decodes, encodes, and writes RDB data

## Commands

The [`commands/`](./commands) folder contains command parsing, dispatch, and implementations:

- [`mod.rs`](./commands/mod.rs) defines `Command` and dispatches commands
- [`helpers.rs`](./commands/helpers.rs) contains shared command utilities
- [`strings/`](./commands/strings) contains string commands such as `SET`, `GET`, and `INCR`
- [`lists/`](./commands/lists) contains list commands such as `LPUSH`, `RPUSH`, `LPOP`, and `LRANGE`
- [`streams/`](./commands/streams) contains `XADD`, `XRANGE`, and `XREAD`
- [`sorted_sets/`](./commands/sorted_sets) contains sorted-set commands
- [`transactions/`](./commands/transactions) contains `MULTI`, `EXEC`, and `DISCARD`
- [`pubsub/`](./commands/pubsub) contains `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`
- [`server/`](./commands/server) contains server, replication, persistence, and miscellaneous commands

When adding a command, place its implementation in the folder matching its behavior and register it in [`commands/mod.rs`](./commands/mod.rs).
