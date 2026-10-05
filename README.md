# Redis-like CLI

A mini, Redis-like cache server and CLI client* fully implemented in Rust. It accepts Redis Serialization Protocol (RESP) commands over TCP with support for strings, lists, streams, sorted sets, transactions, pub/sub, and more.

\* Client CLI is currently in work.

## Features

- Asynchronous TCP server built on Tokio.
- RESP2 command parsing and RESP-style replies, including fragmented frames and pipelined commands.
- In-memory database with typed values:
  - Strings
  - Lists
  - Streams
  - Sorted sets
- Key expiry for `SET ... EX` and `SET ... PX`.
- Transactions with `MULTI`, `EXEC`, and `DISCARD`.
- Blocking list and stream reads.
- Channel pub/sub with `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`.
- RDB loading and `SAVE` persistence using a configured directory and filename. Current RDB round trips support string values.
- Master/replica synchronization using `PSYNC` and `REPLCONF`.
- Initial replica synchronization through a master-generated RDB snapshot, followed by incremental replication polling.
- Independent in-memory replication queues for connected replicas.
- `WAIT` support for waiting on replica acknowledgements.
- Command-aware load balancing with:
  - Writes routed to the master
  - Round-robin stateless reads across configured backends
  - Sticky connections for transactions, blocking commands, and pub/sub
- Malformed input validation and explicit protocol or command errors.
- See [`README.md`](server/src/README.md) for more.

## Requirements

- Rust toolchain with Cargo compatible with the edition declared in
  [`server/Cargo.toml`](./server/Cargo.toml).

## Build and run

The Cargo project is in [`server/`](./server/).

### Start a default server

```bash
cargo run --manifest-path server/Cargo.toml
```

The default listener is `127.0.0.1:6380`.

### Server options

> Options are passed after `cargo run --manifest-path server/Cargo.toml --`

| Option | Default | Description |
| --- | --- | --- |
| `--bind <address>` | `127.0.0.1` | Address on which the server listens. |
| `--port <port>` | `6380` | TCP listening port. |
| `--dir <path>` | `.` | Directory containing the RDB file to load. |
| `--dbfilename <name>` | `dump.rdb` | RDB file name to load. |
| `--replicaof "<host> <port>"` | | Starts as a replica and connects to the specified master. |
| `--load-balancer` | disabled | Starts the command-aware load-balancer mode. |
| `--master <host:port>` | | Master backend for write commands. |
| `--replicas <host:port,...>` | | Comma-separated replica backends used for read round-robin. |

## Connecting and sending commands

The wire protocol is RESP2. A command is encoded as an array of bulk strings:

```text
*<argument-count>\r\n
$<byte-length-of-argument-1>\r\n
<argument-1>\r\n
$<byte-length-of-argument-2>\r\n
<argument-2>\r\n
...
```

> Command names are case-insensitive. Arguments are transmitted as separate array elements; do not send a shell-style command line and expect the server to split quoting for you.

### Using the custom-built CLI

*Coming soon.*

### Using `redis-cli`

Because the server speaks RESP, the standard Redis CLI can be used for basic
commands:

```bash
redis-cli -h 127.0.0.1 -p 6380 PING
redis-cli -h 127.0.0.1 -p 6380 SET greeting "hello"
redis-cli -h 127.0.0.1 -p 6380 GET greeting
```

## Supported commands

### Connection and basic commands

| Command | Supported forms and behavior |
| --- | --- |
| `PING` | Returns `PONG`. |
| `ECHO <value>` | Returns the supplied value. |
| `TYPE <key>` | Returns the stored value type. |
| `KEYS <pattern>` | Returns keys matching the supported glob pattern. |

### Strings and expiry

| Command | Supported forms and behavior |
| --- | --- |
| `SET <key> <value>` | Stores a string. |
| `SET <key> <value> NX` | Stores only when the key does not exist. |
| `SET <key> <value> XX` | Stores only when the key already exists. |
| `SET <key> <value> EX <seconds>` | Stores with a positive seconds expiry. |
| `SET <key> <value> PX <milliseconds>` | Stores with a positive milliseconds expiry. |
| `GET <key>` | Returns the string value or a nil bulk string. |
| `INCR <key>` | Increments an integer string. |

> Expiry is enforced both when reading and by a background timer. Expiry is not currently generalized to every data type.

### Lists

| Command | Supported forms and behavior |
| --- | --- |
| `LPUSH <key> <value> [value ...]` | Pushes values on the left. |
| `RPUSH <key> <value> [value ...]` | Pushes values on the right. |
| `LPOP <key> [count]` | Removes values from the left. |
| `RPOP <key> [count]` | Removes values from the right. |
| `BLPOP <key> [key ...] <timeout>` | Pops immediately or blocks until a value is pushed. |
| `BRPOP <key> [key ...] <timeout>` | Right-sided blocking variant. |
| `LRANGE <key> <start> <stop>` | Returns a list range. |
| `LLEN <key>` | Returns the list length. |

> Blocking list clients are tracked separately and are woken by a subsequent push to one of their requested keys. A timeout of `0` represents an indefinite wait in the command's blocking path.

### Streams

| Command | Supported forms and behavior |
| --- | --- |
| `XADD <key> <id> <field> <value> [field value ...]` | Appends a stream entry. |
| `XRANGE <key> <start> <end>` | Reads entries in an ID range. |
| `XREAD [COUNT n] [BLOCK ms] STREAMS <key ...> <id ...>` | Reads from one or more streams, optionally blocking. |

> Stream IDs support the forms implemented by the server, including `*`, `<milliseconds>-*`, and explicit IDs such as `0-1`.

### Sorted sets

| Command | Supported forms and behavior |
| --- | --- |
| `ZADD <key> <score> <member>` | Adds or updates one member. |
| `ZRANK <key> <member>` | Returns the zero-based rank. |
| `ZRANGE <key> <start> <stop>` | Returns members by rank. |
| `ZCARD <key>` | Returns the number of members. |
| `ZSCORE <key> <member>` | Returns a member's score. |
| `ZREM <key> <member>` | Removes a member. |

> Members are ordered by score, with the member string used as the tie-breaker.

### Transactions

| Command | Supported forms and behavior |
| --- | --- |
| `MULTI` | Starts a transaction for the current connection. |
| `EXEC` | Executes queued commands in order and returns their responses. |
| `DISCARD` | Clears queued commands and exits transaction mode. |

> Nested `MULTI`, `EXEC` without `MULTI`, and `DISCARD` without `MULTI` return errors.

### Pub/sub

| Command | Supported forms and behavior |
| --- | --- |
| `SUBSCRIBE <channel>` | Enters subscription mode and subscribes the connection. |
| `UNSUBSCRIBE <channel>` | Removes the connection from a channel. |
| `PUBLISH <channel> <message>` | Delivers a message to subscribers and returns the subscriber count. |

> While a connection is in subscription mode, only subscription-related commands and `PING` are accepted. Use a separate connection for publishing or ordinary data commands.

### Server and replication commands

| Command | Purpose |
| --- | --- |
| `INFO REPLICATION` | Returns role, replication ID, and replication offset. |
| `CONFIG GET DIR` | Returns the configured RDB directory. |
| `CONFIG GET DBFILENAME` | Returns the configured RDB filename. |
| `REPLCONF` | Internal replication handshake, acknowledgements, and capability exchange. |
| `REPLCONF GETSTREAM <offset>` | Internal polling request for queued replicated writes. |
| `PSYNC` | Internal partial/full synchronization request. |
| `WAIT <replica-count> <timeout>` | Waits for replica acknowledgements. |
| `SAVE` | Writes the current string database to the configured RDB path. |

> `REPLCONF` and `PSYNC` are primarily intended for the built-in replica implementation rather than normal application clients.

## Architecture

```text
┌────────────┐           ┌─────────────────────────────┐
│ CLI Client │─TCP+RESP─►│ Command-Aware Load Balancer ├───┐
└────────────┘           └──┬──────────────────────────┘   |
                          Writes                         Reads
┌───────────────────────────▼──────────────────────────┐   │
│                         MASTER                       │◄──┤
| TCP Listener ─┐                                      |   |
|           RESP Parse ─┐                              |   |
|                   Command Dispatch ─┐                |   |
|                           |     Local Bundle Updates |   |
|                           |     ├─ Replication State |   |
│                           |     ├─ Database          │   │
│                           |     ├─ Blocked Clients   │   │
│                           |     └─ Pub/Sub Registry  │   │
└───────────────────────────┼──────────────────────────┘   │
                          Writes                           |
                  ┌─────────▼──────────┐                   │
                  | Replication Stream |                   │
                  └─────────▲──────────┘                   │
        ┌───────────────────┼───────────────────┐          |
┌───────▼──────┐    ┌───────▼──────┐    ┌───────▼──────┐   |
│   REPLICA X  │    │   REPLICA Y  │    │   REPLICA Z  │   |
│ Local Bundle │    │ Local Bundle │    │ Local Bundle │   |
└───────▲──────┘    └───────▲──────┘    └───────▲──────┘   |
        └───────────────────┴───────────────────┴──────────┘
```

> The command-aware load balancer is implemented as a mode of the server binary, not as a separate service. Start it with `--load-balancer`, provide one `--master <host:port>`, and provide replicas with `--replicas <host:port,...>`. It uses round-robin selection for stateless reads and routes writes to the master. Transactions, blocking commands, and pub/sub sessions use a persistent backend connection so their state and asynchronous responses are preserved.

> The replication stream is implemented over the existing master connection. During the initial `PSYNC` exchange, the master sends an RDB snapshot of its current string database. Each replica then polls the master every 100 ms with `REPLCONF GETSTREAM <offset>`. The master keeps a separate in-memory queue for each replica and returns queued RESP command frames. The replica applies those frames to its local bundle and includes its applied offset in the next poll. This keeps replicas independent, but queued commands are lost when the master process exits.

## Development and testing

- [`server/tests/commands/`](./server/tests/commands/) covers command families.
- [`server/tests/replication/`](./server/tests/replication/) covers master, replica, and polling-stream behavior.
- [`server/tests/blocking/`](./server/tests/blocking/) covers blocking lists and streams.
- [`server/tests/protocol/`](./server/tests/protocol/) covers RESP values and response framing.
- [`server/tests/errors/`](./server/tests/errors/) covers invalid commands and transaction-state errors.
- [`server/tests/persistence/`](./server/tests/persistence/) covers RDB startup and `SAVE` restart behavior.
- [`server/tests/load_balancer/`](./server/tests/load_balancer/) covers backend routing.
- [`server/tests/support/`](./server/tests/support/) contains shared helpers.

Run the full test suite from the repository root:

```bash
cargo test --manifest-path server/Cargo.toml -- --test-threads=1
```

Run one test file at a time when focusing on a subsystem:

```bash
cargo test --manifest-path server/Cargo.toml --test [Folder] -- [File] --test-threads=1
```