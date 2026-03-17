# Redis Node Behavior Specifics

## Connection Lifecycle

### Connection Establishment
1. Client opens TCP connection to Redis server (default port 6379)
2. Server accepts connection, creates client data structures
3. Client optionally authenticates (`AUTH` or `HELLO`)
4. Client optionally selects database (`SELECT`)
5. Client sends commands

### AUTH & Authentication

- Pre-Redis 6: `AUTH password`
- Redis 6+ (ACL): `AUTH username password`
- Can be combined with `HELLO`: `HELLO 3 AUTH username password`
- Authentication must happen before any other commands (except `HELLO`)

### SELECT (Database Selection)

- `SELECT <db-index>` switches the database for the current connection
- Database remains selected until another `SELECT` or connection close
- Default database is 0
- This is **per-connection state** — a major concern for connection pooling
- Most production deployments use database 0 only

### Disconnection
- Client sends `QUIT` (graceful) or closes socket
- Server detects closed connection on next I/O operation
- Server cleans up client state (unsubscribe from pub/sub, release blocking state, etc.)

## Pub/Sub Model

### Protocol-Level Behavior

A subscribed client enters a special **Pub/Sub state**:
- In RESP2: can only issue `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PING`, `RESET`
- In RESP3: can issue **any** commands while subscribed

### Message Types (Wire Format)

All messages are arrays with 3 elements:

**`subscribe` confirmation:**
```
*3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
```

**`unsubscribe` confirmation:**
```
*3\r\n$11\r\nunsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
```

**`message` (pushed from PUBLISH):**
```
*3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<payload>\r\n
```

**`pmessage` (pattern match):**
```
*4\r\n$8\r\npmessage\r\n$<len>\r\n<pattern>\r\n$<len>\r\n<channel>\r\n$<len>\r\n<payload>\r\n
```

### Subscription Count

The last argument in subscribe/unsubscribe messages is the total active subscription count. Client exits Pub/Sub state when count drops to zero.

### Delivery Semantics

- **At-most-once** delivery
- Subscribers and publishers must be connected simultaneously
- No message persistence — missed messages during disconnection are lost
- A client can receive the same message multiple times if subscribed via both channel and pattern

### Sharded Pub/Sub (Redis 7.0+)

Shard channels are assigned to hash slots (same algorithm as keys). Uses `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`.

### Server-Side State

Redis maintains per-client:
- List of subscribed channels
- List of subscribed patterns
- Messages are pushed when `PUBLISH` is called

## Blocking Commands

### BLPOP / BRPOP

Blocking list pop primitives. Block the connection when target lists are empty.

**Non-blocking case:** If any specified key has a non-empty list, immediately pop and return (keys checked in order).

**Blocking case:** If no keys exist or all lists empty:
1. Client is marked as blocked
2. Server tracks which keys the client is waiting on
3. When a write (LPUSH/RPUSH) makes progress possible, Redis marks the client as unblocked (added to internal queue)
4. In `beforeSleep()`, Redis rechecks and serves the client

### Multi-Key Ordering

When blocked on multiple keys and multiple become non-empty from the same command/transaction, the client is served according to key order in the BLPOP call (not write order).

### Client Priority

FIFO ordering among blocked clients on the same key. No priority retention between blocking calls.

### Behavior in MULTI/EXEC

BLPOP inside a transaction does **not** block — it would require blocking the entire server. The command behaves as non-blocking within transactions.

### Reliability

BLPOP removes the element from the list before returning to client. If client crashes, element is lost. `BRPOPLPUSH` (deprecated) / `BLMOVE` provides more reliable pattern by moving to a backup list atomically.

### Module API

Redis modules can create custom blocking commands via `RedisModule_BlockClient()` and `RedisModule_UnblockClient()`.

## CLIENT Commands

### CLIENT ID
Returns unique client ID (integer). Available since Redis 5.0.0.

### CLIENT SETNAME / GETNAME
- `CLIENT SETNAME <name>` — assigns name to connection (shown in CLIENT LIST)
- `CLIENT GETNAME` — returns current name
- Empty string removes name
- Available since Redis 2.6.9

### CLIENT INFO
Returns info about current connection (same format as CLIENT LIST). Available since Redis 6.2.0.

### CLIENT SETINFO
Sets client library metadata. Available since Redis 7.2.0.
```
CLIENT SETINFO LIB-NAME <name>
CLIENT SETINFO LIB-VER <version>
```

### CLIENT LIST
Lists all open connections. Fields evolved over versions:
- 2.8.12: unique client id
- 5.0.0: optional TYPE filter
- 6.0.0: user field (ACL)
- 7.2.0: lib-name, lib-ver fields

### Other CLIENT Subcommands
- `CLIENT KILL` — close a connection
- `CLIENT PAUSE` / `UNPAUSE` — suspend/resume client processing
- `CLIENT REPLY` — control reply mode (ON/OFF/SKIP)
- `CLIENT NO-EVICT` / `NO-TOUCH` — memory management flags
- `CLIENT TRACKING` / `TRACKINGINFO` — client-side caching
- `CLIENT UNBLOCK` — unblock a blocked client
- `CLIENT GETREDIR` — get tracking redirect client ID

## Implications for RedisBox Node Simulator

Key per-connection state to track:
- Selected database index
- Authentication status (username, permissions)
- Pub/Sub subscriptions (channels and patterns)
- Blocking state (which keys, timeout)
- Client name, lib-name, lib-ver
- Client ID (auto-assigned)
- MULTI/EXEC transaction state
- RESP protocol version (2 or 3)

---

[← Back to Node Simulator Research](README.md)
