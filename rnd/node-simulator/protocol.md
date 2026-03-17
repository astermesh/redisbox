# Redis Protocol Specifics

## RESP2 vs RESP3

### Overview

RESP3 is mostly a superset of RESP2. RESP2 became the standard in Redis 2.0. Redis 6.0 introduced experimental opt-in RESP3 support.

### Key Differences

| Feature | RESP2 | RESP3 |
|---------|-------|-------|
| Push messages | Require separate connection | On same connection (out-of-band) |
| Null values | Encoded as special bulk string/array forms | Dedicated null type |
| Semantic types | Everything is string/array/integer/error | Maps, sets, doubles, booleans, etc. |
| Streaming aggregates | Not supported | Aggregated types without upfront length |
| Streaming strings | Private internal extension | Part of specification |
| Pub/Sub + commands | Separate connections required | Same connection (any command in subscribed state) |

### Push Messages (RESP3)

The main reason for RESP3 adoption. In high-usage servers, RESP3 halves the number of connections by allowing push messages and regular command responses on the same connection. Particularly useful in hosted environments where inbound connections are capped.

### Richer Semantic Types (RESP3)

RESP3 conveys semantic meaning in return types:
- `HGETALL` returns a **map** (not a flat array needing client-side pairing)
- `LRANGE` returns an **array**
- `EXISTS` returns a **boolean**
- `ZSCORE` returns a **double** (not a string)

### Protocol Handshake (HELLO)

RESP connections start with `HELLO`:
```
HELLO <protocol-version> [AUTH <username> <password>]
```

This allows servers to be backward-compatible with RESP2. Without HELLO, the connection defaults to RESP2.

### Breaking Changes in RESP3

- Client libraries need updates for new types
- Application code may break (e.g., `ZSCORE` returns double, not string)
- Lua scripts need modification (more semantic types from `redis.call()`)
- Lua can return all new RESP3 data types

### Compatibility

- RESP2: all Redis versions
- RESP3: Redis Software 7.2+
- Both supported simultaneously
- Default clients: Go-Redis v9 and Lettuce v6+ use RESP3 by default

## Inline Commands

Redis supports two command formats:
1. **RESP protocol** — binary-safe, length-prefixed
2. **Inline commands** — plain text, space-separated

Inline format: `SET key value\r\n`
RESP format: `*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n`

Inline commands exist for convenience (e.g., `redis-cli` without RESP framing, telnet debugging). The server detects the format by checking if the first byte is `*` (RESP) or not (inline).

## Pipelining

### How It Works at Protocol Level

Pipelining is not a special protocol feature — it's simply sending multiple commands without waiting for responses. The client writes multiple RESP commands to the socket buffer, and Redis processes them sequentially, queuing responses in order.

```
Client sends:           Server processes:
SET key1 val1           → queued response +OK
SET key2 val2           → queued response +OK
GET key1                → queued response $4\r\nval1

Client reads all three responses in order.
```

### Key Points
- No special framing or protocol support needed
- Commands are processed sequentially (same as without pipelining)
- Responses are guaranteed in order
- Reduces round-trip time significantly
- Server buffers responses in memory until client reads them
- Client must handle response parsing for multiple commands

## Pub/Sub Wire Format

### RESP2 Push Messages

In RESP2, pub/sub messages are pushed as regular arrays:

**subscribe confirmation:**
```
*3\r\n
$9\r\nsubscribe\r\n
$5\r\nfirst\r\n
:1\r\n
```

**message push:**
```
*3\r\n
$7\r\nmessage\r\n
$5\r\nfirst\r\n
$11\r\nhello world\r\n
```

**pattern message:**
```
*4\r\n
$8\r\npmessage\r\n
$3\r\nh?o\r\n
$5\r\nhello\r\n
$11\r\nhello world\r\n
```

### RESP3 Push Messages

In RESP3, push messages use a dedicated push type (prefix `>`):
```
>3\r\n
$7\r\nmessage\r\n
$5\r\nfirst\r\n
$11\r\nhello world\r\n
```

This allows distinguishing push data from regular command responses on the same connection.

## Implications for RedisBox Node Simulator

Protocol considerations for the simulator:
- Must support both RESP2 and RESP3 (switch via HELLO)
- Must support inline commands (for telnet/debugging compatibility)
- Pipelining works automatically if we process commands sequentially from the socket buffer
- Pub/Sub push format differs between RESP2 and RESP3
- Per-connection tracking of protocol version is required
- RESP3 allows commands during pub/sub state — must handle this

---

[← Back to Node Simulator Research](README.md)
