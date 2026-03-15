# RESP Protocol Analysis

Redis Serialization Protocol — the wire protocol between Redis clients and servers.

## Overview

RESP is a binary-safe, text-oriented protocol. It was designed for simplicity and speed of parsing. The first byte of every data element determines its type. All elements are terminated with `\r\n` (CRLF). RESP is used on top of TCP (default port 6379), but the protocol itself is transport-agnostic.

Two versions exist: RESP2 (default, since Redis 1.2) and RESP3 (opt-in since Redis 6.0).

## RESP2 Format

### Data Types (5 types)

| Type | Prefix | Format | Example |
|------|--------|--------|---------|
| Simple String | `+` | `+<string>\r\n` | `+OK\r\n` |
| Error | `-` | `-<type> <msg>\r\n` | `-ERR unknown command 'foobar'\r\n` |
| Integer | `:` | `:<number>\r\n` | `:1000\r\n` |
| Bulk String | `$` | `$<length>\r\n<data>\r\n` | `$6\r\nfoobar\r\n` |
| Array | `*` | `*<count>\r\n<elements>` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

### Null Encoding

Null bulk string: `$-1\r\n`
Null array: `*-1\r\n`
Empty string: `$0\r\n\r\n` (length 0, still has trailing CRLF)
Empty array: `*0\r\n`

### Simple Strings vs Bulk Strings

Simple strings cannot contain `\r` or `\n` (they're delimited by CRLF). Bulk strings are binary-safe — the length prefix allows arbitrary bytes including CRLF within the payload. All client-to-server data uses bulk strings for this reason.

### Error Format

Errors follow the pattern `-<TYPE> <message>\r\n`. Common error types:
- `ERR` — generic error
- `WRONGTYPE` — operation against wrong data type
- `MOVED` — cluster redirect
- `ASK` — cluster ask redirect
- `BUSY` — script in progress
- `NOSCRIPT` — no matching script
- `LOADING` — server is loading dataset
- `OOM` — max memory reached
- `READONLY` — write to read-only replica
- `CROSSSLOT` — keys in different hash slots (cluster)
- `EXECABORT` — transaction aborted

### Command Wire Format

Clients send commands as RESP arrays of bulk strings. First element is command name (case-insensitive), rest are arguments.

Example — `SET mykey "hello world" EX 60 NX`:

```
*6\r\n
$3\r\nSET\r\n
$5\r\nmykey\r\n
$11\r\nhello world\r\n
$2\r\nEX\r\n
$2\r\n60\r\n
$2\r\nNX\r\n
```

Example — `HGETALL user:1000` → server response:

```
*4\r\n
$4\r\nname\r\n
$5\r\nAlice\r\n
$3\r\nage\r\n
$2\r\n30\r\n
```

(Flat array of key-value pairs in RESP2.)

### Nested Arrays

Arrays can contain other arrays. Example — `EXEC` returns array of results:

```
*3\r\n
+OK\r\n
:42\r\n
*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```

(Three results: simple string, integer, and a 2-element array.)

## RESP3 Additions

RESP3 adds 8 new types (13 total). Negotiated via `HELLO 3` command.

### New Types

| Type | Prefix | Format | Example |
|------|--------|--------|---------|
| Null | `_` | `_\r\n` | `_\r\n` |
| Boolean | `#` | `#t\r\n` or `#f\r\n` | `#t\r\n` |
| Double | `,` | `,<float>\r\n` | `,3.14\r\n` |
| Big Number | `(` | `(<bignum>\r\n` | `(3492890328409238509324850943850943825024385\r\n` |
| Bulk Error | `!` | `!<len>\r\n<data>\r\n` | `!21\r\nSYNTAX invalid syntax\r\n` |
| Verbatim String | `=` | `=<len>\r\n<enc>:<data>\r\n` | `=15\r\ntxt:Some string\r\n` |
| Map | `%` | `%<count>\r\n<k1><v1>...` | `%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n` |
| Set | `~` | `~<count>\r\n<elements>` | `~3\r\n+a\r\n+b\r\n+c\r\n` |
| Push | `>` | `><count>\r\n<elements>` | `>3\r\n+message\r\n+channel\r\n$5\r\nhello\r\n` |
| Attribute | `\|` | `\|<count>\r\n<k1><v1>...` | Metadata before next reply |

### HELLO Command

```
HELLO 3 [AUTH username password] [SETNAME clientname]
```

Server responds with a map of server properties (version, mode, role, modules). After `HELLO 3`, the connection speaks RESP3.

## RESP2 vs RESP3 Key Differences

| Feature | RESP2 | RESP3 |
|---------|-------|-------|
| Map responses | Flat arrays `[k1, v1, k2, v2]` | Native `%` map type |
| Null | `$-1\r\n` or `*-1\r\n` | Dedicated `_\r\n` |
| Booleans | Integer 0/1 | Native `#t`/`#f` |
| Floating point | Bulk string `"3.14"` | Native `,3.14\r\n` |
| Push messages | Dedicated pub/sub connection | Inline `>` on any connection |
| Client tracking | Not available | Via push data on same connection |

## Inline Commands

For telnet/debugging, Redis also accepts inline commands — plain text without RESP framing:

```
PING\r\n
SET foo bar\r\n
GET foo\r\n
```

Detection: if the first byte is NOT `*`, parse as inline (split on spaces). Arguments with spaces require quoting: `SET foo "hello world"`.

**Implementation note**: Inline support is needed for `redis-cli` compatibility and manual debugging. The parser must check the first byte: `*` → RESP multibulk, otherwise → inline.

## Pipelining

Clients can send multiple commands without waiting for responses. The server processes them in order and sends all responses in order.

```
Client sends:
  *1\r\n$4\r\nPING\r\n
  *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
  *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n

Server responds (in order):
  +PONG\r\n
  +OK\r\n
  $3\r\nbar\r\n
```

**Implementation note**: The parser must accumulate multiple complete commands from a single buffer read. If a buffer contains 2.5 commands, parse the first 2, buffer the remainder, and wait for more data. Response ordering must be strictly maintained.

## Streaming Parser Design

A practical RESP parser must be a streaming (incremental) parser because:
1. TCP delivers data in arbitrary chunks — a command may span multiple `data` events
2. Multiple commands may arrive in a single chunk (pipelining)
3. Bulk strings can be very large (up to 512MB by default)

### Existing Parser Libraries for JS/TS

| Package | Protocol | Architecture | Performance |
|---------|----------|-------------|-------------|
| `redis-parser` (NodeRedis) | RESP2 | Callback-based, C++ optional binding | Battle-tested, high perf |
| `redis-parser-ts` | RESP2 | TypeScript, buffer-safe streaming | Modern, good for new projects |
| `respjs` | RESP2 | EventEmitter-based | Encoding + decoding |
| `resp3` (tinovyatkin) | RESP3 | Pure streaming, ~300 LOC | Minimal, no dependencies |
| `@ioredis/commands` | N/A | Command metadata (flags, arity) | Used by ioredis for validation |

**Recommendation**: Start with `redis-parser` (used by both ioredis and node-redis — proven in production). For RESP serialization (encoding), write our own — it's trivial (<100 lines). For RESP3, add `resp3` package later if needed.

## Protocol Quirks and Edge Cases

### Binary Safety

Bulk strings are fully binary-safe. Key names and values can contain any bytes including `\0`. The RESP parser must never use string operations that assume null-terminated strings.

### Large Payloads

- Default max bulk string: 512MB (`proto-max-bulk-len` config)
- Default max inline command: 1KB
- Arrays can be deeply nested (though Redis commands rarely nest beyond 2 levels)

### Error Responses Across Commands

Commands return errors in specific patterns:
- Wrong number of arguments: `-ERR wrong number of arguments for '<cmd>' command\r\n`
- Wrong type: `-WRONGTYPE Operation against a key holding the wrong kind of value\r\n`
- Syntax errors: `-ERR syntax error\r\n`
- Out of range: `-ERR value is not an integer or out of range\r\n`

Matching these exact error messages is important for 100% compatibility — clients may parse error strings.

### Sub-Commands

Many Redis commands have sub-commands: `CLIENT LIST`, `OBJECT ENCODING`, `CONFIG GET`, `CLUSTER INFO`, etc. These arrive as regular arrays: `["CLIENT", "LIST"]`. The dispatcher must handle two-word commands.

Since Redis 7.0, sub-commands are first-class entities — `COMMAND DOCS` returns metadata for sub-commands separately from parent commands.

## Protocol Decisions

1. **RESP2 for initial implementation** — all JS clients default to RESP2, covers 100% of commands
2. **RESP3 support later** — needed for client tracking, richer types, but not blocking
3. **Both multibulk and inline parsing** — inline needed for redis-cli and testing
4. **Streaming parser is mandatory** — cannot assume one command per buffer
5. **For the engine**: parser runs on the server side (client→engine) to decode commands for execution
6. **Error message matching**: must replicate exact Redis error strings for full compatibility

---

[← Back](README.md)
