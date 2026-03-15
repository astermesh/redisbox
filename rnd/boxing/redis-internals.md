# Redis Internals Relevant to Emulation

Key Redis mechanisms that RedisBox needs to emulate or be aware of for 100% command coverage.

## Data Model

### Databases

Redis supports 16 databases by default (0-15), selected via `SELECT <db>`. Each database is an independent keyspace. In cluster mode, only database 0 is available.

**Implementation**: `Array<Map<string, Entry>>` of length 16. Each client connection tracks its selected database index.

### Key-Value Entry Structure

Every key in Redis has:
- **Value**: The actual data (string, list, hash, set, sorted set, stream)
- **Type**: One of 6 core types
- **Encoding**: Internal representation (see OBJECT ENCODING below)
- **TTL metadata**: Optional expiration timestamp in milliseconds
- **LRU/LFU metadata**: Last access time or frequency counter (for eviction)

### OBJECT ENCODING (internal representations)

Redis uses different internal encodings for the same logical type based on size:

| Type | Small encoding | Large encoding | Threshold |
|------|---------------|----------------|-----------|
| String | `int` (if numeric, ≤ 2^63) | `embstr` (≤ 44 bytes) or `raw` | 44 bytes |
| List | `listpack` (≤ 128 elements, each ≤ 64 bytes) | `quicklist` | configurable |
| Hash | `listpack` (≤ 128 fields, each ≤ 64 bytes) | `hashtable` | configurable |
| Set | `listpack` (≤ 128 elements, each ≤ 64 bytes) or `intset` (all ints) | `hashtable` | configurable |
| Sorted Set | `listpack` (≤ 128 elements, each ≤ 64 bytes) | `skiplist` + `hashtable` | configurable |
| Stream | `stream` (always uses radix tree + listpacks) | — | — |

**Why this matters**: The `OBJECT ENCODING` command must return the correct encoding name. Some applications check encoding for debugging. More importantly, encoding affects performance characteristics — `listpack` operations are O(N), `hashtable` is O(1).

**For 100% coverage**: Must track encoding state and transition thresholds. The `OBJECT ENCODING` command must return correct encoding names, and encoding transitions must happen at the same thresholds as real Redis.

### Type System

6 core types (returned by `TYPE` command):
- `string` — binary-safe string, max 512MB
- `list` — ordered collection, doubly-linked
- `set` — unordered unique strings
- `zset` — sorted set, each element has a score (float64)
- `hash` — field→value map
- `stream` — append-only log with consumer groups

## Key Expiration: Active vs Lazy Deletion

Redis uses a two-pronged approach:

### Lazy (passive) deletion

Every key access calls `expireIfNeeded()`. Check:
1. Does key have a TTL? If not, return.
2. Is current time ≥ expiry time? If not, return.
3. Delete key, propagate DEL to replicas and AOF.
4. Send keyspace notification if enabled.
5. Return as if key doesn't exist.

**Implementation**: Check `entry.expiresAt !== undefined && now >= entry.expiresAt` before any key read. Simple and essential.

### Active (periodic) deletion

Background cycle runs ~10 times/second (`hz` config, default 10):

```
function activeExpireCycle():
  for each database:
    loop:
      sample = random 20 keys from keys-with-TTL set
      expired = delete all expired from sample
      if expired / sample.length <= 0.25:
        break  // less than 25% expired, stop
      // else repeat — too many expired keys
    time limit: don't exceed 25% of hz cycle (2.5ms at hz=10)
```

### TTL-Related Commands

| Command | What it does |
|---------|-------------|
| `EXPIRE key seconds` | Set TTL in seconds |
| `PEXPIRE key ms` | Set TTL in milliseconds |
| `EXPIREAT key timestamp` | Set absolute expiry (Unix seconds) |
| `PEXPIREAT key timestamp` | Set absolute expiry (Unix ms) |
| `TTL key` | Remaining TTL in seconds (-1 = no TTL, -2 = not exists) |
| `PTTL key` | Remaining TTL in milliseconds |
| `PERSIST key` | Remove TTL |
| `GETEX key EX\|PX\|EXAT\|PXAT\|PERSIST` | GET + set/modify/clear TTL |
| `SET key value EX\|PX\|EXAT\|PXAT` | SET with TTL |

Since Redis 7.0: `EXPIRE` has `NX|XX|GT|LT` flags for conditional TTL updates.

Since Redis 7.4: `HEXPIRE`, `HPEXPIRE`, `HTTL`, `HPTTL`, `HPERSIST`, `HEXPIRETIME`, `HPEXPIRETIME` — per-field expiration on hashes.

**Implications**: Lazy deletion is essential and straightforward. Active deletion should integrate with virtual time. The active cycle is a hook point — RedisSim controls sampling rate, threshold, and when the cycle fires.

## Memory Eviction Policies

When `maxmemory` is reached, Redis evicts keys according to configured policy:

| Policy | Scope | Algorithm |
|--------|-------|-----------|
| `noeviction` | N/A | Return `-OOM` error on writes |
| `allkeys-lru` | All keys | Approximated LRU (sampled) |
| `volatile-lru` | Keys with TTL | Approximated LRU |
| `allkeys-lfu` | All keys | Probabilistic frequency counter (Morris counter) |
| `volatile-lfu` | Keys with TTL | Probabilistic frequency counter |
| `allkeys-random` | All keys | Random eviction |
| `volatile-random` | Keys with TTL | Random eviction |
| `volatile-ttl` | Keys with TTL | Evict nearest-to-expire |

### Approximated LRU

Redis does NOT maintain a true LRU linked list. Instead:
1. Each key stores a 24-bit timestamp of last access (clock with ~10ms resolution)
2. On eviction, sample `maxmemory-samples` (default 5) random keys
3. Evict the one with the oldest access timestamp from the sample
4. Repeat until enough memory freed

### LFU (Least Frequently Used)

Uses a Morris counter (logarithmic probabilistic counter) stored in the same 24 bits:
- 16 bits: last decrement time
- 8 bits: logarithmic frequency counter (0-255)

Frequency decays over time (configurable via `lfu-decay-time`).

**Implications**: `noeviction` is the simplest starting policy. Implement approximated LRU/LFU incrementally. RedisSim can inject eviction behavior at the hook level.

## Sorted Sets (Skip List + Hash Table)

Sorted sets are Redis's most complex data structure.

### Skip List

A probabilistic data structure allowing O(log N) search, insertion, and deletion. Redis's skip list:
- Max 32 levels
- Level probability factor: 0.25
- Each node stores: element (string), score (double), backward pointer, level array (forward pointers + span)
- Span tracking enables O(log N) ZRANK (rank by position)

### Dual Index

For sizes above the listpack threshold, sorted sets maintain TWO structures:
1. **Skip list**: ordered by (score, element) — enables range queries, rank lookups
2. **Hash table**: element → score — enables O(1) ZSCORE lookups

Both must be kept in sync on every mutation.

### Score Comparison

Scores are IEEE 754 doubles. When scores are equal, elements are compared lexicographically (byte comparison). This enables `ZRANGEBYLEX` for equal-score range queries.

Special score values: `+inf`, `-inf` are supported in range commands.

**For JS emulation**: Use a sorted array or B-tree. The `tlhunter-sorted-set` npm package implements a skip list with Redis-like API. For simpler approach, `sorted-btree` provides O(log N) operations with TypeScript support.

## Streams

Streams are append-only log structures with consumer group support. Introduced in Redis 5.0.

### Entry IDs

Format: `<millisecondsTimestamp>-<sequenceNumber>` (e.g., `1526919030474-0`). Auto-generated if `*` is passed to XADD. IDs must be strictly increasing within a stream.

### Consumer Groups

Allow multiple consumers to cooperatively read from a stream:
- `XGROUP CREATE stream group id` — create consumer group
- `XREADGROUP GROUP group consumer COUNT n STREAMS stream >` — read new messages
- `XACK stream group id` — acknowledge message processing
- `XPENDING stream group` — check pending (unacknowledged) messages
- `XCLAIM` / `XAUTOCLAIM` — transfer ownership of pending messages

**For JS emulation**: Streams require careful implementation of:
- ID generation with auto-increment and deduplication
- Consumer group state (pending entries list per consumer)
- Blocking reads (XREAD BLOCK)
- Message acknowledgment and claiming

## Pub/Sub

### Data Structures

- **Server-wide**: `Map<channel, List<Client>>` — subscribers per channel
- **Server-wide**: `Map<pattern, List<Client>>` — pattern subscribers
- **Per-client**: `Set<channel>` and `Set<pattern>` — for cleanup on disconnect

### Key Behaviors

1. **At-most-once delivery**: Messages are not persisted or queued. If a subscriber is slow or disconnected, messages are lost.
2. **Subscriber mode**: A client in subscribed state can only execute SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, RESET, QUIT. All other commands return an error.
3. **Pattern matching**: `PSUBSCRIBE` uses glob patterns (`*`, `?`, `[chars]`). A message to channel `foo.bar` matches pattern `foo.*`.
4. **Duplicate delivery**: If client is subscribed to both `foo.*` pattern and `foo.bar` channel, it receives the message TWICE.
5. **Sharded pub/sub** (Redis 7.0+): `SSUBSCRIBE` / `SPUBLISH` — messages routed only to the shard owning the channel's hash slot.

## Transactions (MULTI/EXEC)

### Command Queue

1. `MULTI` — enter transaction mode. All subsequent commands are queued (server responds with `+QUEUED`).
2. `EXEC` — execute all queued commands atomically. Returns array of results.
3. `DISCARD` — discard queue, exit transaction mode.

### Error Handling

Two types of errors in transactions:
1. **Command syntax errors** (before EXEC): Command is rejected at queue time, entire transaction is automatically aborted on EXEC.
2. **Runtime errors** (during EXEC): Only the failing command returns an error, all other commands still execute. **No rollback.**

### WATCH / Optimistic Locking

- `WATCH key [key ...]` — monitor keys for changes
- If ANY watched key is modified by another client between WATCH and EXEC → EXEC returns null array (transaction aborted)
- `UNWATCH` — clear all watches
- WATCH is per-connection state

**Implementation**: Track version numbers per key. On WATCH, record current versions. On EXEC, compare. If any version changed, abort.

## Lua Scripting (EVAL/EVALSHA)

### Execution Model

- Redis embeds a Lua 5.1 interpreter (single instance, shared across all clients)
- Scripts run atomically — no other client commands execute during a script
- Scripts have access to Redis via `redis.call()` and `redis.pcall()`
- Arguments passed via global tables: `KEYS` (key arguments) and `ARGV` (non-key arguments)

### Caching

- Scripts are cached by SHA1 hash of their source code
- `SCRIPT LOAD script` — cache script, return SHA1
- `EVALSHA sha1 numkeys key1 ...` — execute cached script
- `SCRIPT EXISTS sha1 [sha1 ...]` — check if scripts are cached
- `SCRIPT FLUSH` — clear script cache

### Redis Functions (Redis 7.0+)

Replacement for EVAL with named, persistent functions:
- `FUNCTION LOAD library_code` — load a function library
- `FCALL function_name numkeys key1 ...` — call a function
- Functions persist across restarts (unlike EVAL scripts)

**For JS emulation**: Full Lua support requires embedding a Lua interpreter:
- **fengari** — Lua 5.3 VM in pure JavaScript (no binary deps)
- **wasmoon** — Lua 5.4 compiled to WASM (faster, but WASM dependency)

See [Lua research](../../rnd/engine/lua.md) for VM options and integration approach.

## Blocking Commands

Several commands can block the client connection waiting for data:

| Command | Blocks until |
|---------|-------------|
| `BLPOP key [key ...] timeout` | Element available in any listed list |
| `BRPOP key [key ...] timeout` | Element available in any listed list |
| `BLMOVE src dst LEFT\|RIGHT LEFT\|RIGHT timeout` | Element available in source |
| `BZPOPMIN key [key ...] timeout` | Element available in sorted set |
| `BZPOPMAX key [key ...] timeout` | Element available in sorted set |
| `XREAD BLOCK ms STREAMS key [key ...] id [id ...]` | New stream entries |
| `XREADGROUP ... BLOCK ms ...` | New stream entries for consumer group |

### Blocking Implementation

1. Client sends blocking command
2. If data available immediately → return result (non-blocking path)
3. If no data → add client to per-key blocking list, set timeout
4. When another client pushes to the key → wake up first blocked client, deliver data
5. On timeout → return nil/empty result

**For JS emulation**: Blocking commands require per-key waiting lists, cross-client notification, timeout handling with virtual time, and fairness (FIFO wakeup order).

## Keyspace Notifications

Events published to special pub/sub channels on key modification:

- `__keyspace@<db>__:<key>` — "what happened to key X" (event name as message)
- `__keyevent@<db>__:<event>` — "which keys had event X" (key name as message)

Controlled by `notify-keyspace-events` config string. Piggybacks on pub/sub.

## CONFIG System

Redis has ~200 configuration parameters. Key ones for emulation:

| Config | Default | Purpose |
|--------|---------|---------|
| `maxmemory` | 0 (unlimited) | Memory limit for eviction |
| `maxmemory-policy` | `noeviction` | Eviction algorithm |
| `hz` | 10 | Background task frequency |
| `list-max-listpack-entries` | 128 | List encoding threshold |
| `hash-max-listpack-entries` | 128 | Hash encoding threshold |
| `set-max-listpack-entries` | 128 | Set encoding threshold |
| `zset-max-listpack-entries` | 128 | Sorted set encoding threshold |
| `notify-keyspace-events` | `""` | Keyspace notifications |

## Persistence (RDB/AOF)

- **RDB**: Point-in-time binary snapshots via `BGSAVE`
- **AOF**: Append-only log of write commands

**Implications**: Neither is needed for ephemeral emulation. But commands like `BGSAVE`, `LASTSAVE`, `DBSIZE` must return reasonable responses.

## ACL System (Redis 6.0+)

Access Control Lists for user authentication and authorization:
- `AUTH username password` — authenticate
- `ACL SETUSER username ...` — create/modify users
- `ACL LIST` — show all users and rules

**Implications**: For testing environments, ACL emulation is usually unnecessary. But the commands themselves must exist for 100% coverage.

---

[← Back](README.md)
