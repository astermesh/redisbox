# Redis Engine Architecture for JS/TS Implementation

Comprehensive subsystem decomposition for building a Redis engine with exact behavioral parity in JavaScript/TypeScript.

## Table of Contents

1. [Engine Subsystems Decomposition](#1-engine-subsystems-decomposition)
2. [Dependencies Between Subsystems](#2-dependencies-between-subsystems)
3. [Data Structure Choices for JS](#3-data-structure-choices-for-js)
4. [Interface Contracts Between Subsystems](#4-interface-contracts-between-subsystems)

---

## 1. Engine Subsystems Decomposition

### 1.1 Key Store (Keyspace Manager)

The keyspace is the core data layer. In Redis, each database is an independent namespace of keys. Redis defaults to 16 databases (0-15), selected per-connection via `SELECT`.

**Responsibilities:**

- Store and retrieve key-value entries across multiple databases
- Track metadata per entry: type, encoding, TTL, LRU/LFU access info
- Provide key lookup with lazy expiration (check-on-access)
- Support key-level operations: `DEL`, `EXISTS`, `RENAME`, `RENAMENX`, `TYPE`, `OBJECT`, `RANDOMKEY`, `KEYS`, `SCAN`, `TOUCH`, `UNLINK`, `DUMP`, `RESTORE`, `COPY`, `SORT`, `SORT_RO`
- Maintain a secondary index of keys-with-TTL for active expiration
- Notify the expiration manager and keyspace notification system on mutations
- Track key version counters for WATCH/optimistic locking

**Entry structure:**

```typescript
interface RedisEntry {
  type: 'string' | 'list' | 'set' | 'zset' | 'hash' | 'stream'
  encoding: string       // e.g. 'raw', 'int', 'embstr', 'listpack', 'skiplist', etc.
  value: unknown         // type-specific data structure
  lruClock: number       // 24-bit LRU timestamp or LFU counter
  version: number        // monotonic counter, incremented on every mutation
}

interface Database {
  store: Map<string, RedisEntry>
  expires: Map<string, number>  // key -> absolute expiry timestamp (ms)
}
```

**Key behavioral details:**

- `RENAME` atomically deletes target (if exists), moves source to target. It preserves TTL of the source key. If source has no TTL and target had one, the target's TTL is removed. If source equals target, it returns OK (no-op for same key).
- `UNLINK` is async `DEL` in real Redis (frees memory in background thread). In the JS engine, it can behave identically to `DEL` since there is no background thread, but the command must exist and return the same values.
- `COPY` (Redis 6.2+) supports `DESTINATION db` and `REPLACE` flag.
- `OBJECT ENCODING` must return the correct encoding string matching Redis's internal representation decisions (e.g., `int`, `embstr`, `raw` for strings; `listpack` vs `hashtable` for hashes, etc.).
- `OBJECT HELP`, `OBJECT FREQ`, `OBJECT IDLETIME`, `OBJECT REFCOUNT` must all behave correctly.
- `SORT` and `SORT_RO` are complex: they work on lists, sets, and sorted sets, support `BY`, `GET`, `LIMIT`, `ASC/DESC`, `ALPHA`, `STORE`. The `STORE` variant writes results into a new list key.
- `SCAN` uses a cursor-based iteration with a reverse-bit-increment algorithm to guarantee full iteration even during rehashing. For JS, a simpler approach is acceptable since `Map` does not rehash like Redis's dict, but the cursor encoding must be compatible (integer cursor, 0 means complete).

### 1.2 Type Engines

Each Redis data type has its own set of commands and internal logic. These are best organized as separate modules.

#### 1.2.1 Strings

The simplest type but with the largest command surface due to versatility.

**Commands (~25):** `SET`, `GET`, `GETEX`, `GETDEL`, `APPEND`, `STRLEN`, `SETRANGE`, `GETRANGE`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `DECR`, `DECRBY`, `MGET`, `MSET`, `MSETNX`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `LCS`, `SUBSTR`

**Encoding transitions:**

- Numeric values fitting in a 64-bit signed integer: encoding `int`
- Strings up to 44 bytes: encoding `embstr`
- Strings over 44 bytes: encoding `raw`
- After mutation of an `embstr` (e.g., `APPEND`), it becomes `raw` (embstr is read-only in Redis)

**Behavioral details:**

- `SET` has numerous flags: `EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL`, `GET` (Redis 6.2+). The `GET` flag makes SET return the old value (like GETSET).
- `INCR`/`DECR` family: value must be representable as 64-bit signed integer, otherwise error. Overflow returns error, not wrap-around.
- `INCRBYFLOAT`: uses `long double` internally in Redis. Result is always stored as a string (encoding changes from `int` to `embstr`/`raw`). Trailing zeroes are removed but a `.` is kept if needed (e.g., `10.50` not `10.5` — actually Redis stores minimal representation).
- `LCS` (Longest Common Substring, Redis 7.0+): operates on two string keys with options `LEN`, `IDX`, `MINMATCHLEN`, `WITHMATCHLEN`.
- All string values are binary-safe (can contain null bytes). Max size 512 MB.
- `MSET` is atomic — all keys are set simultaneously. `MSETNX` is atomic — either all keys are set or none (returns 0 if any key exists).

#### 1.2.2 Lists

Ordered sequences of strings supporting push/pop from both ends.

**Commands (~22):** `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `LMPOP`, `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BLPOP` (blocking variants are handled by the Blocking Command Manager)

**Encoding transitions:**

- Small lists (<=128 entries, each <=64 bytes): `listpack`
- Large lists: `quicklist` (linked list of listpacks/ziplists)
- Thresholds configurable via `list-max-listpack-size` and `list-compress-depth`

**Behavioral details:**

- `LPOP`/`RPOP` accept a count argument (Redis 6.2+): `LPOP key count` returns up to `count` elements.
- `LPOS` (Redis 6.0.6+): returns positions of matching elements, supports `RANK`, `COUNT`, `MAXLEN` options.
- `LMPOP` (Redis 7.0+): pop from first non-empty list among multiple keys, with `LEFT|RIGHT` direction and `COUNT`.
- When a list becomes empty after a pop, the key is automatically deleted.

#### 1.2.3 Sets

Unordered collections of unique strings.

**Commands (~17):** `SADD`, `SREM`, `SISMEMBER`, `SMISMEMBER`, `SMEMBERS`, `SCARD`, `SRANDMEMBER`, `SPOP`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE`, `SINTERCARD`, `SMOVE`, `SSCAN`

**Encoding transitions:**

- All-integer sets with <=128 members: `intset`
- Small sets (<=128 members, each <=64 bytes): `listpack`
- Large or mixed sets: `hashtable`

**Behavioral details:**

- `SRANDMEMBER count`: positive count = unique elements (up to set size), negative count = may include duplicates, absolute value is count.
- `SPOP count` (Redis 3.2+): removes and returns `count` random members.
- `SINTERCARD` (Redis 7.0+): returns cardinality of intersection with optional `LIMIT`.
- `SMISMEMBER` (Redis 6.2+): batch version of SISMEMBER.

#### 1.2.4 Sorted Sets

The most complex data type — each element has an associated score (float64).

**Commands (~46):** `ZADD`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZINCRBY`, `ZCARD`, `ZCOUNT`, `ZLEXCOUNT`, `ZRANGE`, `ZRANGEBYLEX`, `ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYLEX`, `ZREVRANGEBYSCORE`, `ZRANGESTORE`, `ZRANK`, `ZREVRANK`, `ZPOPMIN`, `ZPOPMAX`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `ZMPOP`, `ZRANDMEMBER`, `ZUNION`, `ZINTER`, `ZDIFF`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`, `ZINTERCARD`, `ZSCAN`, `ZRANGEBYLEX`, `ZRANGEBYSCORE` (many variants are now unified under `ZRANGE` in Redis 6.2+)

**Encoding transitions:**

- Small sorted sets (<=128 elements, each <=64 bytes): `listpack`
- Large sorted sets: `skiplist` + `hashtable` (dual index)

**Behavioral details:**

- Scores are IEEE 754 doubles. `+inf` and `-inf` are valid scores.
- When scores are equal, elements are sorted lexicographically by their string value (byte comparison).
- `ZADD` flags: `NX` (only add new), `XX` (only update existing), `GT` (only update if new score > current), `LT` (only update if new score < current), `CH` (return count of changed elements, not just added).
- `ZRANGE` (Redis 6.2+ unified): replaces `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGE*` with `BYSCORE`, `BYLEX`, `REV`, `LIMIT` options.
- `ZRANGESTORE` (Redis 6.2+): stores range result into destination key.
- `ZRANDMEMBER` (Redis 6.2+): like `SRANDMEMBER` but for sorted sets, supports `WITHSCORES`.
- `ZMPOP` (Redis 7.0+): pop min/max from first non-empty sorted set among multiple keys.

#### 1.2.5 Hashes

Field-value maps.

**Commands (~28):** `HSET`, `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HSETNX`, `HRANDFIELD`, `HSCAN`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`, `HEXPIRETIME`, `HPEXPIRETIME`

**Encoding transitions:**

- Small hashes (<=128 fields, each field and value <=64 bytes): `listpack`
- Large hashes: `hashtable`

**Behavioral details:**

- `HSET` (Redis 4.0+): variadic — `HSET key f1 v1 f2 v2 ...` replaces old `HMSET`.
- `HRANDFIELD` (Redis 6.2+): like `SRANDMEMBER` for hashes, supports `WITHVALUES`.
- Per-field expiration (Redis 7.4+): `HEXPIRE`, `HPEXPIRE`, `HTTL`, `HPTTL`, `HPERSIST`, `HEXPIRETIME`, `HPEXPIRETIME`. These set TTL on individual hash fields, not the whole key. This is a significant implementation requirement — the expiry index needs a two-level structure: `(key, field) -> timestamp`.

#### 1.2.6 Streams

Append-only log structures with consumer group support. The most complex data type after sorted sets.

**Commands (~27):** `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XTRIM`, `XDEL`, `XINFO` (STREAM, GROUPS, CONSUMERS, HELP), `XGROUP` (CREATE, SETID, DELCONSUMER, DESTROY, CREATECONSUMER), `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`

**Internal structure:**

- Entry IDs: `<ms-timestamp>-<sequence>`. Auto-generated with `*`, partial auto with `<ms>-*`. IDs must be strictly increasing.
- Stream body: radix tree (rax) keyed by entry ID, values are listpack-encoded field-value pairs.
- Consumer groups: each has a last-delivered-ID, a pending entries list (PEL) per consumer, and per-consumer metadata (name, idle time, pending count).

**Behavioral details:**

- `XADD` supports `MAXLEN` and `MINID` trimming options with optional `~` (approximate) prefix.
- `XADD` with `NOMKSTREAM` (Redis 6.2+): do not create stream if it does not exist.
- `XREAD BLOCK 0 STREAMS key $` blocks until new data. The `$` means "last ID in the stream at read time".
- `XREADGROUP GROUP g c STREAMS key >` reads new messages not yet delivered to this consumer. Using a specific ID reads from the consumer's PEL.
- `XPENDING` has both summary and detail forms. Detail form supports `IDLE min-idle-time` filter (Redis 6.2+).
- `XAUTOCLAIM` (Redis 6.2+): combines `XPENDING` + `XCLAIM` for automatic dead-letter processing.
- `XINFO STREAM key FULL` (Redis 6.0+): returns complete stream state including all PELs.
- Stream entries have a `deleted_entry_count` that XINFO reports. `XDEL` does not physically remove from the rax node; it marks entries as deleted within the listpack.

#### 1.2.7 HyperLogLog

Probabilistic cardinality estimation.

**Commands (5):** `PFADD`, `PFCOUNT`, `PFMERGE`, `PFDEBUG`, `PFSELFTEST`

**Internal representation:**

- Uses `string` type internally with a special encoding. The `TYPE` command returns `string` for HyperLogLog keys.
- Two representations: sparse (for small cardinalities, very compact) and dense (16384 registers of 6 bits = 12 KB).
- Sparse representation uses a run-length encoding scheme.

**Behavioral details:**

- `PFCOUNT` on multiple keys creates a temporary merged HLL and returns its count — it does NOT modify the source keys. But `PFCOUNT` on a single key caches the result in the key itself (the key is modified on read!). This is an important behavioral quirk.
- `PFMERGE` creates/overwrites the destination key.
- `PFDEBUG` and `PFSELFTEST` are debug commands that must exist.

#### 1.2.8 Bitmaps

Not a separate type — operations on string values treated as bit arrays.

**Commands (6):** `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD`

**Behavioral details:**

- Bitmap operations work on regular string values. The string is auto-extended with zero bytes when SETBIT sets a bit beyond the current length.
- `BITOP` supports `AND`, `OR`, `XOR`, `NOT` operations across multiple keys, storing result in destination.
- `BITFIELD` is the most complex: supports `GET`, `SET`, `INCRBY` sub-commands with type specifiers (`u8`, `i16`, etc.) and overflow handling (`WRAP`, `SAT`, `FAIL`).
- `BITCOUNT` and `BITPOS` support byte range with `BYTE|BIT` unit flag (Redis 7.0+).

#### 1.2.9 Geospatial

Implemented on top of sorted sets using geohash encoding.

**Commands (~10):** `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEOHASH`, `GEORADIUS`, `GEORADIUSBYMEMBER`, `GEORADIUS_RO`, `GEORADIUSBYMEMBER_RO`

**Implementation:**

- Members are stored in a sorted set with their geohash as the score (52-bit integer encoded as a double).
- `GEOSEARCH` (Redis 6.2+) replaces `GEORADIUS`/`GEORADIUSBYMEMBER` with unified interface: `FROMMEMBER|FROMLONLAT`, `BYRADIUS|BYBOX`, `ASC|DESC`, `COUNT`, `WITHCOORD`, `WITHDIST`, `WITHHASH`.
- Longitude must be -180 to 180, latitude -85.05112878 to 85.05112878 (Mercator projection limit).
- Distance units: `m`, `km`, `mi`, `ft`.

### 1.3 Expiration Manager

Handles both lazy (passive) and active (periodic) key expiration.

**Lazy expiration:**

Every key access calls an expiration check. If the key has a TTL and the current time exceeds it, the key is deleted before the access returns. This is mandatory — no key access can skip it.

```
expireIfNeeded(db, key, now):
  expiresAt = db.expires.get(key)
  if expiresAt === undefined: return false
  if now < expiresAt: return false
  db.store.delete(key)
  db.expires.delete(key)
  // bump key version for WATCH
  // emit keyspace notification if enabled
  // emit "expired" key event
  return true
```

**Active expiration (periodic):**

Runs on a timer (configurable via `hz`, default 10 times/second). Two modes:

1. **Slow cycle** — called from the main timer (`serverCron` equivalent), runs with a time budget of ~25ms at hz=10:
   - For each database with keys-with-TTL:
     - Sample 20 random keys from the expires index
     - Delete all expired keys from sample
     - If >25% of sampled keys were expired, repeat (more likely expired keys remain)
     - Stop if time budget exhausted

2. **Fast cycle** — called before processing events (equivalent to `beforeSleep`), runs at most 1ms:
   - Same algorithm, smaller time budget
   - Only runs if the slow cycle found >25% expired keys on last run

**Hash field expiration (Redis 7.4+):**

Per-field TTL on hashes requires a separate expiration index: `Map<string, Map<string, number>>` mapping `key -> (field -> expiresAt)`. The same lazy+active strategy applies but at the field level.

### 1.4 Memory Eviction

When `maxmemory` is configured and usage exceeds the limit, the eviction system kicks in before executing write commands.

**Policies:**

| Policy | Key Pool | Algorithm |
|--------|----------|-----------|
| `noeviction` | N/A | Reject writes with `-OOM` error |
| `allkeys-lru` | All keys | Approximated LRU (sampled) |
| `volatile-lru` | Keys with TTL | Approximated LRU |
| `allkeys-lfu` | All keys | Approximated LFU (Morris counter) |
| `volatile-lfu` | Keys with TTL | Approximated LFU |
| `allkeys-random` | All keys | Random eviction |
| `volatile-random` | Keys with TTL | Random eviction |
| `volatile-ttl` | Keys with TTL | Nearest-to-expire first |

**Approximated LRU implementation:**

Redis does not use a true LRU linked list. Instead:

1. Each key stores a 24-bit timestamp of last access (~10ms resolution clock).
2. On eviction, sample `maxmemory-samples` (default 5) random keys from the appropriate pool.
3. Insert sampled keys into an **eviction pool** (sorted array of 16 candidates, ordered by idle time).
4. Evict the key with the highest idle time from the pool.
5. Repeat until enough memory is freed.

The eviction pool persists across eviction cycles, accumulating good candidates over time. This makes the approximation very close to true LRU.

**Approximated LFU implementation:**

Uses the same 24-bit metadata field, split differently:

- 16 bits: last decrement time (minutes granularity)
- 8 bits: logarithmic frequency counter (0-255, Morris counter)

On access, the counter is probabilistically incremented (higher counter = lower probability of increment). Over time, the counter decays based on elapsed minutes since last decrement (configurable via `lfu-decay-time`, default 1 minute).

**JS implementation notes:**

- Memory measurement in JS is imprecise. Options: rough estimation based on data structure sizes, or use `process.memoryUsage()` in Node.js (not available in browser).
- For the browser, a key-count or value-size-sum threshold may be more practical than bytes.
- The eviction pool is a simple sorted array of 16 entries — trivial to implement.

### 1.5 Pub/Sub System

Fire-and-forget message broadcasting.

**Commands (~12):** `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB` (CHANNELS, NUMSUB, NUMPAT, SHARDCHANNELS, SHARDNUMSUB), `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH`

**Data structures:**

```typescript
// Server-wide
channelSubscribers: Map<string, Set<ClientId>>      // channel -> subscribers
patternSubscribers: Map<string, Set<ClientId>>       // pattern -> subscribers

// Per-client
subscribedChannels: Set<string>
subscribedPatterns: Set<string>
```

**Behavioral details:**

- A client in subscribed state can ONLY execute: `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PING`, `RESET`, `QUIT`. All other commands return `-ERR Can't execute '...': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`.
- Pattern matching uses glob-style: `*` (any string), `?` (any char), `[abc]` (char class), `[^abc]` (negated class), `\` (escape).
- If a client subscribes to both a channel and a matching pattern, `PUBLISH` delivers the message TWICE to that client — once for the channel subscription and once for the pattern.
- `PUBLISH` returns the number of clients that received the message (including pattern matches).
- Sharded pub/sub (Redis 7.0+): `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH` — same semantics but scoped to a shard (hash slot). In a non-cluster setup, sharded pub/sub behaves identically to regular pub/sub.

### 1.6 Transaction Manager

Implements `MULTI`/`EXEC`/`DISCARD`/`WATCH` with optimistic locking.

**Commands (4+):** `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

**Per-client state:**

```typescript
interface TransactionState {
  inMulti: boolean
  queue: Array<{ command: string; args: string[] }>
  watchedKeys: Map<string, number>   // key -> version at WATCH time
  abortOnError: boolean              // set if command syntax error during queue
}
```

**Command processing in MULTI mode:**

1. `MULTI` — set `inMulti = true`. If already in MULTI, return error.
2. Any command except `EXEC`, `DISCARD`, `WATCH`, `MULTI` — validate syntax (arity, command exists). If invalid, mark transaction for abort. If valid, enqueue and respond `+QUEUED`.
3. `EXEC` — check `abortOnError` (abort if syntax errors were queued). Check all watched key versions against current versions. If any changed, return null array (abort). Otherwise execute all queued commands atomically and return array of results.
4. `DISCARD` — clear queue and watched keys, set `inMulti = false`.
5. `WATCH` — only valid outside MULTI. Record current version of each key.

**Error handling:**

- Command syntax errors during queueing (wrong arity, unknown command) → entire transaction is marked for abort, `EXEC` returns `-EXECABORT Transaction discarded because of previous errors.`
- Runtime errors during `EXEC` (e.g., `WRONGTYPE`) → only that command returns error, all other commands execute normally. **No rollback.**

**WATCH implementation:**

Every key mutation increments the key's version counter. `WATCH` records the version at watch time. `EXEC` compares. If any watched key's version changed (even by the same client in a different context), the transaction aborts.

Important: `WATCH` detects modifications by ANY source — other clients, Lua scripts, expiration, eviction. Key expiration and eviction must also increment the version counter.

### 1.7 Blocking Command Manager

Manages clients that are blocked waiting for data.

**Blocking commands:** `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `XREAD BLOCK`, `XREADGROUP BLOCK`, `WAIT`, `WAITAOF`

**Data structures:**

```typescript
// Per-key blocking queue — which clients are waiting for which keys
blockingIndex: Map<string, Array<BlockedClient>>

interface BlockedClient {
  clientId: string
  keys: string[]            // all keys this client is blocked on
  command: string           // the blocking command
  direction: string         // LEFT/RIGHT for list ops, MIN/MAX for zset ops
  timeout: number           // absolute timestamp when block expires (0 = infinite)
  resolve: (value) => void  // callback to unblock with result
}
```

**Unblocking flow (how Redis does it internally):**

1. A write command (e.g., `LPUSH`) mutates a key.
2. After mutation, the engine calls `signalKeyAsReady(db, key)` — this marks the key as "ready" in a set of ready keys.
3. In the `beforeSleep` phase (after all commands in the current event loop tick), Redis iterates the ready keys.
4. For each ready key, check the blocking index. Process blocked clients in FIFO order.
5. For each blocked client, re-evaluate the blocking condition (e.g., is the list non-empty?). If data is available, pop the element, send the result to the client, and remove from blocking index.
6. If the data was consumed (e.g., list became empty again), stop processing further blocked clients for that key.

**Why re-evaluation matters:** Between the `LPUSH` and `beforeSleep`, other commands may have consumed the data. Also, for `BLPOP key1 key2 key3`, the client is served from the first non-empty key in its key list, not necessarily the key that triggered the signal.

**Timeout handling:** A periodic check or timer-based approach removes clients whose timeout has elapsed, returning a nil response.

**Interaction with MULTI/EXEC:** Blocking commands inside a transaction behave as their non-blocking variants (e.g., `BLPOP` inside `MULTI` acts like `LPOP`). They never actually block.

### 1.8 Keyspace Notifications

Pub/Sub events emitted on key mutations, controlled by configuration.

**Configuration:** `notify-keyspace-events` — a string of flag characters:

| Flag | Meaning |
|------|---------|
| `K` | Enable keyspace events (`__keyspace@<db>__:<key>`) |
| `E` | Enable keyevent events (`__keyevent@<db>__:<event>`) |
| `g` | Generic commands: DEL, EXPIRE, RENAME, ... |
| `$` | String commands |
| `l` | List commands |
| `s` | Set commands |
| `h` | Hash commands |
| `z` | Sorted set commands |
| `x` | Expired events |
| `e` | Evicted events |
| `m` | Key miss events |
| `t` | Stream commands |
| `d` | Module key type events |
| `A` | Alias for `g$lshzxet` (all events) |

**Implementation:**

After every key mutation, call a notification function:

```
notifyKeyspaceEvent(type, event, key, dbid):
  if notifications not enabled for this type: return
  if K flag set:
    publish to `__keyspace@{dbid}__:{key}` message=event
  if E flag set:
    publish to `__keyevent@{dbid}__:{event}` message=key
```

This piggybacks on the Pub/Sub system — keyspace notification channels are regular Pub/Sub channels. Clients subscribe via normal `SUBSCRIBE`.

**Event names:** `set`, `del`, `expire`, `rename_from`, `rename_to`, `lpush`, `rpush`, `hset`, `sadd`, `zadd`, `xadd`, `expired`, `evicted`, etc. Each command has a specific event name.

### 1.9 Lua Scripting Engine

**Commands (~12):** `EVAL`, `EVALSHA`, `EVALSHA_RO`, `EVAL_RO`, `SCRIPT` (LOAD, EXISTS, FLUSH, DEBUG), `FUNCTION` (LOAD, DELETE, DUMP, RESTORE, LIST, STATS, FLUSH), `FCALL`, `FCALL_RO`

**Execution model:**

- Scripts run atomically — the entire server blocks during script execution (no other client commands interleave).
- Scripts access Redis via `redis.call(cmd, ...)` and `redis.pcall(cmd, ...)`. The difference: `call` propagates errors, `pcall` catches them and returns error objects.
- Arguments are passed via `KEYS` and `ARGV` global tables (1-indexed Lua tables).
- Scripts must declare all keys they access via the `KEYS` table (for cluster compatibility).
- Return value type mapping: Lua integer → Redis integer, Lua string → Redis bulk string, Lua table → Redis array, Lua boolean false → Redis nil, Lua boolean true → Redis integer 1.

**Caching:**

- Scripts are cached by SHA1 hash.
- `EVALSHA` executes a cached script. Returns `NOSCRIPT` if not cached.
- Script cache is per-server, not per-database.

**Functions (Redis 7.0+):**

- Named, persistent functions organized in libraries.
- `FUNCTION LOAD` takes Lua code that registers functions via `redis.register_function()`.
- Functions survive restart (persisted in RDB/AOF).
- `FCALL_RO` and `EVAL_RO` variants are read-only — they reject write commands inside the script.

**JS implementation interface:**

The scripting engine is a separate subsystem with a clear interface. It needs:
- A Lua VM (fengari for pure JS, wasmoon for WASM-based)
- Bindings for `redis.call`/`redis.pcall` that route to the command dispatcher
- Script cache (`Map<sha1, compiledScript>`)
- Function library storage
- Atomicity guarantee (no interleaving during script execution — straightforward in single-threaded JS)

### 1.10 Command Dispatcher

The central routing layer between client input and execution.

**Responsibilities:**

1. Parse incoming command (command name + arguments)
2. Look up command in the command table
3. Validate arity (argument count)
4. Check client state (is in MULTI? is in subscribe mode? is authenticated?)
5. Check ACL permissions
6. Check OOM condition (if `maxmemory` set and command is a write)
7. If in MULTI: enqueue (unless it is EXEC/DISCARD/WATCH/MULTI)
8. Execute command handler
9. Post-execution: update stats, update LRU clock, signal ready keys, emit notifications, propagate to replicas/AOF

**Command table structure:**

```typescript
interface CommandDefinition {
  name: string
  handler: (client: ClientState, args: Buffer[]) => RedisReply
  arity: number              // positive = exact, negative = minimum
  flags: Set<CommandFlag>    // 'write', 'readonly', 'denyoom', 'fast', 'loading', etc.
  firstKey: number           // 1-based index of first key arg (0 = no keys)
  lastKey: number            // 1-based index of last key arg (-1 = last arg)
  keyStep: number            // step between key positions
  categories: Set<string>    // ACL categories: @read, @write, @string, @hash, etc.
  subcommands?: Map<string, CommandDefinition>  // for COMMAND INFO, CLIENT, CONFIG, etc.
}
```

**Redis 7.0+ command structure:** Commands with subcommands (like `CLIENT INFO`, `CONFIG SET`, `OBJECT ENCODING`) are represented as command + subcommand pairs. The `COMMAND` introspection commands must reflect this hierarchy.

**Special dispatch cases:**

- `MULTI` mode: all commands except EXEC/DISCARD/WATCH/MULTI are queued
- Subscribe mode: only SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PING/QUIT/RESET are allowed
- Loading mode: only commands with the `loading` flag are allowed (e.g., `INFO`, `SUBSCRIBE`)
- `MONITOR`: client enters monitor mode, receives a copy of all commands processed by the server

### 1.11 ACL / Auth

**Commands (~11):** `AUTH`, `ACL` (SETUSER, DELUSER, GETUSER, LIST, WHOAMI, CAT, LOG, LOAD, SAVE, GENPASS, DRYRUN)

**Default behavior:** With no ACL configuration, a single `default` user exists with full permissions and no password (or a password set via `requirepass`).

**For emulator parity:**

- `AUTH` must work (accept password, return OK or error)
- `ACL CAT` must return the list of command categories
- `ACL WHOAMI` must return current username
- Full ACL rule parsing is lower priority but the commands must exist and return reasonable responses

### 1.12 Persistence Interface

The engine is in-memory, but persistence commands must exist and respond correctly.

**Commands:** `BGSAVE`, `BGREWRITEAOF`, `SAVE`, `LASTSAVE`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SWAPDB`, `DEBUG`

**Behavioral requirements:**

- `BGSAVE` → respond `+Background saving started` (no actual save needed)
- `LASTSAVE` → return a Unix timestamp (can be server start time)
- `DBSIZE` → return actual key count in current database
- `FLUSHDB [ASYNC|SYNC]` → delete all keys in current database. Must also: clear expiry index, unblock any blocked clients on keys in this db, fire keyspace notifications, invalidate WATCH.
- `FLUSHALL [ASYNC|SYNC]` → flush all databases.
- `SWAPDB db1 db2` → atomically swap two databases. Must update all per-db state: blocked clients, pub/sub, watched keys.

### 1.13 Cluster/Replication Stubs

Commands must exist even though the engine is not a real cluster node.

**Replication commands:** `REPLICAOF`/`SLAVEOF`, `REPLCONF`, `PSYNC`, `WAIT`, `WAITAOF`

**Cluster commands (~32):** `CLUSTER` (INFO, NODES, SLOTS, SHARDS, MYID, MEET, RESET, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, etc.)

**Minimal behavior:**

- `CLUSTER INFO` → return info string with `cluster_enabled:0`
- `CLUSTER MYID` → return a node ID (can be random hex string, but must be consistent)
- `CLUSTER KEYSLOT key` → return correct CRC16 hash slot (0-16383). This is a pure function — must be implemented correctly even in non-cluster mode.
- `REPLICAOF NO ONE` → return OK
- `WAIT` → return number of replicas (0 in emulator)

### 1.14 Config System

**Commands:** `CONFIG GET`, `CONFIG SET`, `CONFIG RESETSTAT`, `CONFIG REWRITE`

**Implementation:**

```typescript
class ConfigStore {
  private values: Map<string, string>
  private defaults: Map<string, string>
  private validators: Map<string, (value: string) => boolean>

  get(pattern: string): Array<[string, string]>  // supports glob patterns
  set(key: string, value: string): void
  resetStat(): void
}
```

**Behavioral requirements:**

- `CONFIG GET` supports glob patterns: `CONFIG GET *memory*` returns all matching config keys.
- `CONFIG SET` must validate values (e.g., `maxmemory-policy` must be one of the valid policies).
- Config changes must take effect immediately where applicable (e.g., changing `maxmemory` should trigger eviction if needed).
- Must support all ~200 Redis config parameters, at least for GET. SET can be limited to parameters that actually affect engine behavior.
- `CONFIG RESETSTAT` → reset stats counters (INFO stats section).
- `CONFIG REWRITE` → return OK (no-op in emulator, but must not error).

### 1.15 Slow Log, Debug, Info, Client Management

#### Slow Log

**Commands:** `SLOWLOG GET [count]`, `SLOWLOG LEN`, `SLOWLOG RESET`

Records commands that exceed `slowlog-log-slower-than` microseconds (default 10000 = 10ms). Stores last `slowlog-max-len` entries (default 128).

```typescript
interface SlowLogEntry {
  id: number              // monotonic ID
  timestamp: number       // Unix timestamp
  duration: number        // microseconds
  command: string[]       // command + args
  clientAddr: string
  clientName: string
}
```

#### Debug

**Commands:** `DEBUG SET-ACTIVE-EXPIRE`, `DEBUG SLEEP`, `DEBUG OBJECT`, `DEBUG RELOAD`, `DEBUG LOADAOF`, `DEBUG CHANGE-REPL-ID`, `DEBUG QUICKLIST-PACKED-THRESHOLD`, etc.

Most DEBUG commands are development aids. Minimum viable set:
- `DEBUG SLEEP seconds` — block server for N seconds (useful for testing)
- `DEBUG SET-ACTIVE-EXPIRE 0|1` — enable/disable active expiration
- `DEBUG OBJECT key` — return internal object information

#### Info

**Command:** `INFO [section]`

Returns a bulk string with server information. Sections: `server`, `clients`, `memory`, `stats`, `replication`, `cpu`, `modules`, `commandstats`, `errorstats`, `cluster`, `keyspace`, `all`, `everything`, `default`.

Each section has specific key-value pairs that clients and monitoring tools parse. The format must be exact: `key:value\r\n` lines, section headers as `# Section\r\n`.

Key fields that must be accurate:
- `redis_version` — report a compatible version (e.g., `7.2.0`)
- `used_memory` — best-effort memory estimate
- `connected_clients` — actual count
- `db0:keys=N,expires=N,avg_ttl=N` — per-database stats

#### Client Management

**Commands:** `CLIENT` (LIST, GETNAME, SETNAME, ID, INFO, NO-EVICT, NO-TOUCH, KILL, PAUSE, UNPAUSE, REPLY, CACHING, TRACKING, TRACKINGINFO, GETREDIR)

**Per-client state:**

```typescript
interface ClientState {
  id: number                  // auto-incrementing client ID
  name: string                // set via CLIENT SETNAME
  db: number                  // selected database index
  flags: Set<ClientFlag>      // 'multi', 'blocked', 'subscribed', etc.
  transaction: TransactionState | null
  subscriptions: { channels: Set<string>; patterns: Set<string> }
  blockedState: BlockedClient | null
  createTime: number
  lastCommand: string
  lastCommandTime: number
}
```

**`CLIENT LIST`** output format must be exact — it is a structured string that monitoring tools parse:
```
id=1 addr=127.0.0.1:1234 fd=5 name=myconn age=100 idle=0 flags=N db=0 ...
```

---

## 2. Dependencies Between Subsystems

### 2.1 Dependency Graph

```
Command Dispatcher
├── depends on: Command Table, Client State, ACL, Config, Transaction Manager
├── calls into: All Type Engines, Key Store, Pub/Sub, etc.
└── triggers: Keyspace Notifications, Blocking Signal, WATCH versioning

Key Store
├── depends on: Expiration Manager (lazy check on every access)
├── used by: All Type Engines, Transaction Manager, Blocking Manager
└── triggers: WATCH version bumps, Keyspace Notifications

Expiration Manager
├── depends on: Key Store (to delete keys), Config (hz, active-expire settings)
├── triggers: Keyspace Notifications ("expired" event), Blocking unblock, WATCH bump
└── uses: Time source (virtual time hook)

Type Engines (String, List, Set, ZSet, Hash, Stream)
├── depend on: Key Store (entry access), Config (encoding thresholds)
├── trigger: Keyspace Notifications, Blocking signal (List/ZSet/Stream writes)
└── independent of each other (no cross-type dependencies)

Pub/Sub
├── depends on: Client State (subscription tracking)
├── used by: Keyspace Notifications, SUBSCRIBE/PUBLISH commands
└── independent of Key Store (pub/sub channels are not keys)

Transaction Manager
├── depends on: Key Store (version tracking), Command Dispatcher (queue + execute)
└── independent of specific Type Engines

Blocking Command Manager
├── depends on: Key Store (check data availability), Type Engines (pop operations)
├── triggered by: Type Engine mutations (LPUSH, ZADD, XADD, etc.)
└── uses: Time source (timeout management)

Keyspace Notifications
├── depends on: Pub/Sub (delivery mechanism), Config (which events enabled)
├── triggered by: Key Store mutations, Expiration, Eviction
└── called from: Type Engines, Expiration Manager, Eviction

Memory Eviction
├── depends on: Key Store (sampling, deletion), Config (maxmemory, policy)
├── triggers: Keyspace Notifications ("evicted" event), WATCH bump
└── called from: Command Dispatcher (pre-execution check)

Lua Scripting Engine
├── depends on: Command Dispatcher (redis.call routing)
├── uses: Key Store, all Type Engines (via dispatcher)
└── constraint: atomicity (blocks all other command processing)

Config System
├── used by: Expiration Manager, Eviction, Type Engines, Command Dispatcher
└── independent (leaf dependency)

Client State
├── used by: Command Dispatcher, Pub/Sub, Transaction Manager, Blocking Manager
└── independent (leaf dependency)
```

### 2.2 Subsystems That Can Be Built Independently

These have no inbound dependencies from other subsystems (or minimal ones):

1. **Config System** — pure key-value store with validation. No dependencies.
2. **Client State** — data structure for per-connection state. No dependencies.
3. **Command Table** — static data (command definitions, arity, flags). No dependencies.
4. **RESP Parser/Serializer** — protocol layer, separate from engine logic.
5. **Pub/Sub** — self-contained message routing. Only needs Client State.
6. **Individual Type Engine data structures** — skip list, stream, HLL can be built as standalone modules with unit tests.

### 2.3 Suggested Build Order

The order follows dependency flow — build leaves first, then compose.

**Phase 0: Foundation**
1. Config System (pure data, no deps)
2. Client State model
3. Command Table (static definitions)
4. RESP parser/serializer (if needed for wire protocol)

**Phase 1: Core Key Store**
5. Key Store with basic operations (GET/SET entry, DELETE, EXISTS)
6. Lazy expiration (check on access)
7. Key versioning (for WATCH)

**Phase 2: String Engine + Command Dispatcher**
8. String Type Engine (simplest type, covers most common commands)
9. Command Dispatcher with arity validation, type checking, routing
10. Generic key commands (DEL, EXISTS, TYPE, RENAME, TTL family, SCAN)

**Phase 3: Remaining Core Type Engines**
11. Hash Type Engine
12. List Type Engine
13. Set Type Engine
14. Sorted Set Type Engine (most complex — skip list)

**Phase 4: Cross-Cutting Concerns**
15. Transaction Manager (MULTI/EXEC/WATCH)
16. Pub/Sub system
17. Keyspace Notifications (hooks into Key Store + Pub/Sub)
18. Active Expiration Manager (timer-based cycle)
19. Blocking Command Manager (requires List + ZSet engines)

**Phase 5: Advanced**
20. Stream Type Engine (complex: consumer groups, blocking reads)
21. HyperLogLog, Bitmap, Geospatial
22. Memory Eviction (LRU/LFU)
23. Lua Scripting Engine
24. INFO, SLOWLOG, CLIENT management
25. ACL, Cluster stubs, Persistence stubs

---

## 3. Data Structure Choices for JS

### 3.1 Skip List for Sorted Sets

Redis sorted sets use a skip list with these properties:
- Max 32 levels, level probability 0.25
- Each node: element (string), score (double), backward pointer, level array with forward pointers and span counts
- Span tracking enables O(log N) rank lookup (ZRANK)
- Dual index: skip list (by score) + hash table (element -> score) for O(1) ZSCORE

**JS implementation approach:**

Build a custom skip list. Available npm packages (`redis-sorted-set`, `sorted-set`) exist but may not match Redis behavior exactly (especially edge cases around score comparison, lexicographic ordering, and span tracking).

```typescript
interface SkipListNode {
  element: string
  score: number
  backward: SkipListNode | null
  levels: Array<{
    forward: SkipListNode | null
    span: number
  }>
}

class SkipList {
  head: SkipListNode    // sentinel node
  tail: SkipListNode | null
  length: number
  level: number         // current max level in use

  // Core operations
  insert(score: number, element: string): SkipListNode
  delete(score: number, element: string): boolean
  find(score: number, element: string): SkipListNode | null
  getRank(score: number, element: string): number  // 0-based rank
  getByRank(rank: number): SkipListNode | null
  rangeByScore(min: ScoreBound, max: ScoreBound): SkipListNode[]
  rangeByLex(min: LexBound, max: LexBound): SkipListNode[]  // for equal-score ranges
}
```

**Comparison function (must match Redis exactly):**

```typescript
function compareNodes(s1: number, e1: string, s2: number, e2: string): number {
  if (s1 !== s2) return s1 - s2
  // Scores equal: compare elements lexicographically (byte comparison)
  return e1 < e2 ? -1 : e1 > e2 ? 1 : 0
}
```

**Level generation (must match Redis's probability):**

```typescript
function randomLevel(): number {
  let level = 1
  while (level < 32 && Math.random() < 0.25) {
    level++
  }
  return level
}
```

**Why custom over npm packages:**

- Need exact Redis comparison semantics (score ties broken by lexicographic order)
- Need span tracking for O(log N) ZRANK
- Need `rangeByLex` support for `ZRANGEBYLEX`
- Need to match Redis's level probability (0.25, max 32)
- Encoding transitions: need to switch between listpack-equivalent (sorted array) and skip list based on thresholds

### 3.2 Listpack Equivalent (Compact Encoding)

Redis's `listpack` is a compact sequential byte buffer for small collections. In JS, the equivalent is a **sorted array** or **flat array of pairs**.

**For small sorted sets (listpack encoding):**

```typescript
// Sorted array of [score, element] pairs, sorted by (score, element)
type ListpackZSet = Array<[number, string]>

// Binary search for lookups: O(log N)
// Insert/delete: O(N) due to array shift — acceptable for N <= 128
```

**For small hashes (listpack encoding):**

```typescript
// Flat array of [field, value] pairs
type ListpackHash = Array<[string, string]>

// Linear scan for lookups: O(N) — acceptable for N <= 128
```

**For small sets (listpack encoding):**

```typescript
// Simple array of values
type ListpackSet = string[]

// Linear scan: O(N) — acceptable for N <= 128
```

**For small lists (listpack encoding):**

```typescript
// Simple array
type ListpackList = string[]
```

**Encoding transition logic:**

```typescript
function shouldUpgradeEncoding(type: string, collection: unknown, config: Config): boolean {
  switch (type) {
    case 'hash':
      const hash = collection as ListpackHash
      const maxEntries = config.get('hash-max-listpack-entries') // default 128
      const maxValue = config.get('hash-max-listpack-value')     // default 64 bytes
      return hash.length > maxEntries ||
             hash.some(([f, v]) => f.length > maxValue || v.length > maxValue)
    // similar for other types
  }
}
```

### 3.3 Intset Equivalent

Redis `intset` stores a sorted array of integers in a compact binary format with automatic width selection (16/32/64-bit). In JS:

```typescript
class IntSet {
  private values: number[]  // sorted array of integers

  // All operations maintain sorted order
  add(value: number): boolean     // binary search + insert, O(N) shift
  remove(value: number): boolean  // binary search + remove
  has(value: number): boolean     // binary search, O(log N)
  size(): number
  random(): number
  toArray(): number[]
}
```

JS `number` is float64, so it can exactly represent integers up to 2^53. Redis intset supports up to 64-bit signed integers. For values beyond 2^53, use `BigInt` or switch to string-based storage.

**Transition:** When a non-integer string is added to an intset-encoded set, it must convert to either listpack (if small) or hashtable encoding.

### 3.4 Quicklist Equivalent (List)

Redis `quicklist` is a doubly-linked list of listpack nodes (each node contains multiple elements). In JS:

**Option A: Simple double-ended queue (Deque)**

For most use cases, a ring-buffer-based deque provides O(1) push/pop from both ends:

```typescript
class Deque<T> {
  private buffer: (T | undefined)[]
  private head: number
  private tail: number
  private size: number

  pushFront(value: T): void   // O(1) amortized
  pushBack(value: T): void    // O(1) amortized
  popFront(): T | undefined   // O(1)
  popBack(): T | undefined    // O(1)
  get(index: number): T       // O(1)
  set(index: number, v: T): void
  insertAt(index: number, v: T): void  // O(N) — needed for LINSERT
  removeAt(index: number): T           // O(N) — needed for LREM
}
```

**Option B: Chunked deque (closer to quicklist)**

A doubly-linked list of arrays, each array holding up to K elements. Better for very large lists where `LINSERT`/`LREM` in the middle is needed, as only one chunk needs to be shifted.

**Recommendation:** Start with Option A (simple Deque). It handles all operations correctly. Only optimize to chunked deque if benchmark shows performance issues with large lists and middle mutations.

**Encoding transition:**
- Small lists (<=128 elements, each <=64 bytes): store as plain `string[]` (listpack equivalent)
- Large lists: upgrade to `Deque<string>` or chunked deque

### 3.5 Stream Data Structures

Redis streams use a radix tree (rax) keyed by entry ID, where each node value is a listpack containing multiple entries with delta-compressed IDs and same-field compression.

**JS equivalent:**

A radix tree is overkill for JS. The primary operations are:
- Append (XADD) — always at the end
- Range query by ID (XRANGE, XREVRANGE)
- Trim from the start (XTRIM with MAXLEN or MINID)
- Random access by ID (for XDEL mark-as-deleted)

**Practical implementation:**

```typescript
interface StreamEntry {
  id: StreamId              // { ms: number, seq: number }
  fields: Map<string, string>
  deleted: boolean          // for XDEL (mark, not physical delete)
}

interface ConsumerGroup {
  name: string
  lastDeliveredId: StreamId
  consumers: Map<string, Consumer>
  pel: Map<string, PendingEntry>  // entry ID string -> pending entry
}

interface Consumer {
  name: string
  pel: Map<string, PendingEntry>  // entry ID string -> pending entry
  seenTime: number
  activeTime: number
}

interface PendingEntry {
  id: StreamId
  consumer: string
  deliveryTime: number
  deliveryCount: number
}

class Stream {
  private entries: StreamEntry[]      // sorted by ID (append-only, naturally sorted)
  private lastId: StreamId
  private groups: Map<string, ConsumerGroup>
  private length: number              // excluding deleted entries
  private firstId: StreamId

  xadd(id: string, fields: Map<string, string>): StreamId
  xrange(start: StreamId, end: StreamId, count?: number): StreamEntry[]
  xrevrange(start: StreamId, end: StreamId, count?: number): StreamEntry[]
  xtrim(strategy: 'MAXLEN' | 'MINID', threshold: number | StreamId, approx: boolean): number
  xdel(ids: StreamId[]): number       // marks as deleted
  xlen(): number
}
```

**Why array over radix tree:**

- Entries are always appended in order → array is naturally sorted
- Range queries by ID use binary search on the array: O(log N)
- Trim from start is O(1) with an offset pointer or `shift()`
- Memory overhead of a radix tree in JS would be significant (object allocation per node)
- The radix tree's delta compression benefit does not apply in JS (V8 already handles string interning)

**For very large streams:** If performance degrades with millions of entries, a B-tree or chunked array (similar to quicklist) could be used. But start simple.

### 3.6 Hash Table

JS `Map` is the natural choice. It provides O(1) average lookup, insert, delete — matching Redis's dict.

**Differences from Redis's dict:**

- Redis dict uses incremental rehashing (resizes gradually over multiple operations to avoid latency spikes). JS `Map` handles this internally — not controllable, but acceptable.
- Redis dict stores `dictEntry` objects with next pointers (chaining). JS `Map` implementation details are engine-specific.

**Conclusion:** Use `Map` directly. No custom hash table needed.

### 3.7 HyperLogLog

Must implement the Redis-specific HLL algorithm:

- Dense representation: 16384 registers of 6 bits each (packed into a Buffer/Uint8Array of 12288 bytes + 16 byte header = 12304 bytes total)
- Sparse representation: run-length encoded, transitions to dense when it would exceed 3000 bytes (configurable via `hll-sparse-max-bytes`)
- Hash function: Redis uses a variant of MurmurHash64A (specifically, the low 14 bits select the register, remaining bits determine the longest run of zeros)
- Bias correction: Redis uses the raw HLL estimation formula with bias corrections for different cardinality ranges

```typescript
class HyperLogLog {
  private registers: Uint8Array  // 16384 registers, 6 bits each (packed)
  private isSparse: boolean
  private sparseData: Buffer     // RLE-encoded sparse representation
  private cachedCardinality: number  // -1 = dirty

  pfadd(elements: string[]): boolean  // returns true if any register changed
  pfcount(): number
  pfmerge(other: HyperLogLog): void
}
```

### 3.8 Geospatial Encoding

Geospatial uses sorted sets with geohash scores. The geohash computation must be exact:

```typescript
function geohashEncode(longitude: number, latitude: number): number {
  // Interleave bits of normalized longitude and latitude into a 52-bit integer
  // Redis uses 26 bits for each dimension (52 total, fitting in float64 integer range)
  // The result is stored as the sorted set score
}

function geohashDecode(hash: number): { longitude: number; latitude: number } {
  // Reverse the interleaving
}

function geohashNeighbors(hash: number, bits: number): number[] {
  // Return the 8 neighboring geohash cells (for radius queries)
}
```

The GEOSEARCH radius/box query works by:
1. Computing the geohash of the center point
2. Determining which geohash cells could contain results (center + neighbors at the appropriate precision)
3. Doing a ZRANGEBYSCORE on each cell's hash range
4. Filtering results by actual distance

---

## 4. Interface Contracts Between Subsystems

### 4.1 Command Dispatcher → Type Engines

The dispatcher looks up the command in the command table, validates arity, and calls the handler. Each type engine exposes command handlers that receive the client state and raw arguments.

```typescript
// Command Dispatcher calls a command handler
type CommandHandler = (ctx: CommandContext) => RedisReply

interface CommandContext {
  client: ClientState         // current client (db selection, name, flags, etc.)
  args: Buffer[]              // raw arguments (excluding command name)
  db: Database                // shortcut to client's current database
  server: ServerState         // access to global state (config, pub/sub, etc.)
  now(): number               // current time (virtual time aware)
}

// Type engines register their handlers
interface TypeEngine {
  registerCommands(registry: CommandRegistry): void
}

// Example: String engine registration
class StringEngine implements TypeEngine {
  registerCommands(registry: CommandRegistry): void {
    registry.register('GET', {
      handler: this.getHandler,
      arity: 2,
      flags: ['readonly', 'fast'],
      firstKey: 1, lastKey: 1, keyStep: 1,
    })
    registry.register('SET', {
      handler: this.setHandler,
      arity: -3,  // minimum 3 args
      flags: ['write', 'denyoom'],
      firstKey: 1, lastKey: 1, keyStep: 1,
    })
    // ...
  }
}
```

**Contract rules:**

1. The handler receives already-validated arity (the dispatcher checked argument count).
2. The handler must validate types (e.g., `WRONGTYPE` error if key exists with wrong type).
3. The handler accesses the keyspace via `ctx.db`, which performs lazy expiration on every access.
4. The handler returns a `RedisReply` (typed union matching RESP types).
5. Write handlers must call `ctx.db.signalModifiedKey(key)` to trigger WATCH versioning and keyspace notifications.

### 4.2 Expiration Manager → Key Store

The expiration manager has two integration points:

**Lazy expiration (synchronous, on every access):**

```typescript
// Called by Database.get() and Database.exists() before returning
interface LazyExpiration {
  // Returns true if key was expired (caller should treat key as non-existent)
  checkAndExpire(db: Database, key: string, now: number): boolean
}
```

This is not a separate call — it is embedded in every key access path. The Database class itself performs the check:

```typescript
class Database {
  getEntry(key: string, now: number): RedisEntry | null {
    if (this.isExpired(key, now)) {
      this.deleteKey(key)            // remove from store + expires index
      this.signalModifiedKey(key)    // trigger WATCH version bump
      this.notifyExpired(key)        // keyspace notification
      return null
    }
    return this.store.get(key) ?? null
  }
}
```

**Active expiration (asynchronous, timer-driven):**

```typescript
interface ActiveExpirationCycle {
  // Called periodically (hz times per second)
  // Samples random keys from expires index, deletes expired ones
  // Returns when time budget exhausted or few expired keys found
  runCycle(databases: Database[], now: number, timeBudgetMs: number): void
}
```

The active expiration cycle iterates databases, calling the same `deleteKey` + `signalModifiedKey` + `notifyExpired` path.

### 4.3 Blocking Commands → Data Mutations

This is the most intricate integration. The contract is:

**Step 1: Write command mutates data**

Any command that adds data to a list, sorted set, or stream calls a signal function:

```typescript
// Called by LPUSH, RPUSH, ZADD, XADD, and similar write commands
interface BlockingSignal {
  signalKeyAsReady(db: Database, key: string): void
}
```

This does NOT immediately wake blocked clients. It adds the key to a set of "ready keys" for deferred processing.

**Step 2: Deferred unblocking (beforeSleep equivalent)**

After all commands in the current processing batch are complete:

```typescript
interface BlockingResolver {
  // Process all signaled ready keys
  // For each key with blocked clients, attempt to serve them
  processReadyKeys(): void
}
```

The resolver iterates ready keys and for each:
1. Gets the list of blocked clients for that key (FIFO order).
2. For each blocked client, checks if data is available (re-evaluates the blocking condition).
3. If data is available, performs the operation (e.g., LPOP for BLPOP), sends the result, and removes the client from the blocking index.
4. If the data was consumed (e.g., list is now empty), stops serving further clients for that key.

**Step 3: Timeout expiration**

```typescript
interface BlockingTimeoutHandler {
  // Called periodically to check for expired timeouts
  // Sends nil response to timed-out clients and removes them from blocking index
  expireTimeouts(now: number): void
}
```

**Why deferred processing matters:**

Consider: `LPUSH mylist a b c` pushes 3 elements. If 3 clients are blocked on `BLPOP mylist`, all three should be served (one element each). If we processed immediately during LPUSH, only the signaling for the first push would wake a client, and the remaining two would have to wait for the next event loop tick. By deferring to `beforeSleep`, we can serve all three in one pass.

Also, within a `MULTI/EXEC` transaction, multiple LPUSH commands might fire. The blocked clients should only be served after the entire transaction commits.

### 4.4 Transaction Manager → Command Dispatcher

```typescript
interface TransactionDispatch {
  // Called by dispatcher when client is in MULTI state
  enqueueCommand(client: ClientState, command: string, args: Buffer[]): void

  // Called when EXEC is received
  execTransaction(client: ClientState): RedisReply[]

  // Called when DISCARD is received
  discardTransaction(client: ClientState): void
}
```

During `EXEC`, the transaction manager:
1. Validates WATCH keys (compare versions).
2. Iterates the queue and calls the dispatcher for each command.
3. Collects results into an array.
4. Crucially: blocking commands in the queue execute as non-blocking variants.
5. Keyspace notifications and blocking signals are deferred until after the entire transaction completes.

### 4.5 Keyspace Notifications → Pub/Sub

```typescript
interface KeyspaceNotifier {
  // Called after any key mutation (set, delete, expire, evict, etc.)
  notify(
    type: NotificationType,  // which event category (string, list, generic, etc.)
    event: string,           // event name ("set", "del", "expire", "lpush", etc.)
    key: string,
    dbIndex: number
  ): void
}
```

The notifier checks the current `notify-keyspace-events` config to determine if the event should be published. If enabled, it publishes to the appropriate `__keyspace@<db>__:<key>` and/or `__keyevent@<db>__:<event>` channels via the Pub/Sub system.

**Important:** Keyspace notification delivery is synchronous within the same event loop tick. Subscribers receive the notification before the next command is processed.

### 4.6 Memory Eviction → Command Dispatcher

```typescript
interface EvictionCheck {
  // Called by the dispatcher BEFORE executing a write command
  // Returns true if the command can proceed, false if OOM
  ensureMemoryAvailable(bytesNeeded?: number): boolean
}
```

The eviction flow:
1. Dispatcher checks if `maxmemory` is configured and current usage exceeds it.
2. If over limit and policy is `noeviction`, reject with `-OOM command not allowed when used memory > 'maxmemory'`.
3. If over limit and policy allows eviction, enter eviction loop:
   - Sample keys according to policy
   - Add to eviction pool
   - Evict best candidate from pool
   - Repeat until memory is below limit or no more evictable keys
4. Each eviction triggers the full key deletion path (WATCH bump, keyspace notification, blocking cleanup).

### 4.7 Lua Scripting → Command Dispatcher

```typescript
interface ScriptCommandBridge {
  // Called by redis.call() / redis.pcall() inside Lua scripts
  // Executes a Redis command and returns the result
  executeFromScript(command: string, args: string[]): RedisReply

  // Must enforce:
  // - No nested MULTI/EXEC
  // - No blocking commands (BLPOP etc.)
  // - Read-only commands only if script is EVAL_RO/FCALL_RO
  // - KEYS must match declared keys
}
```

The script engine holds a reference to the command dispatcher. When Lua code calls `redis.call('SET', KEYS[1], ARGV[1])`, the script engine:
1. Converts Lua values to Redis command arguments
2. Calls the dispatcher (bypassing MULTI queue, ACL re-check is optional)
3. Converts the Redis reply back to Lua values
4. Propagates errors for `redis.call`, catches them for `redis.pcall`

### 4.8 Config System → All Consumers

Config changes need to propagate to affected subsystems. Two approaches:

**Option A: Pull model (simple)**

Each subsystem reads config values when needed:
```typescript
// In the expiration cycle
const hz = config.getInt('hz')
const sampleSize = 20  // ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP (hardcoded in Redis)
```

**Option B: Push model (reactive)**

Config system emits events on change:
```typescript
config.on('change', (key, value) => {
  if (key === 'maxmemory') evictionManager.updateLimit(value)
  if (key === 'hz') expirationManager.updateFrequency(value)
  if (key === 'notify-keyspace-events') notifier.updateMask(value)
})
```

**Recommendation:** Use pull model for simplicity. Config reads are cheap (`Map.get`). Push model adds complexity without meaningful benefit since config changes are rare.

---

## Summary

The Redis engine decomposes into ~15 subsystems with clear boundaries. The core insight for implementation order is:

1. **Key Store + Config + Client State** form the foundation
2. **Type Engines** are independent modules that plug into the Key Store
3. **Cross-cutting concerns** (transactions, blocking, notifications, expiration) are wired in as hooks after the type engines work
4. **Advanced features** (scripting, eviction, streams) build on the solid foundation

The JS-specific data structure choices favor simplicity: `Map` for hash tables, sorted arrays for small collections, custom skip list for sorted sets, plain arrays for streams. The key principle is: match Redis behavior exactly, but use JS-idiomatic structures internally.

---

[← Back](README.md)
