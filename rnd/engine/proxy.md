# RESP Proxy Research

Research into implementation details for the RESP proxy component: a Node.js TCP proxy that sits between the Redis client and a real Redis subprocess, with the ability to intercept and hook commands.

## 1. Redis Subprocess Management

### Binary Acquisition Strategy

The `redis-memory-server` npm package is the primary reference for how to manage Redis binaries in Node.js projects. Key findings:

**Download and compilation:**
- Downloads Redis source from `https://download.redis.io/` and compiles `redis-server` locally
- Caches compiled binaries in `node_modules/.cache/redis-memory-server/redis-binaries`
- First startup is slow (compilation); subsequent runs use cached binaries
- Uses `cross-spawn` for cross-platform process spawning

**Platform support:**
- Linux/macOS: downloads source and compiles (requires `make`)
- Windows: uses Memurai pre-built binaries (latest version only, no version pinning)
- No ARM-specific handling documented — relies on native compilation

**Version pinning:**
- Default version is `stable` (doesn't auto-update after initial download)
- Explicit versions supported (e.g., `6.0.10`) via constructor, environment variable `REDISMS_VERSION`, or `package.json` config
- Force re-download: `REDISMS_IGNORE_DOWNLOAD_CACHE=true`

**System binary support:**
- `REDISMS_SYSTEM_BINARY=/usr/local/bin/redis-server` bypasses download/compilation entirely
- Useful for CI environments or users who already have Redis installed

### Startup/Shutdown Lifecycle

**Startup sequence:**
1. Resolve binary (system binary > cache > download+compile)
2. Allocate free port using `get-port` utility
3. Spawn `redis-server` with arguments: `--port <port> --save "" --appendonly no --loglevel warning --protected-mode no --bind 127.0.0.1`
4. Monitor stdout for `"Ready to accept connections"` to detect readiness
5. Also detect failure patterns: `"Address already in use"`, `"permission denied"`
6. Spawn a separate `redis_killer.js` cleanup script as a safety net

**Shutdown sequence:**
1. Send `SIGINT` to Redis process
2. Wait up to 10 seconds for graceful exit
3. Force kill with `SIGKILL` if timeout reached
4. The killer script ensures cleanup even if parent Node.js process dies

**Port management:**
- Uses `get-port` for dynamic port allocation
- Supports port reuse after restart (`_previousInstanceConfig` preserves port)
- Port 0 approach: Redis docs say `--port 0` means "don't listen on TCP" — so `redis-memory-server` picks a free port itself and passes it explicitly

### Recommendations for RedisBox

- **Support both system binary and managed binary.** Check `REDISBOX_SYSTEM_BINARY` first, then look for cached binary, then download.
- **Use Unix socket by default** for proxy-to-Redis communication (eliminates port conflicts and is faster). Use TCP as fallback.
- **Prefer `get-port` over `--port 0`** since Redis treats port 0 as "no TCP listener."
- **Health check via PING.** After detecting "Ready to accept connections" in stdout, confirm with a PING command over the connection before declaring ready.

## 2. RESP Proxy Architecture

### Core Proxy Design

The proxy sits between the client and Redis, parsing RESP on both sides:

```
Client  ←RESP→  [Parser] → [Hooks] → [Forward] → Redis subprocess
                                                      ↓
Client  ←RESP←  [Serializer] ← [Post-hooks] ← [Parser] ← Redis
```

**Dual parser requirement:** The proxy needs a RESP parser on both the client-facing side (parsing commands) and the Redis-facing side (parsing responses). Both must be streaming parsers that handle partial reads and pipelining.

### Connection Model: 1:1 vs Multiplexing

Existing proxy implementations use two models:

**1:1 (one upstream connection per client) — recommended for RedisBox:**
- Codility's `redis-proxy` (Go): every client gets its own isolated upstream connection
- Simpler to implement, no state leakage between clients
- Per-connection state (SELECT, CLIENT SETNAME, auth) just works
- RedisBox typically has few connections (testing), so pooling overhead is unnecessary

**Multiplexed (shared upstream connections):**
- `redis-cluster-proxy`: thread-based, shared connections with fallback to private for MULTI/blocking
- `redisbetween` (Coinbase): connection pool per upstream, requires signal messages for pipelining
- `rmux` (Salesforce): connection pooler for LAMP stacks, key-based multiplexing
- `twemproxy` (Twitter): sharding proxy, zero-copy mbuf design
- More complex, needed for high-connection-count production scenarios — not our use case

**Decision: Use 1:1 model.** RedisBox is for testing/simulation, not production traffic. Simplicity and correctness matter more than connection efficiency.

### Pipeline Handling

Clients may send multiple commands in a single TCP write (pipelining). The proxy must:

1. Parse all complete commands from the incoming buffer
2. Run pre-hooks for each command, in order
3. Forward each command to Redis (or short-circuit if hooks decide)
4. Maintain a FIFO queue matching commands to expected responses
5. Parse each response from Redis
6. Run post-hooks for each response
7. Send responses to client in original order

**Key insight from redis-cluster-proxy:** Pipelining is NOT atomic. Commands from one pipeline can be interleaved with commands from other clients on the same Redis instance. Only MULTI/EXEC provides atomicity.

**Response matching:** Since RESP is strictly ordered (responses come in the same order as commands), the proxy maintains a simple queue. Each forwarded command pushes an entry; each received response pops the front entry.

### MULTI/EXEC Handling

Transactions require special proxy treatment:

- Between MULTI and EXEC, Redis queues commands and responds with `+QUEUED` for each
- EXEC executes all queued commands atomically and returns an array of results
- The proxy must **not** intercept individual commands between MULTI and EXEC at the Redis level — they haven't executed yet
- Pre-hooks should run at MULTI time (to potentially block the transaction) or at EXEC time (when commands actually execute)
- DISCARD cancels the transaction
- WATCH keys must be tracked — any modification between WATCH and EXEC causes EXEC to return null array

**Recommended approach:**
1. On MULTI: enter transaction mode for this connection, start buffering commands
2. On queued commands: still forward to Redis (get `+QUEUED` responses), but also buffer for hook processing
3. On EXEC: run pre-hooks for the entire command batch, then forward EXEC
4. On EXEC response: run post-hooks with the array of results
5. On DISCARD: clear transaction buffer, exit transaction mode

### Pub/Sub Mode

When a client sends SUBSCRIBE or PSUBSCRIBE, the connection enters **subscribe mode**:

- The client can only send SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING, and RESET
- The server pushes messages asynchronously (not in response to commands)
- In RESP2, push messages look like regular arrays: `["message", "channel", "payload"]`
- In RESP3, push messages use the `>` type prefix

**Proxy implications:**
- The proxy must track that a connection is in subscribe mode
- Response matching changes: responses are no longer 1:1 with commands
- The proxy must distinguish between subscription confirmations (responses to SUBSCRIBE) and push messages (server-initiated)
- Pattern: subscription confirmations have the format `["subscribe", "channel", count]`; messages have `["message", "channel", "data"]`

### Blocking Commands

BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, XREAD (with BLOCK), WAIT, etc.:

- These block the connection until data arrives or timeout expires
- The proxy must not timeout the connection while Redis is blocking
- The response comes asynchronously (after potential long delay)
- Response matching still works (it's still one response per command), but the proxy must handle the delay

**redis-cluster-proxy approach:** Allocate a private connection for blocking commands, disabling multiplexing. In our 1:1 model, this is automatic.

### MONITOR Command

MONITOR switches the connection to a streaming mode where Redis sends a line for every command processed:

- Output format: `+<timestamp> [<db> <client>] "<command>" "<arg1>" "<arg2>"...`
- Stream continues until client disconnects
- Monitoring degrades Redis performance by >50%

**Proxy handling:** Track that the connection is in MONITOR mode. All incoming data from Redis is push data (no request-response matching). The proxy should forward MONITOR output unchanged.

### CLIENT Commands Affecting Connection State

Commands the proxy must track per-connection:

| Command | State Change | Proxy Impact |
|---------|-------------|--------------|
| `SELECT <db>` | Changes active database | Must track per connection |
| `CLIENT SETNAME <name>` | Sets connection name | Proxy may need to modify (add prefix) |
| `CLIENT ID` | Returns connection ID | Proxy must return proxy-side ID or Redis-side ID — decision needed |
| `AUTH <password>` | Authenticates | Proxy manages its own auth separately from upstream |
| `HELLO 3` | Switches to RESP3 | Proxy must switch parsers for this connection |
| `CLIENT TRACKING ON` | Enables key invalidation | RESP3 push messages for invalidation |

**Codility redis-proxy pattern:** Track SELECTed database per client, re-issue SELECT after upstream reconnection.

## 3. Command Interception

### Hook Points

Two interception points per command:

**Pre-hook (before forwarding to Redis):**
- Receives parsed command (name + args)
- Can return: `continue` (forward), `short_circuit` (return value without forwarding), `fail` (return error), `execute_with` (modify args), `delay` (add latency)
- Must run synchronously or with minimal async overhead to avoid pipeline stalls

**Post-hook (after receiving Redis response):**
- Receives command + response pair
- Can return: `pass` (forward response), `transform` (modify response), `fail` (replace with error)
- Runs after Redis has already executed the command

### Performance Considerations

- **Fast path:** Commands without registered hooks should bypass interception entirely (just forward raw bytes)
- **Selective parsing:** Only parse command name for hook lookup. Parse full args only if a hook is registered.
- **Zero-copy forwarding:** If no hooks match, forward the raw Buffer from client to Redis without re-serialization.
- **Hook lookup:** Use a Map keyed by uppercase command name. O(1) lookup.

### Commands Needing Interception for SimBox

Based on the architecture design, these command families need hooks:

| Hook Purpose | Commands | Interception Type |
|-------------|----------|-------------------|
| Virtual time | TIME, TTL, PTTL, EXPIRETIME, PEXPIRETIME | Post-hook: transform response |
| TTL rewriting | SET (EX/PX/EXAT/PXAT), EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT | Pre-hook: rewrite TTL args |
| Expiration control | GET, MGET (check virtual expiry) | Post-hook: check virtual TTL |
| Failure injection | Any command | Pre-hook: fail/delay |
| Cache miss simulation | GET, MGET, HGET, HGETALL, etc. | Post-hook: transform to null |
| Latency injection | Any command | Pre-hook: delay |
| Message drop | PUBLISH | Pre-hook: short_circuit |
| Active expiry disable | Config on startup | `DEBUG SET-ACTIVE-EXPIRE 0` |

## 4. Connection Handling

### ioredis Custom Connector

ioredis supports custom connectors via the `Connector` option. The connector returns a `NetStream` (anything implementing `net.Socket` interface):

```typescript
class RedisBoxConnector extends AbstractConnector {
  async connect(): Promise<NetStream> {
    const { socket1, socket2 } = new DuplexPair()
    redisBox.handleConnection(socket2)
    return socket1
  }
}
```

**DuplexPair:** Node.js has a built-in `stream.duplexPair()` (also available as `duplexpair` npm package). Creates two linked Duplex streams — writing to one makes data readable on the other. This eliminates TCP overhead entirely for same-process connections.

**node-redis limitation:** node-redis does NOT expose a custom connector interface. For node-redis users, the proxy must listen on a TCP socket or Unix socket.

### Transport Options

| Transport | Use Case | Latency | Complexity |
|-----------|----------|---------|------------|
| DuplexPair | ioredis in same process | Lowest (no TCP) | Low |
| Unix socket | Any client, same machine | Low | Medium |
| TCP (localhost) | Any client, any language | Higher | Medium |

**Recommended default:** Unix socket in a temp directory (e.g., `/tmp/redisbox-<id>.sock`). Provides universal client compatibility with good performance.

### Connection Lifecycle

Typical client connection sequence:
1. TCP/socket connect
2. Optional: `AUTH <password>` (if configured)
3. Optional: `HELLO 3` (upgrade to RESP3)
4. Optional: `CLIENT SETNAME <name>`
5. Optional: `SELECT <db>`
6. Command execution begins
7. Connection close (client disconnect or server shutdown)

The proxy must handle each of these, tracking per-connection state.

## 5. Edge Cases and Gotchas

### MONITOR Command
- Switches to streaming push mode — breaks request-response model
- Performance impact >50% on Redis — proxy should warn or restrict
- The proxy must track MONITOR state to correctly handle the response stream

### DEBUG Commands
- `DEBUG SET-ACTIVE-EXPIRE 0` — critical for virtual time (disables Redis background expiration)
- `DEBUG SLEEP <seconds>` — blocks the connection
- `DEBUG OBJECT <key>` — returns internal encoding info
- The proxy should allow DEBUG commands through for virtual time support

### CONFIG Commands
- `CONFIG SET` can change Redis behavior (maxmemory, timeout, etc.)
- The proxy may need to intercept certain CONFIG changes that conflict with proxy operation
- `CONFIG SET bind` / `CONFIG SET port` — would break proxy connection

### SHUTDOWN / BGSAVE
- `SHUTDOWN` kills the Redis subprocess — proxy must detect this and handle gracefully
- `BGSAVE` / `BGREWRITEAOF` — spawn background processes, can impact memory
- Proxy should intercept SHUTDOWN and handle through its own lifecycle management

### Large Payloads
- Bulk strings up to 512MB (`proto-max-bulk-len` default)
- The proxy parser must handle streaming large values without buffering entirely in memory
- For hook interception, large values that don't need inspection should be streamed through

### CLUSTER Commands
- RedisBox runs a single Redis instance — CLUSTER commands should return appropriate errors or be stubbed
- `CLUSTER INFO` should return `cluster_enabled:0`

### RESET Command (Redis 6.2+)
- Resets connection state: exits SUBSCRIBE mode, MONITOR mode, transaction, changes to db 0
- Proxy must reset its per-connection state tracking when RESET is forwarded

### WAIT Command
- Blocks until replicas acknowledge writes
- Single-instance RedisBox has no replicas — should return immediately with 0

### CLIENT NO-EVICT / CLIENT NO-TOUCH
- Connection-level flags that affect memory management
- Should be forwarded as-is in proxy mode

## 6. Existing RESP Proxy Implementations

### Node.js Ecosystem

The Node.js ecosystem has **no production-grade RESP proxy library**. Existing packages are either:
- HTTP wrappers around Redis (not RESP-level)
- Simple caching proxies without full protocol support
- Incomplete implementations (e.g., `redis-proxy` npm — pub/sub doesn't work)

This means RedisBox's RESP proxy will be a novel implementation. Reference libraries for RESP parsing:
- `redis-parser` (NodeRedis) — RESP2, battle-tested, used by ioredis and node-redis
- `resp3` (tinovyatkin) — RESP3, pure streaming, ~300 LOC

### Production Proxies (C/Go) — Architecture Lessons

**twemproxy (Twitter, C):**
- Zero-copy mbuf design for high throughput
- Key-based consistent hashing for sharding
- Lesson: mbuf approach reduces memory allocation overhead

**redis-cluster-proxy (Redis Labs, C):**
- Thread-based multiplexing with private connection fallback
- Pre-populated connection pools per thread
- Automatic ASK/MOVED handling
- Lesson: MULTI and blocking commands MUST get private connections in multiplexed designs

**redisbetween (Coinbase, Go):**
- Unix socket per upstream endpoint
- Requires client-side signal messages (`GET [start]` / `GET [end]`) for pipeline delimiting
- Dedicated reserved connections for blocking/subscribe (configurable count)
- Lesson: multiplexing + pipelining is hard without protocol extensions

**rmux (Salesforce, Go):**
- Connection pooler for high-connection-count LAMP stacks
- Key-based routing with failover
- ~10x performance improvement for short-lived connections
- Lesson: connection recycling and pooling critical for short-lived processes

**Codility redis-proxy (Go):**
- 1:1 client-to-upstream mapping (simplest model)
- Tracks SELECT database per client, re-issues after upstream switch
- Separate auth for client-side and upstream-side
- PAUSE/resume for zero-downtime upstream replacement
- Lesson: 1:1 model is cleanest for our use case; per-client state tracking pattern is good

### Key Takeaway

Every production proxy that supports multiplexing has had to carve out exceptions for stateful commands (MULTI/EXEC, blocking, Pub/Sub, SELECT). Since RedisBox uses 1:1 connections, we avoid this complexity entirely. The 1:1 model aligns with Codility's redis-proxy, which is the closest architectural match.

---

[← Back](README.md)
