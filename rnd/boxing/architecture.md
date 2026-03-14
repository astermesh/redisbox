# RedisBox Architecture

Design for the Redis emulator. **Target: 100% Redis command coverage.**

## Dual-Mode Architecture

RedisBox operates in two modes with a shared hook layer:

```
┌── Node.js: Proxy Mode ──────────────────────────┐
│                                                   │
│  Application → ioredis → [Custom Connector]       │
│                               ↓                   │
│                 ┌──────────────────────┐           │
│                 │  RESP Proxy          │           │
│                 │  ┌────────────────┐  │           │
│                 │  │ Parser (in)    │  │           │
│                 │  └───────┬────────┘  │           │
│                 │          ↓           │           │
│                 │  ┌────────────────┐  │           │
│                 │  │ IBI Hooks      │←─── Sim      │
│                 │  └───────┬────────┘  │           │
│                 │          ↓           │           │
│                 │  ┌────────────────┐  │           │
│                 │  │ Forward to     │  │           │
│                 │  │ Redis binary   │  │           │
│                 │  └───────┬────────┘  │           │
│                 │          ↓           │           │
│                 │  ┌────────────────┐  │           │
│                 │  │ Post Hooks     │←─── Sim      │
│                 │  └───────┬────────┘  │           │
│                 │          ↓           │           │
│                 │  ┌────────────────┐  │           │
│                 │  │ Serializer     │  │           │
│                 │  └────────────────┘  │           │
│                 └──────────────────────┘           │
│                               ↓                    │
│                 ┌──────────────────────┐           │
│                 │  Redis Subprocess    │           │
│                 │  (real Redis binary) │           │
│                 └──────────────────────┘           │
└───────────────────────────────────────────────────┘

┌── Browser: JS Engine Mode ──────────────────────┐
│                                                   │
│  Application → RedisBox API                       │
│                        ↓                          │
│              ┌──────────────────────┐             │
│              │  Command Dispatcher  │             │
│              └──────────┬───────────┘             │
│                         ↓                         │
│              ┌──────────────────────┐             │
│              │  IBI Hooks           │←── Sim      │
│              └──────────┬───────────┘             │
│                         ↓                         │
│              ┌──────────────────────┐             │
│              │  In-Memory Engine    │             │
│              │  ┌────────────────┐  │             │
│              │  │ String Store   │  │             │
│              │  │ Hash Store     │  │             │
│              │  │ List Store     │  │             │
│              │  │ Set Store      │  │             │
│              │  │ SortedSet Store│  │             │
│              │  │ Stream Store   │  │             │
│              │  │ PubSub Engine  │  │             │
│              │  │ Script Engine  │  │             │
│              │  └────────────────┘  │             │
│              └──────────┬───────────┘             │
│                         ↓                         │
│              ┌──────────────────────┐             │
│              │  OBI Hooks           │             │
│              │  (time, random,      │←── Sim      │
│              │   persist)           │             │
│              └──────────────────────┘             │
└───────────────────────────────────────────────────┘
```

### Mode Selection

```typescript
// Proxy mode (Node.js) — 100% coverage
const redis = createRedisBox({ mode: 'proxy' })

// JS engine mode (browser or Node.js) — incremental coverage
const redis = createRedisBox({ mode: 'engine' })

// Auto: proxy if Node.js + Redis binary available, else engine
const redis = createRedisBox({ mode: 'auto' })
```

## Connection Approaches

### Approach A: Client-Side API Replacement

Replace ioredis import with mock. No wire protocol.

Pros: Simplest. Works in browser and Node.js.
Cons: Locked to one client library. No protocol-level hooks. Not true RESP.

### Approach B: Custom ioredis Connector (recommended for both modes)

ioredis supports a `Connector` option returning a custom `NetStream`. Return an in-memory Duplex stream pair.

```typescript
import { AbstractConnector } from 'ioredis/built/connectors'
import { createDuplexPair } from 'duplexpair'

class RedisBoxConnector extends AbstractConnector {
  constructor(private box: RedisBox) { super() }

  async connect(): Promise<NetStream> {
    const { client, server } = createDuplexPair()
    this.box.handleConnection(server)
    return client
  }
}

const redis = new Redis({
  Connector: RedisBoxConnector,
  lazyConnect: true,
})
```

Pros: Real ioredis client, application code unchanged. RESP flows through real encoding. Duplex stream is a natural hook point.
Cons: Node.js only (Duplex streams). Requires RESP parser.

Note: `node-redis` does NOT expose a custom connector. For node-redis users, use Approach C.

### Approach C: In-Process TCP Server

Start TCP server on localhost with random port. Works with any Redis client in any language.

Pros: Works with any Redis client. redis-cli compatible.
Cons: Node.js only. Port allocation complexity.

### Approach D: Direct API (Browser)

No networking. Direct function calls with RESP-like command arrays.

```typescript
const box = createRedisBox({ mode: 'engine' })
await box.call('SET', 'mykey', 'hello', 'EX', '60')
const value = await box.call('GET', 'mykey')
```

### Recommended Transport Strategy

| Environment | Primary | Fallback |
|-------------|---------|----------|
| Node.js + ioredis | Custom Connector (B) | TCP server (C) |
| Node.js + node-redis | TCP server (C) | — |
| Browser | Direct API (D) | — |
| Testing / redis-cli | TCP server (C) | — |

## Proxy Mode: Detail Design

### RESP Proxy Core

```typescript
class RespProxy {
  private upstream: net.Socket
  private clientParser: RedisParser
  private serverParser: RedisParser

  handleClientData(data: Buffer): void {
    // 1. Parse RESP command(s) from client
    // 2. For each command: run pre-hooks
    // 3. Forward (possibly modified) command to Redis
    // 4. Parse Redis response
    // 5. Run post-hooks
    // 6. Send (possibly modified) response to client
  }
}
```

### Pipelining in Proxy

Client may send multiple commands in one buffer. The proxy must:
1. Parse all complete commands from buffer
2. Run pre-hooks for each, in order
3. Forward each to Redis (or short-circuit)
4. Match responses to commands (maintain FIFO queue)
5. Run post-hooks for each response
6. Send all responses to client in order

### Redis Binary Manager

```typescript
class RedisBinaryManager {
  async ensureBinary(version?: string): Promise<string>
  async start(options?: RedisStartOptions): Promise<RedisProcess>
  async stop(process: RedisProcess): Promise<void>
}

interface RedisStartOptions {
  port?: number          // 0 = random
  maxmemory?: string     // e.g., '100mb'
  additionalConfig?: Record<string, string>
}
```

Config for subprocess: `redis-server --port 0 --save "" --appendonly no --loglevel warning --protected-mode no --bind 127.0.0.1`

### Virtual Time in Proxy Mode

Real Redis uses real time. Workarounds:

1. **Disable active expiration**: `DEBUG SET-ACTIVE-EXPIRE 0`
2. **Intercept TIME command**: Proxy returns virtual time
3. **TTL rewriting**: Convert relative TTL to absolute timestamp based on virtual time
4. **Lazy expiration override**: On GET, proxy checks virtual TTL. If virtually expired, intercept response
5. **Force expiration**: Advance virtual time → proxy scans keys with TTL < new virtual time → sends DEL

Limitation: Not perfect for all edge cases, but sufficient for testing scenarios.

## JS Engine Mode: Detail Design

### In-Memory Data Store

```typescript
class RedisEngine {
  private databases: Database[] = Array.from({ length: 16 }, () => new Database())
  private currentDb: number = 0

  async execute(command: string, args: string[]): Promise<RespValue> {
    const handler = this.commandHandlers.get(command.toUpperCase())
    if (!handler) throw new RespError(`ERR unknown command '${command}'`)
    return handler(args, this.databases[this.currentDb])
  }
}

class Database {
  private store = new Map<string, RedisEntry>()
  private expiryIndex = new Map<string, number>()

  get(key: string, now: number): RedisEntry | null {
    const entry = this.store.get(key)
    if (!entry) return null
    const expiresAt = this.expiryIndex.get(key)
    if (expiresAt !== undefined && now >= expiresAt) {
      this.store.delete(key)
      this.expiryIndex.delete(key)
      return null
    }
    return entry
  }
}

type RedisEntry =
  | { type: 'string'; value: Buffer }
  | { type: 'list'; value: Deque<Buffer> }
  | { type: 'set'; value: Set<string> }
  | { type: 'zset'; value: SortedSet }
  | { type: 'hash'; value: Map<string, Buffer> }
  | { type: 'stream'; value: StreamStore }
```

### Command Handler Pattern

```typescript
type CommandHandler = (args: string[], db: Database) => Promise<RespValue>

const getHandler: CommandHandler = async (args, db) => {
  if (args.length !== 1) {
    throw new RespError("ERR wrong number of arguments for 'get' command")
  }
  const entry = db.get(args[0], this.now())
  if (!entry) return null
  if (entry.type !== 'string') {
    throw new RespError('WRONGTYPE Operation against a key holding the wrong kind of value')
  }
  return entry.value
}
```

### Command Registration

Use command metadata from `@ioredis/commands` package for automatic validation (argument count, key extraction, read/write classification).

## Hook Surface

Shared between proxy and engine modes.

### IBI Hooks (Inbound Box Interface)

Generic hook on every command:

```typescript
hooks.command = new Hook<CommandCtx, RespValue>(bus, 'redis:command')

interface CommandCtx {
  command: string
  args: string[]
  clientId: string
  db: number
  meta: { isWrite: boolean; isBlocking: boolean; keyCount: number }
}
```

Per-command-family hooks for targeted Sim behavior:

| Hook | Commands |
|------|----------|
| `redis:string:read` | GET, MGET, GETRANGE, STRLEN, etc. |
| `redis:string:write` | SET, MSET, SETNX, INCR, DECR, etc. |
| `redis:hash:read` | HGET, HMGET, HGETALL, HKEYS, etc. |
| `redis:hash:write` | HSET, HMSET, HDEL, HINCRBY, etc. |
| `redis:list:read` | LRANGE, LINDEX, LLEN |
| `redis:list:write` | LPUSH, RPUSH, LPOP, RPOP, etc. |
| `redis:list:block` | BLPOP, BRPOP, BLMOVE, BLMPOP |
| `redis:set:read` | SMEMBERS, SISMEMBER, SCARD, etc. |
| `redis:set:write` | SADD, SREM, SPOP, SMOVE |
| `redis:zset:read` | ZRANGE, ZSCORE, ZRANK, etc. |
| `redis:zset:write` | ZADD, ZREM, ZINCRBY |
| `redis:stream:read` | XRANGE, XREVRANGE, XLEN, etc. |
| `redis:stream:write` | XADD, XDEL, XTRIM, XACK, etc. |
| `redis:pubsub` | PUBLISH, SUBSCRIBE, PSUBSCRIBE, etc. |
| `redis:tx` | MULTI, EXEC, DISCARD, WATCH |
| `redis:script` | EVAL, EVALSHA, FCALL, FUNCTION |
| `redis:key` | DEL, EXISTS, EXPIRE, TTL, SCAN, etc. |
| `redis:server` | INFO, CONFIG, DBSIZE, FLUSHDB, etc. |
| `redis:connection` | AUTH, HELLO, CLIENT, SELECT, PING |

### OBI Hooks (Outbound Box Interface)

```typescript
hooks.time = new Hook<TimeCtx, number>(bus, 'redis:time')
hooks.random = new Hook<RandomCtx, number>(bus, 'redis:random')
hooks.persist = new Hook<PersistCtx, void>(bus, 'redis:persist')
```

### Decision Vocabulary

**Pre-phase** (before execution):

| Decision | Effect |
|----------|--------|
| `continue` | Execute normally |
| `delay` | Add latency (ms) before execution |
| `fail` | Return error without executing |
| `short_circuit` | Return specific value without executing |
| `execute_with` | Modify args, then execute |

**Post-phase** (after execution):

| Decision | Effect |
|----------|--------|
| `pass` | Return response as-is |
| `transform` | Modify response before returning |
| `fail` | Replace response with error |

## RedisSim Design

```typescript
class RedisSim {
  // Time Control
  advanceTime(ms: number): void
  freezeTime(): void
  setTime(timestamp: number): void

  // Failure Injection
  injectLatency(ms: number, options?: { commands?: string[] }): void
  injectError(error: string, options?: { commands?: string[], probability?: number }): void
  injectEviction(keys: string[]): void

  // Behavioral Modification
  setCacheMissRate(rate: number): void
  setMessageDropRate(rate: number): void
}
```

## Implementation Priority

### Phase 1: Proxy Mode (5-7 weeks)

1. RESP2 parser/serializer
2. Redis binary manager (download, start, stop)
3. RESP proxy with command interception
4. ioredis Custom Connector adapter
5. TCP server adapter (for node-redis / redis-cli)
6. Hook layer (IBI + OBI)
7. Basic RedisSim (latency, errors, command interception)
8. Virtual time via proxy

### Phase 2: JS Engine Core (8-12 weeks)

9. In-memory engine scaffold
10. String commands (25)
11. Hash commands (28)
12. List commands (22)
13. Set commands (17)
14. Sorted Set commands (46)
15. Key/Generic commands (40+)
16. Connection/Server commands (49)

### Phase 3: JS Engine Advanced (6-8 weeks)

17. Pub/Sub (12)
18. Transactions (4 + WATCH)
19. Streams (27)
20. Blocking commands (~10)
21. Bitmap (6), HyperLogLog (5), Geo (10)
22. Scripting with Lua embedding (12)

### Phase 4: JS Engine Specialized (6-8 weeks)

23. ACL (11), Cluster stubs (32)
24. JSON module (24)
25. Probabilistic data structures (47)
26. TimeSeries (24), Search (27), Vector Set (12)

### Phase 5: Parity & Polish

27. Redis TCL test suite integration
28. CI pipeline for cross-verification
29. Performance benchmarks
30. Browser adapter / bundling

---

[← Back](README.md)
