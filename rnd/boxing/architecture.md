# RedisBox Architecture

Full Redis reimplementation in TypeScript. **Target: 100% Redis command coverage.**

## Single-Mode Architecture

RedisBox is a standard Node.js TCP server that speaks RESP. One codebase, one code path. Browser support is provided by NodeBox (SimBox ecosystem's Node.js runtime emulator).

```
┌─────────────────────────────────────────────────────┐
│                                                       │
│  Client (ioredis / node-redis / redis-cli)            │
│                        ↓                              │
│              ┌──────────────────────┐                 │
│              │  TCP Server          │                 │
│              │  (RESP2/RESP3)       │                 │
│              └──────────┬───────────┘                 │
│                         ↓                             │
│              ┌──────────────────────┐                 │
│              │  Command Dispatcher  │                 │
│              └──────────┬───────────┘                 │
│                         ↓                             │
│              ┌──────────────────────┐                 │
│              │  IBI Hooks           │←── Sim          │
│              └──────────┬───────────┘                 │
│                         ↓                             │
│              ┌──────────────────────┐                 │
│              │  In-Memory Engine    │                 │
│              │  ┌────────────────┐  │                 │
│              │  │ String Store   │  │                 │
│              │  │ Hash Store     │  │                 │
│              │  │ List Store     │  │                 │
│              │  │ Set Store      │  │                 │
│              │  │ SortedSet Store│  │                 │
│              │  │ Stream Store   │  │                 │
│              │  │ PubSub Engine  │  │                 │
│              │  │ Script Engine  │  │                 │
│              │  └────────────────┘  │                 │
│              └──────────┬───────────┘                 │
│                         ↓                             │
│              ┌──────────────────────┐                 │
│              │  OBI Hooks           │                 │
│              │  (time, random,      │←── Sim          │
│              │   persist)           │                 │
│              └──────────────────────┘                 │
└─────────────────────────────────────────────────────────┘
```

## Connection Model

RedisBox listens on a TCP port (or Unix socket) and speaks standard RESP. Any Redis client connects normally.

### Transport Options

| Environment | Transport | Notes |
|-------------|-----------|-------|
| Node.js + ioredis | TCP or Custom Connector | Connector uses DuplexPair for zero-TCP overhead |
| Node.js + node-redis | TCP or Unix socket | node-redis has no custom connector API |
| Browser (via NodeBox) | TCP (emulated by NodeBox) | Same code, NodeBox provides `net` module |
| Testing / redis-cli | TCP | Standard connection |

### ioredis Custom Connector (optional optimization)

ioredis supports a `Connector` option returning a custom `NetStream`. Return an in-memory Duplex stream pair to eliminate TCP overhead for same-process usage:

```typescript
import { AbstractConnector } from 'ioredis/built/connectors'

class RedisBoxConnector extends AbstractConnector {
  constructor(private box: RedisBox) { super() }

  async connect(): Promise<NetStream> {
    const [client, server] = duplexPair()
    this.box.handleConnection(server)
    return client
  }
}

const redis = new Redis({
  Connector: RedisBoxConnector,
  lazyConnect: true,
})
```

This is an optimization, not a requirement. The default is a standard TCP server.

## In-Memory Engine

### Data Store

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
  unfreezeTime(): void

  // Failure Injection (returns disposers)
  injectLatency(ms: number, options?: { commands?: string[] }): () => void
  injectError(error: string, options?: { commands?: string[], probability?: number }): () => void
  simulateSlowCommand(command: string, durationMs: number): () => void

  // Behavioral Modification (T03)
  setCacheMissRate(rate: number): void
  setMessageDropRate(rate: number): void
}
```

## Implementation Priority

### Phase 1: Foundation

1. RESP2 parser/serializer
2. TCP server (net.createServer, accepts Redis clients)
3. Command dispatcher with `@ioredis/commands` metadata
4. In-memory keyspace (databases, entries, TTL)
5. String commands (25)
6. Key/generic commands (40+)
7. Connection commands (19)

### Phase 2: Core Data Structures

8. Hash commands (28)
9. List commands (22)
10. Set commands (17)
11. Sorted set commands (46)
12. Expiration system (lazy + active deletion)

### Phase 3: Advanced Features

13. Pub/Sub (12)
14. Transactions (4 + WATCH)
15. Streams + consumer groups (27)
16. Blocking commands (~10)
17. Bitmap (6), HyperLogLog (5), Geo (10)

### Phase 4: Scripting & Specialized

18. Lua scripting with wasmoon-lua5.1 or fengari (12)
19. Redis Functions (FUNCTION LOAD, FCALL)
20. ACL system (11)
21. Cluster command stubs (32)
22. Server commands (INFO, CONFIG, etc.)

### Phase 5: Modules & Parity

23. Hook layer (IBI + OBI)
24. RedisSim
25. JSON module (24)
26. Probabilistic data structures (47)
27. TimeSeries, Search, Vector Set — as needed
28. Redis TCL test suite integration
29. CI pipeline for parity verification

---

[← Back](README.md)
