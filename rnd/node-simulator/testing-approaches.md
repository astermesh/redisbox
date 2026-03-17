# Redis Testing Approaches

## Redis Official Test Suite

### Structure

Written in **TCL 8.5+**, located in `tests/` directory:
- `tests/unit/` — individual component tests
- `tests/cluster/` — Redis Cluster specific
- `tests/integration/` — replication, AOF, RDB integration
- `tests/sentinel/` — Sentinel tests
- `tests/modules/` — module API tests
- `tests/support/` — helpers and infrastructure

### Framework

- Helper procedures in `tests/support/redis.tcl` and `tests/support/cli.tcl`
- Result packets: "ok" (pass), "err" (fail), "exception" (halt)
- Can spawn Redis instances per test (configuring master-replica relationships)
- "External" mode communicates with pre-configured server (`--host` parameter)

### Compatibility Tags

- `external:skip` — incompatible with external servers
- `cluster:skip` — incompatible with cluster mode
- `needs:repl`, `needs:debug`, `needs:save` — require specific features
- `large-memory` — requires >100MB

### Unit Test Categories

printver, dump, auth, protocol, keyspace, scan, type/string, type/incr, type/list, type/set, type/zset, type/hash, type/stream, sort, expire, multi, quit, aofrw, and many others.

---

## Client Library Testing Approaches

### ioredis (JavaScript/Node.js)

- **Mixed approach**: mocks + real Redis
- **Mock**: `ioredis-mock` package (separate npm package)
- **Real Redis**: integration tests against real redis-server (recommended)
- **Note**: ioredis-mock persists data between instances, requires `flushall` between test suites

### node-redis (JavaScript/Node.js)

Three approaches:
1. **redis-mock** — in-memory mock, no Redis installation needed
2. **redis-memory-server** — spawns real Redis process from Node.js
3. **Testcontainers** — Docker-based, runs Redis/Redis Cluster

### redis-py (Python)

1. **FakeRedis** — pure-Python implementation, dual-mode testing (same tests run against both mock and real Redis)
2. **pytest-redis** — starts real Redis process, cleans DB after tests
3. **testing.redis** — temporary Redis instance, auto-destroyed

### jedis (Java)

- **Jedis-Mock** — in-memory mock, works at network protocol level
- Compatible with any Redis client (Jedis, Lettuce, Redisson)
- Supports "white box" testing with command interceptors
- Tests executed against both mock and real Redis

---

## Conformance Testing

- **resp-compatibility** — tests if a Redis-like database is compatible with specific Redis versions (1.0.0 through 7.2.0)
- **tair-opensource/compatibility-test-suite-for-redis** — structured conformance testing

---

## Key Pattern: Dual-Mode Testing

Multiple projects use the same pattern:
1. Write one test suite
2. Run it against the emulator (fast, no dependencies)
3. Run it against real Redis (verify behavioral parity)
4. Any difference is a bug in the emulator

Projects using this pattern:
- FakeRedis (Python)
- Jedis-Mock (Java)
- ioredis-mock (JavaScript, partially)

**This is the recommended approach for RedisBox.**

---

## Existing Emulators — Detailed

### fakeredis (Python)
- **Repo**: `cunla/fakeredis-py`
- **Architecture**: pure-Python implementation of Redis Protocol API
- **Coverage**: claims full Redis API + advanced features (RedisJson, RedisBloom, Geo)
- **Maturity**: 122 contributors, 2,240 commits
- **Dual-mode testing**: same tests run against both fakeredis and real Redis
- **Documentation**: [fakeredis.readthedocs.io](https://fakeredis.readthedocs.io/)

### ioredis-mock (JavaScript)
- **Repo**: `stipsan/ioredis-mock`
- **Coverage**: **68% Redis compatibility** (per repository badge)
- **Features**: pub/sub, Lua scripting, experimental Cluster support
- **Limitations**: no `evalsha`/`script`, no dynamic key number arguments
- **Drop-in**: replacement for ioredis API

### redis-memory-server (Node.js)
- **Repo**: `mhassan1/redis-memory-server`
- **Not a mock** — runs actual Redis binary
- Downloads and compiles Redis source, caches binary
- Each process ~4MB memory
- Inspired by `mongodb-memory-server`

### Jedis-Mock (Java)
- Works at **network protocol level** (RESP)
- Compatible with any JVM Redis client
- "White box" testing with command interceptors and reply manipulation
- Can function as test proxy

### DragonflyDB (C++)
- **Repo**: `dragonflydb/dragonfly`
- Not an emulator — a production Redis replacement
- 100% Redis and Memcached API compatible, 240+ commands
- Multi-threaded shared-nothing architecture
- 25X throughput vs Redis
- 30% better memory usage

### Garnet (Microsoft, C#)
- **Repo**: `microsoft/garnet`
- Redis-compatible server on .NET
- RESP wire protocol — works with unmodified Redis clients
- Two Tsavorite key-value stores (main for strings, object for complex types)
- Cluster mode support with sharding and replication

### Comparison

| Project | Type | Language | Compatibility | Approach |
|---------|------|----------|--------------|----------|
| fakeredis | Emulator | Python | High (claims full) | API-level mock |
| ioredis-mock | Emulator | JavaScript | 68% | API-level mock |
| redis-memory-server | Real server | C (spawned) | 100% | Runs actual Redis |
| Jedis-Mock | Emulator | Java | High | Protocol-level mock |
| DragonflyDB | Alternative | C++ | 240+ commands | Full server |
| Garnet | Alternative | C# | Large surface | Full server |

---

## Implications for RedisBox

1. **Dual-mode testing is essential** — run same tests against RedisBox and real Redis
2. **Protocol-level is the right approach** (like Jedis-Mock) — works with any client
3. **68% compatibility (ioredis-mock) is the norm** — achieving higher is a differentiator
4. **resp-compatibility test suite** should be used for conformance validation
5. **FakeRedis's architecture** (pure-language implementation) is closest to RedisBox's approach

---

[← Back to Node Simulator Research](README.md)
