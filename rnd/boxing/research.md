# RedisBox Technical Research — Summary

In-memory Redis emulator. Runs in browser and Node.js, supports Sim attachments for behavior modification, virtual time, and deterministic replay. **Target: 100% Redis command coverage.**

## Key Findings

### Redis Command Surface is Larger Than Initially Estimated

The full Redis command surface (including modules) is ~500-650 commands:

| Version | Core commands | With modules (JSON, Search, TS, etc.) |
|---------|--------------|---------------------------------------|
| Redis 7.4 | ~437-450 | ~500+ |
| Redis 8.0 | ~460+ | ~650+ |

Categories breakdown (Redis 8.0):
- Strings: 25, Hashes: 28, Lists: 22, Sets: 17, Sorted Sets: 46
- Streams: 27, Pub/Sub: 12, Transactions: 4, Scripting: 12
- Keys/Generic: 40+, Connection: 19, Server: 30+, Cluster: 32
- Bitmap: 6, HyperLogLog: 5, Geo: 10, ACL: 11
- JSON module: 24, Search (FT): 27, TimeSeries (TS): 24
- Bloom Filter: 11, Cuckoo Filter: 10, CMS: 6, T-Digest: 13, Top-K: 7
- Vector Set: 12

### No WASM Redis Exists

Fluence Labs (2019) ported Redis to WASM via Clang/WASI — research project, no npm package. Key limitations: syscalls need stubbing, networking requires host environment, no deterministic execution guarantees with Emscripten approach. No other production-ready WASM Redis exists.

### How Others Achieve High Redis Compatibility

| Project | Language | Coverage | Approach |
|---------|----------|----------|----------|
| KeyDB | C++ (Redis fork) | 100% | Fork of Redis source, keeps in sync |
| DragonflyDB | C++ | ~240+ cmds | Reimplemented from scratch, RESP-compatible |
| Kvrocks | C++ | ~308 cmds | C++ rewrite with RocksDB backend |
| Garnet (Microsoft) | C# | Large subset | Reimplemented from scratch |
| ioredis-mock | JS | 385/568 (68%) | API mock, no RESP |
| memory-cache | JS | 224/267 (85%) | Pure JS, no RESP |

**Key insight**: DragonflyDB and Kvrocks are the best analogies — they reimplemented Redis from scratch in C++ and achieved high (but not 100%) coverage. Only forks of actual Redis source (KeyDB) achieve true 100%.

### Three Paths to 100% Coverage

#### Path A: Pure JS Reimplementation (all commands)

Reimplement all ~500 core commands in TypeScript.

| Pro | Con |
|-----|-----|
| Full Sim hook integration | Massive implementation effort (~500 commands) |
| Virtual time is trivial | Edge case parity is hard to guarantee |
| Browser support native | Must track Redis changes across versions |
| Deterministic replay easy | No existing project covers >85% |

Effort: HIGH. Even DragonflyDB (well-funded C++ team) has ~240 commands.

#### Path B: RESP Proxy over Embedded Redis Binary

Run a real Redis binary as a subprocess and proxy RESP traffic through RedisBox for hook injection.

```
Application → ioredis → [RedisBox RESP Proxy] → [Real Redis subprocess]
                              ↑
                         Hook layer here
```

| Pro | Con |
|-----|-----|
| 100% compatibility automatically | Requires Redis binary (no browser) |
| Zero command implementation | Node.js only |
| Always up-to-date with Redis | Virtual time requires external patching |
| Proven pattern (redis-memory-server) | Deterministic replay limited |

The `redis-memory-server` npm package already demonstrates this pattern — it downloads and manages a real Redis binary programmatically.

#### Path C: Hybrid — Embedded Binary + JS Fallback

```
Node.js:   RESP Proxy → Real Redis subprocess (100% fidelity)
Browser:   JS Engine → In-memory (best-effort coverage, growing over time)
Testing:   Verify JS engine parity against real Redis using Redis TCL test suite
```

| Pro | Con |
|-----|-----|
| 100% in Node.js immediately | Browser coverage grows incrementally |
| JS engine catches up over time | Two implementations to maintain |
| Best of both worlds | More complex architecture |

### Redis Test Suite for Verification

Redis ships a comprehensive TCL test suite (`tests/` directory) supporting external server mode:

```bash
./runtest --host <addr> --port <port>
```

This can verify our implementation against real Redis behavior. Categories: unit tests, integration tests, cluster tests, sentinel tests. Tests use a tagging system (`external:skip`, `cluster:skip`, etc.) for compatibility.

**Limitation**: "You'll have to modify the test suite heavily to meet your needs" (Redis maintainer). Many tests depend on each other, and external mode skips tests that require server restart or config changes.

**Strategy**: Run Redis TCL test suite against our JS engine in external mode. Track pass rate as the coverage metric. Target: matching real Redis behavior for every passing test.

## Decision: Path C (Hybrid)

For 100% command coverage, **Path C** is the recommended approach:

1. **Node.js**: RESP proxy over embedded Redis binary. This gives 100% compatibility immediately. The proxy layer is where hooks attach — intercept/modify/delay/fail commands at the RESP level. For virtual time: use Redis `DEBUG SET-ACTIVE-EXPIRE 0` to disable active expiration, and control TTLs via proxy-level time manipulation.

2. **Browser**: Pure JS engine with incremental command coverage. Start with Tier 1+2 (~100 commands), grow toward full coverage. Each command verified against Redis TCL tests.

3. **Parity verification**: Run Redis TCL test suite against JS engine. Track coverage percentage. Goal: 100% pass rate on applicable tests.

### Why This Beats Pure JS Alone

- 500+ commands is 6-12 months of careful implementation and testing
- Edge cases (OBJECT ENCODING behavior, type coercion, error messages) are extremely hard to match perfectly
- DragonflyDB (a well-funded startup) still has ~240 commands after years of development
- The proxy approach gives users 100% Redis on day one

### Why This Beats WASM

- No WASM Redis exists to use
- Building one requires maintaining C patches across Redis versions
- WASM hook integration is much harder than RESP proxy interception
- WASM can't run in browser without heavy Emscripten scaffolding

### Architecture

```
┌── Node.js Mode ─────────────────────────────────┐
│                                                   │
│  App → ioredis → [Custom Connector]               │
│                        ↓                          │
│              ┌─────────────────┐                  │
│              │  RESP Proxy     │ ← hooks          │
│              │  (Sim hooks     │                   │
│              │   attach here)  │                   │
│              └────────┬────────┘                   │
│                       ↓                            │
│              ┌─────────────────┐                   │
│              │  Redis Binary   │ (subprocess)      │
│              │  (real Redis)   │                   │
│              └─────────────────┘                   │
└───────────────────────────────────────────────────┘

┌── Browser Mode ─────────────────────────────────┐
│                                                   │
│  App → RedisBox API → [JS Engine]                 │
│                          ↓                        │
│              ┌─────────────────┐                  │
│              │  Hook Layer     │ ← hooks          │
│              └────────┬────────┘                  │
│                       ↓                           │
│              ┌─────────────────┐                  │
│              │  In-Memory      │                  │
│              │  Data Structures│                  │
│              └────────┬────────┘                  │
│                       ↓                           │
│              ┌─────────────────┐                  │
│              │  OBI Hooks      │ (time, random)   │
│              └─────────────────┘                  │
└───────────────────────────────────────────────────┘
```

## Implementation Priority

### Phase 1: Node.js 100% Coverage (proxy approach)

1. RESP2 parser/serializer (needed for proxy)
2. Embedded Redis binary manager (download/start/stop)
3. RESP proxy with command interception hooks
4. ioredis Custom Connector adapter
5. Basic RedisSim (latency, errors, command interception)
6. Virtual time control via proxy (disable active expiry, TTL manipulation)

### Phase 2: Browser JS Engine (incremental)

7. In-memory engine: Tier 1 commands (strings, keys, connection)
8. Tier 2 commands (hashes, lists, sets, sorted sets)
9. Tier 3 commands (pub/sub, transactions, streams)
10. Tier 4 commands (blocking, bitmap, geo, HyperLogLog, scripting)
11. Module commands (JSON, Search, TimeSeries) — as needed

### Phase 3: Parity Verification

12. Adapt Redis TCL test suite for external mode testing
13. CI pipeline: run TCL tests against JS engine, track pass rate
14. Close gaps between JS engine and real Redis behavior

## Established Facts

1. Redis 8.0 has ~650+ commands including modules, ~460+ core commands
2. No production-ready Redis-WASM exists
3. Only Redis forks (KeyDB) achieve true 100% compatibility; reimplementations (DragonflyDB ~240, Kvrocks ~308) achieve partial
4. RESP proxy over real Redis binary gives 100% coverage immediately (Node.js)
5. Redis TCL test suite supports external server mode for compatibility verification
6. Browser mode requires pure JS engine with incremental coverage
7. Virtual time: proxy can disable active expiry + manipulate TTLs; JS engine uses OBI time hook

## Detailed Research

- [Existing implementations](existing-implementations.md) — survey of JS/WASM Redis projects
- [RESP protocol](resp-protocol.md) — wire protocol analysis
- [Redis internals](redis-internals.md) — expiration, eviction, pub/sub, transactions
- [Architecture](architecture.md) — RedisBox design
- [Full coverage strategy](full-coverage-strategy.md) — analysis of paths to 100% command coverage

---

[← Back](README.md)
