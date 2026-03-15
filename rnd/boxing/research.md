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

**Key insight**: DragonflyDB and Kvrocks are the best analogies — they reimplemented Redis from scratch and achieved high (but not 100%) coverage. RedisBox follows the same path in TypeScript.

### Redis Test Suite for Verification

Redis ships a comprehensive TCL test suite (`tests/` directory) supporting external server mode:

```bash
./runtest --host <addr> --port <port>
```

This can verify our implementation against real Redis behavior. Categories: unit tests, integration tests, cluster tests, sentinel tests. Tests use a tagging system (`external:skip`, `cluster:skip`, etc.) for compatibility.

**Limitation**: "You'll have to modify the test suite heavily to meet your needs" (Redis maintainer). Many tests depend on each other, and external mode skips tests that require server restart or config changes.

**Strategy**: Run Redis TCL test suite against our JS engine in external mode. Track pass rate as the coverage metric. Target: matching real Redis behavior for every passing test.

## Decision: Pure JS Engine

RedisBox is a **full reimplementation of Redis in TypeScript**. No wrappers over real Redis binaries, no proxy, no subprocess management.

### Architecture

```
RedisBox = TCP Server + RESP Protocol + In-Memory Engine + Hooks

Runs natively on Node.js.
Runs in browser via NodeBox (SimBox ecosystem).
One codebase, one interface, one code path.
```

```
┌─────────────────────────────────────────────────┐
│                                                   │
│  Client → TCP / RESP → [Command Dispatcher]       │
│                               ↓                   │
│                 ┌──────────────────────┐           │
│                 │  IBI Hooks           │←── Sim    │
│                 └──────────┬───────────┘           │
│                            ↓                       │
│                 ┌──────────────────────┐           │
│                 │  In-Memory Engine    │           │
│                 │  ┌────────────────┐  │           │
│                 │  │ String Store   │  │           │
│                 │  │ Hash Store     │  │           │
│                 │  │ List Store     │  │           │
│                 │  │ Set Store      │  │           │
│                 │  │ SortedSet Store│  │           │
│                 │  │ Stream Store   │  │           │
│                 │  │ PubSub Engine  │  │           │
│                 │  │ Script Engine  │  │           │
│                 │  └────────────────┘  │           │
│                 └──────────┬───────────┘           │
│                            ↓                       │
│                 ┌──────────────────────┐           │
│                 │  OBI Hooks           │           │
│                 │  (time, random,      │←── Sim    │
│                 │   persist)           │           │
│                 └──────────────────────┘           │
└─────────────────────────────────────────────────────┘
```

### Why Pure JS Engine

- **Full control**: every command passes through our code — hooks, virtual time, deterministic replay all work naturally
- **No external dependencies**: no Redis binary, no subprocess, no platform-specific binaries
- **Single code path**: same engine runs on Node.js and in browser (via NodeBox)
- **SimBox integration**: IBI/OBI hooks attach directly to the engine, not to a wire protocol proxy
- **Virtual time is trivial**: engine controls its own clock, no need to hack Redis internals
- **Deterministic replay**: all randomness and time controlled at the engine level

### Scale of Implementation

The effort is significant (~460 core commands) but feasible:

| Tier | Commands | Est. effort |
|------|----------|-------------|
| Strings + Keys | ~65 | 1-2 weeks |
| Hashes + Lists + Sets | ~67 | 1-2 weeks |
| Sorted Sets | 46 | 2-3 weeks |
| Streams | 27 | 2-3 weeks |
| Pub/Sub | 12 | 1 week |
| Transactions | 4 | 3-5 days |
| Scripting | 12 | 2-3 weeks |
| Blocking cmds | ~10 | 1-2 weeks |
| Server/Connection | ~50 | 1-2 weeks |
| Cluster (stubs) | 32 | 1 week |
| Bitmap/HLL/Geo | 21 | 1-2 weeks |
| ACL | 11 | 1 week |
| **Core total** | ~460 | **~3-4 months** |
| Modules | ~190 | ~3-4 months |
| **Grand total** | ~650 | **~6-8 months** |

### Parity Verification

Every command is verified against real Redis via differential testing:
1. Write test cases based on Redis docs
2. Run tests against real Redis — capture expected results
3. Run tests against JS engine — compare
4. Fix discrepancies

Additionally, adapt Redis TCL test suite for external mode testing against our engine.

## Implementation Priority

### Phase 1: Engine Foundation

1. RESP2 parser/serializer
2. TCP server (accepts Redis client connections)
3. Command dispatcher with metadata from `@ioredis/commands`
4. In-memory keyspace (databases, entries, TTL)
5. String commands (Tier 1)
6. Key/generic commands (DEL, EXISTS, EXPIRE, TTL, KEYS, SCAN, etc.)
7. Connection commands (PING, ECHO, SELECT, AUTH, HELLO, CLIENT, etc.)

### Phase 2: Core Data Structures

8. Hash commands
9. List commands
10. Set commands
11. Sorted set commands
12. Expiration system (lazy + active deletion)

### Phase 3: Advanced Features

13. Pub/Sub
14. Transactions (MULTI/EXEC/WATCH)
15. Streams + consumer groups
16. Blocking commands (BLPOP, BRPOP, XREAD BLOCK, etc.)
17. Bitmap, HyperLogLog, Geo

### Phase 4: Scripting & Specialized

18. Lua scripting (EVAL/EVALSHA via wasmoon-lua5.1 or fengari)
19. Redis Functions (FUNCTION LOAD, FCALL)
20. ACL system
21. Cluster command stubs
22. Server commands (INFO, CONFIG, DBSIZE, etc.)

### Phase 5: Modules & Parity

23. JSON module commands
24. Probabilistic data structures (Bloom, Cuckoo, CMS, T-Digest, Top-K)
25. TimeSeries, Search, Vector Set — as needed
26. Redis TCL test suite integration
27. CI pipeline for parity verification

## Established Facts

1. Redis 8.0 has ~650+ commands including modules, ~460+ core commands
2. No production-ready Redis-WASM exists
3. Only Redis forks (KeyDB) achieve true 100% compatibility; reimplementations (DragonflyDB ~240, Kvrocks ~308) achieve partial
4. Redis TCL test suite supports external server mode for compatibility verification
5. Pure JS engine gives full control over time, randomness, and deterministic replay
6. NodeBox provides browser runtime — RedisBox doesn't need browser-specific code paths
7. Virtual time: engine controls its own clock via OBI hooks

## Detailed Research

- [Existing implementations](existing-implementations.md) — survey of JS/WASM Redis projects
- [RESP protocol](resp-protocol.md) — wire protocol analysis
- [Redis internals](redis-internals.md) — expiration, eviction, pub/sub, transactions
- [Architecture](architecture.md) — RedisBox design
- [Full coverage strategy](full-coverage-strategy.md) — analysis of paths to 100% command coverage
- [NodeBox integration](nodebox-integration.md) — single-runtime architecture via NodeBox

---

[← Back](README.md)
