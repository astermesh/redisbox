# Existing Redis Implementations in JS/WASM

Survey of available projects that could serve as foundation, reference, or alternative for RedisBox. Covers mock libraries, JS servers, RESP parsers, data structure libraries, non-JS Redis alternatives, and WASM attempts.

## Pure JS Mock Libraries

### ioredis-mock (most widely used mock)

- **GitHub**: github.com/stipsan/ioredis-mock — 383 stars
- **npm**: ~192K-700K weekly downloads, actively maintained (v8.13.1)
- **License**: MIT
- **Compatibility**: 385 of 568 commands = **68%** (per compat.md)

Capabilities:
- Strings, lists, hashes, sets, sorted sets, streams (XADD/XREAD/XRANGE)
- Pub/Sub with `createConnectedClient()`
- Lua scripting via `defineCommand()` and `eval()`
- Experimental Cluster support
- Shared state between instances on same host/port (since v6)
- **Browser build**: `import Redis from 'ioredis-mock/browser.js'`

Limitations:
- Does NOT implement RESP wire protocol — emulates ioredis JS API only
- Missing: geospatial, HyperLogLog, blocking operations (BLPOP/BRPOP), ACL, BITFIELD
- Performance not a priority — designed for testing
- Testing approach: multiple Jest configs for standard, browser, integration, and direct Redis comparison

**RedisBox relevance**: Best JS mock option for ioredis users. Cannot accept connections from arbitrary Redis clients. Good reference for command behavior expectations. The compat.md file is a useful checklist for tracking our own coverage.

### redis-mock (for node-redis v3)

- **GitHub**: github.com/yeahoffline/redis-mock
- **npm**: ~120K weekly downloads (still used due to legacy projects)
- **Status**: Not updated in ~5 years, targets deprecated node-redis v3 API
- Claims "100% Redis-compatible (see Cross Verification)" — verification runs tests against both mock and real Redis

Limitations:
- No RESP, no browser support, no RESP3
- Old API, incompatible with node-redis v4+

**RedisBox relevance**: LOW. Legacy, but their cross-verification approach (running same tests against mock and real Redis) is worth adopting.

### @outofsync/memory-cache

- **GitHub**: github.com/OutOfSyncStudios/memory-cache — 5 stars
- **License**: MIT
- **Compatibility**: **224 of 267 (~85%) Redis commands** — highest coverage of any pure JS implementation

Capabilities:
- Strings, lists, hashes, sets, sorted sets
- Over 500 unit tests
- Pure JavaScript, no native dependencies

Limitations:
- No RESP wire protocol, no browser support
- Missing: blocking operations, EVAL/EVALSHA, SCAN variants, pub/sub
- Small community, limited maintenance

**RedisBox relevance**: MEDIUM. Excellent reference for command logic implementations. The 85% coverage means most command edge cases are already worked out in their source code.

### redis-js (Rarefied Redis Project)

- **GitHub**: github.com/nicholascloud/redis-js
- **npm**: ~2,696 weekly downloads
- **License**: MIT

Capabilities:
- All string, hash, set, sorted set commands implemented
- BLPOP/BRPOP/BRPOPLPUSH blocking commands implemented
- Pub/Sub implemented
- Transactions implemented
- SCAN command implemented
- Browser testing via Karma/Mocha/Chai
- `npm run implemented` / `npm run unimplemented` to check coverage

Limitations:
- No RESP wire protocol
- No Lua scripting, no persistence
- Last published ~10 years ago (v0.1.2)
- Single source file (redis-mock.js)

**RedisBox relevance**: MEDIUM. Useful reference for blocking command implementations. Browser testing support is notable.

## JS Redis Servers (with RESP protocol)

### Redjs (closest to RedisBox needs)

- **GitHub**: github.com/ctoesca/redjs — 18 stars
- **License**: MIT

Capabilities:
- **Implements actual Redis wire protocol (RESP)** — works with ioredis, node-redis, redis-cli
- Strings, lists, hashes, sets
- Pub/Sub, Transactions (MULTI/EXEC/DISCARD)
- Key management (DEL, EXISTS, KEYS, EXPIRE, TTL)
- Server commands (FLUSHDB, FLUSHALL, INFO, MONITOR)
- Uses Node.js `net` module for TCP

Limitations:
- Performance ~2-3x slower than Redis (acceptable for emulation)
- No sorted sets, no streams, no persistence, no scripting
- Small community, limited maintenance
- ~60 commands total

**RedisBox relevance**: HIGH. Demonstrates that a working RESP server in JS is feasible. Architecture is a good starting point but needs significant expansion for 100% coverage.

### CorvoStore

- **GitHub**: github.com/corvostore/corvoserver — 8 stars
- **License**: MIT

Capabilities:
- RESP protocol over TCP, interoperable with Redis clients
- Five data types including **sorted sets**
- **LRU eviction** with configurable max memory
- Optional AOF persistence

Limitations:
- No pub/sub, no transactions, no streams
- ~50 commands

**RedisBox relevance**: MEDIUM-HIGH. Clean architecture, sorted sets + LRU are valuable reference implementations.

## Embedded Redis for Testing

### redis-memory-server

- **GitHub**: github.com/mhassan1/redis-memory-server
- **npm**: Actively maintained

Capabilities:
- Downloads and manages a real Redis binary programmatically
- Starts Redis on a random port for tests
- Automatic cleanup on process exit
- Configurable Redis version

**RedisBox relevance**: LOW. RedisBox is a full JS reimplementation, not a wrapper over a real Redis binary. This package is an alternative approach (embedded binary) that we explicitly chose not to follow.

### testcontainers (Redis)

Docker-based approach: spin up a real Redis container for tests. Popular in Java/Go ecosystems, available for Node.js via `testcontainers` npm package.

**RedisBox relevance**: MEDIUM. Heavier than subprocess but more isolated. Relevant as a comparison point — RedisBox should be lighter and faster than testcontainers.

## RESP Protocol Parsers

| Package | Protocol | Architecture | Notes |
|---------|----------|-------------|-------|
| `redis-parser` (NodeRedis) | RESP2 | Callback-based, optional C++ binding | Battle-tested, used by ioredis/node-redis. ~9 years old |
| `respjs` | RESP2 | EventEmitter-based | Both parsing and serialization |
| `redis-parser-ts` | RESP2 | TypeScript, buffer-safe streaming | Modern, good for new TypeScript projects |
| `resp3` (tinovyatkin) | RESP3 | Pure streaming, ~300 LOC | Minimal, no dependencies |
| `@ioredis/commands` | N/A | Command metadata | Flags, arity, key positions for every command |

**Recommendation**: For RESP parsing, `redis-parser` is proven in production (used by ioredis and node-redis). For serialization, write our own — it's <100 lines. For command metadata (validation, routing), use `@ioredis/commands`.

## Data Structure Libraries

| Need | Package | Notes |
|------|---------|-------|
| Sorted sets | `tlhunter-sorted-set` | ~2.37M weekly downloads, Redis-like API, skip list |
| LRU cache with TTL | `lru-cache` (isaacs) | Most popular LRU+TTL, TypeScript |
| Sorted map | `sorted-btree` | B+Tree based, TypeScript, fast cloning |
| Glob pattern matching | `minimatch` or `picomatch` | For KEYS, PSUBSCRIBE patterns |
| SHA1 hashing | `crypto` (Node built-in) | For EVALSHA script caching |

## Non-JS Redis-Compatible Implementations

For context on achieving high Redis compatibility in other languages:

### KeyDB (C++ Redis fork)

- **GitHub**: github.com/Snapchat/KeyDB — 11K+ stars
- **Approach**: Fork of Redis source, keeps in sync
- **Coverage**: **100%** — it IS Redis, with multithreading added
- Protocol: RESP2/RESP3, module-compatible

Key insight: Only forks of actual Redis C source achieve true 100% compatibility. Even well-funded reimplementations don't reach it.

### DragonflyDB (C++ reimplementation)

- **GitHub**: github.com/dragonflydb/dragonfly — 28K+ stars
- **Approach**: Ground-up reimplementation in C++
- **Coverage**: ~240+ commands (as of 2025), claims "100% API compatible"
- Protocol: RESP2/RESP3 + Memcache
- Features: RedisJSON, Search, Bloom filters, Streams

Note: "100% API compatible" claim refers to wire protocol compatibility, not 100% command coverage. Actual command count is ~240 of ~460+ core Redis commands.

### Apache Kvrocks (C++ with RocksDB)

- **GitHub**: github.com/apache/kvrocks — 3K+ stars
- **Approach**: C++ reimplementation with RocksDB storage
- **Coverage**: ~**308 commands** across 21 categories
- Protocol: RESP2/RESP3
- Features: Strings, hashes, lists, sets, sorted sets, streams, geo, JSON, search, bloom filters, functions

Supports Redis TCL test suite in external mode for compatibility verification.

### Garnet (Microsoft, C#)

- **GitHub**: github.com/microsoft/garnet — 10K+ stars
- **Approach**: C# reimplementation from Microsoft Research
- **Coverage**: "Large and growing subset" — not 100%, explicitly stated as "close-enough starting point"

### mini-redis (Rust, educational)

- **GitHub**: github.com/tokio-rs/mini-redis
- **Approach**: Minimal Redis implementation for learning Tokio
- **Coverage**: ~5-10 commands (GET, SET, PUBLISH, SUBSCRIBE, PING)
- Purpose: Teaching async Rust, NOT production use

**RedisBox relevance**: Demonstrates that a minimal RESP server is achievable in few hundred lines. Good architectural reference for the "core loop" of a Redis server.

## WASM-Based Redis

### Fluence Labs Redis-to-WASM Port (2019)

Research/experimental. Compiled Redis C source to WASM using Clang/WASI.

Technical details:
- Redis is mostly single-threaded event loop — fits WASM's single-thread model
- Background threads and external I/O had to be stripped
- Used WASI (not Emscripten) for deterministic execution
- Challenges: `setjmp`/`longjmp` unavailable, Lua interpreter porting failed
- Result: Working WASM binary for Fluence's decentralized compute platform
- No public npm package, no browser deployment

### WASM Feasibility Assessment

Key challenges for compiling Redis to WASM:

1. **Syscalls**: Redis core contains many POSIX syscalls for networking, file I/O, process management. All must be stubbed or emulated.
2. **Networking**: Emscripten provides WebSocket-based networking (not raw TCP). Redis's event loop (`ae.c`) would need adaptation.
3. **Lua interpreter**: Redis embeds Lua 5.1. Porting Lua to WASM is possible but adds complexity.
4. **Memory management**: Redis uses jemalloc — would need WASM-compatible allocator.
5. **Time functions**: `gettimeofday()`, `clock_gettime()` — must be hooked for virtual time.
6. **Random**: `random()` — must be seeded for determinism.
7. **Fork**: `BGSAVE` uses `fork()` — impossible in WASM, must be disabled.

**Verdict**: Technically feasible (Fluence proved it) but:
- No production-ready artifact exists
- Maintaining C patches across Redis versions is expensive
- Hook integration at WASM boundary is much harder than in JS
- A proxy over real Redis would give compatibility but isn't the approach we're taking

## Summary Table

| Project | Lang | RESP | Commands | Browser | Maintained | RedisBox Fit |
|---------|------|------|----------|---------|------------|------------|
| ioredis-mock | JS | No | 385 (68%) | Yes | Yes | API mock |
| memory-cache | JS | No | 224 (85%) | No | Limited | Reference |
| redis-js | JS | No | ~200 | Yes | No | Reference |
| Redjs | JS | Yes | ~60 | No | Limited | Architecture ref |
| CorvoStore | JS | Yes | ~50 | No | Limited | Reference |
| redis-memory-server | JS | N/A | 100% (real) | No | Yes | Different approach |
| KeyDB | C++ | Yes | 100% (fork) | No | Yes | Benchmark |
| DragonflyDB | C++ | Yes | ~240 | No | Yes | Benchmark |
| Kvrocks | C++ | Yes | ~308 | No | Yes | Benchmark |
| Garnet | C# | Yes | Large subset | No | Yes | Benchmark |

---

[← Back](README.md)
