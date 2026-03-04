# Full Coverage Strategy: Paths to 100% Redis Command Coverage

Analysis of approaches to achieve 100% Redis command coverage.

## The Scale of 100%

Redis 8.0 has ~460 core commands and ~650+ including modules.

### Core Commands (~460)

| Category | Count | Complexity |
|----------|-------|------------|
| String | 25 | Low — Map operations |
| Hash | 28 | Low-Medium — Map + per-field TTL (7.4+) |
| List | 22 | Medium — doubly-linked list, blocking ops |
| Set | 17 | Low — Set operations |
| Sorted Set | 46 | HIGH — skip list, dual index, lex ranges |
| Stream | 27 | HIGH — radix tree, consumer groups, blocking |
| Pub/Sub | 12 | Medium — subscription state, pattern matching |
| Transaction | 4 | Medium — command queue, WATCH |
| Scripting | 12 | HIGH — Lua interpreter embedding |
| Keys/Generic | 40+ | Medium — SCAN cursors, SORT, OBJECT |
| Connection | 19 | Low — CLIENT, HELLO, AUTH |
| Server | 30+ | Medium — INFO, CONFIG, SLOWLOG, DEBUG |
| Cluster | 32 | LOW priority — topology, not data |
| Bitmap | 6 | Medium — bitwise ops on strings |
| HyperLogLog | 5 | Medium — probabilistic counting |
| Geo | 10 | Medium — geohash + sorted set |
| ACL | 11 | Medium — user/permission management |

### Module Commands (~190)

| Module | Count | Notes |
|--------|-------|-------|
| JSON | 24 | JSONPath queries, nested mutations |
| Search (FT) | 27 | Full-text search, vector search |
| TimeSeries (TS) | 24 | Time-bucketed data, aggregations |
| Bloom Filter (BF) | 11 | Probabilistic membership |
| Cuckoo Filter (CF) | 10 | Probabilistic membership (deletable) |
| Count-Min Sketch | 6 | Frequency estimation |
| T-Digest | 13 | Percentile estimation |
| Top-K | 7 | Frequent items |
| Vector Set | 12 | Vector similarity search |

## Path A: Pure JS Reimplementation

Build all commands in TypeScript from scratch.

### Effort Estimate

| Tier | Commands | Est. effort | Notes |
|------|----------|-------------|-------|
| Strings + Keys | ~65 | 1-2 weeks | Straightforward Map/string ops |
| Hashes + Lists + Sets | ~67 | 1-2 weeks | Standard data structure ops |
| Sorted Sets | 46 | 2-3 weeks | Complex: skip list, lex ranges |
| Streams | 27 | 2-3 weeks | Complex: consumer groups, blocking |
| Pub/Sub | 12 | 1 week | Pattern matching, subscriber mode |
| Transactions | 4 | 3-5 days | Command queue, WATCH |
| Scripting | 12 | 2-3 weeks | Lua interpreter embedding |
| Blocking cmds | ~10 | 1-2 weeks | Cross-client notification, timeouts |
| Server/Connection | ~50 | 1-2 weeks | Many stubs, INFO is complex |
| Cluster | 32 | 1 week | Mostly stubs for single-node |
| Bitmap/HLL/Geo | 21 | 1-2 weeks | Specialized algorithms |
| ACL | 11 | 1 week | Permission model |
| **Core total** | ~460 | **~3-4 months** | |
| Modules total | ~190 | ~3-4 months | JSON, Search, TS, probabilistic |
| **Grand total** | ~650 | **~6-8 months** | |

### Risks

1. Edge case parity: Redis's exact behavior on edge cases is documented by behavior, not spec
2. Error message matching: Clients may parse error strings. Must replicate exact Redis error messages
3. OBJECT ENCODING: Applications may check internal encoding. Must track encoding transitions
4. Version drift: Redis adds ~10-20 new commands per major release

### Advantages

- Full Sim hook integration on every command
- Virtual time trivial
- Deterministic replay trivial
- Browser support
- No external binary dependency

## Path B: RESP Proxy over Embedded Redis Binary

Run a real Redis subprocess and intercept RESP traffic.

### Effort Estimate

| Component | Est. effort | Notes |
|-----------|-------------|-------|
| RESP parser (for proxy) | 3-5 days | Can use existing `redis-parser` |
| RESP serializer | 1-2 days | Trivial |
| Proxy core (forward/intercept) | 1-2 weeks | Command parsing, hook dispatch |
| Redis binary manager | 1 week | Download, start, stop, port allocation |
| ioredis Custom Connector | 3-5 days | Duplex stream pair |
| Hook layer (IBI + OBI) | 1 week | |
| Virtual time via proxy | 1 week | Active-expire disable, TTL rewriting |
| Basic RedisSim | 1 week | Latency, errors, eviction injection |
| **Total** | **~5-7 weeks** | |

### Risks

1. No browser support: Requires Redis binary, Node.js only
2. Virtual time is imperfect: Can't fully control Redis's internal clock
3. Deterministic replay limited
4. Binary distribution: Platform-specific, download required

### Advantages

- 100% command coverage immediately
- Zero command implementation effort
- Always matches Redis behavior exactly
- Modules work out of the box
- Can use any Redis version

## Path C: Hybrid (Recommended)

Combine proxy (Node.js) and JS engine (browser). Converge over time.

### Phase 1: Proxy-First

Build the proxy layer first. This gives:
- 100% command coverage in Node.js on day one
- Hook layer working and proven
- ioredis integration working
- RedisSim working

### Phase 2: JS Engine (incremental)

Build the JS engine in parallel:
- Each command verified against real Redis via cross-testing
- Browser support grows incrementally
- Node.js users can optionally use JS engine for lighter weight

### Phase 3: Parity Verification

Use Redis TCL test suite (external mode) to verify JS engine:
- Track pass rate as coverage metric
- Target: 100% pass rate on applicable tests

### Testing Strategy

```
For each command:
  1. Write test cases based on Redis docs
  2. Run tests against real Redis → capture expected results
  3. Run tests against JS engine → compare
  4. Run tests against proxy → compare
  5. Fix discrepancies
```

## Comparison of Paths

| Factor | Path A: Pure JS | Path B: Proxy | Path C: Hybrid |
|--------|----------------|---------------|----------------|
| Time to 100% (Node.js) | 6-8 months | 5-7 weeks | 5-7 weeks |
| Time to 100% (Browser) | 6-8 months | Never | 6-8 months |
| Sim hook quality | Excellent | Good | Excellent (JS) / Good (proxy) |
| Virtual time | Perfect | Imperfect | Perfect (JS) / Imperfect (proxy) |
| Deterministic replay | Full | Limited | Full (JS) / Limited (proxy) |
| Browser support | Yes | No | Yes (JS engine) |

## Decision

**Path C (Hybrid)** is the recommended approach:

1. Proxy gives 100% coverage immediately for Node.js users
2. JS engine grows incrementally for browser and deterministic replay
3. Cross-verification ensures JS engine converges toward real Redis behavior
4. Users can choose mode: proxy (100% compat) vs JS (lighter, deterministic)

Long-term vision: JS engine reaches near-100% core commands. Proxy remains for module commands (JSON, Search) which are too complex to reimplement. Users get full Redis in both environments.

---

[← Back](README.md)
