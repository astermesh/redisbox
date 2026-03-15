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

## Paths Considered

### Path A: Pure JS Reimplementation (chosen)

Build all commands in TypeScript from scratch.

#### Effort Estimate

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

#### Risks

1. Edge case parity: Redis's exact behavior on edge cases is documented by behavior, not spec
2. Error message matching: Clients may parse error strings. Must replicate exact Redis error messages
3. OBJECT ENCODING: Applications may check internal encoding. Must track encoding transitions
4. Version drift: Redis adds ~10-20 new commands per major release

#### Advantages

- Full Sim hook integration on every command
- Virtual time trivial
- Deterministic replay trivial
- Browser support (via NodeBox)
- No external binary dependency
- Single code path for all environments

### Path B: RESP Proxy over Embedded Redis Binary (rejected)

Run a real Redis binary as a subprocess and proxy RESP traffic.

**Rejected because:**
- Requires platform-specific Redis binary — no browser, heavy distribution
- Not a real implementation — just a wrapper, doesn't achieve project goals
- Virtual time is imperfect (can't control Redis internal clock)
- Deterministic replay is limited
- Two separate systems to manage and debug
- Contradicts SimBox philosophy of self-contained boxes

### Path C: Hybrid (rejected)

Combine proxy (Node.js) and JS engine (browser).

**Rejected because:**
- Same problems as Path B for Node.js mode
- Two implementations to maintain
- Architectural complexity without clear benefit
- The proxy is a crutch that delays building the real engine

## Decision

**Path A: Pure JS Engine.** Full reimplementation of Redis in TypeScript.

The effort is significant (~3-4 months for core commands) but the result is a proper, self-contained Redis emulator with:
- Full hook integration at every level
- Perfect virtual time and deterministic replay
- No external dependencies
- Single code path across Node.js and browser (via NodeBox)

### Testing Strategy

Differential testing is the primary parity verification technique:

```
For each command:
  1. Write test cases based on Redis docs
  2. Run tests against real Redis → capture expected results
  3. Run tests against JS engine → compare
  4. Fix discrepancies
```

Additionally, adapt Redis TCL test suite for external mode testing. Track pass rate as the coverage metric. Target: 100% pass rate on applicable tests.

Reference: **fakeredis-py** runs every test against both fake and real Redis — same test, two backends, compare results. This is the model to follow.

---

[← Back](README.md)
