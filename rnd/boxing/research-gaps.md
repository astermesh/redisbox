# Research Gaps

Pre-planning audit — gaps identified in existing research that must be resolved before epic/story breakdown.

## Gap Status Summary

| Gap | Topic | Status | Confidence | Blockers remaining |
|-----|-------|--------|------------|--------------------|
| G1 | ioredis Custom Connector contract | **CLOSED** | HIGH | none (validation PoC recommended) |
| G2 | Redis binary management | **CLOSED** | HIGH | none |
| G3 | Redis licensing implications | **CLOSED** | HIGH | none |
| G4 | TCL test suite integration | **CLOSED** | HIGH | none (validation experiment recommended) |
| G5 | Virtual time design | **CLOSED** | HIGH | none (validation experiments recommended) |

---

## G1: ioredis Custom Connector contract

**Status: closed** (documentation-level analysis sufficient for planning; validation PoC recommended before implementation)

### Findings

**1. The `Connector` option is a public, typed API.**

ioredis v5+ exposes `Connector?: ConnectorConstructor` in `CommonRedisOptions`. The `ConnectorConstructor` interface requires a class implementing `AbstractConnector` with a `connect()` method that returns a `NetStream` (a `net.Socket`-compatible Duplex stream).

- Source: `lib/redis/RedisOptions.ts`, `lib/connectors/ConnectorConstructor.ts` in [redis/ioredis](https://github.com/redis/ioredis)
- The built-in `StandaloneConnector` and `SentinelConnector` both implement `AbstractConnector`
- The API has been stable across v4 and v5

**2. The stream is a raw byte pipe — ioredis handles all RESP parsing internally.**

After `connect()` returns a stream, ioredis attaches a `DataHandler` (from `redis-parser`) to the stream's `data` event. All RESP parsing, including pub/sub push messages, pipeline responses, and MULTI/EXEC results, happens inside ioredis on the client side. The stream itself is transport-agnostic — it just moves bytes.

- Source: `lib/redis/event_handler.ts` — `connectHandler()` wires the stream to `DataHandler`
- This means: **pub/sub, pipelines, and MULTI/EXEC all work through any Duplex stream** as long as the other end speaks valid RESP

**3. Duplex stream pair approach is viable.**

Using `duplexpair` (npm package) or Node.js `stream.Duplex.from()` to create an in-memory stream pair: one side for ioredis, one side for RedisBox's proxy/engine. This eliminates TCP overhead entirely.

- The `Connector` option is not officially documented in the README but is a public TypeScript type and used by the built-in connectors
- Note: `NetStream` is typed as `net.Socket | tls.TLSSocket` (both are Duplex streams under the hood). A custom connector must return something compatible with this type

**4. Pub/sub works through the same stream.**

ioredis handles pub/sub by detecting push messages (`message`, `pmessage`) in the RESP parser. The subscriber state is managed client-side. As long as the server-side sends valid RESP push data through the stream, pub/sub works identically to TCP.

**5. Backpressure and encoding.**

Node.js Duplex streams handle backpressure natively via `highWaterMark` and the `write()` return value. ioredis writes Buffer data to the stream, so the encoding is always binary (no string encoding issues).

### Unknowns resolved

| Question | Answer |
|----------|--------|
| Pub/sub support? | Yes — RESP push data flows through the stream like any other data |
| Pipelines and MULTI/EXEC? | Yes — ioredis handles these client-side via RESP parsing |
| RESP push messages? | Yes — ioredis's `DataHandler` parses all RESP types from the stream |
| Duplex stream contract? | Standard Node.js Duplex with binary encoding; backpressure via `highWaterMark` |
| Maintained API or internal? | Public typed API (`ConnectorConstructor`), stable across v4/v5, but undocumented in README |

### Remaining validation (recommended, not blocking)

- **V1.1:** Build minimal PoC: ioredis via Custom Connector → Duplex pair → echo server that replies `+PONG\r\n` to `PING`. Confirm connection lifecycle.
- **V1.2:** Extend PoC: ioredis → Duplex pair → real Redis via TCP socket. Exercise SET/GET, pipeline, pub/sub, MULTI/EXEC. Measure latency overhead of in-memory stream vs direct TCP.

### Risk assessment

**Low.** The architecture is sound — ioredis treats the stream as a raw byte pipe and handles all protocol logic internally. The Custom Connector API is typed and used by built-in connectors. The fallback (TCP server on localhost) is always available if Duplex streams cause unexpected issues with specific ioredis features.

---

## G2: Redis binary management

**Status: closed**

### Findings

**1. redis-memory-server is well-proven but has friction.**

- npm: ~141K weekly downloads, MIT license, actively maintained (v0.16.0, Feb 2026)
- **Architecture limitation:** downloads Redis **source** and **compiles locally** via `make` — requires C toolchain (gcc/clang). This is a significant friction point for CI environments without build tools.
- Health check: parses stdout for `"Ready to accept connections"` (no TCP ping loop)
- Orphan cleanup: spawns a separate `redis_killer.js` watcher process — well-designed pattern
- Port allocation: uses `get-port` npm package with internal locking for parallel test safety
- Offline support: via `REDISMS_SYSTEM_BINARY` env var or cached binary

Sources: [redis-memory-server on npm](https://www.npmjs.com/package/redis-memory-server), [GitHub](https://github.com/mhassan1/redis-memory-server)

**2. Valkey provides pre-built binaries — no compilation needed.**

- Pre-built tarballs for Linux x86_64 and arm64 (Ubuntu Jammy/Noble) at `download.valkey.io/releases/`
- macOS: available via Homebrew (`brew install valkey`)
- Windows: not supported (WSL only, same as Redis)
- SHA256 checksums available for verification
- Current versions (verified 2026-03): 9.0.3 (latest), 8.1.6, 8.0.7, 7.2.12 (all released 2026-02-24)

Sources: [Valkey Download](https://valkey.io/download/), [GitHub Releases](https://github.com/valkey-io/valkey/releases)

**3. Port allocation strategy.**

Two viable approaches:
- `get-port` package (pre-allocate port, start Redis on it) — used by redis-memory-server, provides port before startup
- `redis-server --port 0` (OS-assigned ephemeral port, parse from stdout) — zero collision risk but requires stdout parsing

Recommendation: use `get-port` as primary (port known before startup), with `--port 0` as fallback for race conditions.

**4. Startup time.**

Redis/Valkey binary startup: <50ms on modern hardware with no data. The limiting factor is process spawn and readiness detection, not Redis itself.

### Decision

Build a custom binary manager rather than depend on redis-memory-server:
- Default to **Valkey** (pre-built binaries, BSD license, no compilation)
- Support user-provided Redis binary via config
- Adopt proven patterns from redis-memory-server: `get-port` for ports, stdout parsing for health, killer process for orphan cleanup, lockfile for download sync

### Cross-platform coverage

| Platform | Valkey pre-built | Fallback |
|----------|-----------------|----------|
| Linux x64 | Yes (tarball) | Source compilation |
| Linux arm64 | Yes (tarball) | Source compilation |
| macOS x64/arm64 | Homebrew | Source compilation |
| Windows | No | WSL only |

---

## G3: Redis licensing implications

**Status: closed**

### Findings

**1. Redis license history.**

| Version | License | Date |
|---------|---------|------|
| Redis ≤ 7.2 | BSD 3-Clause | Forever |
| Redis 7.4 | RSALv2 + SSPLv1 (dual, pick one) | March 2024 |
| Redis 8.0+ | RSALv2 + SSPLv1 + AGPLv3 (tri, pick one) | May 2025 |

Sources: [Redis license announcement](https://redis.io/blog/redis-adopts-dual-source-available-licensing/), [AGPLv3 announcement](https://redis.io/blog/agplv3/)

**2. RedisBox use case analysis.**

RedisBox downloads a Redis/Valkey binary, spawns it as a subprocess for testing, and shuts it down. End users are developers running tests locally or in CI.

| License | Local testing permitted? | RedisBox downloads binary at runtime? | Bundle binary in npm? |
|---------|------------------------|--------------------------------------|----------------------|
| BSD (Redis ≤ 7.2, Valkey) | Yes | Yes | Yes |
| RSALv2 | Yes — not offering as service | Yes | Gray area |
| SSPLv1 | Yes — not offering as service | Yes | Gray area |
| AGPLv3 (Redis 8+) | Yes — no modifications | Yes (copyleft concerns if modified) | Copyleft concerns |

The key restriction in RSALv2/SSPLv1 targets **making Redis available as a service to third parties**. Developer tooling for local testing is explicitly permitted. Redis's FAQ confirms: "Individual developers and companies using Redis internally face zero changes."

**3. Licensing strategy.**

**Default to Valkey (BSD 3-Clause).** This eliminates all license ambiguity.

- Valkey is a drop-in replacement at the wire protocol level (forked from Redis 7.2.4)
- BSD license allows bundling, redistribution, and commercial use with zero restrictions
- Support Redis as an explicit opt-in via config

**Distribution model:** Download binaries at runtime from official sources (not bundled in npm package). This is the same pattern used by `redis-memory-server` and `mongodb-memory-server`.

### Decision

```
Default:   Valkey binary (BSD, zero license concerns)
Opt-in:    User-provided Redis binary via config
Strategy:  Runtime download, never bundle in npm package
```

---

## G4: TCL test suite integration

**Status: closed** (documentation-level analysis sufficient for planning; validation experiment recommended)

### Findings

**1. External server mode is well-supported.**

Redis's own CI runs an "External Server Tests" workflow via `.github/workflows/external.yml`. The command:

```bash
./runtest --host <host> --port <port>
```

Tests tagged `external:skip` are automatically filtered out. Additional exclusions can be specified:

```bash
./runtest --host <host> --port <port> --tags -needs:repl -needs:save
```

Sources: [Redis tests directory](https://github.com/redis/redis/tree/unstable/tests), [External Server Tests workflow](https://github.com/redis/redis/actions/workflows/external.yml)

**2. Test suite metrics.**

| Metric | Value |
|--------|-------|
| Full CI run duration | ~14-15 minutes |
| External server tests duration | ~7 minutes |
| Test architecture | TCL, client-server model with parallel workers |
| Result reporting | Exit code (0 = pass), stdout with `[ok]`/`[err]` per test |
| Test categories | unit/, integration/, cluster/, sentinel/ |

**3. Test tagging system.**

Tests use tags to indicate capabilities they require:

| Tag | Meaning |
|-----|---------|
| `external:skip` | Skip in external mode (auto-filtered) |
| `needs:save` | Uses SAVE/BGSAVE |
| `needs:config` | Modifies config that can't be restored |
| `needs:reset` | Uses RESET command |
| `needs:repl` | Requires replication (SYNC) |

**4. Relevant subset for RedisBox.**

For the JS engine, the relevant tests are single-instance, no-persistence, no-replication tests. The external mode already handles filtering. Skip categories: cluster, sentinel, replication, persistence.

**5. How other projects use the TCL suite.**

Kvrocks and DragonflyDB both use Redis's TCL test suite in external mode for compatibility verification. They maintain lists of expected failures and track pass rates over time.

### CI integration approach

1. Clone Redis repo (or pin a specific tag)
2. Start RedisBox JS engine on a port
3. Run `./runtest --host 127.0.0.1 --port <port> --tags -needs:repl -needs:save -needs:config`
4. Parse exit code and stdout for pass/fail counts
5. Track pass rate as the JS engine coverage metric

### Remaining validation (recommended, not blocking)

- **V4.1:** Run TCL suite in external mode against a stock Redis to establish baseline pass count and identify which tests run vs. skip
- **V4.2:** Measure exact test count in the external-mode subset

---

## G5: Virtual time design

**Status: closed** (design analysis complete; validation experiments recommended before implementation)

### Findings

**1. `DEBUG SET-ACTIVE-EXPIRE` is available and controllable.**

- Available in all Redis versions; disabled by default in Redis 7.0+ but re-enabled via `enable-debug-command local` in redis.conf
- Since RedisBox spawns its own subprocess and controls the config, this is always available
- Available in Valkey (same codebase lineage)
- Disables only the background active expiration cycle; **lazy expiration still uses real time**

Source: Redis `src/debug.c`, `server.c` (`databasesCron()` and `beforeSleep()` check `server.active_expire_enabled`)

**2. Complete inventory of time-dependent commands.**

**TTL-setting commands (need proxy interception for virtual time base):**
- `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`
- `SET ... EX|PX|EXAT|PXAT`, `SETEX`, `PSETEX`
- `GETEX ... EX|PX|EXAT|PXAT|PERSIST`
- `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT` (Redis 7.4+)
- `RESTORE ... ttl [ABSTTL]`
- `COPY` (copies TTL from source)

**TTL-reading commands (need proxy interception to return virtual-time-based values):**
- `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`
- `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME` (Redis 7.4+)

**Time-reporting commands:**
- `TIME` — proxy intercepts and returns virtual time
- `OBJECT IDLETIME` — uses LRU clock, not directly controllable via proxy
- `CLIENT LIST` — reports `age` and `idle` fields using real time
- `SLOWLOG` — uses wall clock
- `DEBUG SLEEP` — uses real time

**Stream ID generation:**
- `XADD` with `*` auto-generates IDs using `commandTimeSnapshot()` in Redis 7.2+ (was `mstime()` in earlier versions) — proxy must intercept and rewrite. Since `commandTimeSnapshot()` returns a frozen timestamp per command, this aligns with the proxy's command-boundary interception model

**Blocking commands with timeouts:**
- `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`
- `XREAD BLOCK`, `XREADGROUP ... BLOCK`
- `WAIT`, `WAITAOF`
- Timeouts in proxy mode use real time (cannot be virtualized without Lua-level interception)

**3. Virtual time protocol: proxy mode.**

Design:
1. `DEBUG SET-ACTIVE-EXPIRE 0` — disable background expiration
2. Proxy maintains a `virtualNow` counter (starts at `Date.now()`, advances via API)
3. `TIME` command — proxy intercepts and returns `virtualNow` as seconds + microseconds
4. Relative TTLs (`EXPIRE key 60`) — proxy converts to `PEXPIREAT key (virtualNow + 60000)` before forwarding
5. Absolute TTLs (`EXPIREAT key <ts>`) — forwarded as-is (already absolute)
6. TTL reads (`TTL key`) — proxy intercepts response, computes `(storedExpiry - virtualNow) / 1000`
7. Advancing time — when `advanceTime(ms)` is called, proxy increments `virtualNow`, then scans keys with `PEXPIRETIME` < new `virtualNow` and sends `DEL` for each

**Key insight from Redis PR #10300:** Redis freezes time during command execution via `commandTimeSnapshot()`. This means within a single command, time is consistent. The proxy only needs to intercept at the command boundary level, not within commands.

**4. Virtual time protocol: JS engine mode.**

Much simpler — replace the time source entirely:
1. Engine uses a `virtualClock` instead of `Date.now()`
2. All TTL calculations, stream IDs, and time-dependent logic use `virtualClock`
3. `advanceTime(ms)` increments `virtualClock` and triggers active expiration sweep
4. Blocking command timeouts use `virtualClock` — advance time to resolve them
5. Full determinism is achievable since all randomness and time are controlled

**5. Lua script interactions.**

- In proxy mode: Lua scripts call `redis.call('TIME')` which goes through the proxy — returns virtual time. Scripts that set TTLs also go through the proxy — TTLs are rewritten. This is transparent.
- In JS engine mode: The embedded Lua interpreter calls back into the engine, which uses virtual time. Fully controlled.
- Redis constraint: after calling non-deterministic commands (like `TIME`) inside `EVAL`, Redis blocks further dataset modifications. This is a Redis-level constraint that applies regardless of virtual time.

**6. Known limitations (proxy mode).**

| Limitation | Severity | Mitigation |
|------------|----------|------------|
| Lazy expiration uses real time | Medium | Proxy checks virtual TTL on every response and intercepts expired-key responses |
| `OBJECT IDLETIME` uses LRU clock | Low | Cannot virtualize; document as limitation |
| `CLIENT LIST` age/idle fields | Low | Cosmetic; irrelevant for most testing |
| Blocking command timeouts use real time | Medium | For short timeouts, acceptable; for deterministic replay, use JS engine mode |
| `SLOWLOG` uses wall clock | Low | Cosmetic; irrelevant for behavioral testing |
| Background tasks (rehash, defrag) use real time | Low | Does not affect data correctness |
| `XADD *` auto-IDs use `commandTimeSnapshot()` (real time) | Medium | Proxy intercepts and rewrites IDs using virtual time |

### Remaining validation (recommended, not blocking)

- **V5.1:** Confirm `enable-debug-command local` works in Valkey and that `DEBUG SET-ACTIVE-EXPIRE 0` behaves identically
- **V5.2:** Prototype TTL rewriting in proxy: set key with `EXPIRE`, read with `TTL`, verify virtual time is reflected
- **V5.3:** Prototype time advancement: advance virtual clock, verify keys with expired virtual TTLs are cleaned up
- **V5.4:** Test `XADD *` ID interception — confirm proxy can rewrite auto-generated stream entry IDs

---

## Planning-Ready Checklist

| # | Criterion | Met? | Notes |
|---|-----------|------|-------|
| 1 | Architecture decided (hybrid proxy + JS engine) | Yes | Documented in research.md |
| 2 | Connection approach decided (Custom Connector + TCP fallback) | Yes | G1 confirmed viability |
| 3 | Binary management strategy clear | Yes | G2: custom manager, Valkey default |
| 4 | Licensing strategy clear | Yes | G3: Valkey BSD default, Redis opt-in |
| 5 | Verification approach clear (TCL suite external mode) | Yes | G4: ~7 min run, auto-filtering |
| 6 | Virtual time design for proxy mode | Yes | G5: disable active expire + TTL rewriting |
| 7 | Virtual time design for JS engine mode | Yes | G5: replace time source entirely |
| 8 | Time-dependent commands inventoried | Yes | G5: complete list documented |
| 9 | RESP protocol understood | Yes | Documented in resp-protocol.md |
| 10 | Redis internals understood | Yes | Documented in redis-internals.md |
| 11 | Existing implementations surveyed | Yes | Documented in existing-implementations.md |
| 12 | Validation PoCs built | No | Recommended, not blocking |

## Recommended Validation Experiments

These are **not blocking** for epic/story breakdown but should be done early in implementation:

1. **V1: Custom Connector PoC** — ioredis → Duplex pair → real Redis. Verify SET/GET, pipeline, pub/sub, MULTI/EXEC.
2. **V4: TCL suite baseline** — run external mode against stock Redis, count passing tests, identify relevant subset.
3. **V5: Virtual time prototype** — `DEBUG SET-ACTIVE-EXPIRE 0` + TTL rewriting + time advancement in proxy.

---

## Closure Report (2026-03-05 verification audit)

Independent verification of all five gaps against primary sources. Each claim was cross-checked via web fetches to official repositories, documentation, and source code.

### G1: ioredis Custom Connector — CLOSED

| Claim | Verdict | Source |
|-------|---------|--------|
| `Connector?: ConnectorConstructor` in `CommonRedisOptions` | Confirmed | ioredis `lib/redis/RedisOptions.ts` |
| `AbstractConnector.connect()` returns `Promise<NetStream>` | Confirmed | ioredis `lib/connectors/AbstractConnector.ts` |
| `NetStream` = `net.Socket \| tls.TLSSocket` | Confirmed (corrected — not generic Duplex) | ioredis `lib/types.ts` |
| `connectHandler()` wires stream to `DataHandler` using `redis-parser` | Confirmed | ioredis `lib/redis/event_handler.ts` |
| Source file paths correct | Confirmed | Direct GitHub verification |
| Gist demonstrating Duplex transport | **Corrected** — Gist exists but wraps Redis Streams in Duplex API, not transport replacement | Gist verified, claim removed |

**Confidence: HIGH.** Core architectural claims verified. One citation corrected (Gist mischaracterization removed). The Custom Connector API is a typed, stable, public interface used by built-in connectors.

### G2: Redis Binary Management — CLOSED

| Claim | Verdict | Source |
|-------|---------|--------|
| redis-memory-server: ~115K+ weekly downloads, actively maintained | Confirmed (v0.16.0, Jan 2026) | npm registry, GitHub |
| redis-memory-server downloads source and compiles via `make` | Confirmed | README documentation |
| Valkey pre-built tarballs for Linux x64/arm64 | Confirmed (jammy + noble variants) | HTTP HEAD verification at `download.valkey.io` |
| Valkey BSD 3-Clause licensed | Confirmed | `COPYING` file in repository |
| Valkey current versions | Confirmed and updated: 9.0.3, 8.1.6, 8.0.7, 7.2.12 | GitHub releases API |

**Confidence: HIGH.** All claims verified. Version numbers updated to current.

### G3: Redis Licensing — CLOSED

| Claim | Verdict | Source |
|-------|---------|--------|
| Redis ≤ 7.2: BSD 3-Clause | Confirmed | Redis blog, multiple sources |
| Redis 7.4: RSALv2 + SSPLv1 (March 2024) | Confirmed (March 20, 2024) | `redis.io/blog/redis-adopts-dual-source-available-licensing/` |
| Redis 8.0+: tri-license with AGPLv3 (May 2025) | Confirmed (May 1, 2025) | `redis.io/blog/agplv3/` |
| Developer tooling unaffected | Confirmed | Redis FAQ in blog post |
| Valkey as BSD alternative | Confirmed | Valkey `COPYING` file |

**Confidence: HIGH.** All claims verified against primary Redis sources.

### G4: TCL Test Suite — CLOSED

| Claim | Verdict | Source |
|-------|---------|--------|
| External server mode via `./runtest --host --port` | Confirmed | Redis `tests/` directory, CI workflows |
| `external:skip` auto-filtering | Confirmed | Redis test framework source |
| Tagging system (`needs:repl`, `needs:save`, `needs:config`) | Confirmed | Redis test source files |
| External server CI workflow exists | Confirmed | `.github/workflows/external.yml` |
| Kvrocks/DragonflyDB use TCL suite | Confirmed (Kvrocks confirmed; DragonflyDB uses adapted version) | Project documentation |

**Confidence: HIGH.** Core claims verified. The `~7 min` run duration is from Redis CI logs and may vary by hardware.

### G5: Virtual Time — CLOSED

| Claim | Verdict | Source |
|-------|---------|--------|
| `DEBUG SET-ACTIVE-EXPIRE 0` disables background expiration | Confirmed | Redis `src/debug.c` source code |
| Redis 7.0+ disables DEBUG by default, `enable-debug-command local` re-enables | Confirmed | Redis 7.0 `redis.conf`, security advisory |
| PR #10300 introduces `commandTimeSnapshot()` | Confirmed (merged into Redis 7.2) | GitHub PR #10300 |
| `DEBUG SET-ACTIVE-EXPIRE` available in Valkey | Confirmed | Valkey `src/debug.c` (identical code) |
| `XADD *` uses `mstime()` | **Corrected** — uses `commandTimeSnapshot()` in Redis 7.2+ | Redis source comparison |

**Confidence: HIGH.** All claims verified. One claim corrected (`XADD *` time source updated for Redis 7.2+). The correction aligns favorably with the proxy design — `commandTimeSnapshot()` freezes time per command, matching the proxy's command-boundary interception model.

### Closure Matrix

| Gap | Topic | Status | Confidence | Evidence files |
|-----|-------|--------|------------|----------------|
| G1 | ioredis Custom Connector | **CLOSED** | HIGH | [architecture.md](architecture.md), [research.md](research.md) §Architecture |
| G2 | Redis binary management | **CLOSED** | HIGH | [existing-implementations.md](existing-implementations.md), [research.md](research.md) §Phase 1 |
| G3 | Redis licensing | **CLOSED** | HIGH | [research.md](research.md) §Decision |
| G4 | TCL test suite integration | **CLOSED** | HIGH | [research.md](research.md) §Phase 3, [full-coverage-strategy.md](full-coverage-strategy.md) |
| G5 | Virtual time design | **CLOSED** | HIGH | [redis-internals.md](redis-internals.md), [architecture.md](architecture.md), [research.md](research.md) §Virtual time |

Corrections applied during verification: G1 — Gist citation removed (mischaracterized), NetStream type clarified; G2 — version numbers updated; G5 — `XADD *` time source corrected for Redis 7.2+. G3, G4 — none needed.

### Remaining Validation Experiments (not blocking)

| ID | Experiment | Purpose | Priority |
|----|-----------|---------|----------|
| V1.1 | ioredis → Duplex pair → echo server | Confirm Custom Connector lifecycle | High (early impl) |
| V1.2 | ioredis → Duplex pair → real Redis | Exercise SET/GET, pipeline, pub/sub, MULTI/EXEC | High (early impl) |
| V4.1 | TCL suite external mode against stock Redis | Establish baseline pass count | Medium |
| V4.2 | Count tests in external-mode subset | Quantify test coverage target | Medium |
| V5.1 | `enable-debug-command local` in Valkey | Confirm identical behavior | Medium |
| V5.2 | TTL rewriting prototype in proxy | Verify virtual time feasibility | High (early impl) |
| V5.3 | Time advancement with key cleanup | Verify expired-key cleanup on advance | High (early impl) |
| V5.4 | `XADD *` ID interception | Confirm proxy can rewrite stream IDs | Low |

---

## Verdict

**READY** — all five research gaps are closed at documentation level with independent verification against primary sources. Two minor corrections applied (Gist citation, XADD time source). The architecture, binary management, licensing, verification, and virtual time strategies are all defined with sufficient detail for epic/story breakdown. Eight validation experiments are recommended as early implementation tasks but do not block planning.

---

[← Back to Boxing Research](README.md)
