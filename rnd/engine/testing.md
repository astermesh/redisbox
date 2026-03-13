# Testing Research: Verifying Redis Behavioral Parity

## 1. Redis TCL Test Suite

### Location and Structure

The Redis test suite lives in `tests/` within the [Redis repository](https://github.com/redis/redis/tree/unstable/tests). Written entirely in Tcl.

```
tests/
├── test_helper.tcl        ← main entry point and test runner
├── instances.tcl           ← server instance management
├── support/                ← test framework utilities (server.tcl, redis.tcl, etc.)
├── unit/                   ← core command tests
│   ├── type/               ← data structure type tests
│   ├── cluster/            ← cluster unit tests
│   └── moduleapi/          ← module API tests
├── integration/            ← integration tests (replication, AOF, RDB, etc.)
├── cluster/                ← cluster integration tests
├── sentinel/               ← sentinel tests
├── modules/                ← module tests
├── vectorset/              ← vector set tests
├── helpers/                ← helper scripts
└── assets/                 ← test fixtures
```

### Unit Test Files (tests/unit/)

| File | Size | Covers |
|------|------|--------|
| acl.tcl | 49 KB | ACL commands |
| acl-v2.tcl | 28 KB | ACL v2 features |
| auth.tcl | 4 KB | AUTH command |
| bitfield.tcl | 9 KB | BITFIELD command |
| bitops.tcl | 26 KB | Bit operations (BITOP, BITCOUNT, BITPOS) |
| dump.tcl | 16 KB | DUMP/RESTORE |
| expire.tcl | 34 KB | Key expiration (EXPIRE, PEXPIRE, TTL, etc.) |
| functions.tcl | 45 KB | Redis Functions |
| geo.tcl | 32 KB | Geo commands |
| hyperloglog.tcl | 13 KB | HyperLogLog |
| introspection.tcl | 45 KB | COMMAND, DEBUG, CONFIG introspection |
| keyspace.tcl | 17 KB | Keyspace notifications |
| maxmemory.tcl | 26 KB | Eviction policies |
| multi.tcl | 25 KB | MULTI/EXEC transactions |
| networking.tcl | 17 KB | Connection handling |
| protocol.tcl | 9 KB | RESP protocol edge cases |
| pubsub.tcl | 40 KB | Pub/Sub |
| scan.tcl | 15 KB | SCAN, HSCAN, SSCAN, ZSCAN |
| scripting.tcl | 99 KB | Lua scripting (EVAL, EVALSHA) |
| sort.tcl | 14 KB | SORT command |
| tracking.tcl | 32 KB | Client-side caching / tracking |
| wait.tcl | 19 KB | WAIT command |

### Data Type Test Files (tests/unit/type/)

| File | Size | Covers |
|------|------|--------|
| hash.tcl | 36 KB | Hash commands |
| hash-field-expire.tcl | 111 KB | Per-field TTL (Redis 7.4+) |
| incr.tcl | 7 KB | INCR/DECR family |
| list.tcl | 87 KB | List commands |
| list-2.tcl | 2 KB | Additional list tests |
| list-3.tcl | 8 KB | Additional list tests |
| set.tcl | 47 KB | Set commands |
| stream.tcl | 123 KB | Stream commands |
| stream-cgroups.tcl | 136 KB | Stream consumer groups |
| string.tcl | 54 KB | String commands |
| zset.tcl | 108 KB | Sorted set commands |

### Scale

~50+ test files, ~2000+ individual test cases. Unit tests alone account for about 1.5 MB of Tcl code.

### External Server Mode

The test suite supports running against an external server:

```bash
./runtest --host 127.0.0.1 --port 6379
```

Tests tagged `external:skip` are automatically skipped (those that need server restarts, config changes, log file access, or multi-instance setups).

The Redis maintainer [stated](https://github.com/redis/redis/discussions/11406) that running external mode against alternative implementations requires "heavy modification" because many tests depend on server control and inter-test state.

## 2. Redis Command Metadata (Machine-Readable)

**Source JSON files:** [423 command definition files](https://github.com/redis/redis/tree/unstable/src/commands) in `src/commands/`. Each defines arguments, types, flags, complexity, key specifications, and version history.

**COMMAND DOCS introspection (Redis 7.0+):** Returns structured metadata for all commands:
- `summary`, `since`, `group`, `complexity`
- `arguments` — array of argument definitions with types, flags, optionality
- `history` — version changelog

**COMMAND INFO:** Returns arity, flags, first/last/step key positions, ACL categories.

**Use for RedisBox:**
- Parse `COMMAND DOCS` output to generate a command registry
- Auto-generate test stubs for every command
- Validate argument counts, flags, and key positions
- Detect missing commands automatically

## 3. Differential Testing

The key parity verification technique: run identical commands against both RedisBox and real Redis, compare responses.

```
┌─────────────┐     ┌──────────────┐
│  Test Case   │────▶│  RedisBox    │──── response A
│              │     │  (JS engine) │
│              │     └──────────────┘
│              │     ┌──────────────┐
│              │────▶│  Real Redis  │──── response B
│              │     │  (localhost)  │
│              │     └──────────────┘
│              │
│  assert(A === B)
└─────────────┘
```

### What to Compare

- **Return values:** Exact match (type, structure, content)
- **Error messages:** Exact string match (clients parse these)
- **Error codes:** ERR, WRONGTYPE, MOVED, etc.
- **Side effects:** Key existence, TTL changes, type changes
- **OBJECT ENCODING:** Internal encoding transitions
- **Edge cases:** Empty strings, very large values, special characters, binary data
- **Ordering:** Where Redis guarantees order, match exactly; where it doesn't, compare as sets

### How Other Projects Do It

**fakeredis-py** — gold standard: every test runs against both fake and real Redis. Same test, two backends, compare results. This is the model to follow.

**DragonflyDB** — pytest-based tests (~34 files), starts a server, tests via Redis client libraries. Also tests replication compatibility with real Redis.

**Kvrocks** — Go test cases organized by command group. Starts Kvrocks server, connects via standard Redis clients.

**ioredis-mock** — no differential testing. Result: parity drifts over time. Cautionary example.

**distributedio/redis-integration-tests** — [fork](https://github.com/distributedio/redis-integration-tests) of Redis TCL suite adapted for external testing. Shows TCL suite can be adapted with moderate effort.

---

[← Back](README.md)
