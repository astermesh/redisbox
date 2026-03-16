# E01: RedisBox Engine

Full implementation of an in-memory Redis emulator in TypeScript. RedisBox is a pure JS engine that speaks RESP protocol over TCP, supporting all core Redis commands with exact behavioral parity to real Redis.

## Scope

- RESP2 parser and serializer (RESP3 deferred to future epic)
- TCP server accepting standard Redis client connections
- In-memory keyspace with 16 databases, lazy and active expiration
- All core type engines: strings, lists, hashes, sets, sorted sets, streams
- Command dispatcher with metadata-driven routing and validation
- Pub/Sub engine with channel and pattern subscriptions
- Transaction manager (MULTI/EXEC/WATCH)
- Blocking commands (BLPOP, BRPOP, XREAD BLOCK, etc.)
- Lua scripting engine (EVAL/EVALSHA via embedded Lua VM)
- Memory eviction (LRU/LFU/random/TTL policies)
- Keyspace notifications
- Config system, ACL, server/client management commands
- LATENCY and MEMORY introspection commands
- Cluster command stubs (single-node emulator)
- Hook architecture (IBI/OBI) for SimBox integration
- RedisSim API for simulation control
- Differential testing infrastructure for parity verification

## Out of Scope

- Redis module commands (JSON, Search, TimeSeries, probabilistic) — future epic
- RESP3 protocol support — future epic
- Persistence (RDB/AOF) — commands exist as stubs but no actual persistence
- Real cluster mode — stubs only

## Architecture

```
Client (ioredis / node-redis / redis-cli)
         |
    TCP Server (RESP2)
         |
    Command Dispatcher
         |
    IBI Hooks (Sim)
         |
    In-Memory Engine
    ├── String Store
    ├── Hash Store
    ├── List Store
    ├── Set Store
    ├── Sorted Set Store
    ├── Stream Store
    ├── PubSub Engine
    └── Script Engine
         |
    OBI Hooks (time, random, persist)
```

## Stories

29 stories covering the full implementation, organized by category:

### Foundation (implement first, in order)

0. **S00** — Project setup (TypeScript, build, testing framework, CI)
1. **S01** — RESP2 parser and serializer
2. **S02** — TCP server
3. **S03** — Key store and database layer
4. **S04** — String type engine
5. **S05** — Command dispatcher
6. **S06** — Connection and server commands
7. **S20** — Config system (needed before data structure stories for encoding thresholds)

### Core Data Structures

8. **S07** — Hash type engine
9. **S08** — List type engine
10. **S09** — Set type engine
11. **S10** — Sorted set type engine

### Infrastructure

12. **S11** — Expiration manager (active expiration cycle)

### Advanced Features

13. **S12** — Pub/Sub system
14. **S13** — Transaction manager
15. **S14** — Blocking commands
16. **S15** — Bitmap commands
17. **S26** — HyperLogLog commands
18. **S27** — Geo commands
19. **S16** — Streams and consumer groups
20. **S17** — Lua scripting engine
21. **S18** — Memory eviction manager
22. **S19** — Keyspace notifications

### Server Management

23. **S21** — Server, client, info, LATENCY, and MEMORY commands
24. **S22** — ACL system
25. **S23** — Cluster and replication stubs

### SimBox Integration

26. **S24** — Hook architecture (IBI/OBI)
27. **S25** — RedisSim API

### Testing

28. **S28** — Testing infrastructure (differential testing, TCL suite, CI parity pipeline)

## Dependency Notes

- **S00 must be first** — project scaffolding is prerequisite for all code
- **S03 (Key Store) defines handler functions** that S05 (Command Dispatcher) wires up. S03 tasks T04-T07 implement command handler logic; S05 provides the dispatch layer that routes parsed commands to those handlers. Both are needed for an end-to-end flow.
- **S20 (Config) moved to Foundation** — encoding transition thresholds (listpack→hashtable, etc.) depend on config values. Type engine stories (S07-S10) should read thresholds from config.
- **S28 (Testing Infrastructure)** can start after S00-S05 provide a minimal working engine, but should be set up early to catch parity issues from the start.

---

[← Back](README.md)
