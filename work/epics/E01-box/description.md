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
- Cluster command stubs (single-node emulator)
- Hook architecture (IBI/OBI) for SimBox integration
- RedisSim API for simulation control

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

25 stories covering the full implementation, ordered by dependency:

1. S01 — RESP2 parser and serializer
2. S02 — TCP server
3. S03 — Key store and database layer
4. S04 — String type engine
5. S05 — Command dispatcher
6. S06 — Connection and server commands
7. S07 — Hash type engine
8. S08 — List type engine
9. S09 — Set type engine
10. S10 — Sorted set type engine
11. S11 — Expiration manager
12. S12 — Pub/Sub system
13. S13 — Transaction manager
14. S14 — Blocking commands
15. S15 — Bitmap, HyperLogLog, and Geo commands
16. S16 — Streams and consumer groups
17. S17 — Lua scripting engine
18. S18 — Memory eviction manager
19. S19 — Keyspace notifications
20. S20 — Config system
21. S21 — Server, client, and info commands
22. S22 — ACL system
23. S23 — Cluster and replication stubs
24. S24 — Hook architecture (IBI/OBI)
25. S25 — RedisSim API

---

[← Back](README.md)
