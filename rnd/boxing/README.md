# Boxing Research

Technical research for building an in-memory Redis emulator.

## Contents

- [Research summary](research.md) — consolidated findings, decisions, architecture
- [Existing implementations](existing-implementations.md) — survey of JS/WASM Redis implementations
- [RESP protocol](resp-protocol.md) — wire protocol analysis (RESP2/RESP3)
- [Redis internals](redis-internals.md) — expiration, eviction, pub/sub, transactions, scripting
- [Architecture](architecture.md) — dual-mode design (proxy + JS engine, hooks, RedisSim)
- [Full coverage strategy](full-coverage-strategy.md) — analysis of paths to 100% Redis command coverage

---

[← Back to Research](../README.md)
