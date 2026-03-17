# Node Simulator Research

Research on Redis server internals and simulation approaches for building a Redis node simulator.

- [Networking Model](networking-model.md) — event loop, ae.c, single-threaded architecture, IO threading
- [Node Behavior](node-behavior.md) — connection lifecycle, pub/sub, blocking commands, CLIENT commands
- [Protocol](protocol.md) — RESP2 vs RESP3, pipelining, pub/sub wire format
- [Existing Emulators](existing-emulators.md) — mini-redis, redis-mock, fakeredis, and other projects
- [Network Simulation](network-simulation.md) — Toxiproxy, toxy, in-process approaches for Node.js
- [Transactions](transactions.md) — MULTI/EXEC, WATCH/UNWATCH, error handling
- [INFO and CONFIG](info-config.md) — INFO sections/fields, CONFIG GET/SET, parameters
- [Cluster, Sentinel, Replication](cluster-sentinel-replication.md) — hash slots, redirects, failover, PSYNC
- [Testing Approaches](testing-approaches.md) — Redis TCL suite, client library testing, emulator comparison

---

[← Back to Research](../README.md)
