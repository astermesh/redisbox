# Node Simulator Research

Research on Redis server internals and simulation approaches for building a Redis node simulator.

- [Networking Model](networking-model.md) — event loop, ae.c, single-threaded architecture, IO threading
- [Node Behavior](node-behavior.md) — connection lifecycle, pub/sub, blocking commands, CLIENT commands
- [Protocol](protocol.md) — RESP2 vs RESP3, pipelining, pub/sub wire format
- [Existing Emulators](existing-emulators.md) — mini-redis, redis-mock, fakeredis, and other projects
- [Network Simulation](network-simulation.md) — Toxiproxy, toxy, in-process approaches for Node.js

---

[← Back to Research](../README.md)
