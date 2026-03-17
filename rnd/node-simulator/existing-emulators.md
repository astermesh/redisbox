# Existing Redis Simulators / Emulators

## Overview

There are several projects that simulate or mock Redis behavior, each with different goals and trade-offs.

## Projects

### mini-redis (Rust/Tokio)

**Repository:** tokio-rs/mini-redis

**Purpose:** Learning tool for Tokio async patterns, **not** a production Redis emulator.

**Approach:**
- Full TCP server accepting connections, spawning a task per connection
- Shared `Db` instance accessible from all connections (key-value + pub/sub)
- Wire protocol via intermediate `Frame` representation (`connection.rs` + `frame.rs`)
- Graceful shutdown via `tokio::signal`

**Patterns demonstrated:**
- Async client modeling
- Shared state management
- Wire protocol implementation
- Time mocking in tests (Tokio testing utilities)

**Limitations:**
- Intentionally incomplete — only implements commands needed to teach Tokio concepts
- No persistence
- Will not add features for production use

### fred.rs (Rust Redis client with mocking)

Uses local memory instead of actual Redis server when mocking is enabled. Built-in mock support for testing. This is a production Redis client, not a standalone simulator.

### ioredis-mock (JavaScript)

**Purpose:** In-memory mock for ioredis client, runs in Node.js without a real Redis server.

**Approach:**
- API-level mock — replaces the ioredis client, not the network layer
- Implements Redis commands as JavaScript functions operating on in-memory data structures
- Tests can use the mock instead of connecting to real Redis

**Limitations:**
- Only supports a subset of Redis commands
- Behavior may diverge from real Redis on edge cases
- No protocol-level simulation (no RESP, no TCP)

### redis-mock (JavaScript)

Similar approach to ioredis-mock but for the `redis` (node-redis) client library. Provides an in-memory implementation of Redis commands.

### fakeredis (Python)

**Purpose:** In-memory mock for redis-py client.

**Approach:**
- Replaces the Redis connection with an in-memory backend
- Implements Redis commands in Python
- Supports most common commands

**Limitations:**
- Command coverage is not complete
- Edge case behavior may differ from real Redis
- No network layer simulation

## Approaches Comparison

| Approach | Examples | Protocol | Network | Fidelity |
|----------|----------|----------|---------|----------|
| **TCP server simulator** | mini-redis | Full RESP | Real TCP | High but incomplete |
| **Client API mock** | ioredis-mock, redis-mock, fakeredis | None | None | Medium — command-level only |
| **Client with mock mode** | fred.rs | None | None | Low — memory backend |

## Key Observations

1. **No existing project aims for full behavioral parity** — all are explicitly incomplete or focused on testing convenience.

2. **Two fundamental approaches:**
   - **Protocol-level simulation** (TCP server): Higher fidelity, tests real client code including protocol handling, connection management. More complex to build.
   - **API-level mocking** (client replacement): Easier to build and use, but skips protocol layer, connection handling, and many edge cases.

3. **Common limitations across all projects:**
   - Incomplete command coverage
   - Missing edge case handling (error messages, state transitions)
   - No cluster mode simulation
   - No pub/sub fidelity (or very basic)
   - No blocking command simulation
   - No ACL / permission simulation

4. **RedisBox's differentiator** would be aiming for exact behavioral parity at the protocol level, which no existing project attempts.

## Implications for RedisBox Node Simulator

RedisBox should take the **protocol-level simulation** approach:
- Real TCP server (or in-process net.Server for Node.js)
- Full RESP2/RESP3 protocol support
- Exact behavioral parity with real Redis (our core principle)
- This allows testing any Redis client library, not just specific ones
- Connection state management (auth, select, pub/sub, blocking) must be accurate

---

[← Back to Node Simulator Research](README.md)
