# Redis Server Networking Model

## Event Loop (`ae.c`) Architecture

Redis implements its own event library in `ae.c`. Redis's speed comes from two core architectural decisions: keeping data in memory and using a single-threaded event loop. This is based on the **Reactor Pattern**, allowing a single thread to handle thousands of concurrent client connections.

### Why a Custom Event Library?

`ae` is ~1,300 lines — trivial compared to libuv's 26K. libuv is a far more general library; `ae` was designed for Redis, co-evolved with Redis, and contains only what Redis needs.

`ae.h` provides a platform-independent wrapper for I/O event notification:
- **Linux**: epoll
- **BSD**: kqueue
- **Fallback**: select

### Event Types

Redis has two event types:
- **File events** — I/O on sockets (reads/writes from clients)
- **Time events** — periodic tasks (key expiration, background save)

`aeCreateFileEvent()` registers file events. When using epoll, it calls `epoll_ctl` to add an event on the file descriptor (`EPOLLIN`, `EPOLLOUT`, or both).

### Event Loop Initialization

`initServer()` in `redis.c` initializes the event loop:
1. Creates event loop via `aeCreateEventLoop()`
2. Registers `acceptTcpHandler` callback for read events on the listening socket
3. Sets up time events

### Main Event Loop

`aeMain()` (called from `main()`) runs a while loop calling `aeProcessEvents()`:
- Time events are handled by custom logic
- File events are handled by the underlying epoll/kqueue/select
- The loop efficiently sleeps until there is work to do

### Client Connection Flow

1. **Accept**: `acceptTcpHandler` → `accept()` → `acceptCommonHandler` → `createClient()`
2. **Non-blocking setup**: `createClient` sets socket to non-blocking mode, registers `readQueryFromClient` for read events
3. **Command processing**: `readQueryFromClient` is invoked by the event loop when client sends data
4. **Response writing**: `beforeSleep` calls `handleClientsWithPendingWrites`, which tries immediate writes via `writeToClient`; if socket unavailable, registers `sendReplyToClient` callback

### Why Single-Threaded?

Redis is memory-bound, not CPU-bound. A single thread can saturate network bandwidth before hitting CPU limits. Sequential processing guarantees strict command ordering, eliminating locks, mutexes, and context-switching overhead.

**Trade-offs:**
- Cannot use multiple CPU cores for command execution (scale via multiple instances)
- Long-running commands (`KEYS *`, complex Lua scripts) block all clients
- Cannot preempt a running command

## I/O Threading (Redis 6.0+)

### Motivation

With faster hardware, the bottleneck shifted to network I/O — single thread couldn't keep up with network hardware speed. Read/write syscalls occupy most CPU time.

### Design

I/O threads handle **only** reading requests from sockets and writing responses back. **Command execution remains single-threaded.** This avoids multi-threaded safety concerns for Lua scripts, transactions, etc.

### How It Works

**Write path:**
1. After executing a command, Redis saves the reply to a **pending list** (not written synchronously)
2. On the next event loop cycle, multiple I/O threads write replies in parallel

**Read path:**
1. When `io-threads-do-reads` is enabled, incoming reads are collected to a **pending list**
2. Multiple I/O threads read from sockets in parallel
3. Main thread executes commands sequentially after reads complete

### Task Dispatching

Uses **Round-Robin** across I/O threads. With 4 threads and 7 clients, clients are distributed evenly. The main thread also handles a portion of pending tasks.

### Configuration

```
io-threads N          # 1 = single thread (default), N >= 2 for multi-threaded I/O
io-threads-do-reads yes  # enable threaded reads (disabled by default)
```

Recommended: 4 cores → 2-3 threads, 8 cores → 6 threads. More than 8 threads is not useful.

### Performance

37-112% throughput improvement depending on workload. Can roughly double throughput without pipelining or sharding.

### Redis 8 Evolution

Redis 8 introduces a new I/O threading implementation: main thread assigns clients to specific I/O threads, each I/O thread notifies main thread after client finishes reading/parsing, main thread processes queries and generates replies, I/O threads write replies.

## Implications for RedisBox Node Simulator

For simulating Redis server behavior in RedisBox:
- The single-threaded model is natural for JavaScript/Node.js (also single-threaded event loop)
- We don't need to simulate I/O threading — it's an internal optimization invisible to clients
- The key behavior to replicate is: commands execute atomically and sequentially
- `beforeSleep` pattern maps well to microtask/next-tick patterns in Node.js

---

[← Back to Node Simulator Research](README.md)
