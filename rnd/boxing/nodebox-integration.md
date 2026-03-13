# NodeBox Integration: Simplifying RedisBox Architecture

## Idea

Instead of dual-mode architecture (browser JS engine + Node.js proxy/server), RedisBox can be built as a **standard Node.js application** that relies on NodeBox (SimBox ecosystem's Node.js runtime emulator) for browser execution.

## What This Changes

Current architecture assumes RedisBox must handle two environments itself:

| Concern | Current (dual-mode) | With NodeBox |
|---------|---------------------|--------------|
| Browser support | Custom Direct API, no RESP | NodeBox handles it |
| Node.js support | TCP server + RESP | TCP server + RESP (same) |
| Connection model | 4 approaches (Connector, TCP, Direct API, Proxy) | TCP server only |
| RESP protocol | Optional (not in browser) | Always (single path) |
| Code paths | Two (engine + browser adapter) | One |

## What Becomes Unnecessary

- **Dual-mode switching** (`mode: 'proxy' | 'engine' | 'auto'`)
- **Direct API** (`box.call('SET', ...)` without RESP)
- **Browser-specific adapters** and bundling concerns
- **Connection strategy matrix** (different transports per environment)

## What Stays the Same

- Full JS engine with all Redis commands
- RESP2/RESP3 protocol (now the only interface)
- Hook surface (IBI/OBI)
- RedisSim
- Testing strategy (differential testing, Redis TCL suite)

## Architecture With NodeBox

```
RedisBox = Node.js TCP server + RESP + In-Memory Engine + Hooks

Runs natively on Node.js.
Runs in browser via NodeBox.
One codebase, one interface, one code path.
```

Clients (ioredis, node-redis, redis-cli) connect via standard TCP/RESP — whether the underlying runtime is real Node.js or NodeBox.

## Status

Idea noted. Depends on NodeBox readiness. Revisit when NodeBox is available.

---

[← Back](README.md)
