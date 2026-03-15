# NodeBox Integration: Single-Runtime Architecture

## Decision

RedisBox is built as a **standard Node.js application**. It uses Node.js APIs (TCP server via `net`, Buffer, etc.) and speaks RESP protocol. Browser execution is provided by NodeBox — SimBox ecosystem's Node.js runtime emulator.

This eliminates the need for dual-mode architecture, browser-specific code paths, or any special browser adapters.

## What This Means

| Concern | Approach |
|---------|----------|
| Node.js support | Native — standard Node.js TCP server |
| Browser support | NodeBox provides the runtime (net, Buffer, etc.) |
| Connection model | TCP server + RESP — single path |
| RESP protocol | Always used — the only interface |
| Code paths | One |

## What Is NOT Needed

- **Dual-mode switching** (`mode: 'proxy' | 'engine' | 'auto'`)
- **Direct API** (`box.call('SET', ...)` without RESP)
- **Browser-specific adapters** and bundling concerns
- **Connection strategy matrix** (different transports per environment)
- **Proxy over real Redis binary** — RedisBox IS the Redis

## Architecture

```
RedisBox = Node.js TCP server + RESP + In-Memory Engine + Hooks

Runs natively on Node.js.
Runs in browser via NodeBox.
One codebase, one interface, one code path.
```

Clients (ioredis, node-redis, redis-cli) connect via standard TCP/RESP — whether the underlying runtime is real Node.js or NodeBox.

## What Stays the Same

- Full JS engine with all Redis commands
- RESP2/RESP3 protocol (the only interface)
- Hook surface (IBI/OBI)
- RedisSim
- Testing strategy (differential testing, Redis TCL suite)

---

[← Back](README.md)
