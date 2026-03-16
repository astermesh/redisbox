# ADR-03: Lua VM Choice for Scripting

## Status

Accepted

## Context

Redis embeds Lua 5.1 for EVAL/EVALSHA scripting and Redis 7+ FUNCTION support. RedisBox needs a Lua VM that runs in both Node.js and browser.

Options evaluated:
- **fengari** — Lua 5.3 in pure JavaScript. Actively maintained, ~69 KB gzipped. Version mismatch with Redis (5.3 vs 5.1).
- **wasmoon** — Lua 5.4 via WebAssembly. Fast but even further from Redis's Lua 5.1. ~130 KB gzipped.
- **wasmoon-lua5.1** — Lua 5.1 via WebAssembly. Exact version match. Small community (~8 stars), not actively maintained.

## Decision

Primary: **wasmoon-lua5.1** for exact Lua 5.1 behavioral parity.
Fallback: **fengari** with a Lua 5.1 compatibility shim if wasmoon-lua5.1 proves problematic.

## Rationale

- RedisBox's core principle is exact Redis behavioral parity — Lua 5.1 match eliminates an entire class of behavioral differences
- Lua 5.1 vs 5.3/5.4 differences are real: `unpack` vs `table.unpack`, `setfenv`/`getfenv` removal, integer semantics, bitwise operator syntax
- wasmoon-lua5.1 is a thin wrapper around official Lua 5.1 C source compiled to WASM — low maintenance risk
- If upstream becomes abandoned, forking is feasible (WASM compilation is the hard part, already done)
- LuaEngine abstraction layer allows swapping VM implementations without changing the rest of the codebase

## Consequences

- WASM dependency for Lua scripting (requires WASM support in browser)
- Async initialization of WASM module
- If wasmoon-lua5.1 is abandoned, fallback to fengari requires a compatibility shim for 5.1 behaviors
- All Redis Lua libraries (cjson, cmsgpack, struct, bit) must be implemented as JS shims regardless of VM choice

---

[← Back to ADRs](README.md)
