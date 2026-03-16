# S17: Lua Scripting Engine

Embed a Lua VM for EVAL/EVALSHA scripting support. Redis uses Lua 5.1 — use wasmoon-lua5.1 (primary) or fengari (fallback). Scripts run atomically and access Redis via redis.call()/redis.pcall().

## Tasks

- T01: Lua VM integration
- T02: Redis bridge (redis.call/redis.pcall)
- T03: EVAL/EVALSHA commands
- T04: Script caching and management
- T05: Lua library shims and sandboxing
- T06: Redis Functions

---

[← Back](README.md)
