# T02: Redis Bridge (redis.call/redis.pcall)

**Status:** done

Register redis.call() and redis.pcall() in Lua environment. Bridge calls route to command dispatcher synchronously. redis.call() propagates errors, redis.pcall() catches and returns error objects. Type mapping: Lua integer -> Redis integer, Lua string -> bulk string, Lua table -> array, Lua false -> nil, Lua true -> integer 1.

## Acceptance Criteria

- redis.call() executes Redis commands from Lua
- Type mapping matches Redis exactly

---

[← Back](README.md)
