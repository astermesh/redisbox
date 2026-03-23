# T05: Lua Library Shims and Sandboxing

**Status:** done

Implement cjson (JSON encode/decode), cmsgpack (MessagePack), struct (C-struct packing), bit (bitwise ops for Lua 5.1). Sandbox: remove os, io, require, loadfile, dofile. Block print. Prevent global variable creation (read-only _G). Replace math.random/math.randomseed with Redis-compatible PRNG (redisLrand48 algorithm with hardcoded initial state).

## Acceptance Criteria

- Library shims work correctly
- Sandbox prevents escape
- PRNG produces identical sequences to Redis

---

[← Back](README.md)
