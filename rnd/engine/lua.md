# Lua Scripting in JavaScript: Research for RedisBox

## Context

Redis embeds a Lua 5.1 interpreter for EVAL/EVALSHA scripting. Redis 7+ adds FUNCTION support (still Lua 5.1). RedisBox needs to emulate this in pure JavaScript for browser and Node.js environments.

### What Redis Provides to Lua Scripts

- `redis.call(command, ...)` / `redis.pcall(command, ...)` — execute Redis commands
- `KEYS` and `ARGV` global tables — key and argument passing
- `redis.sha1hex(x)` — SHA1 digest
- `redis.log(level, message)` — server logging
- `redis.error_reply(x)` / `redis.status_reply(x)` — reply helpers
- `redis.setresp(x)` — RESP protocol version switching
- `redis.set_repl(x)` — replication control
- `redis.acl_check_cmd(command, ...)` — ACL permission check
- Libraries: `cjson` (JSON), `cmsgpack` (MessagePack), `struct` (C-struct packing), `bit` (bitwise ops)
- Standard Lua: `string`, `table`, `math`, limited `os` (only `os.clock()`)
- Sandboxed: no `require`, no global variable creation, no `io`, no `os`, no `debug`

### Redis Scripting Commands to Emulate

- `EVAL script numkeys key [key ...] arg [arg ...]`
- `EVALSHA sha1 numkeys key [key ...] arg [arg ...]`
- `EVAL_RO` / `EVALSHA_RO` (Redis 7+) — read-only variants
- `SCRIPT LOAD script` — cache script, return SHA1
- `SCRIPT EXISTS sha1 [sha1 ...]` — check cache
- `SCRIPT FLUSH [ASYNC|SYNC]` — clear cache
- `SCRIPT DEBUG YES|SYNC|NO` — debugger control
- `FUNCTION LOAD library_code` (Redis 7+)
- `FCALL function_name numkeys key [key ...] arg [arg ...]`
- `FCALL_RO` — read-only variant
- `FUNCTION LIST / DELETE / DUMP / RESTORE`

## Lua VM Options for JavaScript

### 1. fengari — Lua 5.3 in Pure JavaScript

**Repository:** [github.com/fengari-lua/fengari](https://github.com/fengari-lua/fengari)

**Overview:** A complete port of the PUC-Rio Lua C implementation to JavaScript ES6. Claims bug-for-bug compatibility with Lua 5.3.

| Property | Value |
|----------|-------|
| Lua version | 5.3 |
| Implementation | Pure JavaScript (ES6) |
| License | MIT |
| GitHub stars | ~2,000 |
| Last activity | December 2025 (v0.1.5 release) |
| npm package | `fengari` |
| Bundle size | ~214 KB plain / ~69 KB gzipped |
| Browser support | Yes (pure JS, no binary deps) |
| Node.js support | Yes |

**Strengths:**
- Pure JS — works everywhere, no WASM dependency
- Smallest bundle size among options
- Actively maintained (as of late 2025)
- Rich ecosystem: `fengari-interop` for JS/Lua bridging, `fengari-web` for browser
- Relies on JS garbage collector — no manual memory management

**Weaknesses:**
- Implements Lua 5.3, not 5.1 — Redis uses Lua 5.1
- Integers are 32-bit (JS limitation), not 64-bit as in standard Lua 5.3
- No `__gc` metamethods or weak tables (relies on JS GC)
- Slower than WASM-based solutions for compute-heavy scripts
- Lua 5.1 vs 5.3 differences require a compatibility shim (see below)

**Lua 5.1 vs 5.3 Differences (relevant to Redis scripts):**
- `unpack()` was moved to `table.unpack()` in 5.2+
- `setfenv()` / `getfenv()` removed in 5.2+
- Integer division operator `//` added in 5.3
- Bitwise operators (`&`, `|`, `~`, `<<`, `>>`) added in 5.3 (5.1 uses `bit` library)
- Number type split: 5.3 has integers and floats, 5.1 has only doubles
- `module()` and `require` changes
- String length operator behavior changes with `__len` metamethod

### 2. wasmoon — Lua 5.4 via WebAssembly

**Repository:** [github.com/ceifa/wasmoon](https://github.com/ceifa/wasmoon)

**Overview:** Compiles official Lua C source to WebAssembly using Emscripten. Provides a JS abstraction layer.

| Property | Value |
|----------|-------|
| Lua version | 5.4 |
| Implementation | WASM (Emscripten-compiled C) |
| License | MIT |
| GitHub stars | ~643 |
| Last activity | January 2025 |
| npm package | `wasmoon` |
| Bundle size | ~393 KB plain / ~130 KB gzipped |
| Browser support | Yes (requires WASM support) |
| Node.js support | Yes |

**Strengths:**
- Compiles official Lua C source — highest fidelity to real Lua
- Significantly faster than fengari for compute-heavy scripts (~25x in benchmarks)
- WASM provides near-native performance

**Weaknesses:**
- Implements Lua 5.4, not 5.1 — even further from Redis than fengari
- Larger bundle size (130 KB gzipped vs 69 KB for fengari)
- WASM initialization is async — adds complexity
- Browser bundler configuration required (webpack/rollup plugins for node module resolution)
- Less active maintenance (last release over a year ago as of early 2026)
- JS/Lua interop overhead can negate WASM speed advantage for frequent bridging

### 3. wasmoon-lua5.1 — Lua 5.1 via WebAssembly

**Repository:** [github.com/X3ZvaWQ/wasmoon-lua5.1](https://github.com/X3ZvaWQ/wasmoon-lua5.1)

**Overview:** A fork of wasmoon adapted for Lua 5.1. Compiles Lua 5.1 C source to WASM.

| Property | Value |
|----------|-------|
| Lua version | 5.1 |
| Implementation | WASM (Emscripten-compiled C) |
| License | MIT |
| GitHub stars | ~8 |
| npm package | `wasmoon-lua5.1` |
| Last npm publish | ~2 years ago (v1.18.10) |
| Browser support | Yes (requires WASM support) |
| Node.js support | Yes |

**Strengths:**
- Exact Lua 5.1 version — matches Redis perfectly
- Compiles official Lua 5.1 C source — maximum behavioral fidelity
- WASM performance advantages
- Same API as wasmoon — well-documented

**Weaknesses:**
- Very small community (8 stars)
- Not actively maintained (last publish 2 years ago)
- Single-maintainer fork — bus factor risk
- Same WASM/bundler complexity as wasmoon
- Same bundle size considerations as wasmoon

### 4. lua.vm.js — Emscripten-compiled Lua (Obsolete)

**Repository:** [github.com/daurnimator/lua.vm.js](https://github.com/daurnimator/lua.vm.js)

**Overview:** The original Lua-in-browser project using Emscripten. Implements Lua 5.2.4.

| Property | Value |
|----------|-------|
| Lua version | 5.2 |
| Status | **Superseded by fengari** |
| Bundle size | ~170 KB gzipped |

**Not viable.** The project is officially superseded by fengari. No longer maintained. Uses Lua 5.2, which does not match Redis either.

## Feature Evaluation Matrix

| Feature | fengari (5.3) | wasmoon (5.4) | wasmoon-lua5.1 |
|---------|:---:|:---:|:---:|
| Lua version match (Redis = 5.1) | Partial | Poor | Exact |
| `redis.call()` bridge feasibility | Easy | Easy | Easy |
| KEYS/ARGV passing | Easy | Easy | Easy |
| `cjson` implementation | JS shim needed | JS shim needed | JS shim needed |
| `cmsgpack` implementation | JS shim needed | JS shim needed | JS shim needed |
| `struct` implementation | JS shim needed | JS shim needed | JS shim needed |
| `bit` library | Not needed (5.3 has bitwise ops) | Not needed (5.4 has bitwise ops) | Need to provide |
| `redis.sha1hex` | JS shim (trivial) | JS shim (trivial) | JS shim (trivial) |
| Sandboxing | Doable | Doable | Doable |
| Script caching (SHA1-based) | Doable | Doable | Doable |
| Performance | Moderate | Fast | Fast |
| Bundle size (gzipped) | ~69 KB | ~130 KB | ~130 KB (est.) |
| Browser compatibility | Excellent | Good (needs WASM) | Good (needs WASM) |
| Maintenance health | Good | Declining | Poor |
| Community size | Large | Medium | Tiny |

Notes on the feature matrix:
- All options require JS-side implementations for `cjson`, `cmsgpack`, `struct`, and `bit` (for 5.1). These are not Lua standard libraries — they are C extensions bundled with Redis. We must reimplement them in JS and inject them into the Lua environment.
- `redis.call()` / `redis.pcall()` bridge: all options support registering JS functions callable from Lua. The bridge would intercept the call, execute it against the RedisBox engine, and return the result.
- Sandboxing: all options allow controlling the Lua global environment. We remove `io`, `os` (except `os.clock`), `require`, `loadfile`, `dofile` and prevent global variable creation.

## Redis 7+ FUNCTION Support

### How FUNCTION Differs from EVAL

| Aspect | EVAL | FUNCTION |
|--------|------|----------|
| Persistence | Ephemeral (cache only) | Persistent (survives restart) |
| Naming | SHA1 digest | User-defined function names |
| Organization | Individual scripts | Named libraries with multiple functions |
| Code reuse | Scripts cannot call each other | Functions within a library can call shared helpers |
| Loading | Client sends source every time (or EVALSHA) | Loaded once via `FUNCTION LOAD` |
| Invocation | `EVAL`/`EVALSHA` | `FCALL`/`FCALL_RO` |
| Flags | Shebang-style (`#!lua flags=...`) since Redis 7 | Per-function flags at registration time |
| Debugging | Lua debugger available | No debugger support |

### FUNCTION Library Structure

```lua
#!lua name=mylib

local function helper(keys)
  -- shared code
end

redis.register_function('myfunc', function(keys, args)
  helper(keys)
  return redis.call('GET', keys[1])
end)

redis.register_function{
  function_name='my_readonly',
  callback=function(keys, args)
    return redis.call('GET', keys[1])
  end,
  flags={ 'no-writes' }
}
```

### Function Flags

- `no-writes` — marks function as read-only (allows `FCALL_RO`, execution on replicas)
- `allow-oom` — allows execution when Redis exceeds memory limits
- `allow-stale` — allows execution on stale replicas
- `no-cluster` — prevents execution in cluster mode

### Implementation Complexity

FUNCTION adds moderate complexity on top of EVAL:
- Library registry: store loaded libraries by name (in addition to script SHA1 cache)
- `redis.register_function` must be available during library loading
- Function metadata: name, flags, library association
- Library-level operations: FUNCTION DELETE removes all functions in a library
- FUNCTION DUMP/RESTORE: serialization (can defer or stub initially)

### Key Insight for RedisBox

In RedisBox (in-memory emulator), FUNCTION persistence across restarts is not meaningful — the entire state is ephemeral. However, FUNCTION state must persist within a session (unlike EVAL scripts which are "only cached"). The behavioral difference: FUNCTION LOAD stores the library as part of the database state, while SCRIPT LOAD is just a cache that can be flushed.

## Recommendation

### Primary Choice: wasmoon-lua5.1

**Rationale:** RedisBox's core principle is exact Redis behavioral parity. Redis uses Lua 5.1. Using a Lua 5.1 VM eliminates an entire class of behavioral differences (integer semantics, removed functions like `setfenv`/`getfenv`, `unpack` location, etc.). The wasmoon-lua5.1 package provides this with good performance via WASM.

**Risk mitigation for maintenance concerns:**
- The package is a relatively thin wrapper around official Lua 5.1 C source compiled to WASM
- If the upstream becomes abandoned, forking and maintaining is straightforward (the WASM compilation is the hard part, and that is already done)
- The API matches wasmoon, which has broader community support

### Fallback: fengari + Lua 5.1 compatibility shim

If wasmoon-lua5.1 proves problematic (bundling issues, WASM initialization overhead, maintenance abandonment), fengari is the fallback:
- More actively maintained
- Pure JS — simpler bundling, no WASM async init
- Smaller bundle size
- Requires a compatibility shim to emulate Lua 5.1 behavior on top of Lua 5.3:
  - Provide global `unpack` (alias for `table.unpack`)
  - Provide `setfenv` / `getfenv` (limited emulation)
  - Disable integer/float distinction (force all numbers to float)
  - Provide `module()` function
  - Hide 5.3-only features (bitwise operators cannot be hidden at syntax level — this is a real gap)

The bitwise operator syntax gap is the biggest concern with fengari: Lua 5.3 code can use `a & b` which is a syntax error in Lua 5.1. A Redis user's Lua 5.1 script would never contain these operators, so the gap is in the other direction — fengari might accept scripts that real Redis would reject. This is a behavioral difference, but not one that would cause incorrect results for valid Redis Lua scripts.

### Integration Approach

1. **Lua VM wrapper:** Create an abstraction layer (`LuaEngine` interface) that hides the specific VM implementation. This allows swapping fengari/wasmoon without changing the rest of the codebase.

2. **Redis bridge:** Register JS functions in the Lua environment:
   - `redis.call()` → synchronous call to RedisBox engine command dispatcher
   - `redis.pcall()` → same, but catches errors and returns error objects
   - `redis.sha1hex()` → JS SHA1 implementation (or Web Crypto API)
   - `redis.log()` → console output or configurable logger
   - `redis.error_reply()` / `redis.status_reply()` → return typed objects

3. **Library shims (JS implementations injected into Lua):**
   - `cjson` → `JSON.parse` / `JSON.stringify` with Redis-compatible type mapping
   - `cmsgpack` → JS MessagePack library (e.g., `@msgpack/msgpack`)
   - `struct` → custom JS implementation of C struct packing
   - `bit` → JS bitwise operations (only needed for Lua 5.1 path)

4. **Sandboxing (verified against Redis source — `script_lua.c`):**
   - The `os` library is **completely removed** (`#if 0` in Redis source) — NOT partially available
   - The `debug` library is loaded only to create the error handler, then set to nil
   - Remove `io`, `require`, `loadfile`, `dofile`, `load` (in restricted mode)
   - `print` is silently blocked (returns nil)
   - Prevent global variable creation (set a restrictive metatable on `_G`, locked read-only via `lua_enablereadonlytable`)
   - Allowed globals (exhaustive allowlist): `string`, `cjson`, `bit`, `cmsgpack`, `math`, `table`, `struct`, `redis`, `xpcall`, `tostring`, `setmetatable`, `next`, `assert`, `tonumber`, `rawequal`, `collectgarbage`, `getmetatable`, `rawset`, `pcall`, `coroutine`, `type`, `_G`, `select`, `unpack`, `gcinfo`, `pairs`, `rawget`, `loadstring`, `ipairs`, `_VERSION`, `load`, `error`
   - Deprecated (require `lua_enable_deprecated_api` config): `newproxy`, `setfenv`, `getfenv`

5. **Time and randomness control (verified against Redis source):**

   **No system time access from Lua.** Redis blocks all time sources:
   - `os.time()` / `os.clock()` / `os.date()` — `os` lib not loaded at all
   - `socket.gettime()` — no socket lib exists
   - The only way to get time is `redis.call('TIME')` — goes through our Command Dispatcher, trivial to hook with virtual time

   **`math.random()` / `math.randomseed()` are replaced by Redis** with custom implementations:
   - Uses `redisLrand48()` — a custom drand48-style PRNG from `rand.c` (~30 lines)
   - Produces identical sequences across all platforms (unlike libc `rand()`)
   - PRNG state is process-global, shared between EVAL and FUNCTION engines
   - Seed is NEVER reset between script executions by Redis itself
   - Default initial state: `x = {0x330E, 0xABCD, 0x1234}` (hardcoded)
   - **RedisBox must reimplement this exact algorithm** with the same constants for bit-identical sequences

   **Implication for boxing:** The Lua VM itself does NOT need boxing. All external information flows (time, randomness) are controlled at the bridge level outside the VM.

5. **Script caching:**
   - On `SCRIPT LOAD` or first `EVAL`: compute SHA1 of script source, store in cache
   - On `EVALSHA`: look up by SHA1, execute cached script
   - On `SCRIPT FLUSH`: clear cache
   - On `SCRIPT EXISTS`: check cache membership

6. **FUNCTION support:**
   - Maintain a library registry (name -> library source + registered functions)
   - On `FUNCTION LOAD`: execute library code in Lua, collect `redis.register_function` calls
   - On `FCALL`: look up function by name, execute with keys/args
   - Store function flags for `FCALL_RO` / read-only enforcement

7. **Atomicity:**
   - Block all other operations while a Lua script executes (this is natural in single-threaded JS)
   - Implement script timeout detection (`SCRIPT DEBUG` / busy script handling)

---

[← Back](README.md)
