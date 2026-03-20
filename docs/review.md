# Architectural Review

Full codebase review with identified issues, ordered by severity.

## High Severity

### 1. command-registry.ts — 1561-line flat array

The entire command catalog (87 top-level commands, 53 subcommands) lives in a single `CommandSpec[]` array. Spec data (arity, flags, key positions, categories) is distant from handler implementations in `commands/*.ts`. Adding a command requires editing two files, and in a 1561-line file it's easy to misplace a spec or copy-paste wrong metadata.

**Fix:** Each command module exports its own `CommandSpec[]`. The registry becomes a thin aggregator.

### 2. Public API is a non-functional stub

`src/redisbox.ts` exports a `RedisBox` class that stores options and does nothing. `createRedisBox()` produces an object that cannot start a server or execute commands. The full server is wired in `src/server/tcp-server.ts` and `src/engine/engine.ts`, but the public API (`src/index.ts`) has no connection to them.

**Fix:** Wire `RedisBox` to actually create an engine and optionally start a TCP server.

### 3. CONFIG SET for encoding thresholds has no effect

`hash-max-listpack-entries`, `set-max-listpack-entries`, `list-max-listpack-size` are defined in `ConfigStore` but never read by command handlers. Instead, hardcoded constants are duplicated across `hash.ts`, `set.ts`, `list.ts`:

```ts
// Duplicated in 3 files with identical TODO comment
const DEFAULT_MAX_LISTPACK_ENTRIES = 128;
const DEFAULT_MAX_LISTPACK_VALUE = 64;
```

**Fix:** Read thresholds from `ctx.config` at runtime. Remove hardcoded constants.

## Medium Severity

### 4. Two `ClientState` types with the same name

- `src/engine/command-dispatcher.ts` — `ClientState` (transaction state: inMulti, multiQueue, subscribed)
- `src/server/client-state.ts` — `ClientState` (connection metadata: id, name, flags, tracking)

`client-connection.ts` already aliases one as `DispatcherClientState` to disambiguate.

**Fix:** Rename dispatcher's type to `TransactionState` or `DispatcherState`.

### 5. Two glob pattern implementations

- `src/engine/glob-pattern.ts` — recursive, used by KEYS/SCAN/HSCAN/SSCAN
- `src/config-store.ts` lines 12–130 — iterative, used by CONFIG GET

Both are Redis-compatible glob matchers with identical feature sets but completely different implementations.

**Fix:** Keep one implementation, import it in both places.

### 6. `strByteLength` + `TextEncoder` duplicated in 4 files

`string.ts`, `hash.ts`, `set.ts`, `list.ts` each declare their own `TextEncoder` instance and `strByteLength()` function.

**Fix:** Extract to `src/engine/utils.ts`.

### 7. Fisher-Yates partial shuffle duplicated in 3+ places

The same 8-line shuffle loop appears in `hash.ts` (hrandfield), `set.ts` (srandmember, spop), and `database.ts` (sampleExpiryKeys).

**Fix:** Extract to shared utility.

### 8. `value: unknown` in `RedisEntry` forces 58+ type casts

Every command that accesses entry data casts: `entry.value as Map<string, string>`, `entry.value as Set<string>`, etc. No compile-time safety.

**Fix:** Make `RedisEntry` a discriminated union by type, with typed value and encoding fields per variant.

### 9. Three different integer parsers

- `incr.ts` — `parseInteger(s): bigint | null` (regex + BigInt)
- `string.ts` — `parseIntArg(s): {value: number, error}` (parseInt + roundtrip)
- `list.ts` — local `parseInteger(s): {value: number, error}` (Number + isInteger)

Different behaviors for edge cases (whitespace, leading zeros, overflow).

**Fix:** Consolidate into one or two canonical parsers (bigint for Redis integers, number for indices).

### 10. `updateEncoding` logic duplicated in 3 files

`hash.ts`, `set.ts`, `list.ts` each have a private `updateEncoding()` that mutates `entry.encoding` directly, bypassing the `Database` abstraction.

**Fix:** Move encoding promotion into `Database` or into a shared encoding module.

### 11. SCAN COUNT 0 returns wrong error vs HSCAN/SSCAN

`scan.ts` returns `NOT_INTEGER_ERR` for `count <= 0`. `hash.ts` and `set.ts` return `SYNTAX_ERR` for `count < 1` (matching real Redis).

**Fix:** Align `scan.ts` with the HSCAN/SSCAN behavior.

### 12. `CommandContext` optional fields require null guards everywhere

Every field except `db` and `engine` is optional. Handlers must guard against undefined for `config`, `pubsub`, `client`, etc.

**Fix:** Provide no-op defaults or split into minimal/full context types.

### 13. `acl.syncRequirePass()` called on every auth check

The dispatcher syncs ACL store with config store on every command dispatch — dual source of truth.

**Fix:** Integrate ACL store and config store properly; sync on CONFIG SET, not on every dispatch.

### 14. WATCH not in command table

WATCH is in `MULTI_PASSTHROUGH` set but has no registry entry. `COMMAND INFO WATCH` returns nothing.

**Fix:** Register WATCH (even if handler is stub) for COMMAND introspection parity.

## Low Severity

### 15. `INT64_MAX`/`INT64_MIN` defined in 4 files

`incr.ts`, `string.ts`, `hash.ts`, `set.ts` — some use `BigInt('...')`, others use `...n` literal syntax.

**Fix:** Extract to shared constants.

### 16. `getOrCreate*`/`getExisting*` boilerplate repeated 4 times

`hash.ts`, `set.ts`, `list.ts`, `sorted-set.ts` — same structural pattern, only type names differ.

**Fix:** Consider a generic helper, but low priority since the pattern is clear.

### 17. `config-commands.ts` and `config-store.ts` misplaced at `src/` root

Every other command file is in `src/engine/commands/`. These are the only exceptions.

**Fix:** Move to `src/engine/commands/config.ts` and `src/engine/config-store.ts`.

### 18. Inline error object literals bypassing `errorReply()`

`set.ts` lines 388–393 and 430 construct `{ kind: 'error', ... }` directly instead of using the `errorReply()` helper.

**Fix:** Use `errorReply()`.

### 19. `list-access.test.ts` has no corresponding source file

Tests for `list.ts` are split into `list.test.ts` and `list-access.test.ts`. The AGENTS.md co-location rule expects `foo.ts` → `foo.test.ts`.

**Fix:** Merge into `list.test.ts`.

### 20. `xadd` receives evaluated `clock()` number, TTL commands receive function reference

Inconsistent handler signature convention. Works correctly but breaks the pattern.

**Fix:** Align with function-reference convention when refactoring command-registry.

---

[← Back](README.md)
