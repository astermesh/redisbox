# D01: Listpack config thresholds accepted but ignored

## Problem

ConfigStore defines and accepts all listpack-related config keys with correct Redis 7.2 defaults:

- `hash-max-listpack-entries` / `hash-max-listpack-value`
- `set-max-listpack-entries` / `set-max-listpack-value`
- `set-max-intset-entries`
- `zset-max-listpack-entries` / `zset-max-listpack-value`
- `list-max-listpack-size`
- Deprecated aliases: `*-ziplist-*`

`CONFIG SET` succeeds for these keys, but the values are silently ignored. Encoding decision functions in hash, list, set, and sorted set modules use hardcoded constants from `src/engine/utils.ts` instead of reading from ConfigStore.

## Impact

A user running `CONFIG SET hash-max-listpack-entries 256` gets `OK` but encoding behavior does not change. This violates Redis behavioral parity — in real Redis, these settings take effect immediately.

## Root cause

Missed integration between S20 (config system) and S07-S10 (data structure engines). The epic description explicitly planned this: "encoding transition thresholds depend on config values." ConfigStore is already wired into `CommandContext` but encoding functions don't use it.

## Affected files

Encoding decision functions that use hardcoded defaults:

- `src/engine/utils.ts` — `DEFAULT_MAX_LISTPACK_ENTRIES`, `DEFAULT_MAX_LISTPACK_VALUE`
- `src/engine/commands/hash/utils.ts` — `updateEncoding()`
- `src/engine/commands/list/utils.ts` — `updateEncoding()`
- `src/engine/commands/set/utils.ts` — `updateEncoding()`, `DEFAULT_MAX_INTSET_ENTRIES`
- `src/engine/commands/sorted-set/types.ts` — `updateEncoding()`

## Fix

1. Update encoding functions to accept config thresholds as parameters
2. Read thresholds from `ctx.config` at call sites
3. Keep hardcoded defaults as fallbacks when config is unavailable (e.g., direct engine usage without server)
4. Handle `list-max-listpack-size` special semantics (negative values)
5. Add integration tests: CONFIG SET → verify encoding change

---

[← Back](README.md)
