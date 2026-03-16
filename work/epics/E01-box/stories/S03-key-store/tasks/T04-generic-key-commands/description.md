# T04: Generic Key Commands

Implement DEL, EXISTS, TYPE, RENAME, RENAMENX, PERSIST, RANDOMKEY, TOUCH, UNLINK, COPY, OBJECT (ENCODING, REFCOUNT, IDLETIME, HELP, FREQ), WAIT (stub returning 0).

Note: SORT and SORT_RO are complex commands extracted into T07.

Note: DUMP and RESTORE require RDB-format serialization. Implement as stubs initially (DUMP returns error, RESTORE returns error) — full implementation deferred until RDB format is understood.

## Key Behavioral Details

- **DEL key [key ...]**: returns count of deleted keys. Non-existent keys are silently ignored.
- **EXISTS key [key ...]**: returns count of existing keys. Same key listed twice counts twice if it exists.
- **RENAME src dst**: error if src does not exist. If src == dst, returns OK (no-op). Preserves TTL of source. Overwrites destination if it exists.
- **RENAMENX src dst**: returns 0 if dst already exists (no rename performed).
- **UNLINK key [key ...]**: same behavior as DEL in this engine (in real Redis it's async deletion, but return value is identical).
- **COPY src dst [DB db] [REPLACE]** (Redis 6.2+): copy value + TTL to destination. Without REPLACE, returns 0 if dst exists.
- **OBJECT ENCODING key**: returns encoding string (see type-specific stories for valid encodings).
- **OBJECT REFCOUNT key**: always returns 1 (no shared objects in this engine).
- **OBJECT IDLETIME key**: seconds since last access (based on LRU clock).
- **OBJECT FREQ key**: LFU frequency counter value.
- **OBJECT HELP**: returns list of OBJECT subcommands and descriptions.

## Acceptance Criteria

- All commands match Redis behavior including exact error messages
- DEL/EXISTS handle multiple keys with correct counts
- RENAME preserves TTL, handles src==dst case
- COPY with REPLACE flag overwrites destination
- COPY with DB flag copies to different database
- OBJECT subcommands return correct values

---

[← Back to Tasks](../README.md)
