# S03: Key Store and Database Layer

Implement the core keyspace — 16 independent databases, key-value entry storage with type/encoding metadata, lazy expiration on access, and key version tracking for WATCH. This is the foundation that all type engines build upon.

## Dependency Note

Tasks T04-T07 define command handler functions (DEL, TTL, KEYS, SORT, etc.). These handlers are standalone functions that receive a database context and arguments. S05 (Command Dispatcher) provides the routing layer that connects parsed RESP commands to these handlers. Both S03 and S05 are needed for an end-to-end command flow.

## Tasks

- T01: Database and entry model
- T02: Lazy expiration
- T03: Key version tracking
- T04: Generic key commands
- T05: TTL commands
- T06: KEYS and SCAN
- T07: SORT and SORT_RO

---

[← Back to S03](README.md)
