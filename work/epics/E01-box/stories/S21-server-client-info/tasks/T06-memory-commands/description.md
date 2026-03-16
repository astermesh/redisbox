# T06: MEMORY Commands

Implement the MEMORY introspection commands (since Redis 4.0).

## Commands

- **MEMORY USAGE key [SAMPLES count]**: estimate memory used by a key in bytes. SAMPLES controls how many elements to sample for aggregate types (default 5). Returns nil for non-existent keys.
- **MEMORY DOCTOR**: return a human-readable report about memory issues. In the emulator, return "Sam, I have no memory problems" (Redis default when no issues).
- **MEMORY MALLOC-STATS**: return allocator statistics. In the emulator, return a stub string describing JS engine memory.
- **MEMORY PURGE**: ask allocator to release memory. In the emulator, trigger garbage collection hint if available, return OK.
- **MEMORY STATS**: return detailed memory usage breakdown as a map. Key fields: peak.allocated, total.allocated, startup.allocated, dataset.bytes, overhead.total, keys.count, etc.
- **MEMORY HELP**: return list of MEMORY subcommands.

## Details

- MEMORY USAGE must provide reasonable estimates for all data types. For JS, estimate based on data structure sizes (string length, number of elements × average element size, Map/Set overhead, etc.)
- MEMORY STATS format is a flat array of key-value pairs in RESP2

## Acceptance Criteria

- MEMORY USAGE returns reasonable byte estimates for all data types
- MEMORY USAGE returns nil for non-existent keys
- MEMORY USAGE SAMPLES controls sampling depth
- MEMORY DOCTOR returns diagnostic text
- MEMORY STATS returns a map with standard field names
- MEMORY HELP lists all subcommands
- All commands exist and return correct response types

---

[← Back](README.md)
