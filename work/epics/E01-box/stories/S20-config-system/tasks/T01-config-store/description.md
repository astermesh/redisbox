# T01: Config Store

Implement ConfigStore with default values for all relevant Redis config parameters (~200). Support get(pattern) with glob matching and set(key, value) with validation. Key configs: maxmemory, maxmemory-policy, maxmemory-samples, hz, list-max-listpack-entries, hash-max-listpack-entries, set-max-listpack-entries, zset-max-listpack-entries, notify-keyspace-events, slowlog-log-slower-than, slowlog-max-len, requirepass, etc.

## Acceptance Criteria

- All config keys accessible via GET
- Glob patterns work
- Validation rejects invalid values

---

[← Back](README.md)
