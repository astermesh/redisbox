# T01: INFO Command

Implement INFO [section]. Sections: server, clients, memory, stats, replication, cpu, modules, commandstats, errorstats, cluster, keyspace, all, everything, default. Format: # Section headers, key:value lines. Key fields: redis_version (report compatible version e.g. 7.2.0), used_memory, connected_clients, db0:keys=N,expires=N,avg_ttl=N.

## Acceptance Criteria

- INFO output format matches Redis
- Section filtering works
- Keyspace stats accurate

---

[← Back](README.md)
