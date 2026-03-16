# T03: Behavioral Modification

Implement setCacheMissRate(rate), setMessageDropRate(rate) for pub/sub, injectEviction(keys) to simulate eviction of specific keys. setCacheMissRate intercepts GET/MGET to return nil at configured rate. setMessageDropRate drops pub/sub messages at configured rate.

## Acceptance Criteria

- Cache miss simulation works
- Message drops work
- Eviction simulation removes correct keys

---

[← Back](README.md)
