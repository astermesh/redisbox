# T03: Approximated LRU

Implement sampled LRU: each key stores 24-bit last-access timestamp. On eviction, sample maxmemory-samples (default 5) random keys, maintain eviction pool (sorted array of 16 candidates by idle time), evict highest idle time. Update access timestamp on every key read/write.

## Acceptance Criteria

- LRU approximation close to true LRU
- Eviction pool persists across cycles

---

[← Back](README.md)
