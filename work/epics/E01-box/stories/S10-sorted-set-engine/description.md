# S10: Sorted Set Type Engine

Implement all non-blocking sorted set commands (~40+). Sorted sets are the most complex data structure -- each element has a float64 score. Requires a skip list implementation with span tracking for O(log N) rank queries. Must support dual-index (skip list + hash table) for large sets. Blocking sorted set commands covered in S14.

---

[← Back to S10](README.md)
