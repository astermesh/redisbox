# T02: HyperLogLog Commands

**Status:** done

Implement PFADD, PFCOUNT, PFMERGE, PFDEBUG, PFSELFTEST. HLL uses string type internally (TYPE returns "string"). Two representations: sparse (small cardinalities) and dense (16384 registers x 6 bits = 12 KB). PFCOUNT on single key caches result in key (key is modified on read). PFCOUNT on multiple keys creates temporary merge without modifying sources.

## Acceptance Criteria

- Cardinality estimation within expected error bounds
- Sparse/dense transitions correct

---

[← Back](README.md)
