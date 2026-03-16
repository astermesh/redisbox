# S26: HyperLogLog Commands

Implement HyperLogLog probabilistic cardinality estimation. HyperLogLog is stored as a special string encoding — the TYPE command returns "string" for HLL keys. Requires implementing the Redis-specific HLL algorithm with sparse and dense representations.

## Commands

PFADD, PFCOUNT, PFMERGE, PFDEBUG, PFSELFTEST (5 commands)

## Key Behavioral Details

- HLL uses string type internally — `TYPE` returns `string`, not a separate type
- Two internal representations:
  - **Sparse**: run-length encoded, very compact for small cardinalities. Transitions to dense when it would exceed `hll-sparse-max-bytes` (default 3000)
  - **Dense**: 16384 registers of 6 bits each, packed into 12288 bytes + 16 byte header = 12304 bytes total
- Hash function: Redis uses a variant of MurmurHash64A — low 14 bits select the register, remaining bits determine the longest run of zeros
- Bias correction: Redis uses the raw HLL estimation formula with bias corrections for different cardinality ranges
- `PFCOUNT` on a **single key** caches the result in the key itself (the key IS modified on read — important behavioral quirk)
- `PFCOUNT` on **multiple keys** creates a temporary merged HLL and returns its count — source keys are NOT modified
- `PFMERGE` creates or overwrites the destination key with the union of all source HLLs
- `PFDEBUG` and `PFSELFTEST` are debug/internal commands that must exist
- Standard error rate: ~0.81%

## Dependencies

- S03 (key store — HLL stored as string type entries)
- S04 (string type — HLL piggybacks on string storage)

## Tasks

1. T01 — HyperLogLog core (PFADD, PFCOUNT, PFMERGE)
2. T02 — HyperLogLog internals and debug commands

---

[← Back](README.md)
