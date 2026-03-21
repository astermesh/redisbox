# T01: HyperLogLog Core Commands

**Status:** done

Implement PFADD, PFCOUNT, and PFMERGE with both sparse and dense representations.

## Details

- **PFADD key [element ...]**: Add elements to the HLL. Return 1 if any register changed, 0 otherwise. Create key if it does not exist.
- **PFCOUNT key [key ...]**:
  - Single key: return estimated cardinality. Cache the result in the key (key is modified on read). Return cached value if no PFADD since last PFCOUNT.
  - Multiple keys: create temporary merge of all HLLs, return its cardinality. Source keys are NOT modified.
- **PFMERGE destkey sourcekey [sourcekey ...]**: Merge multiple HLLs into destkey. If destkey exists, it is overwritten with the union.
- Implement MurmurHash64A variant matching Redis exactly (low 14 bits for register selection)
- Implement sparse representation with RLE encoding
- Implement dense representation (16384 registers × 6 bits)
- Implement sparse-to-dense promotion when sparse exceeds `hll-sparse-max-bytes` threshold
- Implement bias correction matching Redis's algorithm

## Acceptance Criteria

- PFADD adds elements and returns correct changed/unchanged indicator
- PFCOUNT returns cardinality within expected error bounds (~0.81% standard error)
- PFCOUNT on single key caches result (key modified on read — verify with OBJECT ENCODING or by checking the raw value)
- PFCOUNT on multiple keys does NOT modify source keys
- PFMERGE produces correct union
- Sparse-to-dense transition occurs at correct threshold
- TYPE returns "string" for HLL keys
- OBJECT ENCODING returns "raw" for HLL keys

---

[← Back](README.md)
