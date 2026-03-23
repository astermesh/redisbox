# T02: HyperLogLog Internals and Debug Commands

**Status:** done

Implement PFDEBUG and PFSELFTEST debug commands, and verify HLL internal consistency.

## Details

- **PFDEBUG GETREG key**: return raw register values (for testing/debugging)
- **PFDEBUG DECODE key**: return sparse representation decoded
- **PFSELFTEST**: run internal HLL consistency tests, return OK or error
- Verify that the hash function produces identical register assignments as real Redis for the same inputs
- Verify cardinality estimation accuracy across different cardinality ranges (10, 100, 1K, 10K, 100K, 1M)

## Acceptance Criteria

- PFDEBUG GETREG returns register values
- PFSELFTEST completes without error
- Cardinality estimates within 2× standard error of real Redis values for same inputs

---

[← Back](README.md)
