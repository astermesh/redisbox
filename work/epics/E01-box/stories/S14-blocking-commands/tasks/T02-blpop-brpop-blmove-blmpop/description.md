# T02: BLPOP, BRPOP, BLMOVE, BLMPOP

**Status:** done

Blocking list commands. If data available immediately, return result (non-blocking path). If no data, block until data available or timeout (0=infinite). BLPOP/BRPOP: return [key, element] from first non-empty key. BLMOVE: block until source has data, then move. BLMPOP: block until any listed key has data. Inside MULTI, behave as non-blocking variants.

## Acceptance Criteria

- Blocking works with timeout
- Immediate return when data available
- MULTI non-blocking behavior

---

[← Back](README.md)
