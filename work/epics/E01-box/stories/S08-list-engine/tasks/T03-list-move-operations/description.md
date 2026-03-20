# T03: List Move Operations

**Status:** done

Implement LMOVE, LMPOP. LMOVE source destination LEFT|RIGHT LEFT|RIGHT (replaces RPOPLPUSH). LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count] (Redis 7.0+): pop from first non-empty list.

## Acceptance Criteria

- Atomic move between lists
- LMPOP multi-key works correctly

---

[← Back to T03](README.md)
