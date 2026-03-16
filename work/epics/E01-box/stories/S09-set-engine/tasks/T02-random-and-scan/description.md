# T02: Random and Scan

Implement SRANDMEMBER, SPOP, SSCAN. SRANDMEMBER: positive count=unique elements (up to set size), negative count=may duplicate with absolute value as count. SPOP count (Redis 3.2+): remove and return count random members. SSCAN: cursor-based iteration.

## Acceptance Criteria

- Random distribution correct
- SPOP removes elements
- SSCAN iterates all members

---

[← Back to T02](README.md)
