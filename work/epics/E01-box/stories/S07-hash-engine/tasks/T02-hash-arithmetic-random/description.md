# T02: Hash Arithmetic and Random

Implement HINCRBY, HINCRBYFLOAT, HRANDFIELD, HSCAN. HINCRBY: integer increment, error on non-integer. HINCRBYFLOAT: float increment with Redis formatting. HRANDFIELD: positive count=unique, negative count=may duplicate, supports WITHVALUES. HSCAN: cursor-based iteration with MATCH and COUNT.

## Acceptance Criteria

- Arithmetic matches Redis
- HRANDFIELD distribution correct
- HSCAN iterates all fields

---

[← Back to T02](README.md)
