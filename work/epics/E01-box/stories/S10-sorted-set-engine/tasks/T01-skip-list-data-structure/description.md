# T01: Skip List Data Structure

**Status:** done

Implement a skip list matching Redis behavior: max 32 levels, level probability 0.25, span tracking for rank queries. Comparison: primary by score (float64), secondary by element (lexicographic byte comparison). Support +inf and -inf scores. Include backward pointers for reverse iteration.

## Acceptance Criteria

- O(log N) insert/delete/find
- O(log N) rank lookup via spans
- Correct comparison semantics

---

[← Back to T01](README.md)
