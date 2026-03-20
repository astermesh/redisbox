# T03: Range Queries by Score and Rank

**Status:** done

Implement ZRANGE (unified Redis 6.2+ with BYSCORE, BYLEX, REV, LIMIT options), ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX, ZREVRANGEBYLEX, ZCOUNT, ZLEXCOUNT, ZRANGESTORE. Support exclusive ranges with `(` prefix. Handle +inf/-inf bounds. WITHSCORES option.

## Acceptance Criteria

- All range queries match Redis
- Exclusive ranges work
- LIMIT offset/count work

---

[← Back to T03](README.md)
