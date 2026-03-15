# T02: Lazy Expiration

Implement expireIfNeeded check on every key access. If key has TTL and current time >= expiry time, delete key and return null. Expiry index: Map<string, number> mapping key to absolute expiry timestamp in ms.

## Acceptance Criteria

- Expired keys return null on access
- Expiry metadata cleaned up
- Non-expired keys unaffected

---

[← Back to Tasks](../README.md)
