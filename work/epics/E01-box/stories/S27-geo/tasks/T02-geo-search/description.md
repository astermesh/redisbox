# T02: Geo Search and Radius Queries

**Status:** done

Implement GEOSEARCH (unified, Redis 6.2+) and legacy GEORADIUS/GEORADIUSBYMEMBER commands.

## Details

- **GEOSEARCH key FROMMEMBER member|FROMLONLAT longitude latitude BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]**: Unified geo search. Returns members within radius or bounding box.
- **GEOSEARCHSTORE destination source [FROMMEMBER|FROMLONLAT ...] [BYRADIUS|BYBOX ...] [ASC|DESC] [COUNT count [ANY]] [STOREDIST]**: Store results in destination key. STOREDIST stores distances as scores instead of geohashes.
- **GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]**: Legacy radius search from point. Deprecated since Redis 6.2.
- **GEORADIUSBYMEMBER key member radius ...** (same options): Legacy radius search from member.
- **GEORADIUS_RO / GEORADIUSBYMEMBER_RO**: Read-only variants (no STORE option).
- Search algorithm: compute geohash of center, determine neighboring geohash cells at appropriate precision, ZRANGEBYSCORE on each cell's hash range, filter by actual distance.
- COUNT with ANY flag: return as soon as enough matches found (may not be closest).

## Acceptance Criteria

- GEOSEARCH by radius returns correct members within distance
- GEOSEARCH by box returns correct members within bounding box
- ASC/DESC ordering by distance works
- COUNT limits results correctly
- WITHCOORD/WITHDIST/WITHHASH return correct additional data
- GEOSEARCHSTORE stores results correctly
- STOREDIST stores distances as scores
- Legacy GEORADIUS/GEORADIUSBYMEMBER return same results as equivalent GEOSEARCH
- All distance calculations match Redis within floating-point tolerance

---

[← Back](README.md)
