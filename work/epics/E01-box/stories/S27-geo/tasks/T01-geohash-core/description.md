# T01: Geohash Encoding and Core Commands

Implement geohash encoding/decoding and basic geo commands that build on the sorted set engine.

## Details

- Implement 52-bit geohash encoding: interleave 26 bits of normalized longitude and 26 bits of normalized latitude into a single number stored as sorted set score
- Implement geohash decoding: reverse the interleaving to extract longitude and latitude
- **GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]**: Add members with coordinates. Uses ZADD internally with geohash as score. NX/XX/CH flags match ZADD semantics (since Redis 6.2).
- **GEOPOS key member [member ...]**: Return longitude/latitude pairs for members. Returns nil for non-existent members.
- **GEODIST key member1 member2 [M|KM|FT|MI]**: Return distance between two members. Default unit is meters. Uses Haversine formula.
- **GEOHASH key member [member ...]**: Return Geohash strings (11-character base32 encoding) for members.
- Validate coordinate ranges: longitude -180 to 180, latitude -85.05112878 to 85.05112878

## Acceptance Criteria

- Geohash encoding matches Redis exactly (bit-identical scores for same coordinates)
- GEOADD stores members in sorted set with correct geohash scores
- GEOPOS returns coordinates with precision matching Redis (at least 6 decimal places)
- GEODIST calculations match Redis within floating-point tolerance
- GEOHASH returns correct 11-character base32 strings matching Redis output
- Coordinate validation rejects out-of-range values with correct error message
- TYPE returns "zset" for geo keys

---

[← Back](README.md)
