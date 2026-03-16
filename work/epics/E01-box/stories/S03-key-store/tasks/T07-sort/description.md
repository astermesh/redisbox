# T07: SORT and SORT_RO

SORT is one of the most complex Redis commands. It works on lists, sets, and sorted sets, supporting external key lookups, hash field access, and result storage.

## Syntax

```
SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA]
```

SORT_RO (Redis 7.0) is a read-only variant — identical to SORT but without the STORE option.

## Details

- **Works on**: lists, sets, sorted sets
- **Default sort**: numeric (elements parsed as doubles). Error if element is not numeric and ALPHA not specified.
- **ALPHA**: sort lexicographically (byte comparison)
- **ASC|DESC**: ascending (default) or descending
- **LIMIT offset count**: skip `offset` elements, return `count` elements
- **BY pattern**: sort by external keys. Pattern uses `*` as placeholder for the element value. Example: `SORT mylist BY weight_*` looks up `weight_<element>` for each element and sorts by that value. Pattern can reference hash fields via `->`: `SORT mylist BY hash_*->field`.
- **BY nosort**: skip sorting, return elements in stored order. Useful with GET to retrieve external data without sorting overhead.
- **GET pattern**: retrieve external keys for each element. Multiple GET options allowed. `GET #` returns the element itself. Hash field access via `->`: `GET obj_*->name`.
- **STORE destination**: store result as a new list key. Returns the count of stored elements. If result is empty, the destination key is deleted (if it existed).

## Edge Cases

- SORT on non-existent key returns empty array (or stores nothing)
- SORT on wrong type returns WRONGTYPE error
- BY with non-existent external keys: elements with missing sort keys sort as if their value is 0 (numeric) or empty string (ALPHA)
- GET with non-existent external keys: returns nil for that element's GET
- STORE overwrites the destination key (including its TTL)
- In cluster mode, BY and GET with external keys are restricted (require keys on same node)

## Acceptance Criteria

- SORT works on lists, sets, and sorted sets
- Numeric and ALPHA sorting produce correct order
- ASC/DESC ordering correct
- LIMIT offset/count works correctly
- BY pattern resolves external keys (including hash fields via `->`)
- BY nosort returns elements in storage order
- GET pattern retrieves external keys (including hash fields)
- Multiple GET options work (returns interleaved results)
- GET # returns the element itself
- STORE writes result as list, returns count
- STORE on empty result deletes destination key
- SORT_RO rejects STORE option
- All error messages match Redis exactly

---

[← Back](README.md)
