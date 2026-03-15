# T01: RESP2 Streaming Parser

Implement streaming parser that processes incoming bytes incrementally. Must handle all 5 RESP2 types: simple strings (+), errors (-), integers (:), bulk strings ($), and arrays (*). Handle null bulk strings ($-1), null arrays (*-1), empty strings ($0), and nested arrays. Parser must accumulate partial data across multiple `data` events and emit complete parsed values. Use callback-based architecture.

## Acceptance Criteria

- All RESP2 types parsed correctly
- Partial buffer handling works
- Pipelined commands parsed sequentially

---

[← Back to Tasks](../README.md)
