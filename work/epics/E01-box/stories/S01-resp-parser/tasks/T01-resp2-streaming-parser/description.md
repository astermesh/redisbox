# T01: RESP2 Streaming Parser

**Status:** done

Implement streaming parser that processes incoming bytes incrementally. Must handle all 5 RESP2 types: simple strings (+), errors (-), integers (:), bulk strings ($), and arrays (*). Handle null bulk strings ($-1), null arrays (*-1), empty strings ($0), and nested arrays. Parser must accumulate partial data across multiple `data` events and emit complete parsed values. Use callback-based architecture.

## Acceptance Criteria

- All 5 RESP2 types parsed correctly (simple string, error, integer, bulk string, array)
- Null bulk strings (`$-1`) and null arrays (`*-1`) handled
- Empty bulk strings (`$0\r\n\r\n`) parsed as empty Buffer, not null
- Nested arrays parsed correctly (arrays containing arrays)
- Partial buffer handling works (command split across multiple TCP chunks)
- Pipelined commands parsed sequentially from a single buffer
- Binary safety: bulk strings can contain any bytes including `\r\n` and `\0` (length-prefixed, not delimiter-based)
- Parser handles bulk strings up to 512 MB (proto-max-bulk-len default)

---

[← Back to Tasks](../README.md)
