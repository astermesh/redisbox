import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  arrayReply,
  bulkReply,
  errorReply,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
} from '../../types.ts';
import { RedisStream, parseStreamId } from '../../stream.ts';
import type { StreamEntry, StreamId } from '../../stream.ts';

export const INVALID_STREAM_ID_ERR = errorReply(
  'ERR',
  'Invalid stream ID specified as stream command argument'
);

export const MAX_ID: StreamId = {
  ms: Number.MAX_SAFE_INTEGER,
  seq: Number.MAX_SAFE_INTEGER,
};
export const MIN_ID: StreamId = { ms: 0, seq: 0 };

const INVALID_START_RANGE_ERR = errorReply(
  'ERR',
  'invalid start ID for the interval'
);
const INVALID_END_RANGE_ERR = errorReply(
  'ERR',
  'invalid end ID for the interval'
);

/**
 * Parse an entry ID that is known to be valid (was validated on insert).
 * Falls back to 0-0 if somehow invalid (should never happen).
 */
export function safeParseId(id: string): StreamId {
  return parseStreamId(id) ?? { ms: 0, seq: 0 };
}

export function getStream(
  db: Database,
  key: string
):
  | { stream: RedisStream; error: null; exists: true }
  | { stream: null; error: Reply; exists: false }
  | { stream: null; error: null; exists: false } {
  const entry = db.get(key);
  if (!entry) return { stream: null, error: null, exists: false };
  if (entry.type !== 'stream')
    return { stream: null, error: WRONGTYPE_ERR, exists: false };
  return { stream: entry.value as RedisStream, error: null, exists: true };
}

/**
 * Format a stream entry as a Reply: [id, [field1, val1, field2, val2, ...]]
 */
export function entryToReply(entry: StreamEntry): Reply {
  const fields: Reply[] = [];
  for (const [f, v] of entry.fields) {
    fields.push(bulkReply(f));
    fields.push(bulkReply(v));
  }
  return arrayReply([bulkReply(entry.id), arrayReply(fields)]);
}

/**
 * Parse optional COUNT from args at position i.
 * Returns { count, nextIdx } or { error }.
 */
export function parseCount(
  args: string[],
  i: number
): { count: number; nextIdx: number } | { error: Reply } {
  const countStr = args[i + 1];
  if (countStr === undefined) return { error: SYNTAX_ERR };
  const n = Number(countStr);
  if (!Number.isInteger(n) || n < 0) {
    return {
      error: errorReply('ERR', 'value is not an integer or out of range'),
    };
  }
  return { count: n, nextIdx: i + 2 };
}

/**
 * Increment a stream ID to the next possible value.
 * Used for exclusive start ranges: (id → id+1
 */
export function streamIncrId(id: StreamId): StreamId | null {
  if (id.seq < Number.MAX_SAFE_INTEGER) {
    return { ms: id.ms, seq: id.seq + 1 };
  }
  if (id.ms < Number.MAX_SAFE_INTEGER) {
    return { ms: id.ms + 1, seq: 0 };
  }
  return null; // overflow
}

/**
 * Decrement a stream ID to the previous possible value.
 * Used for exclusive end ranges: (id → id-1
 */
export function streamDecrId(id: StreamId): StreamId | null {
  if (id.seq > 0) {
    return { ms: id.ms, seq: id.seq - 1 };
  }
  if (id.ms > 0) {
    return { ms: id.ms - 1, seq: Number.MAX_SAFE_INTEGER };
  }
  return null; // underflow
}

/**
 * Parse a range boundary ID (for XRANGE/XREVRANGE).
 * Handles special IDs: - (min), + (max), exclusive ( prefix, and incomplete IDs (ms only).
 */
export function parseRangeId(
  id: string,
  mode: 'start' | 'end'
): StreamId | { error: Reply } | null {
  if (id === '-') return MIN_ID;
  if (id === '+') return MAX_ID;

  // Exclusive range with ( prefix (Redis 6.2+)
  const exclusive = id.startsWith('(');
  const rawId = exclusive ? id.substring(1) : id;

  let parsed: StreamId | null;
  const dashIdx = rawId.indexOf('-');
  if (dashIdx === -1) {
    // Incomplete ID — just ms part
    const ms = Number(rawId);
    if (!Number.isInteger(ms) || ms < 0) return null;
    parsed = { ms, seq: mode === 'start' ? 0 : Number.MAX_SAFE_INTEGER };
  } else {
    parsed = parseStreamId(rawId);
  }

  if (!parsed) return null;

  if (exclusive) {
    if (mode === 'start') {
      const incr = streamIncrId(parsed);
      if (!incr) return { error: INVALID_START_RANGE_ERR };
      return incr;
    } else {
      const decr = streamDecrId(parsed);
      if (!decr) return { error: INVALID_END_RANGE_ERR };
      return decr;
    }
  }

  return parsed;
}
