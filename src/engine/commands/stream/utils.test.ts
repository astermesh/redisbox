import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import { RedisStream } from '../../stream.ts';
import {
  safeParseId,
  getStream,
  entryToReply,
  parseCount,
  streamIncrId,
  streamDecrId,
  parseRangeId,
  INVALID_STREAM_ID_ERR,
  MAX_ID,
  MIN_ID,
} from './utils.ts';

function createDb() {
  const engine = new RedisEngine({
    clock: () => 1000,
    rng: () => 0.5,
  });
  return engine.db(0);
}

// ─── safeParseId ──────────────────────────────────────────────────────

describe('safeParseId', () => {
  it('parses a valid stream ID', () => {
    expect(safeParseId('1000-5')).toEqual({ ms: 1000, seq: 5 });
  });

  it('parses ID with zero sequence', () => {
    expect(safeParseId('0-0')).toEqual({ ms: 0, seq: 0 });
  });

  it('parses ID without sequence (ms only)', () => {
    expect(safeParseId('42')).toEqual({ ms: 42, seq: 0 });
  });

  it('returns 0-0 for invalid input', () => {
    expect(safeParseId('abc')).toEqual({ ms: 0, seq: 0 });
  });

  it('returns 0-0 for empty string', () => {
    expect(safeParseId('')).toEqual({ ms: 0, seq: 0 });
  });

  it('returns 0-0 for negative numbers', () => {
    expect(safeParseId('-1-0')).toEqual({ ms: 0, seq: 0 });
  });

  it('returns 0-0 for IDs exceeding parser limits', () => {
    // parseNonNegativeInt rejects strings longer than 15 chars
    expect(safeParseId('9007199254740991-9007199254740991')).toEqual({
      ms: 0,
      seq: 0,
    });
  });

  it('parses large valid IDs within parser limits', () => {
    expect(safeParseId('999999999999999-999999999999999')).toEqual({
      ms: 999999999999999,
      seq: 999999999999999,
    });
  });
});

// ─── getStream ────────────────────────────────────────────────────────

describe('getStream', () => {
  it('returns null stream and no error for non-existent key', () => {
    const db = createDb();
    const result = getStream(db, 'missing');
    expect(result).toEqual({ stream: null, error: null, exists: false });
  });

  it('returns WRONGTYPE error for non-stream key', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const result = getStream(db, 'k');
    expect(result.stream).toBeNull();
    expect(result.exists).toBe(false);
    expect(result.error).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns stream for existing stream key', () => {
    const db = createDb();
    const stream = new RedisStream();
    db.set('s', 'stream', 'stream', stream);
    const result = getStream(db, 's');
    expect(result.stream).toBe(stream);
    expect(result.error).toBeNull();
    expect(result.exists).toBe(true);
  });
});

// ─── entryToReply ─────────────────────────────────────────────────────

describe('entryToReply', () => {
  it('formats a single-field entry', () => {
    const reply = entryToReply({
      id: '1000-0',
      fields: [['name', 'Alice']],
    });
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '1000-0' },
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'name' },
            { kind: 'bulk', value: 'Alice' },
          ],
        },
      ],
    });
  });

  it('formats a multi-field entry', () => {
    const reply = entryToReply({
      id: '2000-3',
      fields: [
        ['a', '1'],
        ['b', '2'],
        ['c', '3'],
      ],
    });
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '2000-3' },
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'a' },
            { kind: 'bulk', value: '1' },
            { kind: 'bulk', value: 'b' },
            { kind: 'bulk', value: '2' },
            { kind: 'bulk', value: 'c' },
            { kind: 'bulk', value: '3' },
          ],
        },
      ],
    });
  });

  it('formats an entry with no fields', () => {
    const reply = entryToReply({ id: '0-1', fields: [] });
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '0-1' },
        { kind: 'array', value: [] },
      ],
    });
  });
});

// ─── parseCount ───────────────────────────────────────────────────────

describe('parseCount', () => {
  it('parses a valid count', () => {
    const result = parseCount(['COUNT', '10'], 0);
    expect(result).toEqual({ count: 10, nextIdx: 2 });
  });

  it('parses count of 0', () => {
    const result = parseCount(['COUNT', '0'], 0);
    expect(result).toEqual({ count: 0, nextIdx: 2 });
  });

  it('returns SYNTAX_ERR when count value is missing', () => {
    const result = parseCount(['COUNT'], 0);
    expect(result).toHaveProperty('error');
    expect((result as { error: { kind: string } }).error.kind).toBe('error');
  });

  it('returns error for negative count', () => {
    const result = parseCount(['COUNT', '-1'], 0);
    expect(result).toHaveProperty('error');
    const err = (result as { error: { message: string } }).error;
    expect(err.message).toBe('value is not an integer or out of range');
  });

  it('returns error for non-integer count', () => {
    const result = parseCount(['COUNT', '3.5'], 0);
    expect(result).toHaveProperty('error');
  });

  it('returns error for non-numeric count', () => {
    const result = parseCount(['COUNT', 'abc'], 0);
    expect(result).toHaveProperty('error');
  });

  it('parses count at arbitrary index', () => {
    const result = parseCount(['key', 'COUNT', '5', 'extra'], 1);
    expect(result).toEqual({ count: 5, nextIdx: 3 });
  });

  it('returns SYNTAX_ERR when index is at end of args', () => {
    const result = parseCount(['COUNT', '5'], 1);
    expect(result).toHaveProperty('error');
  });
});

// ─── streamIncrId ─────────────────────────────────────────────────────

describe('streamIncrId', () => {
  it('increments sequence number', () => {
    expect(streamIncrId({ ms: 100, seq: 5 })).toEqual({ ms: 100, seq: 6 });
  });

  it('increments from seq 0', () => {
    expect(streamIncrId({ ms: 100, seq: 0 })).toEqual({ ms: 100, seq: 1 });
  });

  it('rolls over to next ms when seq is at MAX_SAFE_INTEGER', () => {
    expect(streamIncrId({ ms: 100, seq: Number.MAX_SAFE_INTEGER })).toEqual({
      ms: 101,
      seq: 0,
    });
  });

  it('returns null on full overflow (both ms and seq at max)', () => {
    expect(
      streamIncrId({
        ms: Number.MAX_SAFE_INTEGER,
        seq: Number.MAX_SAFE_INTEGER,
      })
    ).toBeNull();
  });

  it('rolls over ms when seq is max but ms is not', () => {
    expect(
      streamIncrId({
        ms: Number.MAX_SAFE_INTEGER - 1,
        seq: Number.MAX_SAFE_INTEGER,
      })
    ).toEqual({ ms: Number.MAX_SAFE_INTEGER, seq: 0 });
  });
});

// ─── streamDecrId ─────────────────────────────────────────────────────

describe('streamDecrId', () => {
  it('decrements sequence number', () => {
    expect(streamDecrId({ ms: 100, seq: 5 })).toEqual({ ms: 100, seq: 4 });
  });

  it('decrements to seq 0', () => {
    expect(streamDecrId({ ms: 100, seq: 1 })).toEqual({ ms: 100, seq: 0 });
  });

  it('rolls over to previous ms when seq is 0', () => {
    expect(streamDecrId({ ms: 100, seq: 0 })).toEqual({
      ms: 99,
      seq: Number.MAX_SAFE_INTEGER,
    });
  });

  it('returns null on full underflow (0-0)', () => {
    expect(streamDecrId({ ms: 0, seq: 0 })).toBeNull();
  });

  it('decrements ms 1 seq 0 to ms 0 seq MAX', () => {
    expect(streamDecrId({ ms: 1, seq: 0 })).toEqual({
      ms: 0,
      seq: Number.MAX_SAFE_INTEGER,
    });
  });
});

// ─── parseRangeId ─────────────────────────────────────────────────────

describe('parseRangeId', () => {
  it('parses - as MIN_ID', () => {
    expect(parseRangeId('-', 'start')).toEqual(MIN_ID);
    expect(parseRangeId('-', 'end')).toEqual(MIN_ID);
  });

  it('parses + as MAX_ID', () => {
    expect(parseRangeId('+', 'start')).toEqual(MAX_ID);
    expect(parseRangeId('+', 'end')).toEqual(MAX_ID);
  });

  it('parses a full ID (ms-seq)', () => {
    expect(parseRangeId('1000-5', 'start')).toEqual({ ms: 1000, seq: 5 });
    expect(parseRangeId('1000-5', 'end')).toEqual({ ms: 1000, seq: 5 });
  });

  it('parses incomplete ID (ms only) in start mode — seq defaults to 0', () => {
    expect(parseRangeId('1000', 'start')).toEqual({ ms: 1000, seq: 0 });
  });

  it('parses incomplete ID (ms only) in end mode — seq defaults to MAX', () => {
    expect(parseRangeId('1000', 'end')).toEqual({
      ms: 1000,
      seq: Number.MAX_SAFE_INTEGER,
    });
  });

  it('returns null for invalid ID', () => {
    expect(parseRangeId('abc', 'start')).toBeNull();
    expect(parseRangeId('abc-def', 'start')).toBeNull();
  });

  it('returns null for input like -5 (not the special - token)', () => {
    // '-5' is not '-', so it's parsed as an ID; the '-' at index 0 makes
    // dashIdx=0, msPart='', which parseStreamId rejects → null
    expect(parseRangeId('-5', 'start')).toBeNull();
  });

  // ─── Exclusive ranges ──────────────────────────────────────────────

  it('exclusive start increments the parsed ID', () => {
    const result = parseRangeId('(1000-5', 'start');
    expect(result).toEqual({ ms: 1000, seq: 6 });
  });

  it('exclusive end decrements the parsed ID', () => {
    const result = parseRangeId('(1000-5', 'end');
    expect(result).toEqual({ ms: 1000, seq: 4 });
  });

  it('exclusive start with incomplete ID (ms only)', () => {
    // Incomplete start: ms=1000, seq=0 → incremented: ms=1000, seq=1
    const result = parseRangeId('(1000', 'start');
    expect(result).toEqual({ ms: 1000, seq: 1 });
  });

  it('exclusive end with incomplete ID (ms only)', () => {
    // Incomplete end: ms=1000, seq=MAX → decremented: ms=1000, seq=MAX-1
    const result = parseRangeId('(1000', 'end');
    expect(result).toEqual({ ms: 1000, seq: Number.MAX_SAFE_INTEGER - 1 });
  });

  it('exclusive start returns null when ID string exceeds parser limits', () => {
    // parseNonNegativeInt rejects strings >15 chars, so MAX_SAFE_INTEGER
    // as string is rejected before the exclusive logic runs
    const result = parseRangeId(
      `(${Number.MAX_SAFE_INTEGER}-${Number.MAX_SAFE_INTEGER}`,
      'start'
    );
    expect(result).toBeNull();
  });

  it('exclusive start with seq at max rolls over ms', () => {
    // 100-999999999999999 exclusive start → incr seq wraps to next ms
    const result = parseRangeId('(100-999999999999999', 'start');
    expect(result).toEqual({ ms: 100, seq: 1000000000000000 });
  });

  it('exclusive end returns error on underflow', () => {
    const result = parseRangeId('(0-0', 'end');
    expect(result).not.toBeNull();
    expect(result).toHaveProperty('error');
    const err = result as { error: { kind: string; prefix: string } };
    expect(err.error.kind).toBe('error');
  });

  it('exclusive with invalid ID returns null', () => {
    expect(parseRangeId('(abc', 'start')).toBeNull();
    expect(parseRangeId('(abc-def', 'end')).toBeNull();
  });

  it('parses 0-0 as start range', () => {
    expect(parseRangeId('0-0', 'start')).toEqual({ ms: 0, seq: 0 });
  });

  it('parses 0 as incomplete start range', () => {
    expect(parseRangeId('0', 'start')).toEqual({ ms: 0, seq: 0 });
  });

  it('parses 0 as incomplete end range', () => {
    expect(parseRangeId('0', 'end')).toEqual({
      ms: 0,
      seq: Number.MAX_SAFE_INTEGER,
    });
  });
});

// ─── Constants ────────────────────────────────────────────────────────

describe('constants', () => {
  it('MAX_ID has max safe integer for both fields', () => {
    expect(MAX_ID).toEqual({
      ms: Number.MAX_SAFE_INTEGER,
      seq: Number.MAX_SAFE_INTEGER,
    });
  });

  it('MIN_ID is 0-0', () => {
    expect(MIN_ID).toEqual({ ms: 0, seq: 0 });
  });

  it('INVALID_STREAM_ID_ERR is a proper error reply', () => {
    expect(INVALID_STREAM_ID_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});
