import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Reply } from '../types.ts';
import * as stream from './stream.ts';

function createDb(time = 1000) {
  let now = time;
  const engine = new RedisEngine({
    clock: () => now,
    rng: () => 0.5,
  });
  return {
    db: engine.db(0),
    engine,
    setTime: (t: number) => {
      now = t;
    },
    getTime: () => now,
  };
}

// ─── XADD ────────────────────────────────────────────────────────────

describe('XADD', () => {
  it('adds entry with auto-generated ID (*)', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), [
      'mystream',
      '*',
      'name',
      'Alice',
    ]);
    expect(reply).toEqual({ kind: 'bulk', value: '1000-0' });
  });

  it('auto-generates increasing IDs for same ms', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    const r2 = stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    expect(r2).toEqual({ kind: 'bulk', value: '1000-1' });
    const r3 = stream.xadd(db, getTime(), ['s', '*', 'c', '3']);
    expect(r3).toEqual({ kind: 'bulk', value: '1000-2' });
  });

  it('resets sequence when ms advances', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    const r2 = stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    expect(r2).toEqual({ kind: 'bulk', value: '2000-0' });
  });

  it('uses lastId ms when clock goes backward', () => {
    const { db, setTime, getTime } = createDb(5000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(3000); // clock goes backward
    const r2 = stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    expect(r2).toEqual({ kind: 'bulk', value: '5000-1' });
  });

  it('adds entry with explicit ID', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', '100-5', 'name', 'Bob']);
    expect(reply).toEqual({ kind: 'bulk', value: '100-5' });
  });

  it('rejects explicit ID 0-0', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', '0-0', 'name', 'Bob']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'The ID specified in XADD must be greater than 0-0',
    });
  });

  it('rejects explicit ID equal to last ID', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '5-3', 'a', '1']);
    const reply = stream.xadd(db, getTime(), ['s', '5-3', 'b', '2']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The ID specified in XADD is equal or smaller than the target stream top item',
    });
  });

  it('rejects explicit ID smaller than last ID', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '10-0', 'a', '1']);
    const reply = stream.xadd(db, getTime(), ['s', '5-0', 'b', '2']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The ID specified in XADD is equal or smaller than the target stream top item',
    });
  });

  it('adds entry with partial auto ID (<ms>-*)', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', '500-*', 'a', '1']);
    expect(reply).toEqual({ kind: 'bulk', value: '500-0' });
  });

  it('partial auto increments sequence for same ms', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '500-*', 'a', '1']);
    const r2 = stream.xadd(db, getTime(), ['s', '500-*', 'b', '2']);
    expect(r2).toEqual({ kind: 'bulk', value: '500-1' });
  });

  it('partial auto rejects when ms < lastId ms', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '500-0', 'a', '1']);
    const reply = stream.xadd(db, getTime(), ['s', '400-*', 'b', '2']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The ID specified in XADD is equal or smaller than the target stream top item',
    });
  });

  it('accepts explicit ID without sequence (defaults seq to 0)', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', '100', 'name', 'Bob']);
    expect(reply).toEqual({ kind: 'bulk', value: '100-0' });
  });

  it('stores multiple field-value pairs', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), [
      's',
      '*',
      'name',
      'Alice',
      'age',
      '30',
      'city',
      'NYC',
    ]);
    const entry = db.get('s');
    expect(entry).not.toBeNull();
    expect(entry?.type).toBe('stream');
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db, getTime } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xadd(db, getTime(), ['k', '*', 'a', '1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('rejects odd number of field args', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', '*', 'name']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'xadd' command",
    });
  });

  it('rejects no field args', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', '*']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'xadd' command",
    });
  });

  it('rejects invalid explicit ID', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), ['s', 'abc-def', 'a', '1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });

  // ─── NOMKSTREAM ──────────────────────────────────────────────────

  it('NOMKSTREAM returns nil when key does not exist', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), [
      's',
      'NOMKSTREAM',
      '*',
      'a',
      '1',
    ]);
    expect(reply).toEqual({ kind: 'bulk', value: null });
  });

  it('NOMKSTREAM adds to existing stream', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    const reply = stream.xadd(db, getTime(), [
      's',
      'NOMKSTREAM',
      '*',
      'b',
      '2',
    ]);
    expect(reply).toEqual({ kind: 'bulk', value: '1000-1' });
  });

  // ─── MAXLEN trimming ────────────────────────────────────────────

  it('MAXLEN trims to exact count', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    setTime(3000);
    stream.xadd(db, getTime(), ['s', 'MAXLEN', '2', '*', 'c', '3']);
    expect(stream.xlen(db, ['s'])).toEqual({ kind: 'integer', value: 2 });
  });

  it('MAXLEN with = modifier', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    setTime(3000);
    stream.xadd(db, getTime(), ['s', 'MAXLEN', '=', '2', '*', 'c', '3']);
    expect(stream.xlen(db, ['s'])).toEqual({ kind: 'integer', value: 2 });
  });

  it('MAXLEN with ~ modifier (approximate)', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    setTime(3000);
    stream.xadd(db, getTime(), ['s', 'MAXLEN', '~', '2', '*', 'c', '3']);
    // approximate may leave more, but we trim to exact for now
    const len = stream.xlen(db, ['s']);
    expect(
      (len as { kind: 'integer'; value: number }).value
    ).toBeLessThanOrEqual(3);
    expect(
      (len as { kind: 'integer'; value: number }).value
    ).toBeGreaterThanOrEqual(2);
  });

  it('MAXLEN 0 removes all entries but keeps stream key', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', 'MAXLEN', '0', '*', 'b', '2']);
    // Redis keeps zero-length streams (unlike other types)
    expect(db.has('s')).toBe(true);
    expect(stream.xlen(db, ['s'])).toEqual({ kind: 'integer', value: 0 });
  });

  it('MAXLEN with LIMIT option is accepted', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    setTime(3000);
    const reply = stream.xadd(db, getTime(), [
      's',
      'MAXLEN',
      '~',
      '2',
      'LIMIT',
      '100',
      '*',
      'c',
      '3',
    ]);
    expect(reply.kind).toBe('bulk');
  });

  // ─── MINID trimming ─────────────────────────────────────────────

  it('MINID removes entries with ID less than threshold', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    setTime(3000);
    stream.xadd(db, getTime(), ['s', 'MINID', '2000', '*', 'c', '3']);
    expect(stream.xlen(db, ['s'])).toEqual({ kind: 'integer', value: 2 });
  });

  // ─── Combined flags ─────────────────────────────────────────────

  it('NOMKSTREAM + MAXLEN together', () => {
    const { db, setTime, getTime } = createDb(1000);
    // first create the stream
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    setTime(3000);
    const reply = stream.xadd(db, getTime(), [
      's',
      'NOMKSTREAM',
      'MAXLEN',
      '2',
      '*',
      'c',
      '3',
    ]);
    expect(reply).toEqual({ kind: 'bulk', value: '3000-0' });
    expect(stream.xlen(db, ['s'])).toEqual({ kind: 'integer', value: 2 });
  });

  it('allows duplicate field names in an entry', () => {
    const { db, getTime } = createDb(1000);
    const reply = stream.xadd(db, getTime(), [
      's',
      '*',
      'name',
      'Alice',
      'name',
      'Bob',
    ]);
    expect(reply).toEqual({ kind: 'bulk', value: '1000-0' });
  });
});

// ─── XLEN ────────────────────────────────────────────────────────────

describe('XLEN', () => {
  it('returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(stream.xlen(db, ['missing'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(stream.xlen(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns correct length after adds', () => {
    const { db, setTime, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    setTime(2000);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    expect(stream.xlen(db, ['s'])).toEqual({
      kind: 'integer',
      value: 2,
    });
  });
});

// ─── helpers ────────────────────────────────────────────────────────────

/** Seed a stream with 5 entries at 1000-0 … 5000-0 */
function seedStream(
  db: ReturnType<typeof createDb>['db'],
  clock: { setTime: (t: number) => void; getTime: () => number }
) {
  for (let i = 1; i <= 5; i++) {
    clock.setTime(i * 1000);
    stream.xadd(db, clock.getTime(), ['s', '*', 'k', String(i)]);
  }
}

function entryReply(id: string, fields: [string, string][]): Reply {
  return {
    kind: 'array',
    value: [
      { kind: 'bulk', value: id },
      {
        kind: 'array',
        value: fields.flatMap(([f, v]) => [
          { kind: 'bulk', value: f },
          { kind: 'bulk', value: v },
        ]),
      },
    ],
  };
}

// ─── XRANGE ─────────────────────────────────────────────────────────────

describe('XRANGE', () => {
  it('returns all entries with - +', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [['k', '1']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('returns subset by ID range', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '2000-0', '4000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
      ],
    });
  });

  it('respects COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', '2']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [['k', '1']]),
        entryReply('2000-0', [['k', '2']]),
      ],
    });
  });

  it('returns empty array for non-existent key', () => {
    const { db } = createDb();
    const reply = stream.xrange(db, ['missing', '-', '+']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xrange(db, ['k', '-', '+']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns empty array when start > end', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '5000-0', '1000-0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('handles incomplete IDs (ms only) for start', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '3000', '5000']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('returns error for invalid ID', () => {
    const { db } = createDb();
    const reply = stream.xrange(db, ['s', 'abc', '+']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });

  it('handles entries with multiple fields', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1', 'b', '2']);
    const reply = stream.xrange(db, ['s', '-', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [
          ['a', '1'],
          ['b', '2'],
        ]),
      ],
    });
  });

  it('COUNT 0 returns empty array', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', '0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('rejects non-integer COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', 'abc']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('rejects negative COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '+', 'COUNT', '-1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });
});

// ─── XREVRANGE ──────────────────────────────────────────────────────────

describe('XREVRANGE', () => {
  it('returns all entries in reverse with + -', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '+', '-']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('5000-0', [['k', '5']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('1000-0', [['k', '1']]),
      ],
    });
  });

  it('returns subset by ID range in reverse', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '4000-0', '2000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('4000-0', [['k', '4']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('2000-0', [['k', '2']]),
      ],
    });
  });

  it('respects COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '+', '-', 'COUNT', '2']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('5000-0', [['k', '5']]),
        entryReply('4000-0', [['k', '4']]),
      ],
    });
  });

  it('returns empty array for non-existent key', () => {
    const { db } = createDb();
    const reply = stream.xrevrange(db, ['missing', '+', '-']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xrevrange(db, ['k', '+', '-']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns empty when end > start (reversed args)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrevrange(ctx.db, ['s', '1000-0', '5000-0']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });
});

// ─── XREAD ──────────────────────────────────────────────────────────────

describe('XREAD', () => {
  it('reads entries after given ID from single stream', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '2000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            {
              kind: 'array',
              value: [
                entryReply('3000-0', [['k', '3']]),
                entryReply('4000-0', [['k', '4']]),
                entryReply('5000-0', [['k', '5']]),
              ],
            },
          ],
        },
      ],
    });
  });

  it('reads from multiple streams', () => {
    const ctx = createDb(0);
    ctx.setTime(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['a', '*', 'x', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['a', '*', 'x', '2']);
    ctx.setTime(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['b', '*', 'y', '10']);

    const reply = stream.xread(ctx.db, ['STREAMS', 'a', 'b', '0-0', '0-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'a' },
            {
              kind: 'array',
              value: [
                entryReply('1000-0', [['x', '1']]),
                entryReply('2000-0', [['x', '2']]),
              ],
            },
          ],
        },
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'b' },
            {
              kind: 'array',
              value: [entryReply('1000-0', [['y', '10']])],
            },
          ],
        },
      ],
    });
  });

  it('respects COUNT', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['COUNT', '2', 'STREAMS', 's', '0-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            {
              kind: 'array',
              value: [
                entryReply('1000-0', [['k', '1']]),
                entryReply('2000-0', [['k', '2']]),
              ],
            },
          ],
        },
      ],
    });
  });

  it('returns nil-array when no entries found', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '5000-0']);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('returns nil-array for non-existent stream', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['STREAMS', 'missing', '0-0']);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = stream.xread(db, ['STREAMS', 'k', '0-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('skips non-existent streams in multi-stream read', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['a', '*', 'x', '1']);

    const reply = stream.xread(ctx.db, [
      'STREAMS',
      'a',
      'missing',
      '0-0',
      '0-0',
    ]);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 'a' },
            {
              kind: 'array',
              value: [entryReply('1000-0', [['x', '1']])],
            },
          ],
        },
      ],
    });
  });

  it('returns syntax error when STREAMS keyword missing', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['s', '0-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns error for unbalanced streams/ids', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['STREAMS', 'a', 'b', '0-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
    });
  });

  it('$ ID returns nil-array (non-blocking)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '$']);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('BLOCK option is accepted (non-blocking returns immediately)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, [
      'COUNT',
      '1',
      'BLOCK',
      '0',
      'STREAMS',
      's',
      '0-0',
    ]);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            {
              kind: 'array',
              value: [entryReply('1000-0', [['k', '1']])],
            },
          ],
        },
      ],
    });
  });

  it('reads entries with ID 0-0 (gets all)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xread(ctx.db, ['STREAMS', 's', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const streamEntries = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamEntries.value).toHaveLength(5);
  });
});
