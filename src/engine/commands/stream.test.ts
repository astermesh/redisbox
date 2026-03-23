import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Reply } from '../types.ts';
import { RedisStream } from '../stream.ts';
import * as stream from './stream.ts';
import type { Database } from '../database.ts';

function getStreamHelper(
  db: Database,
  key: string
): { stream: RedisStream | null } {
  const entry = db.get(key);
  if (!entry || entry.type !== 'stream') return { stream: null };
  return { stream: entry.value as RedisStream };
}

function getGroup(
  s: RedisStream | null,
  groupName: string
): import('../stream.ts').ConsumerGroup {
  if (!s) throw new Error('stream is null');
  const group = s.getGroup(groupName);
  if (!group) throw new Error(`group ${groupName} not found`);
  return group;
}

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

  // ─── Exclusive ranges ( prefix ────────────────────────────────────

  it('exclusive start with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '(2000-0', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('exclusive end with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '-', '(4000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('1000-0', [['k', '1']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
      ],
    });
  });

  it('exclusive both start and end with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    const reply = stream.xrange(ctx.db, ['s', '(1000-0', '(5000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('2000-0', [['k', '2']]),
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
      ],
    });
  });

  it('exclusive with incomplete ID (ms only)', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    // (2000 as start → parses as 2000-0, then increments to 2000-1
    const reply = stream.xrange(ctx.db, ['s', '(2000', '+']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('5000-0', [['k', '5']]),
      ],
    });
  });

  it('exclusive range with invalid ID returns error', () => {
    const { db } = createDb();
    const reply = stream.xrange(db, ['s', '(abc', '+']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
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

  // ─── Exclusive ranges ( prefix ────────────────────────────────────

  it('exclusive end (lower bound) with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    // XREVRANGE s + (2000-0 → entries > 2000-0 in reverse
    const reply = stream.xrevrange(ctx.db, ['s', '+', '(2000-0']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('5000-0', [['k', '5']]),
        entryReply('4000-0', [['k', '4']]),
        entryReply('3000-0', [['k', '3']]),
      ],
    });
  });

  it('exclusive start (upper bound) with ( prefix', () => {
    const ctx = createDb(0);
    seedStream(ctx.db, ctx);
    // XREVRANGE s (4000-0 - → entries < 4000-0 in reverse
    const reply = stream.xrevrange(ctx.db, ['s', '(4000-0', '-']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        entryReply('3000-0', [['k', '3']]),
        entryReply('2000-0', [['k', '2']]),
        entryReply('1000-0', [['k', '1']]),
      ],
    });
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

  it('returns error when STREAMS keyword missing and args unbalanced', () => {
    const { db } = createDb();
    const reply = stream.xread(db, ['STREAMS']);
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

// ─── XGROUP ─────────────────────────────────────────────────────────

const xgroupSpec = stream.specs.find((s) => s.name === 'xgroup');

function execXgroup(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  if (!xgroupSpec) throw new Error('xgroup spec not found');
  return xgroupSpec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

describe('XGROUP CREATE', () => {
  it('creates a consumer group with $ ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'mygroup', '$']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('creates a consumer group with 0 ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('creates a consumer group with explicit ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', '1000-2']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns BUSYGROUP if group already exists', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'mygroup', '$']);
    const reply = execXgroup(ctx, ['CREATE', 's', 'mygroup', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'BUSYGROUP',
      message: 'Consumer Group name already exists',
    });
  });

  it('returns error if key does not exist without MKSTREAM', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['CREATE', 'nokey', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('creates stream with MKSTREAM when key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, [
      'CREATE',
      'newstream',
      'g1',
      '$',
      'MKSTREAM',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    // Stream should now exist
    const entry = ctx.db.get('newstream');
    expect(entry).toBeDefined();
    expect(entry?.type).toBe('stream');
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['CREATE', 'str', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', 'invalid']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });

  it('supports ENTRIESREAD option', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, [
      'CREATE',
      's',
      'g1',
      '0',
      'ENTRIESREAD',
      '5',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('allows multiple groups on same stream', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const r1 = execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const r2 = execXgroup(ctx, ['CREATE', 's', 'g2', '0']);
    expect(r1).toEqual({ kind: 'status', value: 'OK' });
    expect(r2).toEqual({ kind: 'status', value: 'OK' });
  });
});

describe('XGROUP SETID', () => {
  it('sets last-delivered-id to $', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, ['SETID', 's', 'g1', '$']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('sets last-delivered-id to explicit ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, ['SETID', 's', 'g1', '1000-2']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns NOGROUP error if group does not exist', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['SETID', 's', 'nogroup', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['SETID', 'nokey', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, ['SETID', 's', 'g1', 'bad']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});

describe('XGROUP DESTROY', () => {
  it('destroys an existing group and returns 1', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const reply = execXgroup(ctx, ['DESTROY', 's', 'g1']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 for non-existent group', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['DESTROY', 's', 'nogroup']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['DESTROY', 'nokey', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('group cannot be accessed after destroy', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['DESTROY', 's', 'g1']);
    // Creating a consumer in destroyed group should fail
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'g1' for key name 's'",
    });
  });
});

describe('XGROUP CREATECONSUMER', () => {
  it('creates a new consumer and returns 1', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 if consumer already exists', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns NOGROUP error if group does not exist', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'nogroup', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['CREATECONSUMER', 'nokey', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('can create multiple consumers in same group', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const r1 = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    const r2 = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'bob']);
    expect(r1).toEqual({ kind: 'integer', value: 1 });
    expect(r2).toEqual({ kind: 'integer', value: 1 });
  });
});

describe('XGROUP DELCONSUMER', () => {
  it('deletes a consumer with no pending entries and returns 0', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    const reply = execXgroup(ctx, ['DELCONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 for non-existent consumer', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    const reply = execXgroup(ctx, ['DELCONSUMER', 's', 'g1', 'nobody']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns NOGROUP error if group does not exist', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['DELCONSUMER', 's', 'nogroup', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, ['DELCONSUMER', 'nokey', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.',
    });
  });

  it('consumer is gone after deletion', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    execXgroup(ctx, ['DELCONSUMER', 's', 'g1', 'alice']);
    // Re-creating should return 1 (new consumer)
    const reply = execXgroup(ctx, ['CREATECONSUMER', 's', 'g1', 'alice']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });
});

describe('XGROUP edge cases', () => {
  it('returns error for unknown subcommand', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['BADCMD', 's', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "unknown subcommand or wrong number of arguments for 'xgroup|BADCMD' command",
    });
  });

  it('returns error with no args', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, []);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'xgroup' command",
    });
  });

  it('handles case-insensitive subcommands', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const r1 = execXgroup(ctx, ['create', 's', 'g1', '$']);
    expect(r1).toEqual({ kind: 'status', value: 'OK' });
    const r2 = execXgroup(ctx, ['destroy', 's', 'g1']);
    expect(r2).toEqual({ kind: 'integer', value: 1 });
  });

  it('XGROUP CREATE rejects non-integer ENTRIESREAD', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, [
      'CREATE',
      's',
      'g1',
      '0',
      'ENTRIESREAD',
      'abc',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('XGROUP CREATE rejects negative ENTRIESREAD', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, [
      'CREATE',
      's',
      'g1',
      '0',
      'ENTRIESREAD',
      '-1',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('XGROUP CREATE rejects ENTRIESREAD without value', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    const reply = execXgroup(ctx, ['CREATE', 's', 'g1', '0', 'ENTRIESREAD']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('XGROUP SETID with ENTRIESREAD option', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, [
      'SETID',
      's',
      'g1',
      '$',
      'ENTRIESREAD',
      '3',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('XGROUP SETID rejects invalid ENTRIESREAD', () => {
    const ctx = createDb(1000);
    seedStream(ctx.db, ctx);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXgroup(ctx, [
      'SETID',
      's',
      'g1',
      '$',
      'ENTRIESREAD',
      'bad',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('XGROUP CREATE with MKSTREAM and ENTRIESREAD together', () => {
    const ctx = createDb(1000);
    const reply = execXgroup(ctx, [
      'CREATE',
      'newkey',
      'g1',
      '$',
      'MKSTREAM',
      'ENTRIESREAD',
      '0',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    expect(ctx.db.get('newkey')?.type).toBe('stream');
  });

  it('XGROUP DESTROY returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['DESTROY', 'str', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('XGROUP DELCONSUMER returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['DELCONSUMER', 'str', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('XGROUP CREATECONSUMER returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['CREATECONSUMER', 'str', 'g1', 'alice']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('XGROUP SETID returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXgroup(ctx, ['SETID', 'str', 'g1', '$']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });
});

// ─── XREADGROUP ──────────────────────────────────────────────────────

function execXreadgroup(
  ctx: ReturnType<typeof createDb>,
  args: string[]
): Reply {
  const spec = stream.specs.find((s) => s.name === 'xreadgroup');
  if (!spec) throw new Error('xreadgroup spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function setupGroupWithEntries(time = 1000) {
  const ctx = createDb(time);
  // Add 5 entries: 1000-0 through 5000-0
  for (let i = 1; i <= 5; i++) {
    ctx.setTime(i * 1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
  }
  // Create group starting at 0 (all entries are new)
  execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
  return ctx;
}

describe('XREADGROUP', () => {
  it('reads new messages with > ID', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
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
              value: [
                entryReply('1000-0', [['k', '1']]),
                entryReply('2000-0', [['k', '2']]),
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

  it('returns nil-array when no new messages with >', () => {
    const ctx = setupGroupWithEntries();
    // Read all
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
    // Try reading again — no new entries
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply).toEqual({ kind: 'nil-array' });
  });

  it('respects COUNT with >', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // Should only get first 2 entries
    const arr = reply as { kind: 'array'; value: Reply[] };
    const streamArr = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamArr.value.length).toBe(2);
  });

  it('adds messages to PEL when reading with >', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // Now read pending for alice with specific ID 0-0
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '0-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const streamArr = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamArr.value.length).toBe(2);
  });

  it('reads pending entries when using specific ID (not >)', () => {
    const ctx = setupGroupWithEntries();
    // Read 3 entries to add to PEL
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Read pending from start
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
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
              value: [
                entryReply('1000-0', [['k', '1']]),
                entryReply('2000-0', [['k', '2']]),
                entryReply('3000-0', [['k', '3']]),
              ],
            },
          ],
        },
      ],
    });
  });

  it('returns empty array for pending when consumer has none', () => {
    const ctx = setupGroupWithEntries();
    // alice has no pending entries
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '0-0',
    ]);
    // Should return stream with empty array (not nil-array)
    expect(reply).toEqual({
      kind: 'array',
      value: [
        {
          kind: 'array',
          value: [
            { kind: 'bulk', value: 's' },
            { kind: 'array', value: [] },
          ],
        },
      ],
    });
  });

  it('auto-creates consumer on read', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'newconsumer',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.consumers.has('newconsumer')).toBe(true);
  });

  it('increments delivery count on re-read of pending', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    // Read pending — delivery count increments (matches real Redis)
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '0-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    const pe = group.pel.get('1000-0');
    expect(pe).toBeDefined();
    expect(pe?.deliveryCount).toBe(2);
  });

  it('returns NOGROUP for non-existent group', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'nogroup',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message:
        "No such key 's' or consumer group 'nogroup' in XREADGROUP with GROUP option",
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      'str',
      '>',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns NOGROUP for non-existent key', () => {
    const ctx = createDb(1000);
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      'nokey',
      '>',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message:
        "No such key 'nokey' or consumer group 'g1' in XREADGROUP with GROUP option",
    });
  });

  it('accepts NOACK option (entries not added to PEL)', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'NOACK',
      'STREAMS',
      's',
      '>',
    ]);
    // With NOACK, entries should NOT be in PEL
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.pel.size).toBe(0);
  });

  it('updates lastDeliveredId after reading with >', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.lastDeliveredId).toEqual({ ms: 2000, seq: 0 });
  });

  it('handles multiple streams', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s1', '*', 'a', '1']);
    stream.xadd(ctx.db, ctx.getTime(), ['s2', '*', 'b', '2']);
    execXgroup(ctx, ['CREATE', 's1', 'g1', '0']);
    execXgroup(ctx, ['CREATE', 's2', 'g1', '0']);
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's1',
      's2',
      '>',
      '>',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
  });

  it('requires GROUP keyword', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'NOTGROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply.kind).toBe('error');
  });

  it('returns error for $ as ID', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '$',
    ]);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.',
    });
  });

  it('updates delivery time and count when re-reading pending', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // Advance time and re-read pending
    ctx.setTime(20000);
    execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '0-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    const pe = group.pel.get('1000-0');
    expect(pe).toBeDefined();
    expect(pe?.deliveryCount).toBe(2);
    expect(pe?.deliveryTime).toBe(20000);
  });

  it('returns null fields for pending entries that were trimmed', () => {
    const ctx = createDb(1000);
    // Add entries
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    ctx.setTime(3000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '3']);
    // Create group starting at 0
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    // Read all 3 entries
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Trim the stream (removes first entry)
    ctx.setTime(4000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', 'MAXLEN', '2', '*', 'k', '4']);
    // Re-read pending — trimmed entry 1000-0 should return [id, null]
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '0-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const entries = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    // First entry was trimmed — should have null fields
    expect(entries.value[0]).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: '1000-0' },
        { kind: 'bulk', value: null },
      ],
    });
    // Remaining entries should be normal
    expect(entries.value.length).toBe(3);
  });

  it('group starting at $ only reads entries added after group creation', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '$']);
    // No new messages since group was created at $
    const reply = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    expect(reply).toEqual({ kind: 'nil-array' });
    // Now add a new entry
    ctx.setTime(3000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '3']);
    const reply2 = execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'STREAMS',
      's',
      '>',
    ]);
    const arr = reply2 as { kind: 'array'; value: Reply[] };
    const streamArr = (arr.value[0] as { kind: 'array'; value: Reply[] })
      .value[1] as { kind: 'array'; value: Reply[] };
    expect(streamArr.value.length).toBe(1);
  });
});

// ─── XACK ────────────────────────────────────────────────────────────

function execXack(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xack');
  if (!spec) throw new Error('xack spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

describe('XACK', () => {
  it('acknowledges a single message', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXack(ctx, ['s', 'g1', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('removes acknowledged entry from group PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    execXack(ctx, ['s', 'g1', '1000-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    expect(group.pel.has('1000-0')).toBe(false);
    expect(group.pel.size).toBe(2);
  });

  it('removes acknowledged entry from consumer PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    execXack(ctx, ['s', 'g1', '1000-0']);
    const { stream: s } = getStreamHelper(ctx.db, 's');
    const group = getGroup(s, 'g1');
    const consumer = group.consumers.get('alice');
    expect(consumer).toBeDefined();
    expect(consumer?.pending.has('1000-0')).toBe(false);
    expect(consumer?.pending.size).toBe(2);
  });

  it('acknowledges multiple messages at once', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXack(ctx, ['s', 'g1', '1000-0', '2000-0', '3000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 3 });
  });

  it('returns 0 for IDs not in PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXack(ctx, ['s', 'g1', '9999-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('counts only IDs that were actually in PEL', () => {
    const ctx = setupGroupWithEntries();
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    // 1000-0 is in PEL, 9999-0 is not
    const reply = execXack(ctx, ['s', 'g1', '1000-0', '9999-0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns 0 for non-existent group', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXack(ctx, ['s', 'nogroup', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 for non-existent key', () => {
    const ctx = createDb(1000);
    const reply = execXack(ctx, ['nokey', 'g1', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXack(ctx, ['str', 'g1', '1000-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXack(ctx, ['s', 'g1', 'invalid']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});

// ─── XPENDING ────────────────────────────────────────────────────────

function execXpending(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xpending');
  if (!spec) throw new Error('xpending spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

describe('XPENDING', () => {
  it('returns summary form with pending entries', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 3 },
        { kind: 'bulk', value: '1000-0' },
        { kind: 'bulk', value: '3000-0' },
        {
          kind: 'array',
          value: [
            {
              kind: 'array',
              value: [
                { kind: 'bulk', value: 'alice' },
                { kind: 'bulk', value: '3' },
              ],
            },
          ],
        },
      ],
    });
  });

  it('returns summary with zero pending', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXpending(ctx, ['s', 'g1']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'integer', value: 0 },
        { kind: 'bulk', value: null },
        { kind: 'bulk', value: null },
        { kind: 'nil-array' },
      ],
    });
  });

  it('returns summary with multiple consumers', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'bob',
      'COUNT',
      '1',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value[0]).toEqual({ kind: 'integer', value: 3 });
    expect(arr.value[1]).toEqual({ kind: 'bulk', value: '1000-0' });
    expect(arr.value[2]).toEqual({ kind: 'bulk', value: '3000-0' });
    // Consumer list
    const consumers = (arr.value[3] as { kind: 'array'; value: Reply[] }).value;
    expect(consumers.length).toBe(2);
  });

  it('returns detail form with range', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1', '-', '+', '10']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(3);
    // Each entry: [id, consumer, idle-time, delivery-count]
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    expect(entry.value[0]).toEqual({ kind: 'bulk', value: '1000-0' });
    expect(entry.value[1]).toEqual({ kind: 'bulk', value: 'alice' });
    expect(entry.value[2]).toEqual({ kind: 'integer', value: 0 });
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 1 });
  });

  it('detail form respects COUNT', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '5',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1', '-', '+', '2']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
  });

  it('detail form filters by consumer', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'bob',
      'COUNT',
      '2',
      'STREAMS',
      's',
      '>',
    ]);
    const reply = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
    // All entries should be bob's
    for (const entry of arr.value) {
      const e = entry as { kind: 'array'; value: Reply[] };
      expect(e.value[1]).toEqual({ kind: 'bulk', value: 'bob' });
    }
  });

  it('detail form filters by IDLE time (Redis 6.2+)', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Advance time so idle > 5000
    ctx.setTime(16000);
    const reply = execXpending(ctx, [
      's',
      'g1',
      'IDLE',
      '5000',
      '-',
      '+',
      '10',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    // All 3 entries should match since they were delivered at t=10000, now t=16000 (idle=6000)
    expect(arr.value.length).toBe(3);
  });

  it('detail form IDLE filters out non-idle entries', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Only 1ms later — none should be idle for > 5000ms
    ctx.setTime(10001);
    const reply = execXpending(ctx, [
      's',
      'g1',
      'IDLE',
      '5000',
      '-',
      '+',
      '10',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(0);
  });

  it('returns NOGROUP for non-existent group', () => {
    const ctx = setupGroupWithEntries();
    const reply = execXpending(ctx, ['s', 'nogroup']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such key 's' or consumer group 'nogroup'",
    });
  });

  it('returns NOGROUP for non-existent key', () => {
    const ctx = createDb(1000);
    const reply = execXpending(ctx, ['nokey', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such key 'nokey' or consumer group 'g1'",
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXpending(ctx, ['str', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns empty detail for empty range', () => {
    const ctx = setupGroupWithEntries();
    ctx.setTime(10000);
    execXreadgroup(ctx, [
      'GROUP',
      'g1',
      'alice',
      'COUNT',
      '3',
      'STREAMS',
      's',
      '>',
    ]);
    // Range that excludes all entries
    const reply = execXpending(ctx, ['s', 'g1', '9000-0', '9999-0', '10']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(0);
  });
});

// ─── XDEL ─────────────────────────────────────────────────────────────

function execXdel(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  return stream.xdel(ctx.db, args);
}

describe('XDEL', () => {
  it('deletes existing entry and returns 1', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXdel(ctx, ['s', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    // Verify entry is gone
    const range = stream.xrange(ctx.db, ['s', '-', '+']);
    expect(range).toEqual({ kind: 'array', value: [] });
  });

  it('returns 0 for non-existing entry ID', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXdel(ctx, ['s', '9999-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXdel(ctx, ['nokey', '1000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('deletes multiple entries', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXdel(ctx, ['s', '1000-0', '3000-0', '5000-0']);
    expect(reply).toEqual({ kind: 'integer', value: 3 });
    // Only entries 2 and 4 remain
    const range = stream.xrange(ctx.db, ['s', '-', '+']);
    const arr = range as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
  });

  it('counts only actually deleted entries (mix of existing and non-existing)', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    const reply = execXdel(ctx, ['s', '1000-0', '9999-0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
  });

  it('does not change XLEN after deletion beyond actual entries removed', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    ctx.setTime(2000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    execXdel(ctx, ['s', '1000-0']);
    const lenReply = stream.xlen(ctx.db, ['s']);
    expect(lenReply).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXdel(ctx, ['str', '1000-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXdel(ctx, ['s', 'invalid']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });
});

// ─── XTRIM ────────────────────────────────────────────────────────────

function execXtrim(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  return stream.xtrim(ctx.db, args);
}

describe('XTRIM', () => {
  it('trims by MAXLEN', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXtrim(ctx, ['s', 'MAXLEN', '3']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
    const lenReply = stream.xlen(ctx.db, ['s']);
    expect(lenReply).toEqual({ kind: 'integer', value: 3 });
  });

  it('trims by MAXLEN with = operator', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXtrim(ctx, ['s', 'MAXLEN', '=', '2']);
    expect(reply).toEqual({ kind: 'integer', value: 3 });
  });

  it('trims by MAXLEN with ~ (approximate)', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXtrim(ctx, ['s', 'MAXLEN', '~', '3']);
    // Our implementation trims to exact target
    expect(reply).toEqual({ kind: 'integer', value: 2 });
  });

  it('trims by MINID', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    // Remove entries with ID < 3000-0
    const reply = execXtrim(ctx, ['s', 'MINID', '3000']);
    expect(reply).toEqual({ kind: 'integer', value: 2 });
    const lenReply = stream.xlen(ctx.db, ['s']);
    expect(lenReply).toEqual({ kind: 'integer', value: 3 });
  });

  it('returns 0 for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXtrim(ctx, ['nokey', 'MAXLEN', '0']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXtrim(ctx, ['str', 'MAXLEN', '0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('trims to 0 entries', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 3; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXtrim(ctx, ['s', 'MAXLEN', '0']);
    expect(reply).toEqual({ kind: 'integer', value: 3 });
    const lenReply = stream.xlen(ctx.db, ['s']);
    expect(lenReply).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns 0 when stream already at or below MAXLEN', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXtrim(ctx, ['s', 'MAXLEN', '5']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });
});

// ─── XSETID ──────────────────────────────────────────────────────────

function execXsetid(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  return stream.xsetid(ctx.db, args);
}

describe('XSETID', () => {
  it('sets last ID on existing stream', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXsetid(ctx, ['s', '5000-0']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    // Now XADD with auto ID should be > 5000-0
    ctx.setTime(3000); // clock is behind, so should use 5000 ms
    const addReply = stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '2']);
    expect(addReply).toEqual({ kind: 'bulk', value: '5000-1' });
  });

  it('creates stream if key does not exist', () => {
    const ctx = createDb(1000);
    const reply = execXsetid(ctx, ['s', '1000-0']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
    const lenReply = stream.xlen(ctx.db, ['s']);
    expect(lenReply).toEqual({ kind: 'integer', value: 0 });
  });

  it('rejects ID smaller than current last ID', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '5000-0', 'k', '1']);
    const reply = execXsetid(ctx, ['s', '3000-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        'The ID specified in XSETID is smaller than the target stream top item',
    });
  });

  it('accepts ENTRIESADDED option', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXsetid(ctx, ['s', '5000-0', 'ENTRIESADDED', '100']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts MAXDELETEDID option', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXsetid(ctx, ['s', '5000-0', 'MAXDELETEDID', '500-0']);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('accepts both ENTRIESADDED and MAXDELETEDID', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXsetid(ctx, [
      's',
      '5000-0',
      'ENTRIESADDED',
      '100',
      'MAXDELETEDID',
      '500-0',
    ]);
    expect(reply).toEqual({ kind: 'status', value: 'OK' });
  });

  it('returns error for invalid stream ID', () => {
    const ctx = createDb(1000);
    const reply = execXsetid(ctx, ['s', 'invalid']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'Invalid stream ID specified as stream command argument',
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXsetid(ctx, ['str', '1000-0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });
});

// ─── XCLAIM ──────────────────────────────────────────────────────────

function execXclaim(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xclaim');
  if (!spec) throw new Error('xclaim spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function setupClaimScenario() {
  const ctx = createDb(1000);
  // Add 5 entries
  for (let i = 1; i <= 5; i++) {
    ctx.setTime(i * 1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
  }
  // Create group at 0
  execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
  // Alice reads all 5 entries
  ctx.setTime(10000);
  execXreadgroup(ctx, ['GROUP', 'g1', 'alice', 'STREAMS', 's', '>']);
  return ctx;
}

describe('XCLAIM', () => {
  it('transfers ownership of pending entry', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1);
    // Should return the claimed entry
    expect(arr.value[0]).toEqual(entryReply('1000-0', [['k', '1']]));
  });

  it('transfers multiple entries', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '1000-0',
      '2000-0',
      '3000-0',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(3);
  });

  it('ignores IDs not in PEL (without FORCE)', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '9999-0', // not in PEL
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(0);
  });

  it('returns JUSTID — only IDs, not full entries', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXclaim(ctx, [
      's',
      'g1',
      'bob',
      '0',
      '1000-0',
      '2000-0',
      'JUSTID',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);
    expect(arr.value[0]).toEqual({ kind: 'bulk', value: '1000-0' });
    expect(arr.value[1]).toEqual({ kind: 'bulk', value: '2000-0' });
  });

  it('updates delivery count on claim', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0']);
    // Check via XPENDING detail
    const pending = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = pending as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1);
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    // delivery count should be 2 (1 original + 1 claim)
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 2 });
  });

  it('respects RETRYCOUNT option', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0', 'RETRYCOUNT', '5']);
    const pending = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = pending as { kind: 'array'; value: Reply[] };
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    expect(entry.value[3]).toEqual({ kind: 'integer', value: 5 });
  });

  it('respects IDLE option — sets idle time', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0', 'IDLE', '5000']);
    // Delivery time should be 20000-5000=15000, idle=20000-15000=5000
    const pending = execXpending(ctx, ['s', 'g1', '-', '+', '10', 'bob']);
    const arr = pending as { kind: 'array'; value: Reply[] };
    const entry = arr.value[0] as { kind: 'array'; value: Reply[] };
    expect(entry.value[2]).toEqual({ kind: 'integer', value: 5000 });
  });

  it('FORCE claims entry not in PEL if it exists in stream', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    // Don't read with XREADGROUP, so nothing in PEL
    const reply = execXclaim(ctx, ['s', 'g1', 'bob', '0', '1000-0', 'FORCE']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1);
  });

  it('returns NOGROUP for non-existing group', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXclaim(ctx, ['s', 'nogroup', 'bob', '0', '1000-0']);
    expect(reply.kind).toBe('error');
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXclaim(ctx, ['nokey', 'g1', 'bob', '0', '1000-0']);
    expect(reply.kind).toBe('error');
  });
});

// ─── XAUTOCLAIM ──────────────────────────────────────────────────────

function execXautoclaim(
  ctx: ReturnType<typeof createDb>,
  args: string[]
): Reply {
  const spec = stream.specs.find((s) => s.name === 'xautoclaim');
  if (!spec) throw new Error('xautoclaim spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

describe('XAUTOCLAIM', () => {
  it('claims idle pending entries', () => {
    const ctx = setupClaimScenario();
    // Advance time so entries are idle > 5000ms
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000', // min-idle-time
      '0-0', // start
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    // [cursor, claimed-entries, deleted-ids]
    expect(arr.value.length).toBe(3);
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    expect(cursor.value).toBe('0-0'); // no more entries
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(5); // all 5 entries
    const deletedIds = arr.value[2] as { kind: 'array'; value: Reply[] };
    expect(deletedIds.value.length).toBe(0);
  });

  it('returns 0-0 cursor when all entries claimed', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, ['s', 'g1', 'bob', '5000', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    expect(cursor.value).toBe('0-0');
  });

  it('respects COUNT limit and returns non-zero cursor', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '0-0',
      'COUNT',
      '2',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const cursor = arr.value[0] as { kind: 'bulk'; value: string };
    // Cursor should point to next entry after the 2 claimed
    expect(cursor.value).not.toBe('0-0');
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(2);
  });

  it('filters by start ID', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '3000-0', // start from 3000-0
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(3); // 3000-0, 4000-0, 5000-0
  });

  it('skips entries that are not idle enough', () => {
    const ctx = setupClaimScenario();
    // Only 1ms later — nothing is idle for > 5000ms
    ctx.setTime(10001);
    const reply = execXautoclaim(ctx, ['s', 'g1', 'bob', '5000', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    expect(claimed.value.length).toBe(0);
  });

  it('reports deleted entries in third array element', () => {
    const ctx = setupClaimScenario();
    // Delete an entry that alice has pending
    execXdel(ctx, ['s', '2000-0']);
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, ['s', 'g1', 'bob', '5000', '0-0']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    const deletedIds = arr.value[2] as { kind: 'array'; value: Reply[] };
    // 4 entries claimed (1,3,4,5), 1 deleted (2)
    expect(claimed.value.length).toBe(4);
    expect(deletedIds.value.length).toBe(1);
    expect(deletedIds.value[0]).toEqual({ kind: 'bulk', value: '2000-0' });
  });

  it('JUSTID returns only IDs', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(20000);
    const reply = execXautoclaim(ctx, [
      's',
      'g1',
      'bob',
      '5000',
      '0-0',
      'JUSTID',
    ]);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const claimed = arr.value[1] as { kind: 'array'; value: Reply[] };
    // Should be bulk strings, not arrays
    expect(claimed.value[0]).toEqual({ kind: 'bulk', value: '1000-0' });
  });

  it('returns NOGROUP for non-existing group', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXautoclaim(ctx, ['s', 'nogroup', 'bob', '0', '0-0']);
    expect(reply.kind).toBe('error');
  });
});

// ─── XINFO ───────────────────────────────────────────────────────────

function execXinfo(ctx: ReturnType<typeof createDb>, args: string[]): Reply {
  const spec = stream.specs.find((s) => s.name === 'xinfo');
  if (!spec) throw new Error('xinfo spec not found');
  return spec.handler({ db: ctx.db, engine: ctx.engine }, args);
}

function findField(arr: Reply[], fieldName: string): Reply | undefined {
  const values = (arr as unknown as { kind: 'array'; value: Reply[] }).kind
    ? (arr as unknown as { kind: 'array'; value: Reply[] }).value
    : arr;
  for (let i = 0; i < values.length - 1; i++) {
    const item = values[i] as { kind: string; value: string };
    if (item.kind === 'bulk' && item.value === fieldName) {
      return values[i + 1];
    }
  }
  return undefined;
}

describe('XINFO STREAM', () => {
  it('returns stream metadata', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 3; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    const reply = execXinfo(ctx, ['STREAM', 's']);
    const arr = reply as { kind: 'array'; value: Reply[] };

    const length = findField(arr.value, 'length');
    expect(length).toEqual({ kind: 'integer', value: 3 });

    const lastId = findField(arr.value, 'last-generated-id');
    expect(lastId).toEqual({ kind: 'bulk', value: '3000-0' });

    const entriesAdded = findField(arr.value, 'entries-added');
    expect(entriesAdded).toEqual({ kind: 'integer', value: 3 });

    const groups = findField(arr.value, 'groups');
    expect(groups).toEqual({ kind: 'integer', value: 0 });

    const firstEntry = findField(arr.value, 'first-entry');
    expect(firstEntry).toEqual(entryReply('1000-0', [['k', '1']]));

    const lastEntry = findField(arr.value, 'last-entry');
    expect(lastEntry).toEqual(entryReply('3000-0', [['k', '3']]));
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['STREAM', 'nokey']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });

  it('returns WRONGTYPE for non-stream key', () => {
    const ctx = createDb(1000);
    ctx.db.set('str', 'string', 'raw', 'hello');
    const reply = execXinfo(ctx, ['STREAM', 'str']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('FULL returns entries and group details', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 3; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);

    const reply = execXinfo(ctx, ['STREAM', 's', 'FULL']);
    const arr = reply as { kind: 'array'; value: Reply[] };

    const entries = findField(arr.value, 'entries');
    const entriesArr = entries as { kind: 'array'; value: Reply[] };
    expect(entriesArr.value.length).toBe(3);

    const groups = findField(arr.value, 'groups');
    const groupsArr = groups as { kind: 'array'; value: Reply[] };
    expect(groupsArr.value.length).toBe(1);
  });

  it('FULL COUNT limits entries', () => {
    const ctx = createDb(1000);
    for (let i = 1; i <= 5; i++) {
      ctx.setTime(i * 1000);
      stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', String(i)]);
    }

    const reply = execXinfo(ctx, ['STREAM', 's', 'FULL', 'COUNT', '2']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const entries = findField(arr.value, 'entries');
    const entriesArr = entries as { kind: 'array'; value: Reply[] };
    expect(entriesArr.value.length).toBe(2);
  });
});

describe('XINFO GROUPS', () => {
  it('returns group list', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    execXgroup(ctx, ['CREATE', 's', 'g2', '$']);

    const reply = execXinfo(ctx, ['GROUPS', 's']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(2);

    const g1 = arr.value[0] as { kind: 'array'; value: Reply[] };
    const name = findField(g1.value, 'name');
    expect(name).toEqual({ kind: 'bulk', value: 'g1' });
  });

  it('returns empty array for stream with no groups', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXinfo(ctx, ['GROUPS', 's']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('shows pending count and consumer count', () => {
    const ctx = setupClaimScenario();
    const reply = execXinfo(ctx, ['GROUPS', 's']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    const g1 = arr.value[0] as { kind: 'array'; value: Reply[] };

    const consumers = findField(g1.value, 'consumers');
    expect(consumers).toEqual({ kind: 'integer', value: 1 }); // alice

    const pending = findField(g1.value, 'pending');
    expect(pending).toEqual({ kind: 'integer', value: 5 });
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['GROUPS', 'nokey']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });
});

describe('XINFO CONSUMERS', () => {
  it('returns consumer list with idle times', () => {
    const ctx = setupClaimScenario();
    ctx.setTime(15000);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'g1']);
    const arr = reply as { kind: 'array'; value: Reply[] };
    expect(arr.value.length).toBe(1); // alice

    const alice = arr.value[0] as { kind: 'array'; value: Reply[] };
    const name = findField(alice.value, 'name');
    expect(name).toEqual({ kind: 'bulk', value: 'alice' });

    const pending = findField(alice.value, 'pending');
    expect(pending).toEqual({ kind: 'integer', value: 5 });

    const idle = findField(alice.value, 'idle');
    expect(idle).toEqual({ kind: 'integer', value: 5000 });
  });

  it('returns NOGROUP for non-existing group', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'nogroup']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'NOGROUP',
      message: "No such consumer group 'nogroup' for key name 's'",
    });
  });

  it('returns error for non-existing key', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['CONSUMERS', 'nokey', 'g1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });

  it('returns empty array when no consumers exist', () => {
    const ctx = createDb(1000);
    stream.xadd(ctx.db, ctx.getTime(), ['s', '*', 'k', '1']);
    execXgroup(ctx, ['CREATE', 's', 'g1', '0']);
    const reply = execXinfo(ctx, ['CONSUMERS', 's', 'g1']);
    expect(reply).toEqual({ kind: 'array', value: [] });
  });

  it('returns error for unknown subcommand', () => {
    const ctx = createDb(1000);
    const reply = execXinfo(ctx, ['UNKNOWN', 's']);
    expect(reply.kind).toBe('error');
  });
});
