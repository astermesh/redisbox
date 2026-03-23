import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Reply } from '../../types.ts';
import * as stream from './index.ts';

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

  it('returns correct count after adds', () => {
    const { db, getTime } = createDb(1000);
    stream.xadd(db, getTime(), ['s', '*', 'a', '1']);
    stream.xadd(db, getTime(), ['s', '*', 'b', '2']);
    expect(stream.xlen(db, ['s'])).toEqual({
      kind: 'integer',
      value: 2,
    });
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
