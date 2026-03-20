import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
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
