import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import * as incr from './incr.ts';

function createDb(time = 1000): {
  db: Database;
  engine: RedisEngine;
  clock: () => number;
  setTime: (t: number) => void;
} {
  let now = time;
  const clock = () => now;
  const engine = new RedisEngine({ clock, rng: () => 0.5 });
  return {
    db: engine.db(0),
    engine,
    clock,
    setTime: (t: number) => {
      now = t;
    },
  };
}

// --- INCR ---

describe('INCR', () => {
  it('increments existing integer key by 1', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incr(db, ['k'])).toEqual({ kind: 'integer', value: 11 });
    expect(db.get('k')?.value).toBe('11');
  });

  it('initializes non-existent key to 0 then increments', () => {
    const { db } = createDb();
    expect(incr.incr(db, ['k'])).toEqual({ kind: 'integer', value: 1 });
    expect(db.get('k')?.value).toBe('1');
  });

  it('sets encoding to int', () => {
    const { db } = createDb();
    incr.incr(db, ['k']);
    expect(db.get('k')?.encoding).toBe('int');
  });

  it('returns error for non-integer string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for float string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '3.14');
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error on overflow (max int64)', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '9223372036854775807');
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
    // value unchanged
    expect(db.get('k')?.value).toBe('9223372036854775807');
  });

  it('handles negative values', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '-5');
    expect(incr.incr(db, ['k'])).toEqual({ kind: 'integer', value: -4 });
  });

  it('works with zero', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '0');
    expect(incr.incr(db, ['k'])).toEqual({ kind: 'integer', value: 1 });
  });

  it('returns error for empty string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '');
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for value with spaces', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', ' 10');
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for value exceeding int64 range', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '9223372036854775808');
    expect(incr.incr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });
});

// --- DECR ---

describe('DECR', () => {
  it('decrements existing integer key by 1', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.decr(db, ['k'])).toEqual({ kind: 'integer', value: 9 });
    expect(db.get('k')?.value).toBe('9');
  });

  it('initializes non-existent key to 0 then decrements', () => {
    const { db } = createDb();
    expect(incr.decr(db, ['k'])).toEqual({ kind: 'integer', value: -1 });
    expect(db.get('k')?.value).toBe('-1');
  });

  it('returns error for non-integer string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(incr.decr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(incr.decr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error on underflow (min int64)', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '-9223372036854775808');
    expect(incr.decr(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
    expect(db.get('k')?.value).toBe('-9223372036854775808');
  });

  it('works with zero', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '0');
    expect(incr.decr(db, ['k'])).toEqual({ kind: 'integer', value: -1 });
  });
});

// --- INCRBY ---

describe('INCRBY', () => {
  it('increments by specified amount', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrby(db, ['k', '5'])).toEqual({ kind: 'integer', value: 15 });
    expect(db.get('k')?.value).toBe('15');
  });

  it('initializes non-existent key to 0 then increments', () => {
    const { db } = createDb();
    expect(incr.incrby(db, ['k', '100'])).toEqual({
      kind: 'integer',
      value: 100,
    });
  });

  it('accepts negative increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrby(db, ['k', '-3'])).toEqual({ kind: 'integer', value: 7 });
  });

  it('increments by zero', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrby(db, ['k', '0'])).toEqual({ kind: 'integer', value: 10 });
  });

  it('returns error for non-integer increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrby(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for float increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrby(db, ['k', '3.14'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for non-integer string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(incr.incrby(db, ['k', '5'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(incr.incrby(db, ['k', '5'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error on overflow', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '9223372036854775800');
    expect(incr.incrby(db, ['k', '100'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
    expect(db.get('k')?.value).toBe('9223372036854775800');
  });

  it('returns error on underflow', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '-9223372036854775800');
    expect(incr.incrby(db, ['k', '-100'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
  });

  it('returns error for increment exceeding int64 range', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '0');
    expect(incr.incrby(db, ['k', '9223372036854775808'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('sets encoding to int', () => {
    const { db } = createDb();
    incr.incrby(db, ['k', '42']);
    expect(db.get('k')?.encoding).toBe('int');
  });
});

// --- DECRBY ---

describe('DECRBY', () => {
  it('decrements by specified amount', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.decrby(db, ['k', '3'])).toEqual({ kind: 'integer', value: 7 });
    expect(db.get('k')?.value).toBe('7');
  });

  it('initializes non-existent key to 0 then decrements', () => {
    const { db } = createDb();
    expect(incr.decrby(db, ['k', '5'])).toEqual({ kind: 'integer', value: -5 });
  });

  it('accepts negative decrement (effectively adds)', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.decrby(db, ['k', '-3'])).toEqual({
      kind: 'integer',
      value: 13,
    });
  });

  it('returns error for non-integer decrement', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.decrby(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(incr.decrby(db, ['k', '5'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error on underflow', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '-9223372036854775800');
    expect(incr.decrby(db, ['k', '100'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
  });

  it('returns error on overflow (decrby negative near max)', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '9223372036854775800');
    expect(incr.decrby(db, ['k', '-100'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment or decrement would overflow',
    });
  });

  it('returns error for decrement exceeding int64 range', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '0');
    expect(incr.decrby(db, ['k', '9223372036854775808'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });
});

// --- INCRBYFLOAT ---

describe('INCRBYFLOAT', () => {
  it('increments by float value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '10.5');
    expect(incr.incrbyfloat(db, ['k', '0.1'])).toEqual({
      kind: 'bulk',
      value: '10.6',
    });
    expect(db.get('k')?.value).toBe('10.6');
  });

  it('increments integer by float', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrbyfloat(db, ['k', '1.5'])).toEqual({
      kind: 'bulk',
      value: '11.5',
    });
  });

  it('initializes non-existent key to 0 then increments', () => {
    const { db } = createDb();
    expect(incr.incrbyfloat(db, ['k', '3.14'])).toEqual({
      kind: 'bulk',
      value: '3.14',
    });
  });

  it('accepts negative increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '10.5');
    expect(incr.incrbyfloat(db, ['k', '-5'])).toEqual({
      kind: 'bulk',
      value: '5.5',
    });
  });

  it('accepts integer increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrbyfloat(db, ['k', '5'])).toEqual({
      kind: 'bulk',
      value: '15',
    });
  });

  it('trims trailing zeroes', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '1.0');
    expect(incr.incrbyfloat(db, ['k', '2.0'])).toEqual({
      kind: 'bulk',
      value: '3',
    });
  });

  it('handles scientific notation in increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '0');
    const reply = incr.incrbyfloat(db, ['k', '3.1415e2']);
    expect(reply).toEqual({
      kind: 'bulk',
      value: '314.15',
    });
  });

  it('returns error for non-numeric string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(incr.incrbyfloat(db, ['k', '1.0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not a valid float',
    });
  });

  it('returns error for non-numeric increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrbyfloat(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not a valid float',
    });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(incr.incrbyfloat(db, ['k', '1.0'])).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns error for infinity result', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '1.7976931348623157e+308');
    expect(incr.incrbyfloat(db, ['k', '1.7976931348623157e+308'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment would produce NaN or Infinity',
    });
  });

  it('returns error for NaN increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrbyfloat(db, ['k', 'nan'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not a valid float',
    });
  });

  it('returns error for inf increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrbyfloat(db, ['k', 'inf'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment would produce NaN or Infinity',
    });
  });

  it('returns error for -inf increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    expect(incr.incrbyfloat(db, ['k', '-inf'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'increment would produce NaN or Infinity',
    });
  });

  it('updates encoding based on result', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '10');
    incr.incrbyfloat(db, ['k', '0.5']);
    // result is "10.5" — not an integer, so embstr
    expect(db.get('k')?.encoding).toBe('embstr');
  });

  it('sets int encoding when result is integer', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '1.5');
    incr.incrbyfloat(db, ['k', '0.5']);
    // result is "2" — integer
    expect(db.get('k')?.encoding).toBe('int');
  });

  it('handles zero increment', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '3.14');
    expect(incr.incrbyfloat(db, ['k', '0'])).toEqual({
      kind: 'bulk',
      value: '3.14',
    });
  });

  it('result stored as string', () => {
    const { db } = createDb();
    incr.incrbyfloat(db, ['k', '3.14']);
    expect(typeof db.get('k')?.value).toBe('string');
  });
});
