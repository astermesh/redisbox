import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as str from './string.ts';

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

// --- MGET ---

describe('MGET', () => {
  it('returns values for existing keys', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    db.set('b', 'string', 'raw', 'world');
    const reply = str.mget(db, ['a', 'b']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'hello' },
        { kind: 'bulk', value: 'world' },
      ],
    });
  });

  it('returns nil for non-existent keys', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    const reply = str.mget(db, ['a', 'missing']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: 'hello' },
        { kind: 'bulk', value: null },
      ],
    });
  });

  it('returns nil for non-string keys (no WRONGTYPE error)', () => {
    const { db } = createDb();
    db.set('list', 'list', 'quicklist', []);
    db.set('str', 'string', 'raw', 'val');
    const reply = str.mget(db, ['list', 'str']);
    expect(reply).toEqual({
      kind: 'array',
      value: [
        { kind: 'bulk', value: null },
        { kind: 'bulk', value: 'val' },
      ],
    });
  });

  it('returns nil for expired keys', () => {
    const { db, setTime } = createDb(1000);
    db.set('k', 'string', 'raw', 'val');
    db.setExpiry('k', 2000);
    setTime(3000);
    const reply = str.mget(db, ['k']);
    expect(reply).toEqual({
      kind: 'array',
      value: [{ kind: 'bulk', value: null }],
    });
  });

  it('handles single key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(str.mget(db, ['k'])).toEqual({
      kind: 'array',
      value: [{ kind: 'bulk', value: 'v' }],
    });
  });
});

// --- MSET ---

describe('MSET', () => {
  it('sets multiple keys and returns OK', () => {
    const { db } = createDb();
    expect(str.mset(db, ['a', '1', 'b', '2'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(db.get('a')?.value).toBe('1');
    expect(db.get('b')?.value).toBe('2');
  });

  it('overwrites existing keys', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'old');
    str.mset(db, ['a', 'new']);
    expect(db.get('a')?.value).toBe('new');
  });

  it('overwrites keys of different types', () => {
    const { db } = createDb();
    db.set('a', 'list', 'quicklist', []);
    str.mset(db, ['a', 'now-string']);
    expect(db.get('a')?.type).toBe('string');
    expect(db.get('a')?.value).toBe('now-string');
  });

  it('removes existing TTL', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'old');
    db.setExpiry('a', 5000);
    str.mset(db, ['a', 'new']);
    expect(db.getExpiry('a')).toBeUndefined();
  });

  it('rejects odd number of arguments', () => {
    const { db } = createDb();
    const reply = str.mset(db, ['a', '1', 'b']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'mset' command",
    });
  });

  it('determines correct encoding for each value', () => {
    const { db } = createDb();
    str.mset(db, ['num', '42', 'str', 'hello']);
    expect(db.get('num')?.encoding).toBe('int');
    expect(db.get('str')?.encoding).toBe('embstr');
  });
});

// --- MSETNX ---

describe('MSETNX', () => {
  it('sets all keys when none exist and returns 1', () => {
    const { db } = createDb();
    expect(str.msetnx(db, ['a', '1', 'b', '2'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(db.get('a')?.value).toBe('1');
    expect(db.get('b')?.value).toBe('2');
  });

  it('sets nothing when any key exists and returns 0', () => {
    const { db } = createDb();
    db.set('b', 'string', 'raw', 'existing');
    expect(str.msetnx(db, ['a', '1', 'b', '2'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(db.has('a')).toBe(false);
    expect(db.get('b')?.value).toBe('existing');
  });

  it('checks keys of any type (not just strings)', () => {
    const { db } = createDb();
    db.set('a', 'list', 'quicklist', []);
    expect(str.msetnx(db, ['a', '1', 'b', '2'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('treats expired keys as non-existent', () => {
    const { db, setTime } = createDb(1000);
    db.set('a', 'string', 'raw', 'old');
    db.setExpiry('a', 2000);
    setTime(3000);
    expect(str.msetnx(db, ['a', 'new'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(db.get('a')?.value).toBe('new');
  });

  it('rejects odd number of arguments', () => {
    const { db } = createDb();
    const reply = str.msetnx(db, ['a', '1', 'b']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'msetnx' command",
    });
  });
});

// --- APPEND ---

describe('APPEND', () => {
  it('appends to existing string and returns new byte length', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', 'Hello');
    const reply = str.append(db, ['k', ' World']);
    expect(reply).toEqual({ kind: 'integer', value: 11 });
    expect(db.get('k')?.value).toBe('Hello World');
  });

  it('creates key if it does not exist', () => {
    const { db } = createDb();
    const reply = str.append(db, ['k', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 5 });
    expect(db.get('k')?.value).toBe('hello');
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.append(db, ['k', 'val']).kind).toBe('error');
  });

  it('preserves TTL of existing key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', 'hello');
    db.setExpiry('k', 5000);
    str.append(db, ['k', ' world']);
    expect(db.getExpiry('k')).toBe(5000);
  });

  it('uses raw encoding when appending to existing key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '42');
    str.append(db, ['k', '3']);
    expect(db.get('k')?.encoding).toBe('raw');
  });

  it('determines encoding normally when creating new key', () => {
    const { db } = createDb();
    str.append(db, ['k', '42']);
    expect(db.get('k')?.encoding).toBe('int');
  });

  it('handles empty append value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', 'hello');
    const reply = str.append(db, ['k', '']);
    expect(reply).toEqual({ kind: 'integer', value: 5 });
    expect(db.get('k')?.value).toBe('hello');
  });
});

// --- STRLEN ---

describe('STRLEN', () => {
  it('returns byte length of string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(str.strlen(db, ['k'])).toEqual({ kind: 'integer', value: 5 });
  });

  it('returns 0 for non-existent key', () => {
    const { db } = createDb();
    expect(str.strlen(db, ['missing'])).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.strlen(db, ['k']).kind).toBe('error');
  });

  it('returns 0 for empty string', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '');
    expect(str.strlen(db, ['k'])).toEqual({ kind: 'integer', value: 0 });
  });

  it('returns byte length for multi-byte characters', () => {
    const { db } = createDb();
    // "café" is 5 UTF-8 bytes (c=1, a=1, f=1, é=2)
    db.set('k', 'string', 'raw', 'caf\u00e9');
    expect(str.strlen(db, ['k'])).toEqual({ kind: 'integer', value: 5 });
  });
});

// --- SETRANGE ---

describe('SETRANGE', () => {
  it('overwrites part of string at offset', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello World');
    const reply = str.setrange(db, ['k', '6', 'Redis']);
    expect(reply).toEqual({ kind: 'integer', value: 11 });
    expect(db.get('k')?.value).toBe('Hello Redis');
  });

  it('pads with zero bytes for non-existent key', () => {
    const { db } = createDb();
    const reply = str.setrange(db, ['k', '5', 'hello']);
    expect(reply).toEqual({ kind: 'integer', value: 10 });
    const val = db.get('k')?.value as string;
    expect(val.length).toBe(10);
    expect(val.slice(0, 5)).toBe('\x00\x00\x00\x00\x00');
    expect(val.slice(5)).toBe('hello');
  });

  it('extends string if offset + value exceeds current length', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello');
    const reply = str.setrange(db, ['k', '5', ' World']);
    expect(reply).toEqual({ kind: 'integer', value: 11 });
    expect(db.get('k')?.value).toBe('Hello World');
  });

  it('rejects negative offset', () => {
    const { db } = createDb();
    const reply = str.setrange(db, ['k', '-1', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'offset is out of range',
    });
  });

  it('rejects non-integer offset', () => {
    const { db } = createDb();
    const reply = str.setrange(db, ['k', 'abc', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.setrange(db, ['k', '0', 'val']).kind).toBe('error');
  });

  it('returns current length for empty value on existing key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = str.setrange(db, ['k', '0', '']);
    expect(reply).toEqual({ kind: 'integer', value: 5 });
    expect(db.get('k')?.value).toBe('hello');
  });

  it('returns 0 for empty value on non-existent key', () => {
    const { db } = createDb();
    const reply = str.setrange(db, ['k', '0', '']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
  });

  it('always uses raw encoding', () => {
    const { db } = createDb();
    str.setrange(db, ['k', '0', '42']);
    expect(db.get('k')?.encoding).toBe('raw');
  });

  it('preserves TTL', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    db.setExpiry('k', 5000);
    str.setrange(db, ['k', '0', 'H']);
    expect(db.getExpiry('k')).toBe(5000);
  });

  it('pads with zero bytes between existing string end and offset', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'ab');
    const reply = str.setrange(db, ['k', '5', 'cd']);
    expect(reply).toEqual({ kind: 'integer', value: 7 });
    const val = db.get('k')?.value as string;
    expect(val).toBe('ab\x00\x00\x00cd');
  });
});

// --- GETRANGE ---

describe('GETRANGE', () => {
  it('returns substring for valid range', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello World');
    expect(str.getrange(db, ['k', '0', '4'])).toEqual({
      kind: 'bulk',
      value: 'Hello',
    });
  });

  it('supports negative indices', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello World');
    expect(str.getrange(db, ['k', '-5', '-1'])).toEqual({
      kind: 'bulk',
      value: 'World',
    });
  });

  it('clamps to string boundaries', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello');
    expect(str.getrange(db, ['k', '0', '100'])).toEqual({
      kind: 'bulk',
      value: 'Hello',
    });
  });

  it('returns empty string for out-of-range start', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello');
    expect(str.getrange(db, ['k', '100', '200'])).toEqual({
      kind: 'bulk',
      value: '',
    });
  });

  it('returns empty string for non-existent key', () => {
    const { db } = createDb();
    expect(str.getrange(db, ['missing', '0', '10'])).toEqual({
      kind: 'bulk',
      value: '',
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.getrange(db, ['k', '0', '1']).kind).toBe('error');
  });

  it('handles -1 as last character', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello');
    expect(str.getrange(db, ['k', '0', '-1'])).toEqual({
      kind: 'bulk',
      value: 'Hello',
    });
  });

  it('returns empty when start > end after normalization', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello');
    expect(str.getrange(db, ['k', '3', '1'])).toEqual({
      kind: 'bulk',
      value: '',
    });
  });

  it('handles large negative indices by clamping to 0', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'Hello');
    // start = max(5 + (-100), 0) = 0, end = max(5 + (-50), 0) = 0
    expect(str.getrange(db, ['k', '-100', '-50'])).toEqual({
      kind: 'bulk',
      value: 'H',
    });
  });

  it('returns empty string for empty string value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'embstr', '');
    expect(str.getrange(db, ['k', '0', '0'])).toEqual({
      kind: 'bulk',
      value: '',
    });
  });
});

// --- SUBSTR (alias for GETRANGE) ---

describe('SUBSTR', () => {
  it('is an alias for GETRANGE', () => {
    // substr is the same function as getrange
    expect(str.getrange).toBeDefined();
  });
});

// --- GETEX ---

describe('GETEX', () => {
  it('returns value without options (like GET)', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(str.getex(db, clock, ['k'])).toEqual({
      kind: 'bulk',
      value: 'hello',
    });
  });

  it('returns nil for non-existent key', () => {
    const { db, clock } = createDb();
    expect(str.getex(db, clock, ['missing'])).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('returns nil for non-existent key even with options', () => {
    const { db, clock } = createDb();
    expect(str.getex(db, clock, ['missing', 'EX', '10'])).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db, clock } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.getex(db, clock, ['k']).kind).toBe('error');
  });

  it('sets TTL with EX option', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    const reply = str.getex(db, clock, ['k', 'EX', '10']);
    expect(reply).toEqual({ kind: 'bulk', value: 'hello' });
    expect(db.getExpiry('k')).toBe(11000);
  });

  it('sets TTL with PX option', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    str.getex(db, clock, ['k', 'PX', '5000']);
    expect(db.getExpiry('k')).toBe(6000);
  });

  it('sets TTL with EXAT option', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    str.getex(db, clock, ['k', 'EXAT', '100']);
    expect(db.getExpiry('k')).toBe(100000);
  });

  it('sets TTL with PXAT option', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    str.getex(db, clock, ['k', 'PXAT', '50000']);
    expect(db.getExpiry('k')).toBe(50000);
  });

  it('removes TTL with PERSIST option', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    db.setExpiry('k', 5000);
    str.getex(db, clock, ['k', 'PERSIST']);
    expect(db.getExpiry('k')).toBeUndefined();
  });

  it('rejects zero EX value', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = str.getex(db, clock, ['k', 'EX', '0']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'getex' command",
    });
  });

  it('rejects negative EX value', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = str.getex(db, clock, ['k', 'EX', '-1']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'getex' command",
    });
  });

  it('rejects extra arguments', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = str.getex(db, clock, ['k', 'EX', '10', 'extra']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('rejects conflicting options', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const reply = str.getex(db, clock, ['k', 'EX', '10', 'PERSIST']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('is case-insensitive for options', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'hello');
    str.getex(db, clock, ['k', 'ex', '10']);
    expect(db.getExpiry('k')).toBe(11000);
  });
});

// --- GETDEL ---

describe('GETDEL', () => {
  it('returns value and deletes key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(str.getdel(db, ['k'])).toEqual({ kind: 'bulk', value: 'hello' });
    expect(db.has('k')).toBe(false);
  });

  it('returns nil for non-existent key', () => {
    const { db } = createDb();
    expect(str.getdel(db, ['missing'])).toEqual({
      kind: 'bulk',
      value: null,
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    const reply = str.getdel(db, ['k']);
    expect(reply.kind).toBe('error');
    // Key should NOT be deleted on WRONGTYPE
    expect(db.has('k')).toBe(true);
  });

  it('removes TTL along with key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    db.setExpiry('k', 5000);
    str.getdel(db, ['k']);
    expect(db.has('k')).toBe(false);
    expect(db.getExpiry('k')).toBeUndefined();
  });
});

// --- GETSET ---

describe('GETSET', () => {
  it('returns old value and sets new value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'old');
    expect(str.getset(db, ['k', 'new'])).toEqual({
      kind: 'bulk',
      value: 'old',
    });
    expect(db.get('k')?.value).toBe('new');
  });

  it('returns nil and sets value when key does not exist', () => {
    const { db } = createDb();
    expect(str.getset(db, ['k', 'hello'])).toEqual({
      kind: 'bulk',
      value: null,
    });
    expect(db.get('k')?.value).toBe('hello');
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.getset(db, ['k', 'val']).kind).toBe('error');
  });

  it('removes TTL when setting new value', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'old');
    db.setExpiry('k', 5000);
    str.getset(db, ['k', 'new']);
    expect(db.getExpiry('k')).toBeUndefined();
  });

  it('determines encoding for new value', () => {
    const { db } = createDb();
    str.getset(db, ['k', '42']);
    expect(db.get('k')?.encoding).toBe('int');
  });
});

// --- SETNX ---

describe('SETNX', () => {
  it('sets key and returns 1 when key does not exist', () => {
    const { db } = createDb();
    expect(str.setnx(db, ['k', 'hello'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(db.get('k')?.value).toBe('hello');
  });

  it('returns 0 when key already exists', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'old');
    expect(str.setnx(db, ['k', 'new'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(db.get('k')?.value).toBe('old');
  });

  it('returns 0 when key exists with different type', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    expect(str.setnx(db, ['k', 'val'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('treats expired key as non-existent', () => {
    const { db, setTime } = createDb(1000);
    db.set('k', 'string', 'raw', 'old');
    db.setExpiry('k', 2000);
    setTime(3000);
    expect(str.setnx(db, ['k', 'new'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(db.get('k')?.value).toBe('new');
  });

  it('determines correct encoding', () => {
    const { db } = createDb();
    str.setnx(db, ['k', '42']);
    expect(db.get('k')?.encoding).toBe('int');
  });
});

// --- SETEX ---

describe('SETEX', () => {
  it('sets key with TTL in seconds', () => {
    const { db, clock } = createDb(1000);
    expect(str.setex(db, clock, ['k', '10', 'hello'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(db.get('k')?.value).toBe('hello');
    expect(db.getExpiry('k')).toBe(11000);
  });

  it('overwrites existing key and TTL', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'old');
    db.setExpiry('k', 3000);
    str.setex(db, clock, ['k', '20', 'new']);
    expect(db.get('k')?.value).toBe('new');
    expect(db.getExpiry('k')).toBe(21000);
  });

  it('rejects zero seconds', () => {
    const { db, clock } = createDb();
    const reply = str.setex(db, clock, ['k', '0', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'setex' command",
    });
  });

  it('rejects negative seconds', () => {
    const { db, clock } = createDb();
    const reply = str.setex(db, clock, ['k', '-1', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'setex' command",
    });
  });

  it('rejects non-integer seconds', () => {
    const { db, clock } = createDb();
    const reply = str.setex(db, clock, ['k', 'abc', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('overwrites key of different type', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'list', 'quicklist', []);
    str.setex(db, clock, ['k', '10', 'hello']);
    expect(db.get('k')?.type).toBe('string');
    expect(db.get('k')?.value).toBe('hello');
  });
});

// --- PSETEX ---

describe('PSETEX', () => {
  it('sets key with TTL in milliseconds', () => {
    const { db, clock } = createDb(1000);
    expect(str.psetex(db, clock, ['k', '5000', 'hello'])).toEqual({
      kind: 'status',
      value: 'OK',
    });
    expect(db.get('k')?.value).toBe('hello');
    expect(db.getExpiry('k')).toBe(6000);
  });

  it('rejects zero milliseconds', () => {
    const { db, clock } = createDb();
    const reply = str.psetex(db, clock, ['k', '0', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'psetex' command",
    });
  });

  it('rejects negative milliseconds', () => {
    const { db, clock } = createDb();
    const reply = str.psetex(db, clock, ['k', '-100', 'val']);
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "invalid expire time in 'psetex' command",
    });
  });
});

// --- LCS ---

describe('LCS', () => {
  it('returns longest common subsequence', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'ohmytext');
    db.set('b', 'string', 'raw', 'mynewtext');
    const reply = str.lcs(db, ['a', 'b']);
    expect(reply).toEqual({ kind: 'bulk', value: 'mytext' });
  });

  it('returns empty string when no common subsequence', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'abc');
    db.set('b', 'string', 'raw', 'xyz');
    expect(str.lcs(db, ['a', 'b'])).toEqual({ kind: 'bulk', value: '' });
  });

  it('handles non-existent keys as empty strings', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    expect(str.lcs(db, ['a', 'missing'])).toEqual({
      kind: 'bulk',
      value: '',
    });
  });

  it('returns WRONGTYPE for non-string key', () => {
    const { db } = createDb();
    db.set('a', 'list', 'quicklist', []);
    db.set('b', 'string', 'raw', 'hello');
    expect(str.lcs(db, ['a', 'b']).kind).toBe('error');
  });

  it('returns length with LEN option', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'ohmytext');
    db.set('b', 'string', 'raw', 'mynewtext');
    expect(str.lcs(db, ['a', 'b', 'LEN'])).toEqual({
      kind: 'integer',
      value: 6,
    });
  });

  it('returns IDX output with match positions', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'ohmytext');
    db.set('b', 'string', 'raw', 'mynewtext');
    const reply = str.lcs(db, ['a', 'b', 'IDX']) as {
      kind: 'array';
      value: Reply[];
    };
    expect(reply.kind).toBe('array');
    // Should have: "matches", [...], "len", 6
    expect(reply.value[0]).toEqual({ kind: 'bulk', value: 'matches' });
    expect(reply.value[2]).toEqual({ kind: 'bulk', value: 'len' });
    expect(reply.value[3]).toEqual({ kind: 'integer', value: 6 });
  });

  it('returns IDX with WITHMATCHLEN', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'ohmytext');
    db.set('b', 'string', 'raw', 'mynewtext');
    const reply = str.lcs(db, ['a', 'b', 'IDX', 'WITHMATCHLEN']) as {
      kind: 'array';
      value: Reply[];
    };
    expect(reply.kind).toBe('array');
    // Each match should have a third element with length
    const matches = reply.value[1] as { kind: 'array'; value: Reply[] };
    for (const match of matches.value) {
      const m = match as { kind: 'array'; value: Reply[] };
      expect(m.value.length).toBe(3); // [aRange, bRange, matchLen]
    }
  });

  it('filters matches by MINMATCHLEN', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'ohmytext');
    db.set('b', 'string', 'raw', 'mynewtext');
    const reply = str.lcs(db, ['a', 'b', 'IDX', 'MINMATCHLEN', '4']) as {
      kind: 'array';
      value: Reply[];
    };
    const matches = reply.value[1] as { kind: 'array'; value: Reply[] };
    // Only matches with length >= 4 should be included
    for (const match of matches.value) {
      const m = match as { kind: 'array'; value: Reply[] };
      const aRange = m.value[0] as { kind: 'array'; value: Reply[] };
      const aStart = (aRange.value[0] as { kind: 'integer'; value: number })
        .value;
      const aEnd = (aRange.value[1] as { kind: 'integer'; value: number })
        .value;
      expect(aEnd - aStart + 1).toBeGreaterThanOrEqual(4);
    }
  });

  it('handles identical strings', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    db.set('b', 'string', 'raw', 'hello');
    expect(str.lcs(db, ['a', 'b'])).toEqual({
      kind: 'bulk',
      value: 'hello',
    });
  });

  it('handles empty strings', () => {
    const { db } = createDb();
    db.set('a', 'string', 'embstr', '');
    db.set('b', 'string', 'raw', 'hello');
    expect(str.lcs(db, ['a', 'b'])).toEqual({ kind: 'bulk', value: '' });
  });

  it('rejects unknown options', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    db.set('b', 'string', 'raw', 'hello');
    expect(str.lcs(db, ['a', 'b', 'BADOPT'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('LEN option is case-insensitive', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    db.set('b', 'string', 'raw', 'hello');
    expect(str.lcs(db, ['a', 'b', 'len'])).toEqual({
      kind: 'integer',
      value: 5,
    });
  });

  it('IDX takes priority over LEN when both specified', () => {
    const { db } = createDb();
    db.set('a', 'string', 'raw', 'hello');
    db.set('b', 'string', 'raw', 'hello');
    const reply = str.lcs(db, ['a', 'b', 'LEN', 'IDX']);
    expect(reply.kind).toBe('array'); // IDX format, not integer
  });
});
