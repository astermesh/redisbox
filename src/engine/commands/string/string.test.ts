import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import * as str from './index.ts';

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

// --- GET ---

describe('GET', () => {
  it('returns nil for non-existent key', () => {
    const { db } = createDb();
    expect(str.get(db, ['missing'])).toEqual({ kind: 'bulk', value: null });
  });

  it('returns value for existing string key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    expect(str.get(db, ['k'])).toEqual({ kind: 'bulk', value: 'hello' });
  });

  it('returns WRONGTYPE error for non-string key', () => {
    const { db } = createDb();
    db.set('k', 'list', 'quicklist', []);
    const reply = str.get(db, ['k']);
    expect(reply.kind).toBe('error');
    expect(reply).toEqual({
      kind: 'error',
      prefix: 'WRONGTYPE',
      message: 'Operation against a key holding the wrong kind of value',
    });
  });

  it('returns nil for expired key', () => {
    const { db, setTime } = createDb(1000);
    db.set('k', 'string', 'raw', 'val');
    db.setExpiry('k', 2000);
    setTime(3000);
    expect(str.get(db, ['k'])).toEqual({ kind: 'bulk', value: null });
  });

  it('returns numeric value as string', () => {
    const { db } = createDb();
    db.set('k', 'string', 'int', '42');
    expect(str.get(db, ['k'])).toEqual({ kind: 'bulk', value: '42' });
  });
});

// --- SET ---

describe('SET', () => {
  describe('basic', () => {
    it('sets a key and returns OK', () => {
      const { db, clock } = createDb();
      expect(str.set(db, clock, ['mykey', 'myvalue'])).toEqual({
        kind: 'status',
        value: 'OK',
      });
      expect(db.get('mykey')?.value).toBe('myvalue');
    });

    it('overwrites existing key', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', 'old']);
      str.set(db, clock, ['k', 'new']);
      expect(db.get('k')?.value).toBe('new');
    });

    it('removes TTL when overwriting without TTL flags', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', 'v1']);
      db.setExpiry('k', 5000);
      str.set(db, clock, ['k', 'v2']);
      expect(db.getExpiry('k')).toBeUndefined();
    });

    it('overwrites key of different type', () => {
      const { db, clock } = createDb();
      db.set('k', 'list', 'quicklist', []);
      str.set(db, clock, ['k', 'now-a-string']);
      expect(db.get('k')?.type).toBe('string');
      expect(db.get('k')?.value).toBe('now-a-string');
    });
  });

  describe('encoding tracking', () => {
    it('uses int encoding for integer values', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '42']);
      expect(db.get('k')?.encoding).toBe('int');
    });

    it('uses int encoding for zero', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '0']);
      expect(db.get('k')?.encoding).toBe('int');
    });

    it('uses int encoding for negative numbers', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '-100']);
      expect(db.get('k')?.encoding).toBe('int');
    });

    it('uses int encoding for max int64', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '9223372036854775807']);
      expect(db.get('k')?.encoding).toBe('int');
    });

    it('uses int encoding for min int64', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '-9223372036854775808']);
      expect(db.get('k')?.encoding).toBe('int');
    });

    it('uses embstr encoding for short non-numeric strings', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', 'hello']);
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('uses embstr encoding for empty string', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '']);
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('uses embstr for 44-byte string', () => {
      const { db, clock } = createDb();
      const s44 = 'a'.repeat(44);
      str.set(db, clock, ['k', s44]);
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('uses raw encoding for 45-byte string', () => {
      const { db, clock } = createDb();
      const s45 = 'a'.repeat(45);
      str.set(db, clock, ['k', s45]);
      expect(db.get('k')?.encoding).toBe('raw');
    });

    it('uses raw encoding for long strings', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', 'x'.repeat(100)]);
      expect(db.get('k')?.encoding).toBe('raw');
    });

    it('uses embstr for float strings', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '3.14']);
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('uses embstr for numbers with leading zeros', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '007']);
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('uses embstr for values exceeding int64 range', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '9223372036854775808']);
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('encoding updates on overwrite', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '42']);
      expect(db.get('k')?.encoding).toBe('int');
      str.set(db, clock, ['k', 'hello']);
      expect(db.get('k')?.encoding).toBe('embstr');
    });
  });

  describe('EX flag', () => {
    it('sets TTL in seconds', () => {
      const { db, clock } = createDb(1000);
      str.set(db, clock, ['k', 'v', 'EX', '10']);
      expect(db.getExpiry('k')).toBe(11000);
    });

    it('is case-insensitive', () => {
      const { db, clock } = createDb(1000);
      str.set(db, clock, ['k', 'v', 'ex', '10']);
      expect(db.getExpiry('k')).toBe(11000);
    });

    it('rejects non-integer EX value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', 'abc']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });

    it('rejects zero EX value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '0']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "invalid expire time in 'set' command",
      });
    });

    it('rejects negative EX value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '-1']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "invalid expire time in 'set' command",
      });
    });

    it('rejects float EX value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '3.14']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });
  });

  describe('PX flag', () => {
    it('sets TTL in milliseconds', () => {
      const { db, clock } = createDb(1000);
      str.set(db, clock, ['k', 'v', 'PX', '5000']);
      expect(db.getExpiry('k')).toBe(6000);
    });

    it('rejects zero PX value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PX', '0']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "invalid expire time in 'set' command",
      });
    });

    it('rejects negative PX value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PX', '-100']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "invalid expire time in 'set' command",
      });
    });
  });

  describe('EXAT flag', () => {
    it('sets expiry at unix timestamp in seconds', () => {
      const { db, clock } = createDb(1000);
      str.set(db, clock, ['k', 'v', 'EXAT', '100']);
      expect(db.getExpiry('k')).toBe(100000);
    });

    it('rejects zero EXAT value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EXAT', '0']);
      expect(reply.kind).toBe('error');
    });

    it('rejects negative EXAT value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EXAT', '-1']);
      expect(reply.kind).toBe('error');
    });
  });

  describe('PXAT flag', () => {
    it('sets expiry at unix timestamp in milliseconds', () => {
      const { db, clock } = createDb(1000);
      str.set(db, clock, ['k', 'v', 'PXAT', '50000']);
      expect(db.getExpiry('k')).toBe(50000);
    });

    it('rejects zero PXAT value', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PXAT', '0']);
      expect(reply.kind).toBe('error');
    });
  });

  describe('NX flag', () => {
    it('sets key only if it does not exist', () => {
      const { db, clock } = createDb();
      expect(str.set(db, clock, ['k', 'v', 'NX'])).toEqual({
        kind: 'status',
        value: 'OK',
      });
      expect(db.get('k')?.value).toBe('v');
    });

    it('returns nil if key already exists', () => {
      const { db, clock } = createDb();
      db.set('k', 'string', 'raw', 'old');
      expect(str.set(db, clock, ['k', 'new', 'NX'])).toEqual({
        kind: 'bulk',
        value: null,
      });
      expect(db.get('k')?.value).toBe('old');
    });

    it('succeeds if key was expired', () => {
      const { db, clock, setTime } = createDb(1000);
      db.set('k', 'string', 'raw', 'old');
      db.setExpiry('k', 2000);
      setTime(3000);
      expect(str.set(db, clock, ['k', 'new', 'NX'])).toEqual({
        kind: 'status',
        value: 'OK',
      });
      expect(db.get('k')?.value).toBe('new');
    });
  });

  describe('XX flag', () => {
    it('sets key only if it exists', () => {
      const { db, clock } = createDb();
      db.set('k', 'string', 'raw', 'old');
      expect(str.set(db, clock, ['k', 'new', 'XX'])).toEqual({
        kind: 'status',
        value: 'OK',
      });
      expect(db.get('k')?.value).toBe('new');
    });

    it('returns nil if key does not exist', () => {
      const { db, clock } = createDb();
      expect(str.set(db, clock, ['k', 'v', 'XX'])).toEqual({
        kind: 'bulk',
        value: null,
      });
      expect(db.has('k')).toBe(false);
    });
  });

  describe('KEEPTTL flag', () => {
    it('preserves existing TTL', () => {
      const { db, clock } = createDb(1000);
      db.set('k', 'string', 'raw', 'old');
      db.setExpiry('k', 5000);
      str.set(db, clock, ['k', 'new', 'KEEPTTL']);
      expect(db.get('k')?.value).toBe('new');
      expect(db.getExpiry('k')).toBe(5000);
    });

    it('does not set TTL if none existed', () => {
      const { db, clock } = createDb();
      db.set('k', 'string', 'raw', 'old');
      str.set(db, clock, ['k', 'new', 'KEEPTTL']);
      expect(db.getExpiry('k')).toBeUndefined();
    });
  });

  describe('GET flag (SET ... GET)', () => {
    it('returns nil when key does not exist', () => {
      const { db, clock } = createDb();
      expect(str.set(db, clock, ['k', 'v', 'GET'])).toEqual({
        kind: 'bulk',
        value: null,
      });
      expect(db.get('k')?.value).toBe('v');
    });

    it('returns old value when key exists', () => {
      const { db, clock } = createDb();
      db.set('k', 'string', 'raw', 'old');
      expect(str.set(db, clock, ['k', 'new', 'GET'])).toEqual({
        kind: 'bulk',
        value: 'old',
      });
      expect(db.get('k')?.value).toBe('new');
    });

    it('returns WRONGTYPE error for non-string key', () => {
      const { db, clock } = createDb();
      db.set('k', 'list', 'quicklist', []);
      const reply = str.set(db, clock, ['k', 'v', 'GET']);
      expect(reply.kind).toBe('error');
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'WRONGTYPE',
        message: 'Operation against a key holding the wrong kind of value',
      });
    });
  });

  describe('flag combinations', () => {
    it('EX + NX: sets with TTL only if key does not exist', () => {
      const { db, clock } = createDb(1000);
      expect(str.set(db, clock, ['k', 'v', 'EX', '10', 'NX'])).toEqual({
        kind: 'status',
        value: 'OK',
      });
      expect(db.getExpiry('k')).toBe(11000);
    });

    it('EX + NX: returns nil if key exists (no TTL change)', () => {
      const { db, clock } = createDb(1000);
      db.set('k', 'string', 'raw', 'old');
      db.setExpiry('k', 5000);
      expect(str.set(db, clock, ['k', 'new', 'EX', '10', 'NX'])).toEqual({
        kind: 'bulk',
        value: null,
      });
      expect(db.get('k')?.value).toBe('old');
      expect(db.getExpiry('k')).toBe(5000);
    });

    it('PX + XX: sets with ms TTL only if key exists', () => {
      const { db, clock } = createDb(1000);
      db.set('k', 'string', 'raw', 'old');
      str.set(db, clock, ['k', 'new', 'PX', '5000', 'XX']);
      expect(db.get('k')?.value).toBe('new');
      expect(db.getExpiry('k')).toBe(6000);
    });

    it('GET + EX: returns old value and sets TTL', () => {
      const { db, clock } = createDb(1000);
      db.set('k', 'string', 'raw', 'old');
      const reply = str.set(db, clock, ['k', 'new', 'GET', 'EX', '10']);
      expect(reply).toEqual({ kind: 'bulk', value: 'old' });
      expect(db.getExpiry('k')).toBe(11000);
    });

    it('GET + NX: returns specific incompatibility error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'GET', 'NX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'NX and GET options at the same time are not compatible',
      });
    });

    it('KEEPTTL + EX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'EX', '10']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('KEEPTTL + PX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'PX', '10']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('KEEPTTL + EXAT: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'EXAT', '100']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('KEEPTTL + PXAT: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'PXAT', '100']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('EX + KEEPTTL: syntax error (reverse order)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '10', 'KEEPTTL']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('EX + PX: syntax error (multiple TTL options)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '10', 'PX', '5000']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('EX + EXAT: syntax error (multiple TTL options)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '10', 'EXAT', '100']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('NX + XX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'NX', 'XX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('XX + NX: syntax error (reverse order)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'XX', 'NX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('duplicate NX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'NX', 'NX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('duplicate XX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'XX', 'XX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('duplicate KEEPTTL: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'KEEPTTL']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('duplicate GET: allowed (idempotent)', () => {
      const { db, clock } = createDb();
      db.set('k', 'string', 'raw', 'old');
      const reply = str.set(db, clock, ['k', 'new', 'GET', 'GET']);
      expect(reply).toEqual({ kind: 'bulk', value: 'old' });
    });

    it('KEEPTTL + XX: sets value, preserves TTL, only if key exists', () => {
      const { db, clock } = createDb(1000);
      db.set('k', 'string', 'raw', 'old');
      db.setExpiry('k', 5000);
      const reply = str.set(db, clock, ['k', 'new', 'KEEPTTL', 'XX']);
      expect(reply).toEqual({ kind: 'status', value: 'OK' });
      expect(db.get('k')?.value).toBe('new');
      expect(db.getExpiry('k')).toBe(5000);
    });

    it('GET + XX: returns old value only if key exists', () => {
      const { db, clock } = createDb();
      db.set('k', 'string', 'raw', 'old');
      const reply = str.set(db, clock, ['k', 'new', 'GET', 'XX']);
      expect(reply).toEqual({ kind: 'bulk', value: 'old' });
    });

    it('GET + XX: returns nil if key does not exist', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'GET', 'XX']);
      expect(reply).toEqual({ kind: 'bulk', value: null });
      expect(db.has('k')).toBe(false);
    });
  });

  describe('syntax errors', () => {
    it('rejects unknown flag with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'BADOPT']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('rejects EX without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('rejects PX without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PX']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('rejects EXAT without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EXAT']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('rejects PXAT without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PXAT']);
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });
  });

  describe('edge cases', () => {
    it('handles empty string value', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '']);
      expect(db.get('k')?.value).toBe('');
      expect(db.get('k')?.encoding).toBe('embstr');
    });

    it('handles empty string key', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['', 'val']);
      expect(db.get('')?.value).toBe('val');
    });

    it('stores value as string (not number)', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', '42']);
      expect(db.get('k')?.value).toBe('42');
      expect(typeof db.get('k')?.value).toBe('string');
    });

    it('handles value with spaces', () => {
      const { db, clock } = createDb();
      str.set(db, clock, ['k', 'hello world']);
      expect(db.get('k')?.value).toBe('hello world');
    });
  });
});

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
