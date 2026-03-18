import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
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

// --- encoding helper ---

describe('determineStringEncoding', () => {
  it('returns int for zero', () => {
    expect(str.determineStringEncoding('0')).toBe('int');
  });

  it('returns int for positive integers', () => {
    expect(str.determineStringEncoding('42')).toBe('int');
    expect(str.determineStringEncoding('123456789')).toBe('int');
  });

  it('returns int for negative integers', () => {
    expect(str.determineStringEncoding('-1')).toBe('int');
    expect(str.determineStringEncoding('-999')).toBe('int');
  });

  it('returns int for max 64-bit signed integer', () => {
    expect(str.determineStringEncoding('9223372036854775807')).toBe('int');
  });

  it('returns int for min 64-bit signed integer', () => {
    expect(str.determineStringEncoding('-9223372036854775808')).toBe('int');
  });

  it('returns embstr for values exceeding 64-bit signed integer range', () => {
    expect(str.determineStringEncoding('9223372036854775808')).toBe('embstr');
    expect(str.determineStringEncoding('-9223372036854775809')).toBe('embstr');
  });

  it('returns embstr for short non-numeric strings', () => {
    expect(str.determineStringEncoding('hello')).toBe('embstr');
    expect(str.determineStringEncoding('')).toBe('embstr');
  });

  it('returns embstr for strings up to 44 bytes', () => {
    const s44 = 'a'.repeat(44);
    expect(str.determineStringEncoding(s44)).toBe('embstr');
  });

  it('returns raw for strings over 44 bytes', () => {
    const s45 = 'a'.repeat(45);
    expect(str.determineStringEncoding(s45)).toBe('raw');
  });

  it('returns embstr for float strings (not int)', () => {
    expect(str.determineStringEncoding('3.14')).toBe('embstr');
    expect(str.determineStringEncoding('-0.5')).toBe('embstr');
  });

  it('returns embstr for numeric strings with leading zeros', () => {
    expect(str.determineStringEncoding('007')).toBe('embstr');
    expect(str.determineStringEncoding('00')).toBe('embstr');
  });

  it('returns embstr for numeric strings with leading/trailing spaces', () => {
    expect(str.determineStringEncoding(' 42')).toBe('embstr');
    expect(str.determineStringEncoding('42 ')).toBe('embstr');
  });

  it('returns int for long numeric string within int range', () => {
    expect(str.determineStringEncoding('1000000000000000000')).toBe('int');
  });
});

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
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('KEEPTTL + PX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'PX', '10']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('KEEPTTL + EXAT: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'EXAT', '100']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('KEEPTTL + PXAT: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'PXAT', '100']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('EX + KEEPTTL: syntax error (reverse order)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '10', 'KEEPTTL']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('EX + PX: syntax error (multiple TTL options)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '10', 'PX', '5000']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('EX + EXAT: syntax error (multiple TTL options)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX', '10', 'EXAT', '100']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('NX + XX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'NX', 'XX']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('XX + NX: syntax error (reverse order)', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'XX', 'NX']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('duplicate NX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'NX', 'NX']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('duplicate XX: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'XX', 'XX']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('duplicate KEEPTTL: syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'KEEPTTL', 'KEEPTTL']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
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
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('rejects EX without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EX']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('rejects PX without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PX']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('rejects EXAT without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'EXAT']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
    });

    it('rejects PXAT without value with syntax error', () => {
      const { db, clock } = createDb();
      const reply = str.set(db, clock, ['k', 'v', 'PXAT']);
      expect(reply).toEqual({ kind: 'error', prefix: 'ERR', message: 'syntax error' });
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
