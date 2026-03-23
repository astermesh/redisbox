import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { ConfigStore } from '../../../config-store.ts';
import {
  fitsListpack,
  getOrCreateHash,
  getExistingHash,
  updateEncoding,
  HASH_NOT_INTEGER_ERR,
  HASH_NOT_FLOAT_ERR,
} from './utils.ts';
import { hset } from './hash.ts';

function createDb(): Database {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return engine.db(0);
}

const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

describe('HASH_NOT_INTEGER_ERR', () => {
  it('has correct error message', () => {
    expect(HASH_NOT_INTEGER_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'hash value is not an integer',
    });
  });
});

describe('HASH_NOT_FLOAT_ERR', () => {
  it('has correct error message', () => {
    expect(HASH_NOT_FLOAT_ERR).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'hash value is not a valid float',
    });
  });
});

describe('fitsListpack', () => {
  it('returns true for empty hash', () => {
    expect(fitsListpack(new Map())).toBe(true);
  });

  it('returns true for small hash within defaults', () => {
    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
    ]);
    expect(fitsListpack(hash)).toBe(true);
  });

  it('returns false when exceeding max entries', () => {
    const hash = new Map<string, string>();
    for (let i = 0; i < 3; i++) {
      hash.set(`f${i}`, `v${i}`);
    }
    expect(fitsListpack(hash, 2, 64)).toBe(false);
  });

  it('returns true at exact max entries', () => {
    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
    ]);
    expect(fitsListpack(hash, 2, 64)).toBe(true);
  });

  it('returns false when field name exceeds max value size', () => {
    const hash = new Map([['x'.repeat(10), 'v1']]);
    expect(fitsListpack(hash, 128, 5)).toBe(false);
  });

  it('returns false when field value exceeds max value size', () => {
    const hash = new Map([['f1', 'x'.repeat(10)]]);
    expect(fitsListpack(hash, 128, 5)).toBe(false);
  });

  it('returns true when field and value are at exact max size', () => {
    const hash = new Map([['x'.repeat(5), 'y'.repeat(5)]]);
    expect(fitsListpack(hash, 128, 5)).toBe(true);
  });
});

describe('getOrCreateHash', () => {
  it('creates new hash when key does not exist', () => {
    const db = createDb();
    const result = getOrCreateHash(db, 'k');
    expect(result.error).toBeNull();
    expect(result.hash).toBeInstanceOf(Map);
    expect(result.hash?.size).toBe(0);
    expect(db.get('k')?.type).toBe('hash');
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('returns existing hash', () => {
    const db = createDb();
    const hash = new Map([['f1', 'v1']]);
    db.set('k', 'hash', 'listpack', hash);
    const result = getOrCreateHash(db, 'k');
    expect(result.error).toBeNull();
    expect(result.hash).toBe(hash);
  });

  it('returns WRONGTYPE error for non-hash key', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', 'val');
    const result = getOrCreateHash(db, 'k');
    expect(result.hash).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });
});

describe('getExistingHash', () => {
  it('returns null hash and null error when key does not exist', () => {
    const db = createDb();
    const result = getExistingHash(db, 'k');
    expect(result.hash).toBeNull();
    expect(result.error).toBeNull();
  });

  it('returns existing hash', () => {
    const db = createDb();
    const hash = new Map([['f1', 'v1']]);
    db.set('k', 'hash', 'listpack', hash);
    const result = getExistingHash(db, 'k');
    expect(result.error).toBeNull();
    expect(result.hash).toBe(hash);
  });

  it('returns WRONGTYPE error for non-hash key', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', 'val');
    const result = getExistingHash(db, 'k');
    expect(result.hash).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });
});

describe('updateEncoding', () => {
  it('does nothing when key does not exist', () => {
    const db = createDb();
    updateEncoding(db, 'k'); // should not throw
  });

  it('does nothing for non-hash key', () => {
    const db = createDb();
    db.set('k', 'string', 'raw', 'val');
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('raw');
  });

  it('keeps listpack for small hash', () => {
    const db = createDb();
    const hash = new Map([['f1', 'v1']]);
    db.set('k', 'hash', 'listpack', hash);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('promotes to hashtable when hash exceeds entry count', () => {
    const db = createDb();
    const hash = new Map<string, string>();
    for (let i = 0; i <= 128; i++) {
      hash.set(`f${i}`, `v${i}`);
    }
    db.set('k', 'hash', 'listpack', hash);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('promotes to hashtable when value exceeds size limit', () => {
    const db = createDb();
    const hash = new Map([['f1', 'x'.repeat(65)]]);
    db.set('k', 'hash', 'listpack', hash);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('never demotes from hashtable', () => {
    const db = createDb();
    const hash = new Map([['f1', 'v1']]);
    db.set('k', 'hash', 'hashtable', hash);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('uses config hash-max-listpack-entries when provided', () => {
    const db = createDb();
    const config = new ConfigStore();
    config.set('hash-max-listpack-entries', '2');

    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
      ['f3', 'v3'],
    ]);
    db.set('k', 'hash', 'listpack', hash);

    updateEncoding(db, 'k', config);
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('uses defaults when config absent', () => {
    const db = createDb();
    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
      ['f3', 'v3'],
    ]);
    db.set('k', 'hash', 'listpack', hash);

    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });
});

// --- config integration ---

describe('hash config thresholds', () => {
  it('CONFIG SET hash-max-listpack-entries lowers threshold', () => {
    const db = createDb();
    const config = new ConfigStore();
    config.set('hash-max-listpack-entries', '2');

    hset(db, ['myhash', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3'], config);

    expect(db.get('myhash')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET hash-max-listpack-entries raises threshold', () => {
    const db = createDb();
    const config = new ConfigStore();
    config.set('hash-max-listpack-entries', '256');

    const args = ['myhash'];
    for (let i = 0; i < 200; i++) args.push(`f${i}`, `v${i}`);
    hset(db, args, config);

    expect(db.get('myhash')?.encoding).toBe('listpack');
  });

  it('CONFIG SET hash-max-listpack-value lowers threshold', () => {
    const db = createDb();
    const config = new ConfigStore();
    config.set('hash-max-listpack-value', '5');

    hset(db, ['myhash', 'f1', 'longval'], config);

    expect(db.get('myhash')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET hash-max-listpack-value raises threshold', () => {
    const db = createDb();
    const config = new ConfigStore();
    config.set('hash-max-listpack-value', '200');

    hset(db, ['myhash', 'f1', 'x'.repeat(100)], config);

    expect(db.get('myhash')?.encoding).toBe('listpack');
  });
});
