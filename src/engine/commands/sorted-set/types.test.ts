import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { ConfigStore } from '../../../config-store.ts';
import {
  chooseEncoding,
  updateEncoding,
  formatScore,
  getOrCreateZset,
  getExistingZset,
} from './types.ts';
import { set } from '../string/index.ts';
import { zadd } from './sorted-set.ts';

let rngValue = 0.5;
function createDb(): { db: Database; engine: RedisEngine; rng: () => number } {
  rngValue = 0.5;
  const rng = () => rngValue;
  const engine = new RedisEngine({ clock: () => 1000, rng });
  return { db: engine.db(0), engine, rng };
}

const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- chooseEncoding ---

describe('chooseEncoding', () => {
  it('returns listpack for empty map', () => {
    expect(chooseEncoding(new Map())).toBe('listpack');
  });

  it('returns listpack for small map with short keys', () => {
    const dict = new Map<string, number>();
    for (let i = 0; i < 10; i++) {
      dict.set(`k${i}`, i);
    }
    expect(chooseEncoding(dict)).toBe('listpack');
  });

  it('returns listpack at exactly 128 entries with short keys', () => {
    const dict = new Map<string, number>();
    for (let i = 0; i < 128; i++) {
      dict.set(`k${i}`, i);
    }
    expect(chooseEncoding(dict)).toBe('listpack');
  });

  it('returns skiplist when entries exceed 128', () => {
    const dict = new Map<string, number>();
    for (let i = 0; i < 129; i++) {
      dict.set(`k${i}`, i);
    }
    expect(chooseEncoding(dict)).toBe('skiplist');
  });

  it('returns skiplist when a member exceeds 64 bytes', () => {
    const dict = new Map<string, number>();
    // 65 ASCII characters = 65 bytes, exceeds the 64-byte threshold
    dict.set('a'.repeat(65), 1);
    expect(chooseEncoding(dict)).toBe('skiplist');
  });

  it('returns listpack when member is exactly 64 bytes', () => {
    const dict = new Map<string, number>();
    dict.set('a'.repeat(64), 1);
    expect(chooseEncoding(dict)).toBe('listpack');
  });

  it('returns skiplist for multi-byte characters exceeding 64 bytes', () => {
    const dict = new Map<string, number>();
    // Each emoji is 4 bytes in UTF-8, so 17 emojis = 68 bytes > 64
    dict.set('😀'.repeat(17), 1);
    expect(chooseEncoding(dict)).toBe('skiplist');
  });

  it('returns listpack when multi-byte chars fit within 64 bytes', () => {
    const dict = new Map<string, number>();
    // 16 emojis = 64 bytes, exactly at the limit
    dict.set('😀'.repeat(16), 1);
    expect(chooseEncoding(dict)).toBe('listpack');
  });

  it('returns skiplist if any single member is too long even with few entries', () => {
    const dict = new Map<string, number>();
    dict.set('short', 1);
    dict.set('a'.repeat(65), 2);
    dict.set('also-short', 3);
    expect(chooseEncoding(dict)).toBe('skiplist');
  });
});

// --- updateEncoding ---

describe('updateEncoding', () => {
  it('does nothing for non-existent key', () => {
    const { db } = createDb();
    // Should not throw
    updateEncoding(db, 'nonexistent');
  });

  it('does nothing for non-zset key', () => {
    const { db, engine } = createDb();
    set(db, engine.clock, ['mykey', 'hello']);
    updateEncoding(db, 'mykey');
    const entry = db.get('mykey');
    expect(entry?.type).toBe('string');
  });

  it('does not demote skiplist to listpack', () => {
    const { db, rng } = createDb();
    // Create a zset
    getOrCreateZset(db, 'zk', rng);
    const entry = db.get('zk');
    if (!entry) throw new Error('expected entry');
    // Force encoding to skiplist
    entry.encoding = 'skiplist';
    updateEncoding(db, 'zk');
    expect(entry.encoding).toBe('skiplist');
  });

  it('promotes listpack to skiplist when entries exceed threshold', () => {
    const { db, rng } = createDb();
    const result = getOrCreateZset(db, 'zk', rng);
    if (result.error) throw new Error('unexpected error');
    const zset = result.zset;
    // Add 129 members to exceed the 128-entry threshold
    for (let i = 0; i < 129; i++) {
      zset.dict.set(`m${i}`, i);
    }
    const entry = db.get('zk');
    if (!entry) throw new Error('expected entry');
    expect(entry.encoding).toBe('listpack');
    updateEncoding(db, 'zk');
    expect(entry.encoding).toBe('skiplist');
  });

  it('promotes listpack to skiplist when a member is too long', () => {
    const { db, rng } = createDb();
    const result = getOrCreateZset(db, 'zk', rng);
    if (result.error) throw new Error('unexpected error');
    const zset = result.zset;
    zset.dict.set('a'.repeat(65), 1);
    updateEncoding(db, 'zk');
    const entry = db.get('zk');
    if (!entry) throw new Error('expected entry');
    expect(entry.encoding).toBe('skiplist');
  });

  it('keeps listpack when within thresholds', () => {
    const { db, rng } = createDb();
    const result = getOrCreateZset(db, 'zk', rng);
    if (result.error) throw new Error('unexpected error');
    const zset = result.zset;
    zset.dict.set('small', 1);
    updateEncoding(db, 'zk');
    const entry = db.get('zk');
    if (!entry) throw new Error('expected entry');
    expect(entry.encoding).toBe('listpack');
  });
});

// --- formatScore ---

describe('formatScore', () => {
  it('formats positive infinity', () => {
    expect(formatScore(Infinity)).toBe('inf');
  });

  it('formats negative infinity', () => {
    expect(formatScore(-Infinity)).toBe('-inf');
  });

  it('formats zero', () => {
    expect(formatScore(0)).toBe('0');
  });

  it('formats negative zero as zero', () => {
    expect(formatScore(-0)).toBe('0');
  });

  it('formats integer scores', () => {
    expect(formatScore(1)).toBe('1');
    expect(formatScore(-5)).toBe('-5');
    expect(formatScore(100)).toBe('100');
  });

  it('formats floating point scores', () => {
    expect(formatScore(1.5)).toBe('1.5');
    expect(formatScore(-3.14)).toBe('-3.14');
  });

  it('formats very small numbers', () => {
    // 0.00001 has precision artifacts; use 1e-5 directly
    expect(formatScore(1e-5)).toBe('1.0000000000000001e-05');
  });

  it('formats very large numbers', () => {
    expect(formatScore(1e17)).toBe('1e+17');
  });
});

// --- getOrCreateZset ---

describe('getOrCreateZset', () => {
  it('creates a new zset for non-existent key', () => {
    const { db, rng } = createDb();
    const result = getOrCreateZset(db, 'newkey', rng);
    expect(result.error).toBeNull();
    expect(result.zset).not.toBeNull();
    expect(result.zset?.dict).toBeInstanceOf(Map);
    expect(result.zset?.dict.size).toBe(0);
  });

  it('stores the created zset in the database', () => {
    const { db, rng } = createDb();
    getOrCreateZset(db, 'newkey', rng);
    const entry = db.get('newkey');
    expect(entry).not.toBeUndefined();
    expect(entry?.type).toBe('zset');
    expect(entry?.encoding).toBe('listpack');
  });

  it('returns existing zset for existing key', () => {
    const { db, rng } = createDb();
    const first = getOrCreateZset(db, 'k', rng);
    expect(first.error).toBeNull();
    first.zset?.dict.set('member', 1);

    const second = getOrCreateZset(db, 'k', rng);
    expect(second.error).toBeNull();
    // Should be the same object
    expect(second.zset).toBe(first.zset);
    expect(second.zset?.dict.get('member')).toBe(1);
  });

  it('returns WRONGTYPE error for non-zset key', () => {
    const { db, engine, rng } = createDb();
    set(db, engine.clock, ['k', 'hello']);
    const result = getOrCreateZset(db, 'k', rng);
    expect(result.zset).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });

  it('creates independent zsets for different keys', () => {
    const { db, rng } = createDb();
    const r1 = getOrCreateZset(db, 'a', rng);
    const r2 = getOrCreateZset(db, 'b', rng);
    expect(r1.zset).not.toBe(r2.zset);
  });
});

// --- getExistingZset ---

describe('getExistingZset', () => {
  it('returns null zset and null error for non-existent key', () => {
    const { db } = createDb();
    const result = getExistingZset(db, 'nokey');
    expect(result.zset).toBeNull();
    expect(result.error).toBeNull();
  });

  it('returns the zset for an existing zset key', () => {
    const { db, rng } = createDb();
    const created = getOrCreateZset(db, 'k', rng);
    expect(created.error).toBeNull();
    created.zset?.dict.set('x', 42);

    const result = getExistingZset(db, 'k');
    expect(result.error).toBeNull();
    expect(result.zset).not.toBeNull();
    expect(result.zset?.dict.get('x')).toBe(42);
    // Same object reference
    expect(result.zset).toBe(created.zset);
  });

  it('returns WRONGTYPE error for non-zset key', () => {
    const { db, engine } = createDb();
    set(db, engine.clock, ['k', 'value']);
    const result = getExistingZset(db, 'k');
    expect(result.zset).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });

  it('does not create a key in the database', () => {
    const { db } = createDb();
    getExistingZset(db, 'ghost');
    expect(db.get('ghost')).toBeNull();
  });
});

// --- zset config thresholds ---

describe('zset config thresholds', () => {
  it('CONFIG SET zset-max-listpack-entries lowers threshold', () => {
    const { db, rng } = createDb();
    const config = new ConfigStore();
    config.set('zset-max-listpack-entries', '2');

    zadd(db, ['myzset', '1', 'a', '2', 'b', '3', 'c'], rng, config);

    expect(db.get('myzset')?.encoding).toBe('skiplist');
  });

  it('CONFIG SET zset-max-listpack-entries raises threshold', () => {
    const { db, rng } = createDb();
    const config = new ConfigStore();
    config.set('zset-max-listpack-entries', '256');

    const args = ['myzset'];
    for (let i = 0; i < 200; i++) args.push(String(i), `m${i}`);
    zadd(db, args, rng, config);

    expect(db.get('myzset')?.encoding).toBe('listpack');
  });

  it('CONFIG SET zset-max-listpack-value lowers threshold', () => {
    const { db, rng } = createDb();
    const config = new ConfigStore();
    config.set('zset-max-listpack-value', '5');

    zadd(db, ['myzset', '1', 'longmember'], rng, config);

    expect(db.get('myzset')?.encoding).toBe('skiplist');
  });

  it('CONFIG SET zset-max-listpack-value raises threshold', () => {
    const { db, rng } = createDb();
    const config = new ConfigStore();
    config.set('zset-max-listpack-value', '200');

    zadd(db, ['myzset', '1', 'x'.repeat(100)], rng, config);

    expect(db.get('myzset')?.encoding).toBe('listpack');
  });

  it('chooseEncoding respects config thresholds', () => {
    const config = new ConfigStore();
    config.set('zset-max-listpack-entries', '2');

    const dict = new Map<string, number>([
      ['a', 1],
      ['b', 2],
      ['c', 3],
    ]);
    expect(chooseEncoding(dict, config)).toBe('skiplist');
    expect(chooseEncoding(dict)).toBe('listpack');
  });

  it('updateEncoding uses config when provided', () => {
    const { db, rng } = createDb();
    const config = new ConfigStore();
    config.set('zset-max-listpack-entries', '1');

    const result = getOrCreateZset(db, 'zk', rng);
    if (result.error || !result.zset) throw new Error('unexpected error');
    result.zset.dict.set('a', 1);
    result.zset.dict.set('b', 2);

    updateEncoding(db, 'zk', config);
    expect(db.get('zk')?.encoding).toBe('skiplist');
  });
});
