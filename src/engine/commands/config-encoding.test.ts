import { describe, it, expect, beforeEach } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import { ConfigStore } from '../../config-store.ts';
import { hset } from './hash/hash.ts';
import { lpush } from './list/list.ts';
import { sadd } from './set/set.ts';
import { zadd } from './sorted-set/sorted-set.ts';
import { updateEncoding as hashUpdateEncoding } from './hash/utils.ts';
import {
  updateEncoding as listUpdateEncoding,
  fitsListpackBySize,
} from './list/utils.ts';
import {
  updateEncoding as setUpdateEncoding,
  chooseInitialEncoding,
} from './set/utils.ts';
import {
  updateEncoding as zsetUpdateEncoding,
  chooseEncoding,
  getOrCreateZset,
} from './sorted-set/types.ts';

let db: Database;
let config: ConfigStore;
const rng = () => 0.5;

beforeEach(() => {
  const engine = new RedisEngine({ clock: () => 1000, rng });
  db = engine.db(0);
  config = new ConfigStore();
});

// ---------------------------------------------------------------------------
// Hash: hash-max-listpack-entries / hash-max-listpack-value
// ---------------------------------------------------------------------------

describe('hash config thresholds', () => {
  it('CONFIG SET hash-max-listpack-entries lowers threshold', () => {
    config.set('hash-max-listpack-entries', '2');

    hset(db, ['myhash', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3'], config);

    expect(db.get('myhash')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET hash-max-listpack-entries raises threshold', () => {
    config.set('hash-max-listpack-entries', '256');

    const args = ['myhash'];
    for (let i = 0; i < 200; i++) args.push(`f${i}`, `v${i}`);
    hset(db, args, config);

    expect(db.get('myhash')?.encoding).toBe('listpack');
  });

  it('CONFIG SET hash-max-listpack-value lowers threshold', () => {
    config.set('hash-max-listpack-value', '5');

    hset(db, ['myhash', 'f1', 'longval'], config);

    expect(db.get('myhash')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET hash-max-listpack-value raises threshold', () => {
    config.set('hash-max-listpack-value', '200');

    hset(db, ['myhash', 'f1', 'x'.repeat(100)], config);

    expect(db.get('myhash')?.encoding).toBe('listpack');
  });

  it('updateEncoding uses config when provided', () => {
    config.set('hash-max-listpack-entries', '2');

    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
      ['f3', 'v3'],
    ]);
    db.set('k', 'hash', 'listpack', hash);

    hashUpdateEncoding(db, 'k', config);
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('updateEncoding uses defaults when config absent', () => {
    const hash = new Map([
      ['f1', 'v1'],
      ['f2', 'v2'],
      ['f3', 'v3'],
    ]);
    db.set('k', 'hash', 'listpack', hash);

    hashUpdateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });
});

// ---------------------------------------------------------------------------
// List: list-max-listpack-size
// ---------------------------------------------------------------------------

describe('list config thresholds', () => {
  it('CONFIG SET list-max-listpack-size positive value limits entry count', () => {
    config.set('list-max-listpack-size', '3');

    lpush(db, ['mylist', 'a', 'b', 'c', 'd'], config);

    expect(db.get('mylist')?.encoding).toBe('quicklist');
  });

  it('CONFIG SET list-max-listpack-size positive value allows within limit', () => {
    config.set('list-max-listpack-size', '5');

    lpush(db, ['mylist', 'a', 'b', 'c'], config);

    expect(db.get('mylist')?.encoding).toBe('listpack');
  });

  it('CONFIG SET list-max-listpack-size negative value uses byte limit', () => {
    config.set('list-max-listpack-size', '-1'); // 4096 bytes

    const items = ['mylist'];
    for (let i = 0; i < 5; i++) items.push('x'.repeat(1000));
    lpush(db, items, config);

    expect(db.get('mylist')?.encoding).toBe('quicklist');
  });

  it('CONFIG SET list-max-listpack-size -2 allows up to 8192 bytes', () => {
    config.set('list-max-listpack-size', '-2'); // 8192 bytes

    const items = ['mylist'];
    for (let i = 0; i < 8; i++) items.push('x'.repeat(1000));
    lpush(db, items, config);

    expect(db.get('mylist')?.encoding).toBe('listpack');
  });

  it('fitsListpackBySize handles positive size', () => {
    expect(fitsListpackBySize(['a', 'b', 'c'], 3)).toBe(true);
    expect(fitsListpackBySize(['a', 'b', 'c', 'd'], 3)).toBe(false);
  });

  it('fitsListpackBySize handles negative fill factors', () => {
    expect(fitsListpackBySize(['x'.repeat(4096)], -1)).toBe(true);
    expect(fitsListpackBySize(['x'.repeat(4097)], -1)).toBe(false);
    expect(fitsListpackBySize(['x'.repeat(65536)], -5)).toBe(true);
    expect(fitsListpackBySize(['x'.repeat(65537)], -5)).toBe(false);
  });

  it('fitsListpackBySize rejects invalid negative values', () => {
    expect(fitsListpackBySize(['a'], -6)).toBe(false);
    expect(fitsListpackBySize(['a'], -10)).toBe(false);
    expect(fitsListpackBySize(['a'], 0)).toBe(false);
  });

  it('updateEncoding with config uses list-max-listpack-size', () => {
    config.set('list-max-listpack-size', '2');

    db.set('k', 'list', 'listpack', ['a', 'b', 'c']);
    listUpdateEncoding(db, 'k', config);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('updateEncoding without config uses legacy defaults', () => {
    db.set('k', 'list', 'listpack', ['a', 'b', 'c']);
    listUpdateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });
});

// ---------------------------------------------------------------------------
// Set: set-max-listpack-entries / set-max-listpack-value / set-max-intset-entries
// ---------------------------------------------------------------------------

describe('set config thresholds', () => {
  it('CONFIG SET set-max-listpack-entries lowers threshold', () => {
    config.set('set-max-listpack-entries', '2');

    sadd(db, ['myset', 'a', 'b', 'c'], config);

    expect(db.get('myset')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET set-max-listpack-entries raises threshold', () => {
    config.set('set-max-listpack-entries', '256');

    const args = ['myset'];
    for (let i = 0; i < 200; i++) args.push(`m${i}`);
    sadd(db, args, config);

    expect(db.get('myset')?.encoding).toBe('listpack');
  });

  it('CONFIG SET set-max-listpack-value lowers threshold', () => {
    config.set('set-max-listpack-value', '3');

    sadd(db, ['myset', 'longmember'], config);

    expect(db.get('myset')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET set-max-intset-entries lowers threshold', () => {
    config.set('set-max-intset-entries', '3');

    sadd(db, ['myset', '1', '2', '3', '4'], config);

    // 4 integers > intset limit 3, fits listpack default 128
    expect(db.get('myset')?.encoding).toBe('listpack');
  });

  it('CONFIG SET set-max-intset-entries raises threshold', () => {
    config.set('set-max-intset-entries', '1000');

    const args = ['myset'];
    for (let i = 0; i < 600; i++) args.push(String(i));
    sadd(db, args, config);

    expect(db.get('myset')?.encoding).toBe('intset');
  });

  it('chooseInitialEncoding respects config thresholds', () => {
    config.set('set-max-intset-entries', '2');
    config.set('set-max-listpack-entries', '5');

    const s = new Set(['1', '2', '3']);
    expect(chooseInitialEncoding(s, config)).toBe('listpack');
  });

  it('updateEncoding promotes intset with config', () => {
    config.set('set-max-intset-entries', '2');

    const s = new Set(['1', '2', '3']);
    db.set('k', 'set', 'intset', s);

    setUpdateEncoding(db, 'k', config);
    expect(db.get('k')?.encoding).toBe('listpack');
  });
});

// ---------------------------------------------------------------------------
// Sorted set: zset-max-listpack-entries / zset-max-listpack-value
// ---------------------------------------------------------------------------

describe('zset config thresholds', () => {
  it('CONFIG SET zset-max-listpack-entries lowers threshold', () => {
    config.set('zset-max-listpack-entries', '2');

    zadd(db, ['myzset', '1', 'a', '2', 'b', '3', 'c'], rng, config);

    expect(db.get('myzset')?.encoding).toBe('skiplist');
  });

  it('CONFIG SET zset-max-listpack-entries raises threshold', () => {
    config.set('zset-max-listpack-entries', '256');

    const args = ['myzset'];
    for (let i = 0; i < 200; i++) args.push(String(i), `m${i}`);
    zadd(db, args, rng, config);

    expect(db.get('myzset')?.encoding).toBe('listpack');
  });

  it('CONFIG SET zset-max-listpack-value lowers threshold', () => {
    config.set('zset-max-listpack-value', '5');

    zadd(db, ['myzset', '1', 'longmember'], rng, config);

    expect(db.get('myzset')?.encoding).toBe('skiplist');
  });

  it('CONFIG SET zset-max-listpack-value raises threshold', () => {
    config.set('zset-max-listpack-value', '200');

    zadd(db, ['myzset', '1', 'x'.repeat(100)], rng, config);

    expect(db.get('myzset')?.encoding).toBe('listpack');
  });

  it('chooseEncoding respects config thresholds', () => {
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
    config.set('zset-max-listpack-entries', '1');

    const result = getOrCreateZset(db, 'zk', rng);
    if (result.error || !result.zset) throw new Error('unexpected error');
    result.zset.dict.set('a', 1);
    result.zset.dict.set('b', 2);

    zsetUpdateEncoding(db, 'zk', config);
    expect(db.get('zk')?.encoding).toBe('skiplist');
  });
});

// ---------------------------------------------------------------------------
// configInt helper
// ---------------------------------------------------------------------------

describe('configInt helper', () => {
  it('reads integer from config', async () => {
    const { configInt } = await import('../utils.ts');
    config.set('hash-max-listpack-entries', '42');
    expect(configInt(config, 'hash-max-listpack-entries', 128)).toBe(42);
  });

  it('returns fallback when config is undefined', async () => {
    const { configInt } = await import('../utils.ts');
    expect(configInt(undefined, 'hash-max-listpack-entries', 128)).toBe(128);
  });

  it('returns fallback for unknown key', async () => {
    const { configInt } = await import('../utils.ts');
    expect(configInt(config, 'nonexistent-key', 99)).toBe(99);
  });
});
