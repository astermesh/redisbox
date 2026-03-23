import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { ConfigStore } from '../../../config-store.ts';
import {
  DEFAULT_MAX_INTSET_ENTRIES,
  isIntegerString,
  allIntegers,
  fitsListpack,
  chooseInitialEncoding,
  updateEncoding,
  getOrCreateSet,
  getExistingSet,
  collectSets,
  findSmallest,
  computeIntersection,
  computeDifference,
  storeSetResult,
} from './utils.ts';
import { sadd } from './set.ts';

function createDb(): { db: Database; engine: RedisEngine } {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return { db: engine.db(0), engine };
}

function integer(value: number): Reply {
  return { kind: 'integer', value };
}

const ZERO = integer(0);
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- isIntegerString ---

describe('isIntegerString', () => {
  it('returns true for zero', () => {
    expect(isIntegerString('0')).toBe(true);
  });

  it('returns true for positive integers', () => {
    expect(isIntegerString('1')).toBe(true);
    expect(isIntegerString('42')).toBe(true);
    expect(isIntegerString('123456789')).toBe(true);
  });

  it('returns true for negative integers', () => {
    expect(isIntegerString('-1')).toBe(true);
    expect(isIntegerString('-100')).toBe(true);
  });

  it('returns true for INT64_MAX', () => {
    expect(isIntegerString('9223372036854775807')).toBe(true);
  });

  it('returns true for INT64_MIN', () => {
    expect(isIntegerString('-9223372036854775808')).toBe(true);
  });

  it('returns false for values exceeding INT64_MAX', () => {
    expect(isIntegerString('9223372036854775808')).toBe(false);
  });

  it('returns false for values below INT64_MIN', () => {
    expect(isIntegerString('-9223372036854775809')).toBe(false);
  });

  it('returns false for empty string', () => {
    expect(isIntegerString('')).toBe(false);
  });

  it('returns false for strings longer than 20 chars', () => {
    expect(isIntegerString('123456789012345678901')).toBe(false);
  });

  it('returns false for leading zeros', () => {
    expect(isIntegerString('007')).toBe(false);
    expect(isIntegerString('00')).toBe(false);
    expect(isIntegerString('01')).toBe(false);
  });

  it('returns false for non-numeric strings', () => {
    expect(isIntegerString('hello')).toBe(false);
    expect(isIntegerString('abc')).toBe(false);
    expect(isIntegerString('12a')).toBe(false);
  });

  it('returns false for floating-point strings', () => {
    expect(isIntegerString('1.5')).toBe(false);
    expect(isIntegerString('3.14')).toBe(false);
  });

  it('returns false for whitespace', () => {
    expect(isIntegerString(' ')).toBe(false);
    expect(isIntegerString(' 1')).toBe(false);
    expect(isIntegerString('1 ')).toBe(false);
  });

  it('returns false for plus-prefixed numbers', () => {
    expect(isIntegerString('+1')).toBe(false);
  });

  it('returns false for just a minus sign', () => {
    expect(isIntegerString('-')).toBe(false);
  });

  it('returns true for negative zero as -0 is canonically "0"', () => {
    // BigInt('-0').toString() === '0', so '-0' !== '0' => false
    expect(isIntegerString('-0')).toBe(false);
  });
});

// --- allIntegers ---

describe('allIntegers', () => {
  it('returns true for empty set', () => {
    expect(allIntegers(new Set())).toBe(true);
  });

  it('returns true when all members are integers', () => {
    expect(allIntegers(new Set(['1', '2', '-3', '0']))).toBe(true);
  });

  it('returns false when any member is not an integer', () => {
    expect(allIntegers(new Set(['1', '2', 'hello']))).toBe(false);
  });

  it('returns false when all members are non-integers', () => {
    expect(allIntegers(new Set(['foo', 'bar']))).toBe(false);
  });

  it('returns false for leading-zero members', () => {
    expect(allIntegers(new Set(['1', '007']))).toBe(false);
  });
});

// --- fitsListpack ---

describe('fitsListpack', () => {
  it('returns true for empty set', () => {
    expect(fitsListpack(new Set())).toBe(true);
  });

  it('returns true for small set with short values', () => {
    expect(fitsListpack(new Set(['a', 'b', 'c']))).toBe(true);
  });

  it('returns false when size exceeds maxEntries', () => {
    const s = new Set<string>();
    for (let i = 0; i < 129; i++) s.add(`m${i}`);
    expect(fitsListpack(s)).toBe(false);
  });

  it('returns true at exact maxEntries boundary (128)', () => {
    const s = new Set<string>();
    for (let i = 0; i < 128; i++) s.add(`m${i}`);
    expect(fitsListpack(s)).toBe(true);
  });

  it('returns false when any member exceeds maxValue (64 bytes)', () => {
    const s = new Set(['a', 'x'.repeat(65)]);
    expect(fitsListpack(s)).toBe(false);
  });

  it('returns true when member is exactly maxValue (64 bytes)', () => {
    const s = new Set(['a', 'x'.repeat(64)]);
    expect(fitsListpack(s)).toBe(true);
  });

  it('respects custom maxEntries parameter', () => {
    const s = new Set(['a', 'b', 'c']);
    expect(fitsListpack(s, 2)).toBe(false);
    expect(fitsListpack(s, 3)).toBe(true);
  });

  it('respects custom maxValue parameter', () => {
    const s = new Set(['abc']);
    expect(fitsListpack(s, 128, 2)).toBe(false);
    expect(fitsListpack(s, 128, 3)).toBe(true);
  });

  it('handles multi-byte UTF-8 characters for byte length', () => {
    // A 2-byte UTF-8 char repeated enough to exceed 64 bytes
    const s = new Set(['a'.repeat(64) + '\u00e9']); // 64 + 2 = 66 bytes
    expect(fitsListpack(s)).toBe(false);
  });
});

// --- chooseInitialEncoding ---

describe('chooseInitialEncoding', () => {
  it('returns intset for small all-integer set', () => {
    expect(chooseInitialEncoding(new Set(['1', '2', '3']))).toBe('intset');
  });

  it('returns listpack for small non-integer set', () => {
    expect(chooseInitialEncoding(new Set(['a', 'b']))).toBe('listpack');
  });

  it('returns hashtable when exceeding listpack entry count', () => {
    const s = new Set<string>();
    for (let i = 0; i < 129; i++) s.add(`m${i}`);
    expect(chooseInitialEncoding(s)).toBe('hashtable');
  });

  it('returns hashtable when member exceeds listpack value size', () => {
    expect(chooseInitialEncoding(new Set(['x'.repeat(65)]))).toBe('hashtable');
  });

  it('returns intset at intset entry limit', () => {
    const s = new Set<string>();
    for (let i = 0; i < DEFAULT_MAX_INTSET_ENTRIES; i++) s.add(String(i));
    expect(chooseInitialEncoding(s)).toBe('intset');
  });

  it('returns hashtable when exceeding intset limit with integers', () => {
    const s = new Set<string>();
    for (let i = 0; i < DEFAULT_MAX_INTSET_ENTRIES + 1; i++) s.add(String(i));
    // 513 > 512 (intset) and 513 > 128 (listpack) => hashtable
    expect(chooseInitialEncoding(s)).toBe('hashtable');
  });

  it('returns intset for empty set (vacuously all-integer)', () => {
    expect(chooseInitialEncoding(new Set())).toBe('intset');
  });

  it('returns listpack for mixed integer and non-integer within listpack limits', () => {
    expect(chooseInitialEncoding(new Set(['1', 'hello']))).toBe('listpack');
  });
});

// --- updateEncoding ---

describe('updateEncoding', () => {
  it('does nothing for non-existing key', () => {
    const { db } = createDb();
    updateEncoding(db, 'missing');
    expect(db.has('missing')).toBe(false);
  });

  it('does nothing for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('raw');
  });

  it('keeps intset when still all integers within limit', () => {
    const { db } = createDb();
    const s = new Set(['1', '2', '3']);
    db.set('k', 'set', 'intset', s);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('intset');
  });

  it('promotes intset to listpack when non-integer added', () => {
    const { db } = createDb();
    const s = new Set(['1', '2', 'hello']);
    db.set('k', 'set', 'intset', s);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('promotes intset to hashtable when exceeding both intset and listpack limits', () => {
    const { db } = createDb();
    const s = new Set<string>();
    for (let i = 0; i < 513; i++) s.add(`m${i}`);
    db.set('k', 'set', 'intset', s);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('keeps listpack when within limits', () => {
    const { db } = createDb();
    const s = new Set(['a', 'b', 'c']);
    db.set('k', 'set', 'listpack', s);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('promotes listpack to hashtable when exceeding entry count', () => {
    const { db } = createDb();
    const s = new Set<string>();
    for (let i = 0; i < 129; i++) s.add(`m${i}`);
    db.set('k', 'set', 'listpack', s);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('promotes listpack to hashtable when member exceeds value size', () => {
    const { db } = createDb();
    const s = new Set(['a', 'x'.repeat(65)]);
    db.set('k', 'set', 'listpack', s);
    updateEncoding(db, 'k');
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('does not change hashtable encoding (never demotes)', () => {
    const { db } = createDb();
    const s = new Set(['1', '2']);
    db.set('k', 'set', 'hashtable', s);
    updateEncoding(db, 'k');
    // hashtable is not covered by the function — it stays as-is
    expect(db.get('k')?.encoding).toBe('hashtable');
  });
});

// --- getOrCreateSet ---

describe('getOrCreateSet', () => {
  it('creates new set for non-existing key', () => {
    const { db } = createDb();
    const result = getOrCreateSet(db, 'k');
    expect(result.error).toBeNull();
    expect(result.set).toBeInstanceOf(Set);
    expect(result.set?.size).toBe(0);
    // Key should now exist in db
    expect(db.has('k')).toBe(true);
    expect(db.get('k')?.type).toBe('set');
    expect(db.get('k')?.encoding).toBe('intset');
  });

  it('returns existing set', () => {
    const { db } = createDb();
    const original = new Set(['a', 'b']);
    db.set('k', 'set', 'listpack', original);
    const result = getOrCreateSet(db, 'k');
    expect(result.error).toBeNull();
    expect(result.set).toBe(original);
  });

  it('returns WRONGTYPE error for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'hello');
    const result = getOrCreateSet(db, 'k');
    expect(result.set).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });
});

// --- getExistingSet ---

describe('getExistingSet', () => {
  it('returns null set and null error for non-existing key', () => {
    const { db } = createDb();
    const result = getExistingSet(db, 'missing');
    expect(result.set).toBeNull();
    expect(result.error).toBeNull();
  });

  it('returns the set for existing set key', () => {
    const { db } = createDb();
    const original = new Set(['x', 'y']);
    db.set('k', 'set', 'listpack', original);
    const result = getExistingSet(db, 'k');
    expect(result.set).toBe(original);
    expect(result.error).toBeNull();
  });

  it('returns WRONGTYPE error for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    const result = getExistingSet(db, 'k');
    expect(result.set).toBeNull();
    expect(result.error).toEqual(WRONGTYPE);
  });
});

// --- collectSets ---

describe('collectSets', () => {
  it('returns sets for multiple existing set keys', () => {
    const { db } = createDb();
    const s1 = new Set(['a']);
    const s2 = new Set(['b']);
    db.set('k1', 'set', 'listpack', s1);
    db.set('k2', 'set', 'listpack', s2);
    const result = collectSets(db, ['k1', 'k2']);
    expect(result.error).toBeNull();
    expect(result.sets).toEqual([s1, s2]);
  });

  it('returns null for non-existing keys', () => {
    const { db } = createDb();
    const s1 = new Set(['a']);
    db.set('k1', 'set', 'listpack', s1);
    const result = collectSets(db, ['k1', 'missing']);
    expect(result.error).toBeNull();
    expect(result.sets).toEqual([s1, null]);
  });

  it('returns error on first WRONGTYPE key', () => {
    const { db } = createDb();
    const s1 = new Set(['a']);
    db.set('k1', 'set', 'listpack', s1);
    db.set('k2', 'string', 'raw', 'val');
    db.set('k3', 'set', 'listpack', new Set(['c']));
    const result = collectSets(db, ['k1', 'k2', 'k3']);
    expect(result.error).toEqual(WRONGTYPE);
    expect(result.sets).toEqual([]);
  });

  it('returns empty array of sets for empty keys array', () => {
    const { db } = createDb();
    const result = collectSets(db, []);
    expect(result.error).toBeNull();
    expect(result.sets).toEqual([]);
  });
});

// --- findSmallest ---

describe('findSmallest', () => {
  it('returns the smallest set', () => {
    const s1 = new Set(['a', 'b', 'c']);
    const s2 = new Set(['x']);
    const s3 = new Set(['p', 'q']);
    expect(findSmallest([s1, s2, s3])).toBe(s2);
  });

  it('returns the first set if all same size', () => {
    const s1 = new Set(['a']);
    const s2 = new Set(['b']);
    expect(findSmallest([s1, s2])).toBe(s1);
  });

  it('returns the only set in a single-element array', () => {
    const s1 = new Set(['a', 'b']);
    expect(findSmallest([s1])).toBe(s1);
  });

  it('returns an empty set when present', () => {
    const empty = new Set<string>();
    const s1 = new Set(['a']);
    expect(findSmallest([s1, empty])).toBe(empty);
  });
});

// --- computeIntersection ---

describe('computeIntersection', () => {
  it('returns intersection of two sets', () => {
    const s1 = new Set(['a', 'b', 'c']);
    const s2 = new Set(['b', 'c', 'd']);
    const result = computeIntersection([s1, s2]);
    expect(result).toEqual(new Set(['b', 'c']));
  });

  it('returns null when any set is null (non-existing key)', () => {
    const s1 = new Set(['a', 'b']);
    expect(computeIntersection([s1, null])).toBeNull();
    expect(computeIntersection([null, s1])).toBeNull();
    expect(computeIntersection([null])).toBeNull();
  });

  it('returns empty set when no common members', () => {
    const s1 = new Set(['a', 'b']);
    const s2 = new Set(['c', 'd']);
    expect(computeIntersection([s1, s2])).toEqual(new Set());
  });

  it('returns the set itself when intersecting with itself', () => {
    const s1 = new Set(['a', 'b', 'c']);
    expect(computeIntersection([s1, s1])).toEqual(new Set(['a', 'b', 'c']));
  });

  it('handles intersection of three sets', () => {
    const s1 = new Set(['a', 'b', 'c', 'd']);
    const s2 = new Set(['b', 'c', 'd', 'e']);
    const s3 = new Set(['c', 'd', 'e', 'f']);
    expect(computeIntersection([s1, s2, s3])).toEqual(new Set(['c', 'd']));
  });

  it('returns a copy when given a single non-null set', () => {
    const s1 = new Set(['a', 'b']);
    const result = computeIntersection([s1]);
    expect(result).toEqual(new Set(['a', 'b']));
    expect(result).not.toBe(s1); // should be a new Set
  });

  it('uses smallest set for iteration efficiency', () => {
    // Large set intersected with small set — result should still be correct
    const small = new Set(['a']);
    const large = new Set<string>();
    for (let i = 0; i < 1000; i++) large.add(`m${i}`);
    large.add('a');
    expect(computeIntersection([large, small])).toEqual(new Set(['a']));
  });

  it('returns empty set when intersecting with empty set', () => {
    const s1 = new Set(['a', 'b']);
    const s2 = new Set<string>();
    expect(computeIntersection([s1, s2])).toEqual(new Set());
  });
});

// --- computeDifference ---

describe('computeDifference', () => {
  it('returns difference of two sets', () => {
    const s1 = new Set(['a', 'b', 'c']);
    const s2 = new Set(['b', 'c', 'd']);
    expect(computeDifference([s1, s2])).toEqual(new Set(['a']));
  });

  it('returns empty set when first set is null', () => {
    expect(computeDifference([null])).toEqual(new Set());
    expect(computeDifference([null, new Set(['a'])])).toEqual(new Set());
  });

  it('returns first set when other sets are null', () => {
    const s1 = new Set(['a', 'b']);
    expect(computeDifference([s1, null])).toEqual(new Set(['a', 'b']));
  });

  it('returns first set when no overlap with others', () => {
    const s1 = new Set(['a', 'b']);
    const s2 = new Set(['c', 'd']);
    expect(computeDifference([s1, s2])).toEqual(new Set(['a', 'b']));
  });

  it('returns empty set when first set is subset of others', () => {
    const s1 = new Set(['a', 'b']);
    const s2 = new Set(['a', 'b', 'c']);
    expect(computeDifference([s1, s2])).toEqual(new Set());
  });

  it('handles difference with multiple sets', () => {
    const s1 = new Set(['a', 'b', 'c', 'd']);
    const s2 = new Set(['a']);
    const s3 = new Set(['c']);
    expect(computeDifference([s1, s2, s3])).toEqual(new Set(['b', 'd']));
  });

  it('returns copy of first set when only one set given', () => {
    const s1 = new Set(['a', 'b']);
    const result = computeDifference([s1]);
    expect(result).toEqual(new Set(['a', 'b']));
    expect(result).not.toBe(s1);
  });

  it('returns empty set when first set is empty', () => {
    const s1 = new Set<string>();
    const s2 = new Set(['a']);
    expect(computeDifference([s1, s2])).toEqual(new Set());
  });
});

// --- storeSetResult ---

describe('storeSetResult', () => {
  it('stores non-empty set and returns its size', () => {
    const { db } = createDb();
    const members = new Set(['a', 'b', 'c']);
    const result = storeSetResult(db, 'dest', members);
    expect(result).toEqual(integer(3));
    expect(db.has('dest')).toBe(true);
    expect(db.get('dest')?.type).toBe('set');
    expect(db.get('dest')?.value).toBe(members);
  });

  it('deletes key and returns 0 for empty set', () => {
    const { db } = createDb();
    // Pre-populate destination
    db.set('dest', 'set', 'listpack', new Set(['old']));
    const result = storeSetResult(db, 'dest', new Set());
    expect(result).toEqual(ZERO);
    expect(db.has('dest')).toBe(false);
  });

  it('chooses intset encoding for all-integer set', () => {
    const { db } = createDb();
    storeSetResult(db, 'dest', new Set(['1', '2', '3']));
    expect(db.get('dest')?.encoding).toBe('intset');
  });

  it('chooses listpack encoding for non-integer small set', () => {
    const { db } = createDb();
    storeSetResult(db, 'dest', new Set(['a', 'b']));
    expect(db.get('dest')?.encoding).toBe('listpack');
  });

  it('chooses hashtable encoding for large set', () => {
    const { db } = createDb();
    const s = new Set<string>();
    for (let i = 0; i < 129; i++) s.add(`m${i}`);
    storeSetResult(db, 'dest', s);
    expect(db.get('dest')?.encoding).toBe('hashtable');
  });

  it('overwrites existing key of different type', () => {
    const { db } = createDb();
    db.set('dest', 'string', 'raw', 'hello');
    storeSetResult(db, 'dest', new Set(['a']));
    expect(db.get('dest')?.type).toBe('set');
  });

  it('removes expiry on the destination key', () => {
    const { db } = createDb();
    db.set('dest', 'set', 'listpack', new Set(['old']));
    db.setExpiry('dest', 5000);
    storeSetResult(db, 'dest', new Set(['a']));
    expect(db.getExpiry('dest')).toBeUndefined();
  });
});

// --- DEFAULT_MAX_INTSET_ENTRIES ---

describe('DEFAULT_MAX_INTSET_ENTRIES', () => {
  it('equals 512', () => {
    expect(DEFAULT_MAX_INTSET_ENTRIES).toBe(512);
  });
});

// --- set config thresholds ---

describe('set config thresholds', () => {
  it('CONFIG SET set-max-listpack-entries lowers threshold', () => {
    const { db } = createDb();
    const config = new ConfigStore();
    config.set('set-max-listpack-entries', '2');

    sadd(db, ['myset', 'a', 'b', 'c'], config);

    expect(db.get('myset')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET set-max-listpack-entries raises threshold', () => {
    const { db } = createDb();
    const config = new ConfigStore();
    config.set('set-max-listpack-entries', '256');

    const args = ['myset'];
    for (let i = 0; i < 200; i++) args.push(`m${i}`);
    sadd(db, args, config);

    expect(db.get('myset')?.encoding).toBe('listpack');
  });

  it('CONFIG SET set-max-listpack-value lowers threshold', () => {
    const { db } = createDb();
    const config = new ConfigStore();
    config.set('set-max-listpack-value', '3');

    sadd(db, ['myset', 'longmember'], config);

    expect(db.get('myset')?.encoding).toBe('hashtable');
  });

  it('CONFIG SET set-max-intset-entries lowers threshold', () => {
    const { db } = createDb();
    const config = new ConfigStore();
    config.set('set-max-intset-entries', '3');

    sadd(db, ['myset', '1', '2', '3', '4'], config);

    expect(db.get('myset')?.encoding).toBe('listpack');
  });

  it('CONFIG SET set-max-intset-entries raises threshold', () => {
    const { db } = createDb();
    const config = new ConfigStore();
    config.set('set-max-intset-entries', '1000');

    const args = ['myset'];
    for (let i = 0; i < 600; i++) args.push(String(i));
    sadd(db, args, config);

    expect(db.get('myset')?.encoding).toBe('intset');
  });

  it('chooseInitialEncoding respects config thresholds', () => {
    const config = new ConfigStore();
    config.set('set-max-intset-entries', '2');
    config.set('set-max-listpack-entries', '5');

    const s = new Set(['1', '2', '3']);
    expect(chooseInitialEncoding(s, config)).toBe('listpack');
  });

  it('updateEncoding promotes intset with config', () => {
    const { db } = createDb();
    const config = new ConfigStore();
    config.set('set-max-intset-entries', '2');

    const s = new Set(['1', '2', '3']);
    db.set('k', 'set', 'intset', s);

    updateEncoding(db, 'k', config);
    expect(db.get('k')?.encoding).toBe('listpack');
  });
});
