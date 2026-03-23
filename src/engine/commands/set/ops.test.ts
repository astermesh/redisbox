import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  sadd,
  smembers,
  sunion,
  sinter,
  sdiff,
  sunionstore,
  sinterstore,
  sdiffstore,
  sintercard,
} from './index.ts';

function createDb(): { db: Database; engine: RedisEngine } {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return { db: engine.db(0), engine };
}

function integer(value: number): Reply {
  return { kind: 'integer', value };
}

const ZERO = integer(0);
const ONE = integer(1);
const EMPTY_ARRAY: Reply = { kind: 'array', value: [] };
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};
const NOT_INTEGER_ERR: Reply = {
  kind: 'error',
  prefix: 'ERR',
  message: 'value is not an integer or out of range',
};

function extractMembers(reply: Reply): string[] {
  if (reply.kind !== 'array') return [];
  return (reply as { kind: 'array'; value: Reply[] }).value.map(
    (r) => (r as { kind: 'bulk'; value: string }).value
  );
}

// --- SUNION ---

describe('SUNION', () => {
  it('returns union of two sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    const result = sunion(db, ['s1', 's2']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b', 'c', 'd']);
  });

  it('returns union of three sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    sadd(db, ['s2', 'b']);
    sadd(db, ['s3', 'c']);
    const result = sunion(db, ['s1', 's2', 's3']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b', 'c']);
  });

  it('treats non-existing key as empty set', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    const result = sunion(db, ['s1', 'nokey']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b']);
  });

  it('returns empty array when all keys missing', () => {
    const { db } = createDb();
    expect(sunion(db, ['nokey1', 'nokey2'])).toEqual(EMPTY_ARRAY);
  });

  it('returns single set members for single key', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    const result = sunion(db, ['s1']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b']);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    sadd(db, ['s1', 'a']);
    expect(sunion(db, ['s1', 'k'])).toEqual(WRONGTYPE);
  });
});

// --- SINTER ---

describe('SINTER', () => {
  it('returns intersection of two sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    const result = sinter(db, ['s1', 's2']);
    expect(extractMembers(result).sort()).toEqual(['b', 'c']);
  });

  it('returns intersection of three sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    sadd(db, ['s3', 'c', 'd', 'e']);
    const result = sinter(db, ['s1', 's2', 's3']);
    expect(extractMembers(result).sort()).toEqual(['c']);
  });

  it('returns empty array when non-existing key in intersection', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    const result = sinter(db, ['s1', 'nokey']);
    expect(result).toEqual(EMPTY_ARRAY);
  });

  it('returns empty array when no common members', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    sadd(db, ['s2', 'b']);
    const result = sinter(db, ['s1', 's2']);
    expect(result).toEqual(EMPTY_ARRAY);
  });

  it('returns single set members for single key', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    const result = sinter(db, ['s1']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b']);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    sadd(db, ['s1', 'a']);
    expect(sinter(db, ['k', 's1'])).toEqual(WRONGTYPE);
  });
});

// --- SDIFF ---

describe('SDIFF', () => {
  it('returns difference of two sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    const result = sdiff(db, ['s1', 's2']);
    expect(extractMembers(result).sort()).toEqual(['a']);
  });

  it('returns difference with multiple sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c', 'd']);
    sadd(db, ['s2', 'b']);
    sadd(db, ['s3', 'c']);
    const result = sdiff(db, ['s1', 's2', 's3']);
    expect(extractMembers(result).sort()).toEqual(['a', 'd']);
  });

  it('treats non-existing key as empty set', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    const result = sdiff(db, ['s1', 'nokey']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b']);
  });

  it('returns empty when first key does not exist', () => {
    const { db } = createDb();
    sadd(db, ['s2', 'a']);
    const result = sdiff(db, ['nokey', 's2']);
    expect(result).toEqual(EMPTY_ARRAY);
  });

  it('returns all members when diffed with empty', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    const result = sdiff(db, ['s1']);
    expect(extractMembers(result).sort()).toEqual(['a', 'b']);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    sadd(db, ['s1', 'a']);
    expect(sdiff(db, ['s1', 'k'])).toEqual(WRONGTYPE);
  });

  it('returns WRONGTYPE when first key is wrong type', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    sadd(db, ['s1', 'a']);
    expect(sdiff(db, ['k', 's1'])).toEqual(WRONGTYPE);
  });
});

// --- SUNIONSTORE ---

describe('SUNIONSTORE', () => {
  it('stores union and returns cardinality', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    sadd(db, ['s2', 'b', 'c']);
    expect(sunionstore(db, ['dst', 's1', 's2'])).toEqual(integer(3));
    expect(extractMembers(smembers(db, ['dst'])).sort()).toEqual([
      'a',
      'b',
      'c',
    ]);
  });

  it('overwrites existing destination key', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'x', 'y', 'z']);
    sadd(db, ['s1', 'a']);
    expect(sunionstore(db, ['dst', 's1'])).toEqual(ONE);
    expect(extractMembers(smembers(db, ['dst']))).toEqual(['a']);
  });

  it('deletes destination when result is empty', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'x']);
    expect(sunionstore(db, ['dst', 'nokey'])).toEqual(ZERO);
    expect(db.has('dst')).toBe(false);
  });

  it('returns WRONGTYPE for non-set source', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(sunionstore(db, ['dst', 'k'])).toEqual(WRONGTYPE);
  });

  it('destination can be same as source', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    sadd(db, ['s2', 'c']);
    expect(sunionstore(db, ['s1', 's1', 's2'])).toEqual(integer(3));
    expect(extractMembers(smembers(db, ['s1'])).sort()).toEqual([
      'a',
      'b',
      'c',
    ]);
  });

  it('removes expiry on destination', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'old']);
    db.setExpiry('dst', 9999);
    sadd(db, ['s1', 'a']);
    sunionstore(db, ['dst', 's1']);
    expect(db.getExpiry('dst')).toBeUndefined();
  });
});

// --- SINTERSTORE ---

describe('SINTERSTORE', () => {
  it('stores intersection and returns cardinality', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    expect(sinterstore(db, ['dst', 's1', 's2'])).toEqual(integer(2));
    expect(extractMembers(smembers(db, ['dst'])).sort()).toEqual(['b', 'c']);
  });

  it('deletes destination when result is empty', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'x']);
    sadd(db, ['s1', 'a']);
    expect(sinterstore(db, ['dst', 's1', 'nokey'])).toEqual(ZERO);
    expect(db.has('dst')).toBe(false);
  });

  it('overwrites existing destination', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'x', 'y']);
    sadd(db, ['s1', 'a', 'b']);
    expect(sinterstore(db, ['dst', 's1'])).toEqual(integer(2));
    expect(extractMembers(smembers(db, ['dst'])).sort()).toEqual(['a', 'b']);
  });

  it('returns WRONGTYPE for non-set source', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(sinterstore(db, ['dst', 'k'])).toEqual(WRONGTYPE);
  });
});

// --- SDIFFSTORE ---

describe('SDIFFSTORE', () => {
  it('stores difference and returns cardinality', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    expect(sdiffstore(db, ['dst', 's1', 's2'])).toEqual(ONE);
    expect(extractMembers(smembers(db, ['dst']))).toEqual(['a']);
  });

  it('deletes destination when result is empty', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'x']);
    sadd(db, ['s1', 'a']);
    sadd(db, ['s2', 'a']);
    expect(sdiffstore(db, ['dst', 's1', 's2'])).toEqual(ZERO);
    expect(db.has('dst')).toBe(false);
  });

  it('overwrites existing destination', () => {
    const { db } = createDb();
    sadd(db, ['dst', 'x', 'y']);
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b']);
    expect(sdiffstore(db, ['dst', 's1', 's2'])).toEqual(integer(2));
    expect(extractMembers(smembers(db, ['dst'])).sort()).toEqual(['a', 'c']);
  });

  it('returns WRONGTYPE for non-set source', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(sdiffstore(db, ['dst', 'k'])).toEqual(WRONGTYPE);
  });

  it('destination can be same as source', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b']);
    expect(sdiffstore(db, ['s1', 's1', 's2'])).toEqual(integer(2));
    expect(extractMembers(smembers(db, ['s1'])).sort()).toEqual(['a', 'c']);
  });
});

// --- SINTERCARD ---

describe('SINTERCARD', () => {
  it('returns cardinality of intersection', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    expect(sintercard(db, ['2', 's1', 's2'])).toEqual(integer(2));
  });

  it('returns 0 when no common members', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    sadd(db, ['s2', 'b']);
    expect(sintercard(db, ['2', 's1', 's2'])).toEqual(ZERO);
  });

  it('returns 0 when key does not exist', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    expect(sintercard(db, ['2', 's1', 'nokey'])).toEqual(ZERO);
  });

  it('respects LIMIT option', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c', 'd']);
    sadd(db, ['s2', 'a', 'b', 'c', 'd']);
    expect(sintercard(db, ['2', 's1', 's2', 'LIMIT', '2'])).toEqual(integer(2));
  });

  it('LIMIT 0 means no limit', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'a', 'b', 'c']);
    expect(sintercard(db, ['2', 's1', 's2', 'LIMIT', '0'])).toEqual(integer(3));
  });

  it('LIMIT larger than intersection returns full count', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b']);
    sadd(db, ['s2', 'a', 'b']);
    expect(sintercard(db, ['2', 's1', 's2', 'LIMIT', '100'])).toEqual(
      integer(2)
    );
  });

  it('returns error for negative LIMIT', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    sadd(db, ['s2', 'a']);
    const result = sintercard(db, ['2', 's1', 's2', 'LIMIT', '-1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "LIMIT can't be negative",
    });
  });

  it('returns error for non-integer numkeys', () => {
    const { db } = createDb();
    const result = sintercard(db, ['abc', 's1']);
    expect(result).toEqual(NOT_INTEGER_ERR);
  });

  it('returns error for numkeys 0', () => {
    const { db } = createDb();
    const result = sintercard(db, ['0']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'numkeys should be greater than 0',
    });
  });

  it('returns error for negative numkeys', () => {
    const { db } = createDb();
    const result = sintercard(db, ['-1', 's1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'numkeys should be greater than 0',
    });
  });

  it('returns error for wrong number of keys', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    // numkeys=2 but only 1 key provided, with no LIMIT
    const result = sintercard(db, ['2', 's1']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "Number of keys can't be greater than number of args",
    });
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(sintercard(db, ['1', 'k'])).toEqual(WRONGTYPE);
  });

  it('works with three sets', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a', 'b', 'c']);
    sadd(db, ['s2', 'b', 'c', 'd']);
    sadd(db, ['s3', 'c', 'd', 'e']);
    expect(sintercard(db, ['3', 's1', 's2', 's3'])).toEqual(ONE);
  });

  it('returns syntax error for unknown option after keys', () => {
    const { db } = createDb();
    sadd(db, ['s1', 'a']);
    const result = sintercard(db, ['1', 's1', 'BADOPT', '5']);
    expect(result).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});
