import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as set from './set.ts';

function createDb(): { db: Database; engine: RedisEngine } {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return { db: engine.db(0), engine };
}

function integer(value: number): Reply {
  return { kind: 'integer', value };
}

function arr(...items: Reply[]): Reply {
  return { kind: 'array', value: items };
}

const ZERO = integer(0);
const ONE = integer(1);
const EMPTY_ARRAY: Reply = { kind: 'array', value: [] };
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- SADD ---

describe('SADD', () => {
  it('creates set with single member', () => {
    const { db } = createDb();
    expect(set.sadd(db, ['k', 'a'])).toEqual(ONE);
  });

  it('creates set with multiple members', () => {
    const { db } = createDb();
    expect(set.sadd(db, ['k', 'a', 'b', 'c'])).toEqual(integer(3));
  });

  it('returns count of new members only (not duplicates)', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    expect(set.sadd(db, ['k', 'a', 'c'])).toEqual(ONE);
  });

  it('ignores duplicate members in single call', () => {
    const { db } = createDb();
    expect(set.sadd(db, ['k', 'a', 'a', 'a'])).toEqual(ONE);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.sadd(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns error for wrong arity', () => {
    const { db } = createDb();
    expect(set.sadd(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'sadd' command",
    });
  });
});

// --- SREM ---

describe('SREM', () => {
  it('removes existing member', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    expect(set.srem(db, ['k', 'a'])).toEqual(ONE);
  });

  it('returns 0 for non-existing member', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.srem(db, ['k', 'x'])).toEqual(ZERO);
  });

  it('removes multiple members', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    expect(set.srem(db, ['k', 'a', 'b', 'x'])).toEqual(integer(2));
  });

  it('deletes key when set becomes empty', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a']);
    set.srem(db, ['k', 'a']);
    expect(db.has('k')).toBe(false);
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(set.srem(db, ['k', 'a'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.srem(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns error for wrong arity', () => {
    const { db } = createDb();
    expect(set.srem(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'srem' command",
    });
  });
});

// --- SISMEMBER ---

describe('SISMEMBER', () => {
  it('returns 1 for existing member', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.sismember(db, ['k', 'a'])).toEqual(ONE);
  });

  it('returns 0 for non-existing member', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.sismember(db, ['k', 'b'])).toEqual(ZERO);
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(set.sismember(db, ['k', 'a'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.sismember(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- SMISMEMBER ---

describe('SMISMEMBER', () => {
  it('returns array of 1/0 for each member', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    expect(set.smismember(db, ['k', 'a', 'c', 'b'])).toEqual(
      arr(ONE, ZERO, ONE)
    );
  });

  it('returns all zeros for non-existing key', () => {
    const { db } = createDb();
    expect(set.smismember(db, ['k', 'a', 'b'])).toEqual(arr(ZERO, ZERO));
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.smismember(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns error for wrong arity', () => {
    const { db } = createDb();
    expect(set.smismember(db, ['k'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "wrong number of arguments for 'smismember' command",
    });
  });
});

// --- SMEMBERS ---

describe('SMEMBERS', () => {
  it('returns all members', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    const result = set.smembers(db, ['k']);
    expect(result).toEqual(
      expect.objectContaining({
        kind: 'array',
      })
    );
    // Members are unordered, so sort for comparison
    const members = (result as { kind: 'array'; value: Reply[] }).value.map(
      (r) => (r as { kind: 'bulk'; value: string }).value
    );
    expect(members.sort()).toEqual(['a', 'b']);
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(set.smembers(db, ['k'])).toEqual(EMPTY_ARRAY);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.smembers(db, ['k'])).toEqual(WRONGTYPE);
  });
});

// --- SCARD ---

describe('SCARD', () => {
  it('returns set cardinality', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    expect(set.scard(db, ['k'])).toEqual(integer(3));
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(set.scard(db, ['k'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.scard(db, ['k'])).toEqual(WRONGTYPE);
  });
});

// --- SMOVE ---

describe('SMOVE', () => {
  it('moves member from source to destination', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a', 'b']);
    set.sadd(db, ['dst', 'c']);
    expect(set.smove(db, ['src', 'dst', 'a'])).toEqual(ONE);
    expect(set.sismember(db, ['src', 'a'])).toEqual(ZERO);
    expect(set.sismember(db, ['dst', 'a'])).toEqual(ONE);
  });

  it('returns 0 when member not in source', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a']);
    set.sadd(db, ['dst', 'c']);
    expect(set.smove(db, ['src', 'dst', 'x'])).toEqual(ZERO);
  });

  it('returns 0 when source does not exist', () => {
    const { db } = createDb();
    expect(set.smove(db, ['src', 'dst', 'a'])).toEqual(ZERO);
  });

  it('creates destination if it does not exist', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a', 'b']);
    expect(set.smove(db, ['src', 'dst', 'a'])).toEqual(ONE);
    expect(set.sismember(db, ['dst', 'a'])).toEqual(ONE);
    expect(set.scard(db, ['dst'])).toEqual(ONE);
  });

  it('deletes source when it becomes empty', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a']);
    set.smove(db, ['src', 'dst', 'a']);
    expect(db.has('src')).toBe(false);
  });

  it('returns WRONGTYPE for non-set source', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'val');
    expect(set.smove(db, ['src', 'dst', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns WRONGTYPE for non-set destination when member exists in source', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a']);
    db.set('dst', 'string', 'raw', 'val');
    expect(set.smove(db, ['src', 'dst', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns 0 when member absent from source even if destination is wrong type', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a']);
    db.set('dst', 'string', 'raw', 'val');
    expect(set.smove(db, ['src', 'dst', 'x'])).toEqual(ZERO);
  });

  it('handles move to same key (member already exists)', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    expect(set.smove(db, ['k', 'k', 'a'])).toEqual(ONE);
    // Member should still exist
    expect(set.sismember(db, ['k', 'a'])).toEqual(ONE);
    expect(set.scard(db, ['k'])).toEqual(integer(2));
  });

  it('moves member that already exists in destination (no-op for dst)', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'a', 'b']);
    set.sadd(db, ['dst', 'a', 'c']);
    expect(set.smove(db, ['src', 'dst', 'a'])).toEqual(ONE);
    expect(set.sismember(db, ['src', 'a'])).toEqual(ZERO);
    expect(set.scard(db, ['src'])).toEqual(ONE);
    expect(set.scard(db, ['dst'])).toEqual(integer(2));
  });
});

// --- Encoding transitions ---

describe('encoding transitions', () => {
  it('uses intset for integer-only members', () => {
    const { db } = createDb();
    set.sadd(db, ['k', '1', '2', '3']);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('intset');
  });

  it('uses listpack for non-integer members', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'hello', 'world']);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
  });

  it('transitions from intset to listpack when non-integer added', () => {
    const { db } = createDb();
    set.sadd(db, ['k', '1', '2', '3']);
    expect(db.get('k')?.encoding).toBe('intset');
    set.sadd(db, ['k', 'hello']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('transitions from intset to hashtable when exceeding intset entries with large values', () => {
    const { db } = createDb();
    // Add 513 integers (exceeds default 512)
    const args: string[] = ['k'];
    for (let i = 0; i < 513; i++) {
      args.push(String(i));
    }
    set.sadd(db, args);
    // 513 > 512 (intset limit), but all integers; 513 > 128 (listpack limit)
    // Should be hashtable since count exceeds both intset AND listpack limits
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('transitions from intset to listpack when exceeding intset entries but within listpack', () => {
    const { db } = createDb();
    // Need to exceed intset limit but stay within listpack limit
    // Default intset limit is 512, listpack limit is 128
    // So intset limit (512) > listpack limit (128), which means
    // when intset entries are exceeded, we always exceed listpack too → hashtable
    // But if we add a non-integer to a set with ≤128 members, we get listpack
    const args: string[] = ['k'];
    for (let i = 0; i < 50; i++) {
      args.push(String(i));
    }
    set.sadd(db, args);
    expect(db.get('k')?.encoding).toBe('intset');
    set.sadd(db, ['k', 'hello']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('transitions from listpack to hashtable when exceeding entry count', () => {
    const { db } = createDb();
    // Default threshold is 128 entries for listpack
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      args.push(`member${i}`);
    }
    set.sadd(db, args);
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('transitions from listpack to hashtable when member exceeds value size', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65); // > 64 bytes
    set.sadd(db, ['k', longValue]);
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('stays listpack at exact entry count threshold', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i < 128; i++) {
      args.push(`m${i}`);
    }
    set.sadd(db, args);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('stays listpack at exact value size threshold', () => {
    const { db } = createDb();
    const exactValue = 'x'.repeat(64); // exactly 64 bytes
    set.sadd(db, ['k', exactValue]);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('stays intset at exact intset entry threshold', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i < 512; i++) {
      args.push(String(i));
    }
    set.sadd(db, args);
    expect(db.get('k')?.encoding).toBe('intset');
  });

  it('never demotes encoding after SREM reduces size', () => {
    const { db } = createDb();
    // Create a set that triggers hashtable
    const args: string[] = ['k'];
    for (let i = 0; i < 129; i++) {
      args.push(`m${i}`);
    }
    set.sadd(db, args);
    expect(db.get('k')?.encoding).toBe('hashtable');
    // Remove members to get back below threshold
    set.srem(db, ['k', 'm0']);
    expect(db.get('k')?.encoding).toBe('hashtable');
  });

  it('never demotes from listpack to intset after removing non-integer members', () => {
    const { db } = createDb();
    set.sadd(db, ['k', '1', '2', 'hello']);
    expect(db.get('k')?.encoding).toBe('listpack');
    set.srem(db, ['k', 'hello']);
    // Even though remaining members are all integers, encoding stays listpack
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('handles negative integers in intset', () => {
    const { db } = createDb();
    set.sadd(db, ['k', '-1', '-100', '0', '42']);
    expect(db.get('k')?.encoding).toBe('intset');
  });

  it('treats leading-zero numbers as non-integer', () => {
    const { db } = createDb();
    set.sadd(db, ['k', '007']);
    // "007" is not a canonical integer representation
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('handles mixed integer and non-integer in single SADD', () => {
    const { db } = createDb();
    set.sadd(db, ['k', '1', '2', 'abc']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('updates encoding on SMOVE destination', () => {
    const { db } = createDb();
    set.sadd(db, ['src', 'hello']);
    set.sadd(db, ['dst', '1', '2']);
    expect(db.get('dst')?.encoding).toBe('intset');
    set.smove(db, ['src', 'dst', 'hello']);
    expect(db.get('dst')?.encoding).toBe('listpack');
  });
});
