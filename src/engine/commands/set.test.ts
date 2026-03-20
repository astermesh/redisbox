import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as set from './set.ts';

let rngValue = 0.5;
function createDb(): { db: Database; engine: RedisEngine } {
  rngValue = 0.5;
  const engine = new RedisEngine({ clock: () => 1000, rng: () => rngValue });
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
const NIL: Reply = { kind: 'bulk', value: null };
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

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

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

// --- SRANDMEMBER ---

describe('SRANDMEMBER', () => {
  it('returns nil for non-existing key (no count)', () => {
    const { db, engine } = createDb();
    expect(set.srandmember(db, ['k'], engine.rng)).toEqual(NIL);
  });

  it('returns a single member (no count)', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    const result = set.srandmember(db, ['k'], engine.rng);
    expect(result.kind).toBe('bulk');
    const val = (result as { kind: 'bulk'; value: string }).value;
    expect(['a', 'b', 'c']).toContain(val);
  });

  it('returns empty array for non-existing key with count', () => {
    const { db, engine } = createDb();
    expect(set.srandmember(db, ['k', '3'], engine.rng)).toEqual(EMPTY_ARRAY);
  });

  it('returns empty array for count 0', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    expect(set.srandmember(db, ['k', '0'], engine.rng)).toEqual(EMPTY_ARRAY);
  });

  it('positive count: returns unique elements up to set size', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    const result = set.srandmember(db, ['k', '2'], engine.rng);
    expect(result.kind).toBe('array');
    const items = (result as { kind: 'array'; value: Reply[] }).value;
    expect(items.length).toBe(2);
    // Elements must be unique
    const vals = items.map((r) => (r as { kind: 'bulk'; value: string }).value);
    expect(new Set(vals).size).toBe(2);
    for (const v of vals) {
      expect(['a', 'b', 'c']).toContain(v);
    }
  });

  it('positive count > set size: returns all elements', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    const result = set.srandmember(db, ['k', '10'], engine.rng);
    const items = (result as { kind: 'array'; value: Reply[] }).value;
    expect(items.length).toBe(2);
  });

  it('negative count: may return duplicates', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a']);
    const result = set.srandmember(db, ['k', '-3'], engine.rng);
    const items = (result as { kind: 'array'; value: Reply[] }).value;
    expect(items.length).toBe(3);
    // All should be 'a' since there's only one member
    for (const item of items) {
      expect(item).toEqual(bulk('a'));
    }
  });

  it('negative count: returns |count| elements', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    const result = set.srandmember(db, ['k', '-5'], engine.rng);
    const items = (result as { kind: 'array'; value: Reply[] }).value;
    expect(items.length).toBe(5);
  });

  it('does not modify the set', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    set.srandmember(db, ['k', '2'], engine.rng);
    expect(set.scard(db, ['k'])).toEqual(integer(3));
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db, engine } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.srandmember(db, ['k'], engine.rng)).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer count', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.srandmember(db, ['k', 'abc'], engine.rng)).toEqual(
      NOT_INTEGER_ERR
    );
  });
});

// --- SPOP ---

describe('SPOP', () => {
  it('returns nil for non-existing key (no count)', () => {
    const { db, engine } = createDb();
    expect(set.spop(db, ['k'], engine.rng)).toEqual(NIL);
  });

  it('pops a single member and removes it (no count)', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    const result = set.spop(db, ['k'], engine.rng);
    expect(result.kind).toBe('bulk');
    const val = (result as { kind: 'bulk'; value: string }).value;
    expect(['a', 'b', 'c']).toContain(val);
    expect(set.scard(db, ['k'])).toEqual(integer(2));
    expect(set.sismember(db, ['k', val])).toEqual(ZERO);
  });

  it('deletes key when last member is popped', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a']);
    set.spop(db, ['k'], engine.rng);
    expect(db.has('k')).toBe(false);
  });

  it('returns empty array for non-existing key with count', () => {
    const { db, engine } = createDb();
    expect(set.spop(db, ['k', '3'], engine.rng)).toEqual(EMPTY_ARRAY);
  });

  it('returns empty array for count 0', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.spop(db, ['k', '0'], engine.rng)).toEqual(EMPTY_ARRAY);
  });

  it('pops count members and removes them', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c', 'd']);
    const result = set.spop(db, ['k', '2'], engine.rng);
    const items = (result as { kind: 'array'; value: Reply[] }).value;
    expect(items.length).toBe(2);
    // All popped members should be unique
    const vals = items.map((r) => (r as { kind: 'bulk'; value: string }).value);
    expect(new Set(vals).size).toBe(2);
    expect(set.scard(db, ['k'])).toEqual(integer(2));
    for (const v of vals) {
      expect(set.sismember(db, ['k', v])).toEqual(ZERO);
    }
  });

  it('count > set size: pops all members', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a', 'b']);
    const result = set.spop(db, ['k', '10'], engine.rng);
    const items = (result as { kind: 'array'; value: Reply[] }).value;
    expect(items.length).toBe(2);
    expect(db.has('k')).toBe(false);
  });

  it('returns error for negative count', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.spop(db, ['k', '-1'], engine.rng)).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'index out of range',
    });
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db, engine } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.spop(db, ['k'], engine.rng)).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer count', () => {
    const { db, engine } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.spop(db, ['k', 'abc'], engine.rng)).toEqual(NOT_INTEGER_ERR);
  });
});

// --- SSCAN ---

describe('SSCAN', () => {
  it('returns cursor 0 and empty array for non-existing key', () => {
    const { db } = createDb();
    expect(set.sscan(db, ['k', '0'])).toEqual(arr(bulk('0'), EMPTY_ARRAY));
  });

  it('scans all members of a small set', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    const result = set.sscan(db, ['k', '0']);
    expect(result.kind).toBe('array');
    const outer = (result as { kind: 'array'; value: Reply[] }).value;
    // Cursor should be 0 (complete scan)
    expect(outer[0]).toEqual(bulk('0'));
    // Members array
    const members = (outer[1] as { kind: 'array'; value: Reply[] }).value;
    expect(members.length).toBe(3);
    const vals = members.map(
      (r) => (r as { kind: 'bulk'; value: string }).value
    );
    expect(vals.sort()).toEqual(['a', 'b', 'c']);
  });

  it('supports cursor-based iteration with COUNT', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c', 'd', 'e']);
    // First scan with COUNT 2
    const result1 = set.sscan(db, ['k', '0', 'COUNT', '2']);
    const outer1 = (result1 as { kind: 'array'; value: Reply[] }).value;
    const cursor1 = (outer1[0] as { kind: 'bulk'; value: string }).value;
    expect(cursor1).not.toBe('0'); // Not done yet
    const members1 = (outer1[1] as { kind: 'array'; value: Reply[] }).value;
    expect(members1.length).toBe(2);

    // Continue scanning
    const result2 = set.sscan(db, ['k', cursor1, 'COUNT', '2']);
    const outer2 = (result2 as { kind: 'array'; value: Reply[] }).value;
    const cursor2 = (outer2[0] as { kind: 'bulk'; value: string }).value;
    const members2 = (outer2[1] as { kind: 'array'; value: Reply[] }).value;
    expect(members2.length).toBe(2);

    // Final scan
    const result3 = set.sscan(db, ['k', cursor2, 'COUNT', '2']);
    const outer3 = (result3 as { kind: 'array'; value: Reply[] }).value;
    expect(outer3[0]).toEqual(bulk('0')); // Done
    const members3 = (outer3[1] as { kind: 'array'; value: Reply[] }).value;
    expect(members3.length).toBe(1);

    // All members should be found
    const all = [...members1, ...members2, ...members3].map(
      (r) => (r as { kind: 'bulk'; value: string }).value
    );
    expect(all.sort()).toEqual(['a', 'b', 'c', 'd', 'e']);
  });

  it('supports MATCH pattern', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'apple', 'banana', 'apricot', 'cherry']);
    const result = set.sscan(db, ['k', '0', 'MATCH', 'ap*']);
    const outer = (result as { kind: 'array'; value: Reply[] }).value;
    const members = (outer[1] as { kind: 'array'; value: Reply[] }).value;
    const vals = members.map(
      (r) => (r as { kind: 'bulk'; value: string }).value
    );
    expect(vals.sort()).toEqual(['apple', 'apricot']);
  });

  it('returns WRONGTYPE for non-set key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(set.sscan(db, ['k', '0'])).toEqual(WRONGTYPE);
  });

  it('returns error for invalid cursor', () => {
    const { db } = createDb();
    expect(set.sscan(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'invalid cursor',
    });
  });

  it('returns syntax error for unknown option', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a']);
    expect(set.sscan(db, ['k', '0', 'BADOPT'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns empty results for MATCH with no matches', () => {
    const { db } = createDb();
    set.sadd(db, ['k', 'a', 'b', 'c']);
    const result = set.sscan(db, ['k', '0', 'MATCH', 'z*']);
    const outer = (result as { kind: 'array'; value: Reply[] }).value;
    expect(outer[0]).toEqual(bulk('0'));
    const members = (outer[1] as { kind: 'array'; value: Reply[] }).value;
    expect(members.length).toBe(0);
  });
});
