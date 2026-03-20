import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as list from './list.ts';

function createDb(): { db: Database; engine: RedisEngine } {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  return { db: engine.db(0), engine };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function integer(value: number): Reply {
  return { kind: 'integer', value };
}

function arr(...items: Reply[]): Reply {
  return { kind: 'array', value: items };
}

function status(value: string): Reply {
  return { kind: 'status', value };
}

const NIL = bulk(null);
const ZERO = integer(0);
const ONE = integer(1);
const OK: Reply = status('OK');
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

/** Helper: create a list [a, b, c, d, e] */
function makeList(db: Database): void {
  list.rpush(db, ['k', 'a', 'b', 'c', 'd', 'e']);
}

// --- LPUSH ---

describe('LPUSH', () => {
  it('creates list with single element', () => {
    const { db } = createDb();
    expect(list.lpush(db, ['k', 'a'])).toEqual(ONE);
  });

  it('creates list with multiple elements', () => {
    const { db } = createDb();
    expect(list.lpush(db, ['k', 'a', 'b', 'c'])).toEqual(integer(3));
  });

  it('prepends elements to existing list', () => {
    const { db } = createDb();
    list.lpush(db, ['k', 'a']);
    expect(list.lpush(db, ['k', 'b'])).toEqual(integer(2));
  });

  it('elements are pushed left (head) in order', () => {
    const { db } = createDb();
    // LPUSH k a b c → list is [c, b, a] (each pushed to head)
    list.lpush(db, ['k', 'a', 'b', 'c']);
    // Verify order via LPOP
    expect(list.lpop(db, ['k'])).toEqual(bulk('c'));
    expect(list.lpop(db, ['k'])).toEqual(bulk('b'));
    expect(list.lpop(db, ['k'])).toEqual(bulk('a'));
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lpush(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- RPUSH ---

describe('RPUSH', () => {
  it('creates list with single element', () => {
    const { db } = createDb();
    expect(list.rpush(db, ['k', 'a'])).toEqual(ONE);
  });

  it('creates list with multiple elements', () => {
    const { db } = createDb();
    expect(list.rpush(db, ['k', 'a', 'b', 'c'])).toEqual(integer(3));
  });

  it('appends elements to existing list', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpush(db, ['k', 'b'])).toEqual(integer(2));
  });

  it('elements are pushed right (tail) in order', () => {
    const { db } = createDb();
    // RPUSH k a b c → list is [a, b, c]
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(list.lpop(db, ['k'])).toEqual(bulk('a'));
    expect(list.lpop(db, ['k'])).toEqual(bulk('b'));
    expect(list.lpop(db, ['k'])).toEqual(bulk('c'));
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.rpush(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- LPUSHX ---

describe('LPUSHX', () => {
  it('pushes when key exists', () => {
    const { db } = createDb();
    list.lpush(db, ['k', 'a']);
    expect(list.lpushx(db, ['k', 'b'])).toEqual(integer(2));
  });

  it('does nothing when key does not exist', () => {
    const { db } = createDb();
    expect(list.lpushx(db, ['k', 'a'])).toEqual(ZERO);
  });

  it('supports multiple elements', () => {
    const { db } = createDb();
    list.lpush(db, ['k', 'a']);
    expect(list.lpushx(db, ['k', 'b', 'c'])).toEqual(integer(3));
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lpushx(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- RPUSHX ---

describe('RPUSHX', () => {
  it('pushes when key exists', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpushx(db, ['k', 'b'])).toEqual(integer(2));
  });

  it('does nothing when key does not exist', () => {
    const { db } = createDb();
    expect(list.rpushx(db, ['k', 'a'])).toEqual(ZERO);
  });

  it('supports multiple elements', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpushx(db, ['k', 'b', 'c'])).toEqual(integer(3));
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.rpushx(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- LPOP ---

describe('LPOP', () => {
  it('pops single element from head', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(list.lpop(db, ['k'])).toEqual(bulk('a'));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(list.lpop(db, ['k'])).toEqual(NIL);
  });

  it('deletes key when list becomes empty', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    list.lpop(db, ['k']);
    expect(db.has('k')).toBe(false);
  });

  it('pops with count argument', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'd']);
    expect(list.lpop(db, ['k', '2'])).toEqual(arr(bulk('a'), bulk('b')));
  });

  it('pops with count greater than list length', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b']);
    expect(list.lpop(db, ['k', '5'])).toEqual(arr(bulk('a'), bulk('b')));
  });

  it('pops with count 0 returns empty array', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.lpop(db, ['k', '0'])).toEqual(EMPTY_ARRAY);
  });

  it('returns nil for non-existing key with count', () => {
    const { db } = createDb();
    expect(list.lpop(db, ['k', '2'])).toEqual(NIL);
  });

  it('returns error for negative count', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.lpop(db, ['k', '-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for non-integer count', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.lpop(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for float count', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.lpop(db, ['k', '2.0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns nil for non-existing key with count 0', () => {
    const { db } = createDb();
    expect(list.lpop(db, ['k', '0'])).toEqual(NIL);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lpop(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('deletes key when count pops all elements', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b']);
    list.lpop(db, ['k', '10']);
    expect(db.has('k')).toBe(false);
  });
});

// --- RPOP ---

describe('RPOP', () => {
  it('pops single element from tail', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(list.rpop(db, ['k'])).toEqual(bulk('c'));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(list.rpop(db, ['k'])).toEqual(NIL);
  });

  it('deletes key when list becomes empty', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    list.rpop(db, ['k']);
    expect(db.has('k')).toBe(false);
  });

  it('pops with count argument', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'd']);
    expect(list.rpop(db, ['k', '2'])).toEqual(arr(bulk('d'), bulk('c')));
  });

  it('pops with count greater than list length', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b']);
    expect(list.rpop(db, ['k', '5'])).toEqual(arr(bulk('b'), bulk('a')));
  });

  it('pops with count 0 returns empty array', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpop(db, ['k', '0'])).toEqual(EMPTY_ARRAY);
  });

  it('returns nil for non-existing key with count', () => {
    const { db } = createDb();
    expect(list.rpop(db, ['k', '2'])).toEqual(NIL);
  });

  it('returns error for negative count', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpop(db, ['k', '-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for non-integer count', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpop(db, ['k', 'abc'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns error for float count', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.rpop(db, ['k', '1.5'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'value is not an integer or out of range',
    });
  });

  it('returns nil for non-existing key with count 0', () => {
    const { db } = createDb();
    expect(list.rpop(db, ['k', '0'])).toEqual(NIL);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.rpop(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('deletes key when count pops all elements', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b']);
    list.rpop(db, ['k', '10']);
    expect(db.has('k')).toBe(false);
  });
});

// --- LLEN ---

describe('LLEN', () => {
  it('returns length of list', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(list.llen(db, ['k'])).toEqual(integer(3));
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(list.llen(db, ['k'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.llen(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('reflects changes after push/pop', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(list.llen(db, ['k'])).toEqual(integer(3));
    list.lpop(db, ['k']);
    expect(list.llen(db, ['k'])).toEqual(integer(2));
    list.lpush(db, ['k', 'x', 'y']);
    expect(list.llen(db, ['k'])).toEqual(integer(4));
  });
});

// --- Encoding ---

describe('list encoding', () => {
  it('uses listpack for small lists', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
    expect(entry?.type).toBe('list');
  });

  it('transitions to quicklist when exceeding entry count', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      args.push(`v${i}`);
    }
    list.rpush(db, args);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('quicklist');
  });

  it('transitions to quicklist when element exceeds value size', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65); // > 64 bytes
    list.rpush(db, ['k', longValue]);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('quicklist');
  });

  it('stays listpack at exact threshold', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i < 128; i++) {
      args.push(`v${i}`);
    }
    list.rpush(db, args);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
  });

  it('stays listpack at exact value size threshold', () => {
    const { db } = createDb();
    const exactValue = 'x'.repeat(64); // exactly 64 bytes
    list.rpush(db, ['k', exactValue]);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
  });

  it('never demotes from quicklist back to listpack', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    list.rpush(db, ['k', 'keep', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');

    // Pop the long element — encoding stays quicklist (Redis never demotes)
    list.rpop(db, ['k']);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });
});

// --- LRANGE ---

describe('LRANGE', () => {
  it('returns full range 0 -1', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'), bulk('d'), bulk('e'))
    );
  });

  it('returns subset with positive indices', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', '1', '3'])).toEqual(
      arr(bulk('b'), bulk('c'), bulk('d'))
    );
  });

  it('returns subset with negative indices', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', '-3', '-1'])).toEqual(
      arr(bulk('c'), bulk('d'), bulk('e'))
    );
  });

  it('clamps out-of-range indices', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', '-100', '100'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'), bulk('d'), bulk('e'))
    );
  });

  it('returns empty array when start > stop', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', '3', '1'])).toEqual(EMPTY_ARRAY);
  });

  it('returns empty array for non-existing key', () => {
    const { db } = createDb();
    expect(list.lrange(db, ['nonexist', '0', '-1'])).toEqual(EMPTY_ARRAY);
  });

  it('returns single element range', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', '2', '2'])).toEqual(arr(bulk('c')));
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer indices', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrange(db, ['k', 'abc', '1'])).toEqual(NOT_INTEGER_ERR);
  });
});

// --- LINDEX ---

describe('LINDEX', () => {
  it('returns element at positive index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lindex(db, ['k', '0'])).toEqual(bulk('a'));
    expect(list.lindex(db, ['k', '2'])).toEqual(bulk('c'));
    expect(list.lindex(db, ['k', '4'])).toEqual(bulk('e'));
  });

  it('returns element at negative index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lindex(db, ['k', '-1'])).toEqual(bulk('e'));
    expect(list.lindex(db, ['k', '-5'])).toEqual(bulk('a'));
  });

  it('returns nil for out-of-range index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lindex(db, ['k', '5'])).toEqual(NIL);
    expect(list.lindex(db, ['k', '-6'])).toEqual(NIL);
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(list.lindex(db, ['nonexist', '0'])).toEqual(NIL);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lindex(db, ['k', '0'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lindex(db, ['k', 'abc'])).toEqual(NOT_INTEGER_ERR);
  });
});

// --- LSET ---

describe('LSET', () => {
  it('sets element at positive index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lset(db, ['k', '2', 'X'])).toEqual(OK);
    expect(list.lindex(db, ['k', '2'])).toEqual(bulk('X'));
  });

  it('sets element at negative index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lset(db, ['k', '-1', 'Z'])).toEqual(OK);
    expect(list.lindex(db, ['k', '-1'])).toEqual(bulk('Z'));
  });

  it('returns error for out-of-range index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lset(db, ['k', '10', 'X'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'index out of range',
    });
  });

  it('returns error for non-existing key', () => {
    const { db } = createDb();
    expect(list.lset(db, ['nonexist', '0', 'X'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'no such key',
    });
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lset(db, ['k', '0', 'X'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer index', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lset(db, ['k', 'abc', 'X'])).toEqual(NOT_INTEGER_ERR);
  });

  it('updates encoding if new value exceeds threshold', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
    const longValue = 'x'.repeat(65);
    list.lset(db, ['k', '0', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });
});

// --- LINSERT ---

describe('LINSERT', () => {
  it('inserts before pivot', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'BEFORE', 'c', 'X'])).toEqual(integer(6));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('X'), bulk('c'), bulk('d'), bulk('e'))
    );
  });

  it('inserts after pivot', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'AFTER', 'c', 'X'])).toEqual(integer(6));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'), bulk('X'), bulk('d'), bulk('e'))
    );
  });

  it('returns -1 when pivot not found', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'BEFORE', 'notfound', 'X'])).toEqual(
      integer(-1)
    );
  });

  it('returns 0 when key does not exist', () => {
    const { db } = createDb();
    expect(list.linsert(db, ['nonexist', 'BEFORE', 'x', 'y'])).toEqual(ZERO);
  });

  it('is case-insensitive for BEFORE/AFTER', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'before', 'c', 'X'])).toEqual(integer(6));
  });

  it('returns error for invalid direction', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'INVALID', 'c', 'X'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.linsert(db, ['k', 'BEFORE', 'x', 'y'])).toEqual(WRONGTYPE);
  });

  it('inserts before first element', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'BEFORE', 'a', 'Z'])).toEqual(integer(6));
    expect(list.lindex(db, ['k', '0'])).toEqual(bulk('Z'));
  });

  it('inserts after last element', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.linsert(db, ['k', 'AFTER', 'e', 'Z'])).toEqual(integer(6));
    expect(list.lindex(db, ['k', '-1'])).toEqual(bulk('Z'));
  });

  it('updates encoding after insert', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    const longValue = 'x'.repeat(65);
    list.linsert(db, ['k', 'AFTER', 'a', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });
});

// --- LREM ---

describe('LREM', () => {
  it('removes first N occurrences from head (count > 0)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'a', 'c', 'a']);
    expect(list.lrem(db, ['k', '2', 'a'])).toEqual(integer(2));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'), bulk('a'))
    );
  });

  it('removes last N occurrences from tail (count < 0)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'a', 'c', 'a']);
    expect(list.lrem(db, ['k', '-2', 'a'])).toEqual(integer(2));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'))
    );
  });

  it('removes all occurrences (count = 0)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'a', 'c', 'a']);
    expect(list.lrem(db, ['k', '0', 'a'])).toEqual(integer(3));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'))
    );
  });

  it('returns 0 when element not found', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrem(db, ['k', '1', 'notfound'])).toEqual(ZERO);
  });

  it('returns 0 for non-existing key', () => {
    const { db } = createDb();
    expect(list.lrem(db, ['nonexist', '1', 'a'])).toEqual(ZERO);
  });

  it('deletes key when all elements removed', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'a', 'a']);
    list.lrem(db, ['k', '0', 'a']);
    expect(db.has('k')).toBe(false);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lrem(db, ['k', '1', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer count', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lrem(db, ['k', 'abc', 'a'])).toEqual(NOT_INTEGER_ERR);
  });

  it('removes fewer than count when not enough occurrences', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'a']);
    expect(list.lrem(db, ['k', '5', 'a'])).toEqual(integer(2));
  });
});

// --- LTRIM ---

describe('LTRIM', () => {
  it('trims to specified range', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', '1', '3'])).toEqual(OK);
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'), bulk('d'))
    );
  });

  it('trims with negative indices', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', '-3', '-1'])).toEqual(OK);
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('c'), bulk('d'), bulk('e'))
    );
  });

  it('deletes key when start > stop', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', '3', '1'])).toEqual(OK);
    expect(db.has('k')).toBe(false);
  });

  it('deletes key when range is out of bounds', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', '10', '20'])).toEqual(OK);
    expect(db.has('k')).toBe(false);
  });

  it('clamps indices to list bounds', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', '-100', '100'])).toEqual(OK);
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'), bulk('d'), bulk('e'))
    );
  });

  it('returns OK for non-existing key', () => {
    const { db } = createDb();
    expect(list.ltrim(db, ['nonexist', '0', '1'])).toEqual(OK);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.ltrim(db, ['k', '0', '1'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer indices', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', 'abc', '1'])).toEqual(NOT_INTEGER_ERR);
  });

  it('keeps single element when start == stop', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.ltrim(db, ['k', '2', '2'])).toEqual(OK);
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(arr(bulk('c')));
  });
});

// --- LPOS ---

describe('LPOS', () => {
  it('returns position of first occurrence', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'd']);
    expect(list.lpos(db, ['k', 'b'])).toEqual(integer(1));
  });

  it('returns nil when element not found', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'notfound'])).toEqual(NIL);
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(list.lpos(db, ['nonexist', 'a'])).toEqual(NIL);
  });

  it('supports RANK option (positive)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'd']);
    // RANK 2 means second match
    expect(list.lpos(db, ['k', 'b', 'RANK', '2'])).toEqual(integer(3));
  });

  it('supports RANK option (negative)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'd']);
    // RANK -1 means first match from tail
    expect(list.lpos(db, ['k', 'b', 'RANK', '-1'])).toEqual(integer(3));
  });

  it('supports RANK -2 (second match from tail)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'd']);
    expect(list.lpos(db, ['k', 'b', 'RANK', '-2'])).toEqual(integer(1));
  });

  it('returns error for RANK 0', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'a', 'RANK', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message:
        "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the last match",
    });
  });

  it('supports COUNT option', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'b']);
    // COUNT 2 means return first 2 matches
    expect(list.lpos(db, ['k', 'b', 'COUNT', '2'])).toEqual(
      arr(integer(1), integer(3))
    );
  });

  it('supports COUNT 0 (all matches)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'b']);
    expect(list.lpos(db, ['k', 'b', 'COUNT', '0'])).toEqual(
      arr(integer(1), integer(3), integer(4))
    );
  });

  it('supports MAXLEN option', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'd']);
    // MAXLEN 3: only scan first 3 elements
    expect(list.lpos(db, ['k', 'b', 'COUNT', '0', 'MAXLEN', '3'])).toEqual(
      arr(integer(1))
    );
  });

  it('supports MAXLEN 0 (no limit, same as default)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b', 'b']);
    expect(list.lpos(db, ['k', 'b', 'COUNT', '0', 'MAXLEN', '0'])).toEqual(
      arr(integer(1), integer(3), integer(4))
    );
  });

  it('combines RANK and COUNT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'b', 'a', 'b', 'b', 'b']);
    // RANK 2 COUNT 2: skip first match, return next 2
    expect(list.lpos(db, ['k', 'b', 'RANK', '2', 'COUNT', '2'])).toEqual(
      arr(integer(2), integer(3))
    );
  });

  it('combines negative RANK and COUNT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'b', 'a', 'b', 'b', 'b']);
    // RANK -2 COUNT 2: second match from tail (index 3), then continue backwards (index 2)
    expect(list.lpos(db, ['k', 'b', 'RANK', '-2', 'COUNT', '2'])).toEqual(
      arr(integer(3), integer(2))
    );
  });

  it('returns empty array for COUNT with no match', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'notfound', 'COUNT', '0'])).toEqual(EMPTY_ARRAY);
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'val');
    expect(list.lpos(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer RANK', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'a', 'RANK', 'abc'])).toEqual(NOT_INTEGER_ERR);
  });

  it('returns error for non-integer COUNT', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'a', 'COUNT', 'abc'])).toEqual(NOT_INTEGER_ERR);
  });

  it('returns error for negative COUNT', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'a', 'COUNT', '-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "COUNT can't be negative",
    });
  });

  it('returns error for negative MAXLEN', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'a', 'MAXLEN', '-1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "MAXLEN can't be negative",
    });
  });

  it('returns error for unknown option', () => {
    const { db } = createDb();
    makeList(db);
    expect(list.lpos(db, ['k', 'a', 'UNKNOWN', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('options are case-insensitive', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c', 'b']);
    expect(
      list.lpos(db, ['k', 'b', 'rank', '1', 'count', '0', 'maxlen', '0'])
    ).toEqual(arr(integer(1), integer(3)));
  });

  it('returns nil for RANK beyond number of matches (no COUNT)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'a']);
    // only 2 occurrences of 'a', RANK 3 is beyond
    expect(list.lpos(db, ['k', 'a', 'RANK', '3'])).toEqual(NIL);
  });
});

// --- LMOVE ---

describe('LMOVE', () => {
  it('moves element from left of source to left of destination', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b', 'c']);
    list.rpush(db, ['dst', 'x', 'y']);
    expect(list.lmove(db, ['src', 'dst', 'LEFT', 'LEFT'])).toEqual(bulk('a'));
    expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'))
    );
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('x'), bulk('y'))
    );
  });

  it('moves element from left of source to right of destination', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b', 'c']);
    list.rpush(db, ['dst', 'x', 'y']);
    expect(list.lmove(db, ['src', 'dst', 'LEFT', 'RIGHT'])).toEqual(bulk('a'));
    expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'))
    );
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(
      arr(bulk('x'), bulk('y'), bulk('a'))
    );
  });

  it('moves element from right of source to left of destination', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b', 'c']);
    list.rpush(db, ['dst', 'x', 'y']);
    expect(list.lmove(db, ['src', 'dst', 'RIGHT', 'LEFT'])).toEqual(bulk('c'));
    expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'))
    );
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(
      arr(bulk('c'), bulk('x'), bulk('y'))
    );
  });

  it('moves element from right of source to right of destination', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b', 'c']);
    list.rpush(db, ['dst', 'x', 'y']);
    expect(list.lmove(db, ['src', 'dst', 'RIGHT', 'RIGHT'])).toEqual(bulk('c'));
    expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'))
    );
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(
      arr(bulk('x'), bulk('y'), bulk('c'))
    );
  });

  it('returns nil when source does not exist', () => {
    const { db } = createDb();
    expect(list.lmove(db, ['nosrc', 'dst', 'LEFT', 'RIGHT'])).toEqual(NIL);
  });

  it('creates destination if it does not exist', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b']);
    expect(list.lmove(db, ['src', 'dst', 'LEFT', 'LEFT'])).toEqual(bulk('a'));
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(arr(bulk('a')));
  });

  it('deletes source when last element is moved', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    list.lmove(db, ['src', 'dst', 'LEFT', 'LEFT']);
    expect(db.has('src')).toBe(false);
  });

  it('handles same key — rotation LEFT LEFT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    // LEFT LEFT: pop head, push to head — no change
    expect(list.lmove(db, ['k', 'k', 'LEFT', 'LEFT'])).toEqual(bulk('a'));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'))
    );
  });

  it('handles same key — rotation LEFT RIGHT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    // LEFT RIGHT: pop head, push to tail — rotate left
    expect(list.lmove(db, ['k', 'k', 'LEFT', 'RIGHT'])).toEqual(bulk('a'));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'), bulk('a'))
    );
  });

  it('handles same key — rotation RIGHT LEFT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    // RIGHT LEFT: pop tail, push to head — rotate right
    expect(list.lmove(db, ['k', 'k', 'RIGHT', 'LEFT'])).toEqual(bulk('c'));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('c'), bulk('a'), bulk('b'))
    );
  });

  it('handles same key — rotation RIGHT RIGHT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    // RIGHT RIGHT: pop tail, push to tail — no change
    expect(list.lmove(db, ['k', 'k', 'RIGHT', 'RIGHT'])).toEqual(bulk('c'));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'))
    );
  });

  it('handles same key with single element', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(list.lmove(db, ['k', 'k', 'LEFT', 'RIGHT'])).toEqual(bulk('a'));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(arr(bulk('a')));
  });

  it('returns WRONGTYPE when source is not a list', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'val');
    list.rpush(db, ['dst', 'x']);
    expect(list.lmove(db, ['src', 'dst', 'LEFT', 'LEFT'])).toEqual(WRONGTYPE);
  });

  it('returns WRONGTYPE when destination is not a list', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    db.set('dst', 'string', 'raw', 'val');
    expect(list.lmove(db, ['src', 'dst', 'LEFT', 'LEFT'])).toEqual(WRONGTYPE);
  });

  it('does not modify source when destination is wrong type', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b', 'c']);
    db.set('dst', 'string', 'raw', 'val');
    list.lmove(db, ['src', 'dst', 'LEFT', 'LEFT']);
    // Source must be untouched
    expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'), bulk('c'))
    );
  });

  it('is case-insensitive for direction', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b']);
    expect(list.lmove(db, ['src', 'dst', 'left', 'right'])).toEqual(bulk('a'));
  });

  it('returns syntax error for invalid wherefrom', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    expect(list.lmove(db, ['src', 'dst', 'UP', 'LEFT'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns syntax error for invalid whereto', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    expect(list.lmove(db, ['src', 'dst', 'LEFT', 'UP'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

// --- LMPOP ---

describe('LMPOP', () => {
  it('pops one element from first non-empty list (LEFT)', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b', 'c']);
    list.rpush(db, ['k2', 'x', 'y']);
    expect(list.lmpop(db, ['2', 'k1', 'k2', 'LEFT'])).toEqual(
      arr(bulk('k1'), arr(bulk('a')))
    );
    expect(list.lrange(db, ['k1', '0', '-1'])).toEqual(
      arr(bulk('b'), bulk('c'))
    );
  });

  it('pops one element from first non-empty list (RIGHT)', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b', 'c']);
    expect(list.lmpop(db, ['1', 'k1', 'RIGHT'])).toEqual(
      arr(bulk('k1'), arr(bulk('c')))
    );
    expect(list.lrange(db, ['k1', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'))
    );
  });

  it('pops with COUNT option', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b', 'c', 'd']);
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'COUNT', '3'])).toEqual(
      arr(bulk('k1'), arr(bulk('a'), bulk('b'), bulk('c')))
    );
    expect(list.lrange(db, ['k1', '0', '-1'])).toEqual(arr(bulk('d')));
  });

  it('pops with COUNT greater than list length', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b']);
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'COUNT', '10'])).toEqual(
      arr(bulk('k1'), arr(bulk('a'), bulk('b')))
    );
    expect(db.has('k1')).toBe(false);
  });

  it('skips non-existing keys to find first non-empty', () => {
    const { db } = createDb();
    list.rpush(db, ['k2', 'x', 'y']);
    expect(list.lmpop(db, ['2', 'k1', 'k2', 'LEFT'])).toEqual(
      arr(bulk('k2'), arr(bulk('x')))
    );
  });

  it('returns nil when all keys are empty/non-existing', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['2', 'k1', 'k2', 'LEFT'])).toEqual(NIL);
  });

  it('deletes key when all elements are popped', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a']);
    list.lmpop(db, ['1', 'k1', 'LEFT']);
    expect(db.has('k1')).toBe(false);
  });

  it('pops from right with COUNT', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b', 'c', 'd']);
    expect(list.lmpop(db, ['1', 'k1', 'RIGHT', 'COUNT', '2'])).toEqual(
      arr(bulk('k1'), arr(bulk('d'), bulk('c')))
    );
    expect(list.lrange(db, ['k1', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'))
    );
  });

  it('returns WRONGTYPE for non-list key', () => {
    const { db } = createDb();
    db.set('k1', 'string', 'raw', 'val');
    expect(list.lmpop(db, ['1', 'k1', 'LEFT'])).toEqual(WRONGTYPE);
  });

  it('returns error for non-integer numkeys', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['abc', 'k1', 'LEFT'])).toEqual(NOT_INTEGER_ERR);
  });

  it('returns error for numkeys 0', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['0', 'LEFT'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "numkeys can't be non-positive value",
    });
  });

  it('returns error for negative numkeys', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['-1', 'k1', 'LEFT'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: "numkeys can't be non-positive value",
    });
  });

  it('returns syntax error for invalid direction', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['1', 'k1', 'UP'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns syntax error for invalid option after direction', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'BADOPT', '1'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns error for non-integer COUNT value', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'COUNT', 'abc'])).toEqual(
      NOT_INTEGER_ERR
    );
  });

  it('returns error for COUNT 0', () => {
    const { db } = createDb();
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'COUNT', '0'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'count should be greater than 0',
    });
  });

  it('returns error for negative COUNT', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a']);
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'COUNT', '-1'])).toEqual(
      NOT_INTEGER_ERR
    );
  });

  it('is case-insensitive for direction', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b']);
    expect(list.lmpop(db, ['1', 'k1', 'left'])).toEqual(
      arr(bulk('k1'), arr(bulk('a')))
    );
  });

  it('is case-insensitive for COUNT keyword', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b']);
    expect(list.lmpop(db, ['1', 'k1', 'LEFT', 'count', '2'])).toEqual(
      arr(bulk('k1'), arr(bulk('a'), bulk('b')))
    );
  });

  it('returns syntax error when numkeys does not match key count', () => {
    const { db } = createDb();
    // numkeys=2 but only direction follows after 1 key — direction is treated as key
    // so "LEFT" never appears as direction. Actually need to check exact Redis behavior.
    // With numkeys=2, args after numkeys are: k1, LEFT (treated as 2 keys), then no direction
    expect(list.lmpop(db, ['2', 'k1', 'LEFT'])).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });

  it('returns syntax error for duplicate COUNT option', () => {
    const { db } = createDb();
    list.rpush(db, ['k1', 'a', 'b', 'c']);
    expect(
      list.lmpop(db, ['1', 'k1', 'LEFT', 'COUNT', '2', 'COUNT', '3'])
    ).toEqual({
      kind: 'error',
      prefix: 'ERR',
      message: 'syntax error',
    });
  });
});

// --- RPOPLPUSH ---

describe('RPOPLPUSH', () => {
  it('pops from right of source and pushes to left of destination', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b', 'c']);
    list.rpush(db, ['dst', 'x', 'y']);
    expect(list.rpoplpush(db, ['src', 'dst'])).toEqual(bulk('c'));
    expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
      arr(bulk('a'), bulk('b'))
    );
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(
      arr(bulk('c'), bulk('x'), bulk('y'))
    );
  });

  it('returns nil when source does not exist', () => {
    const { db } = createDb();
    expect(list.rpoplpush(db, ['nosrc', 'dst'])).toEqual(NIL);
  });

  it('creates destination if it does not exist', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a', 'b']);
    expect(list.rpoplpush(db, ['src', 'dst'])).toEqual(bulk('b'));
    expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(arr(bulk('b')));
  });

  it('handles same key (rotate right to left)', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(list.rpoplpush(db, ['k', 'k'])).toEqual(bulk('c'));
    expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
      arr(bulk('c'), bulk('a'), bulk('b'))
    );
  });

  it('deletes source when last element is moved', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    list.rpoplpush(db, ['src', 'dst']);
    expect(db.has('src')).toBe(false);
  });

  it('returns WRONGTYPE for non-list source', () => {
    const { db } = createDb();
    db.set('src', 'string', 'raw', 'val');
    expect(list.rpoplpush(db, ['src', 'dst'])).toEqual(WRONGTYPE);
  });

  it('returns WRONGTYPE for non-list destination', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    db.set('dst', 'string', 'raw', 'val');
    expect(list.rpoplpush(db, ['src', 'dst'])).toEqual(WRONGTYPE);
  });
});
