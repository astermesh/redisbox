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
