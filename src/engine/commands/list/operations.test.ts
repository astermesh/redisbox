import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
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

const NIL = bulk(null);
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
