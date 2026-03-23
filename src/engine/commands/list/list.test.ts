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
  // --- Threshold boundary tests ---

  it('uses listpack for small lists', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    const entry = db.get('k');
    expect(entry?.encoding).toBe('listpack');
    expect(entry?.type).toBe('list');
  });

  it('stays listpack at exactly 128 entries', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i < 128; i++) {
      args.push(`v${i}`);
    }
    list.rpush(db, args);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('transitions to quicklist at 129 entries', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      args.push(`v${i}`);
    }
    list.rpush(db, args);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('stays listpack at exact 64-byte value', () => {
    const { db } = createDb();
    const exactValue = 'x'.repeat(64);
    list.rpush(db, ['k', exactValue]);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('transitions to quicklist when value exceeds 64 bytes', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    list.rpush(db, ['k', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('measures byte length, not character count (multi-byte chars)', () => {
    const { db } = createDb();
    // 22 emoji × 4 bytes each = 88 bytes > 64
    const multiByteValue = '😀'.repeat(22);
    list.rpush(db, ['k', multiByteValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('stays listpack when multi-byte string is within byte limit', () => {
    const { db } = createDb();
    // 16 emoji × 4 bytes each = 64 bytes — exactly at threshold
    const multiByteValue = '😀'.repeat(16);
    list.rpush(db, ['k', multiByteValue]);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  // --- No demotion ---

  it('never demotes from quicklist back to listpack after pop', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    list.rpush(db, ['k', 'keep', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
    list.rpop(db, ['k']);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('never demotes from quicklist after LREM removes all large elements', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    list.rpush(db, ['k', 'a', longValue, 'b']);
    expect(db.get('k')?.encoding).toBe('quicklist');
    list.lrem(db, ['k', '0', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('never demotes from quicklist after LTRIM shrinks the list', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) args.push(`v${i}`);
    list.rpush(db, args);
    expect(db.get('k')?.encoding).toBe('quicklist');
    list.ltrim(db, ['k', '0', '2']);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  // --- Transitions via different mutation commands ---

  it('transitions via LPUSH', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    list.lpush(db, ['k', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('transitions via LPUSHX', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
    const longValue = 'x'.repeat(65);
    list.lpushx(db, ['k', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('transitions via RPUSHX', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
    const longValue = 'x'.repeat(65);
    list.rpushx(db, ['k', longValue]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('transitions via LSET replacing short value with long value', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
    list.lset(db, ['k', '0', 'x'.repeat(65)]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('transitions via LINSERT', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
    list.linsert(db, ['k', 'AFTER', 'a', 'x'.repeat(65)]);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  it('transitions via LMOVE to destination', () => {
    const { db } = createDb();
    const longValue = 'x'.repeat(65);
    list.rpush(db, ['src', longValue]);
    list.rpush(db, ['dst', 'a']);
    expect(db.get('dst')?.encoding).toBe('listpack');
    list.lmove(db, ['src', 'dst', 'LEFT', 'RIGHT']);
    expect(db.get('dst')?.encoding).toBe('quicklist');
  });

  it('transitions via LMOVE same-key rotation', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i < 128; i++) args.push(`v${i}`);
    list.rpush(db, args);
    expect(db.get('k')?.encoding).toBe('listpack');
    // Rotate doesn't change count, stays listpack
    list.lmove(db, ['k', 'k', 'LEFT', 'RIGHT']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  // --- OBJECT ENCODING integration ---

  it('OBJECT ENCODING returns listpack for small list', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a', 'b', 'c']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('OBJECT ENCODING returns quicklist for large list', () => {
    const { db } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) args.push(`v${i}`);
    list.rpush(db, args);
    expect(db.get('k')?.encoding).toBe('quicklist');
  });

  // --- Initial encoding ---

  it('new list created by LPUSH starts as listpack', () => {
    const { db } = createDb();
    list.lpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('new list created by RPUSH starts as listpack', () => {
    const { db } = createDb();
    list.rpush(db, ['k', 'a']);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('new list created by LMOVE to new destination starts as listpack', () => {
    const { db } = createDb();
    list.rpush(db, ['src', 'a']);
    list.lmove(db, ['src', 'dst', 'LEFT', 'RIGHT']);
    expect(db.get('dst')?.encoding).toBe('listpack');
  });

  // --- Entry count transition with incremental pushes ---

  it('stays listpack during incremental pushes up to 128', () => {
    const { db } = createDb();
    for (let i = 0; i < 128; i++) {
      list.rpush(db, ['k', `v${i}`]);
    }
    expect(db.get('k')?.encoding).toBe('listpack');
    // 129th element triggers transition
    list.rpush(db, ['k', 'overflow']);
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
