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

const NIL = bulk(null);
const ZERO = integer(0);
const ONE = integer(1);
const EMPTY_ARRAY: Reply = { kind: 'array', value: [] };
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

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
