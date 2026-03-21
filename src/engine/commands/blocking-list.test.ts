import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { CommandContext, Reply } from '../types.ts';
import { BlockingManager } from '../blocking-manager.ts';
import * as list from './list.ts';
import * as blockingList from './blocking-list.ts';

function createDb(): {
  db: Database;
  engine: RedisEngine;
  blocking: BlockingManager;
  ctx: CommandContext;
} {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  const blocking = new BlockingManager();
  const db = engine.db(0);
  return {
    db,
    engine,
    blocking,
    ctx: { db, engine, blocking },
  };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function arr(...items: Reply[]): Reply {
  return { kind: 'array', value: items };
}

const NIL_ARRAY: Reply = { kind: 'nil-array' };
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- BLPOP ---

describe('BLPOP', () => {
  describe('non-blocking path (data available)', () => {
    it('pops from a single non-empty list', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b']);
      const result = blockingList.blpop(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('a')));
    });

    it('pops from the first non-empty key', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k2', 'x']);
      const result = blockingList.blpop(ctx, ['k1', 'k2', '0']);
      expect(result).toEqual(arr(bulk('k2'), bulk('x')));
    });

    it('skips non-existent keys', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k3', 'val']);
      const result = blockingList.blpop(ctx, ['k1', 'k2', 'k3', '0']);
      expect(result).toEqual(arr(bulk('k3'), bulk('val')));
    });

    it('removes element from list', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'only']);
      blockingList.blpop(ctx, ['k', '0']);
      // key should be deleted since list is now empty
      expect(db.get('k')).toBeNull();
    });

    it('returns WRONGTYPE for non-list key', () => {
      const { ctx, db } = createDb();
      db.set('k', 'string', 'raw', 'val');
      const result = blockingList.blpop(ctx, ['k', '0']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('pops from left (head)', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c']);
      const result = blockingList.blpop(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('a')));
      // verify remaining: b, c
      expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
        arr(bulk('b'), bulk('c'))
      );
    });
  });

  describe('blocking path (no data)', () => {
    it('returns nil-array when all keys are empty (blocking indicator)', () => {
      const { ctx } = createDb();
      const result = blockingList.blpop(ctx, ['k1', 'k2', '5']);
      expect(result).toEqual(NIL_ARRAY);
    });

    it('returns nil-array for non-existent keys', () => {
      const { ctx } = createDb();
      const result = blockingList.blpop(ctx, ['nokey', '0']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });

  describe('timeout parsing', () => {
    it('accepts integer timeout', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'v']);
      const result = blockingList.blpop(ctx, ['k', '10']);
      expect(result).toEqual(arr(bulk('k'), bulk('v')));
    });

    it('accepts float timeout', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'v']);
      const result = blockingList.blpop(ctx, ['k', '0.5']);
      expect(result).toEqual(arr(bulk('k'), bulk('v')));
    });

    it('rejects negative timeout', () => {
      const { ctx } = createDb();
      const result = blockingList.blpop(ctx, ['k', '-1']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });

    it('rejects non-numeric timeout', () => {
      const { ctx } = createDb();
      const result = blockingList.blpop(ctx, ['k', 'abc']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });
  });
});

// --- BRPOP ---

describe('BRPOP', () => {
  describe('non-blocking path (data available)', () => {
    it('pops from a single non-empty list', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b']);
      const result = blockingList.brpop(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('b')));
    });

    it('pops from the first non-empty key', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k2', 'x', 'y']);
      const result = blockingList.brpop(ctx, ['k1', 'k2', '0']);
      expect(result).toEqual(arr(bulk('k2'), bulk('y')));
    });

    it('removes element from list', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'only']);
      blockingList.brpop(ctx, ['k', '0']);
      expect(db.get('k')).toBeNull();
    });

    it('returns WRONGTYPE for non-list key', () => {
      const { ctx, db } = createDb();
      db.set('k', 'string', 'raw', 'val');
      const result = blockingList.brpop(ctx, ['k', '0']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('pops from right (tail)', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c']);
      const result = blockingList.brpop(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('c')));
      expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
        arr(bulk('a'), bulk('b'))
      );
    });
  });

  describe('blocking path', () => {
    it('returns nil-array when all keys are empty', () => {
      const { ctx } = createDb();
      const result = blockingList.brpop(ctx, ['k1', 'k2', '5']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });
});

// --- BLMOVE ---

describe('BLMOVE', () => {
  describe('non-blocking path (data available)', () => {
    it('moves element from source to destination', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['src', 'a', 'b', 'c']);
      const result = blockingList.blmove(ctx, [
        'src',
        'dst',
        'LEFT',
        'RIGHT',
        '0',
      ]);
      expect(result).toEqual(bulk('a'));
      expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
        arr(bulk('b'), bulk('c'))
      );
      expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(arr(bulk('a')));
    });

    it('moves RIGHT to LEFT', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['src', 'a', 'b']);
      list.rpush(db, ['dst', 'x']);
      const result = blockingList.blmove(ctx, [
        'src',
        'dst',
        'RIGHT',
        'LEFT',
        '0',
      ]);
      expect(result).toEqual(bulk('b'));
      expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(
        arr(bulk('b'), bulk('x'))
      );
    });

    it('works with same key as source and destination', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c']);
      const result = blockingList.blmove(ctx, ['k', 'k', 'LEFT', 'RIGHT', '0']);
      expect(result).toEqual(bulk('a'));
      expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
        arr(bulk('b'), bulk('c'), bulk('a'))
      );
    });

    it('returns WRONGTYPE for non-list source', () => {
      const { ctx, db } = createDb();
      db.set('src', 'string', 'raw', 'val');
      const result = blockingList.blmove(ctx, [
        'src',
        'dst',
        'LEFT',
        'RIGHT',
        '0',
      ]);
      expect(result).toEqual(WRONGTYPE);
    });

    it('returns WRONGTYPE for non-list destination', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['src', 'a']);
      db.set('dst', 'string', 'raw', 'val');
      const result = blockingList.blmove(ctx, [
        'src',
        'dst',
        'LEFT',
        'RIGHT',
        '0',
      ]);
      expect(result).toEqual(WRONGTYPE);
    });

    it('rejects invalid direction', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['src', 'a']);
      const result = blockingList.blmove(ctx, [
        'src',
        'dst',
        'UP',
        'RIGHT',
        '0',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });
  });

  describe('blocking path (no source data)', () => {
    it('returns nil-array when source is empty', () => {
      const { ctx } = createDb();
      const result = blockingList.blmove(ctx, [
        'src',
        'dst',
        'LEFT',
        'RIGHT',
        '5',
      ]);
      expect(result).toEqual(NIL_ARRAY);
    });

    it('returns nil-array when source does not exist', () => {
      const { ctx } = createDb();
      const result = blockingList.blmove(ctx, [
        'nosrc',
        'dst',
        'LEFT',
        'RIGHT',
        '0',
      ]);
      expect(result).toEqual(NIL_ARRAY);
    });
  });
});

// --- BLMPOP ---

describe('BLMPOP', () => {
  describe('non-blocking path (data available)', () => {
    it('pops single element from first non-empty list (LEFT)', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c']);
      const result = blockingList.blmpop(ctx, ['0', '1', 'k', 'LEFT']);
      expect(result).toEqual(arr(bulk('k'), arr(bulk('a'))));
    });

    it('pops single element from first non-empty list (RIGHT)', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c']);
      const result = blockingList.blmpop(ctx, ['0', '1', 'k', 'RIGHT']);
      expect(result).toEqual(arr(bulk('k'), arr(bulk('c'))));
    });

    it('pops multiple elements with COUNT', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c']);
      const result = blockingList.blmpop(ctx, [
        '0',
        '1',
        'k',
        'LEFT',
        'COUNT',
        '2',
      ]);
      expect(result).toEqual(arr(bulk('k'), arr(bulk('a'), bulk('b'))));
    });

    it('pops from first non-empty key among multiple', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k2', 'x']);
      const result = blockingList.blmpop(ctx, ['0', '2', 'k1', 'k2', 'LEFT']);
      expect(result).toEqual(arr(bulk('k2'), arr(bulk('x'))));
    });

    it('pops right elements reversed correctly', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b', 'c', 'd']);
      const result = blockingList.blmpop(ctx, [
        '0',
        '1',
        'k',
        'RIGHT',
        'COUNT',
        '3',
      ]);
      expect(result).toEqual(
        arr(bulk('k'), arr(bulk('d'), bulk('c'), bulk('b')))
      );
    });

    it('returns WRONGTYPE for non-list key', () => {
      const { ctx, db } = createDb();
      db.set('k', 'string', 'raw', 'val');
      const result = blockingList.blmpop(ctx, ['0', '1', 'k', 'LEFT']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('deletes key when all elements popped', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a']);
      blockingList.blmpop(ctx, ['0', '1', 'k', 'LEFT']);
      expect(db.get('k')).toBeNull();
    });
  });

  describe('blocking path', () => {
    it('returns nil-array when all keys are empty', () => {
      const { ctx } = createDb();
      const result = blockingList.blmpop(ctx, ['5', '2', 'k1', 'k2', 'LEFT']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });

  describe('argument parsing', () => {
    it('rejects invalid numkeys', () => {
      const { ctx } = createDb();
      const result = blockingList.blmpop(ctx, ['0', 'abc', 'k', 'LEFT']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });

    it('rejects numkeys = 0', () => {
      const { ctx } = createDb();
      const result = blockingList.blmpop(ctx, ['0', '0', 'LEFT']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "numkeys can't be non-positive value",
      });
    });

    it('rejects negative numkeys', () => {
      const { ctx } = createDb();
      const result = blockingList.blmpop(ctx, ['0', '-1', 'k', 'LEFT']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "numkeys can't be non-positive value",
      });
    });

    it('rejects invalid direction', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a']);
      const result = blockingList.blmpop(ctx, ['0', '1', 'k', 'UP']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('rejects negative timeout', () => {
      const { ctx } = createDb();
      const result = blockingList.blmpop(ctx, ['-1', '1', 'k', 'LEFT']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });

    it('rejects COUNT 0', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a']);
      const result = blockingList.blmpop(ctx, [
        '0',
        '1',
        'k',
        'LEFT',
        'COUNT',
        '0',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'count should be greater than 0',
      });
    });

    it('rejects negative COUNT', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a']);
      const result = blockingList.blmpop(ctx, [
        '0',
        '1',
        'k',
        'LEFT',
        'COUNT',
        '-1',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });

    it('rejects non-integer COUNT', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a']);
      const result = blockingList.blmpop(ctx, [
        '0',
        '1',
        'k',
        'LEFT',
        'COUNT',
        'abc',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });

    it('rejects float numkeys', () => {
      const { ctx } = createDb();
      const result = blockingList.blmpop(ctx, ['0', '1.5', 'k', 'LEFT']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });

    it('rejects unknown option after direction', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a']);
      const result = blockingList.blmpop(ctx, [
        '0',
        '1',
        'k',
        'LEFT',
        'UNKNOWN',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });
  });
});

// --- BRPOPLPUSH ---

describe('BRPOPLPUSH', () => {
  describe('non-blocking path', () => {
    it('moves element from source tail to destination head', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['src', 'a', 'b', 'c']);
      const result = blockingList.brpoplpush(ctx, ['src', 'dst', '0']);
      expect(result).toEqual(bulk('c'));
      expect(list.lrange(db, ['dst', '0', '-1'])).toEqual(arr(bulk('c')));
      expect(list.lrange(db, ['src', '0', '-1'])).toEqual(
        arr(bulk('a'), bulk('b'))
      );
    });

    it('works with same key', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['k', 'a', 'b']);
      const result = blockingList.brpoplpush(ctx, ['k', 'k', '0']);
      expect(result).toEqual(bulk('b'));
      expect(list.lrange(db, ['k', '0', '-1'])).toEqual(
        arr(bulk('b'), bulk('a'))
      );
    });
  });

  describe('blocking path', () => {
    it('returns nil-array when source is empty', () => {
      const { ctx } = createDb();
      const result = blockingList.brpoplpush(ctx, ['src', 'dst', '0']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });

  describe('error handling', () => {
    it('returns WRONGTYPE for non-list source', () => {
      const { ctx, db } = createDb();
      db.set('src', 'string', 'raw', 'val');
      const result = blockingList.brpoplpush(ctx, ['src', 'dst', '0']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('returns WRONGTYPE for non-list destination', () => {
      const { ctx, db } = createDb();
      list.rpush(db, ['src', 'a']);
      db.set('dst', 'string', 'raw', 'val');
      const result = blockingList.brpoplpush(ctx, ['src', 'dst', '0']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('rejects negative timeout', () => {
      const { ctx } = createDb();
      const result = blockingList.brpoplpush(ctx, ['src', 'dst', '-1']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });
  });
});
