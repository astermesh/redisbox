import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { CommandContext, Reply } from '../../types.ts';
import { BlockingManager } from '../../blocking/blocking-manager.ts';
import * as sortedSet from './index.ts';
import * as blockingSortedSet from './blocking.ts';

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

// --- BZPOPMIN ---

describe('BZPOPMIN', () => {
  describe('non-blocking path (data available)', () => {
    it('pops from a single non-empty sorted set', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], engine.rng);
      const result = blockingSortedSet.bzpopmin(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('a'), bulk('1')));
    });

    it('pops from the first non-empty key', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k2', '5', 'x'], engine.rng);
      const result = blockingSortedSet.bzpopmin(ctx, ['k1', 'k2', '0']);
      expect(result).toEqual(arr(bulk('k2'), bulk('x'), bulk('5')));
    });

    it('skips non-existent keys', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k3', '10', 'val'], engine.rng);
      const result = blockingSortedSet.bzpopmin(ctx, ['k1', 'k2', 'k3', '0']);
      expect(result).toEqual(arr(bulk('k3'), bulk('val'), bulk('10')));
    });

    it('removes element from sorted set', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '1', 'only'], engine.rng);
      blockingSortedSet.bzpopmin(ctx, ['k', '0']);
      expect(db.get('k')).toBeNull();
    });

    it('returns WRONGTYPE for non-zset key', () => {
      const { ctx, db } = createDb();
      db.set('k', 'string', 'raw', 'val');
      const result = blockingSortedSet.bzpopmin(ctx, ['k', '0']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('pops element with lowest score', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '3', 'c', '1', 'a', '2', 'b'], engine.rng);
      const result = blockingSortedSet.bzpopmin(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('a'), bulk('1')));
    });
  });

  describe('blocking path (no data)', () => {
    it('returns NIL_ARRAY when no keys exist', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzpopmin(ctx, ['k1', 'k2', '0']);
      expect(result).toEqual(NIL_ARRAY);
    });

    it('returns NIL_ARRAY when sorted sets are empty', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '1', 'a'], engine.rng);
      sortedSet.zpopmin(db, ['k']); // empty the set
      const result = blockingSortedSet.bzpopmin(ctx, ['k', '0']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });

  describe('timeout validation', () => {
    it('rejects negative timeout', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzpopmin(ctx, ['k', '-1']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });

    it('rejects non-numeric timeout', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzpopmin(ctx, ['k', 'abc']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });

    it('accepts float timeout', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzpopmin(ctx, ['k', '1.5']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });
});

// --- BZPOPMAX ---

describe('BZPOPMAX', () => {
  describe('non-blocking path (data available)', () => {
    it('pops from a single non-empty sorted set', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], engine.rng);
      const result = blockingSortedSet.bzpopmax(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('b'), bulk('2')));
    });

    it('pops from the first non-empty key', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k2', '5', 'x', '10', 'y'], engine.rng);
      const result = blockingSortedSet.bzpopmax(ctx, ['k1', 'k2', '0']);
      expect(result).toEqual(arr(bulk('k2'), bulk('y'), bulk('10')));
    });

    it('removes element from sorted set', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '1', 'only'], engine.rng);
      blockingSortedSet.bzpopmax(ctx, ['k', '0']);
      expect(db.get('k')).toBeNull();
    });

    it('returns WRONGTYPE for non-zset key', () => {
      const { ctx, db } = createDb();
      db.set('k', 'string', 'raw', 'val');
      const result = blockingSortedSet.bzpopmax(ctx, ['k', '0']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('pops element with highest score', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '3', 'c', '1', 'a', '2', 'b'], engine.rng);
      const result = blockingSortedSet.bzpopmax(ctx, ['k', '0']);
      expect(result).toEqual(arr(bulk('k'), bulk('c'), bulk('3')));
    });
  });

  describe('blocking path (no data)', () => {
    it('returns NIL_ARRAY when no keys exist', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzpopmax(ctx, ['k1', 'k2', '0']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });

  describe('timeout validation', () => {
    it('rejects negative timeout', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzpopmax(ctx, ['k', '-1']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });
  });
});

// --- BZMPOP ---

describe('BZMPOP', () => {
  describe('non-blocking path (data available)', () => {
    it('pops MIN from first non-empty set', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], engine.rng);
      const result = blockingSortedSet.bzmpop(ctx, ['0', '1', 'k1', 'MIN']);
      expect(result).toEqual(arr(bulk('k1'), arr(arr(bulk('a'), bulk('1')))));
    });

    it('pops MAX from first non-empty set', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], engine.rng);
      const result = blockingSortedSet.bzmpop(ctx, ['0', '1', 'k1', 'MAX']);
      expect(result).toEqual(arr(bulk('k1'), arr(arr(bulk('b'), bulk('2')))));
    });

    it('pops multiple with COUNT', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], engine.rng);
      const result = blockingSortedSet.bzmpop(ctx, [
        '0',
        '1',
        'k1',
        'MIN',
        'COUNT',
        '2',
      ]);
      expect(result).toEqual(
        arr(
          bulk('k1'),
          arr(arr(bulk('a'), bulk('1')), arr(bulk('b'), bulk('2')))
        )
      );
    });

    it('skips empty keys', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k2', '5', 'x'], engine.rng);
      const result = blockingSortedSet.bzmpop(ctx, [
        '0',
        '2',
        'k1',
        'k2',
        'MIN',
      ]);
      expect(result).toEqual(arr(bulk('k2'), arr(arr(bulk('x'), bulk('5')))));
    });

    it('returns WRONGTYPE for non-zset key', () => {
      const { ctx, db } = createDb();
      db.set('k', 'string', 'raw', 'val');
      const result = blockingSortedSet.bzmpop(ctx, ['0', '1', 'k', 'MIN']);
      expect(result).toEqual(WRONGTYPE);
    });

    it('deletes key when all elements popped', () => {
      const { ctx, db, engine } = createDb();
      sortedSet.zadd(db, ['k', '1', 'a'], engine.rng);
      blockingSortedSet.bzmpop(ctx, ['0', '1', 'k', 'MIN']);
      expect(db.get('k')).toBeNull();
    });
  });

  describe('blocking path (no data)', () => {
    it('returns NIL_ARRAY when all keys empty', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzmpop(ctx, ['0', '1', 'k1', 'MIN']);
      expect(result).toEqual(NIL_ARRAY);
    });
  });

  describe('argument validation', () => {
    it('rejects negative timeout', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzmpop(ctx, ['-1', '1', 'k', 'MIN']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'timeout is not a float or out of range',
      });
    });

    it('rejects non-integer numkeys', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzmpop(ctx, ['0', 'abc', 'k', 'MIN']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'value is not an integer or out of range',
      });
    });

    it('rejects non-positive numkeys', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzmpop(ctx, ['0', '0', 'k', 'MIN']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: "numkeys can't be non-positive value",
      });
    });

    it('rejects invalid direction', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzmpop(ctx, ['0', '1', 'k', 'INVALID']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'syntax error',
      });
    });

    it('rejects count 0', () => {
      const { ctx } = createDb();
      const result = blockingSortedSet.bzmpop(ctx, [
        '0',
        '1',
        'k',
        'MIN',
        'COUNT',
        '0',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'ERR',
        message: 'count should be greater than 0',
      });
    });
  });
});
