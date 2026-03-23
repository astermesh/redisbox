import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { zadd, zscore } from './sorted-set.ts';
import {
  zunion,
  zinter,
  zdiff,
  zunionstore,
  zinterstore,
  zdiffstore,
  zintercard,
} from './ops.ts';
import { set } from '../string/index.ts';

let rngValue = 0.5;
function createDb(): { db: Database; engine: RedisEngine; rng: () => number } {
  rngValue = 0.5;
  const rng = () => rngValue;
  const engine = new RedisEngine({ clock: () => 1000, rng });
  return { db: engine.db(0), engine, rng };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function integer(value: number | bigint): Reply {
  return { kind: 'integer', value };
}

function err(prefix: string, message: string): Reply {
  return { kind: 'error', prefix, message };
}

const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- ZUNION ---

describe('ZUNION', () => {
  it('returns union of two sets', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = zunion(db, ['2', 'k1', 'k2'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('c'), bulk('b')],
    });
  });

  it('WITHSCORES returns scores', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = zunion(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('c'), bulk('4'), bulk('b'), bulk('5')],
    });
  });

  it('WEIGHTS applies multipliers', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = zunion(
      db,
      ['2', 'k1', 'k2', 'WEIGHTS', '2', '1', 'WITHSCORES'],
      rng
    );
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('2'), bulk('c'), bulk('4'), bulk('b'), bulk('7')],
    });
  });

  it('AGGREGATE MIN', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '5', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = zunion(
      db,
      ['2', 'k1', 'k2', 'AGGREGATE', 'MIN', 'WITHSCORES'],
      rng
    );
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('3'), bulk('c'), bulk('4')],
    });
  });

  it('AGGREGATE MAX', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '5', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = zunion(
      db,
      ['2', 'k1', 'k2', 'AGGREGATE', 'MAX', 'WITHSCORES'],
      rng
    );
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('c'), bulk('4'), bulk('b'), bulk('5')],
    });
  });

  it('non-existent key treated as empty set', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    const result = zunion(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1')],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zunion(db, ['1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZINTER ---

describe('ZINTER', () => {
  it('returns intersection of two sets', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    zadd(db, ['k2', '4', 'b', '5', 'c', '6', 'd'], rng);
    const result = zinter(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('6'), bulk('c'), bulk('8')],
    });
  });

  it('empty intersection when key missing', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    expect(zinter(db, ['2', 'k1', 'k2'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('WEIGHTS and AGGREGATE MIN', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '10', 'a', '20', 'b'], rng);
    zadd(db, ['k2', '1', 'a', '2', 'b'], rng);
    const result = zinter(
      db,
      ['2', 'k1', 'k2', 'WEIGHTS', '1', '10', 'AGGREGATE', 'MIN', 'WITHSCORES'],
      rng
    );
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('10'), bulk('b'), bulk('20')],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zinter(db, ['1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZDIFF ---

describe('ZDIFF', () => {
  it('returns difference of two sets', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    zadd(db, ['k2', '4', 'b', '5', 'd'], rng);
    const result = zdiff(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('c'), bulk('3')],
    });
  });

  it('all elements when second key missing', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    const result = zdiff(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('empty result when first key missing', () => {
    const { db, rng } = createDb();
    zadd(db, ['k2', '1', 'a'], rng);
    expect(zdiff(db, ['2', 'k1', 'k2'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zdiff(db, ['1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZUNIONSTORE ---

describe('ZUNIONSTORE', () => {
  it('stores union result', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    expect(zunionstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(integer(3));
    expect(zscore(db, ['out', 'a'])).toEqual(bulk('1'));
    expect(zscore(db, ['out', 'b'])).toEqual(bulk('5'));
    expect(zscore(db, ['out', 'c'])).toEqual(bulk('4'));
  });

  it('with WEIGHTS', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    zadd(db, ['k2', '2', 'a'], rng);
    expect(
      zunionstore(db, ['out', '2', 'k1', 'k2', 'WEIGHTS', '10', '1'], rng)
    ).toEqual(integer(1));
    expect(zscore(db, ['out', 'a'])).toEqual(bulk('12'));
  });

  it('overwrites destination', () => {
    const { db, rng } = createDb();
    zadd(db, ['out', '99', 'old'], rng);
    zadd(db, ['k1', '1', 'a'], rng);
    zunionstore(db, ['out', '1', 'k1'], rng);
    expect(zscore(db, ['out', 'old'])).toEqual(bulk(null));
    expect(zscore(db, ['out', 'a'])).toEqual(bulk('1'));
  });

  it('destination can be a source key', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    expect(zunionstore(db, ['k1', '1', 'k1'], rng)).toEqual(integer(2));
    expect(zscore(db, ['k1', 'a'])).toEqual(bulk('1'));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zunionstore(db, ['out', '1', 'k1'], rng)).toEqual(WRONGTYPE);
  });

  it('rejects WITHSCORES option', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    expect(zunionstore(db, ['out', '1', 'k1', 'WITHSCORES'], rng)).toEqual(
      err('ERR', 'syntax error')
    );
  });
});

// --- ZINTERSTORE ---

describe('ZINTERSTORE', () => {
  it('stores intersection result', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    expect(zinterstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(integer(1));
    expect(zscore(db, ['out', 'b'])).toEqual(bulk('5'));
  });

  it('empty result when no intersection', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    zadd(db, ['k2', '2', 'b'], rng);
    expect(zinterstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(integer(0));
  });

  it('with AGGREGATE MAX', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '10', 'a', '20', 'b'], rng);
    zadd(db, ['k2', '1', 'a', '2', 'b'], rng);
    expect(
      zinterstore(db, ['out', '2', 'k1', 'k2', 'AGGREGATE', 'MAX'], rng)
    ).toEqual(integer(2));
    expect(zscore(db, ['out', 'a'])).toEqual(bulk('10'));
    expect(zscore(db, ['out', 'b'])).toEqual(bulk('20'));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zinterstore(db, ['out', '1', 'k1'], rng)).toEqual(WRONGTYPE);
  });

  it('rejects WITHSCORES option', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    zadd(db, ['k2', '2', 'a'], rng);
    expect(
      zinterstore(db, ['out', '2', 'k1', 'k2', 'WITHSCORES'], rng)
    ).toEqual(err('ERR', 'syntax error'));
  });
});

// --- ZDIFFSTORE ---

describe('ZDIFFSTORE', () => {
  it('stores difference result', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    zadd(db, ['k2', '4', 'b'], rng);
    expect(zdiffstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(integer(2));
    expect(zscore(db, ['out', 'a'])).toEqual(bulk('1'));
    expect(zscore(db, ['out', 'c'])).toEqual(bulk('3'));
    expect(zscore(db, ['out', 'b'])).toEqual(bulk(null));
  });

  it('empty result when all elements in other sets', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    zadd(db, ['k2', '2', 'a'], rng);
    expect(zdiffstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(integer(0));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zdiffstore(db, ['out', '1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZINTERCARD ---

describe('ZINTERCARD', () => {
  it('returns cardinality of intersection', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    zadd(db, ['k2', '4', 'b', '5', 'c', '6', 'd'], rng);
    expect(zintercard(db, ['2', 'k1', 'k2'])).toEqual(integer(2));
  });

  it('with LIMIT stops early', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    zadd(db, ['k2', '4', 'a', '5', 'b', '6', 'c'], rng);
    expect(zintercard(db, ['2', 'k1', 'k2', 'LIMIT', '2'])).toEqual(integer(2));
  });

  it('returns 0 when key missing', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    expect(zintercard(db, ['2', 'k1', 'k2'])).toEqual(integer(0));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    zadd(db, ['k2', '1', 'a'], rng);
    expect(zintercard(db, ['2', 'k1', 'k2'])).toEqual(WRONGTYPE);
  });

  it('rejects negative LIMIT', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a'], rng);
    expect(zintercard(db, ['1', 'k1', 'LIMIT', '-1'])).toEqual(
      err('ERR', "LIMIT can't be negative")
    );
  });

  it('rejects numkeys 0', () => {
    const { db } = createDb();
    expect(zintercard(db, ['0', 'k1'])).toEqual(
      err('ERR', 'numkeys should be greater than 0')
    );
  });
});
