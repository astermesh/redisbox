import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import { zadd, zscore, zcard } from './sorted-set.ts';
import {
  zcount,
  zlexcount,
  zrangebyscore,
  zrevrangebyscore,
  zrangebylex,
  zrevrangebylex,
  zrange,
  zrangestore,
} from './range.ts';
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

const ZERO = integer(0);
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- ZCOUNT ---

describe('ZCOUNT', () => {
  it('counts all elements with -inf +inf', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcount(db, ['k', '-inf', '+inf'])).toEqual(integer(3));
  });

  it('counts elements in inclusive range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcount(db, ['k', '1', '2'])).toEqual(integer(2));
  });

  it('counts elements with exclusive min', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcount(db, ['k', '(1', '3'])).toEqual(integer(2));
  });

  it('counts elements with exclusive max', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcount(db, ['k', '1', '(3'])).toEqual(integer(2));
  });

  it('counts elements with both exclusive', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcount(db, ['k', '(1', '(3'])).toEqual(integer(1));
  });

  it('returns 0 for empty range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcount(db, ['k', '(3', '+inf'])).toEqual(ZERO);
  });

  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(zcount(db, ['k', '-inf', '+inf'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zcount(db, ['k', '-inf', '+inf'])).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db } = createDb();
    expect(zcount(db, ['k', '0'])).toEqual(
      err('ERR', "wrong number of arguments for 'zcount' command")
    );
  });

  it('rejects invalid min', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zcount(db, ['k', 'notanumber', '5'])).toEqual(
      err('ERR', 'min or max is not a float')
    );
  });

  it('rejects invalid max', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zcount(db, ['k', '0', 'notanumber'])).toEqual(
      err('ERR', 'min or max is not a float')
    );
  });
});

// --- ZLEXCOUNT ---

describe('ZLEXCOUNT', () => {
  it('counts all with - +', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    expect(zlexcount(db, ['k', '-', '+'])).toEqual(integer(4));
  });

  it('counts inclusive range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    expect(zlexcount(db, ['k', '[b', '[c'])).toEqual(integer(2));
  });

  it('counts exclusive range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    expect(zlexcount(db, ['k', '(a', '(d'])).toEqual(integer(2));
  });

  it('returns 0 for empty range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c'], rng);
    expect(zlexcount(db, ['k', '(c', '+'])).toEqual(ZERO);
  });

  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(zlexcount(db, ['k', '-', '+'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zlexcount(db, ['k', '-', '+'])).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db } = createDb();
    expect(zlexcount(db, ['k', '-'])).toEqual(
      err('ERR', "wrong number of arguments for 'zlexcount' command")
    );
  });

  it('rejects invalid min spec', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a'], rng);
    expect(zlexcount(db, ['k', 'a', '+'])).toEqual(
      err('ERR', 'min or max not valid string range item')
    );
  });
});

// --- ZRANGEBYSCORE ---

describe('ZRANGEBYSCORE', () => {
  it('returns elements in score range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangebyscore(db, ['k', '1', '2']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('returns all with -inf +inf', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangebyscore(db, ['k', '-inf', '+inf']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b'), bulk('c')],
    });
  });

  it('handles exclusive bounds', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangebyscore(db, ['k', '(1', '(3']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b')],
    });
  });

  it('WITHSCORES option', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = zrangebyscore(db, ['k', '-inf', '+inf', 'WITHSCORES']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c', '4', 'd', '5', 'e'], rng);
    const result = zrangebyscore(db, ['k', '-inf', '+inf', 'LIMIT', '1', '2']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('LIMIT with WITHSCORES', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangebyscore(db, [
      'k',
      '-inf',
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      '2',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('LIMIT with count -1 returns all from offset', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangebyscore(db, ['k', '-inf', '+inf', 'LIMIT', '1', '-1']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('empty result for nonexistent key', () => {
    const { db } = createDb();
    expect(zrangebyscore(db, ['k', '-inf', '+inf'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('WRONGTYPE error', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zrangebyscore(db, ['k', '0', '1'])).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db } = createDb();
    expect(zrangebyscore(db, ['k', '0'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrangebyscore' command")
    );
  });
});

// --- ZREVRANGEBYSCORE ---

describe('ZREVRANGEBYSCORE', () => {
  it('returns elements in reverse score order (max first)', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrevrangebyscore(db, ['k', '3', '1']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('handles exclusive bounds', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrevrangebyscore(db, ['k', '(3', '(1']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b')],
    });
  });

  it('WITHSCORES', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrevrangebyscore(db, ['k', '+inf', '-inf', 'WITHSCORES']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('3'), bulk('b'), bulk('2'), bulk('a'), bulk('1')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c', '4', 'd'], rng);
    const result = zrevrangebyscore(db, [
      'k',
      '+inf',
      '-inf',
      'LIMIT',
      '1',
      '2',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });

  it('empty result for nonexistent key', () => {
    const { db } = createDb();
    expect(zrevrangebyscore(db, ['k', '+inf', '-inf'])).toEqual({
      kind: 'array',
      value: [],
    });
  });
});

// --- ZRANGEBYLEX ---

describe('ZRANGEBYLEX', () => {
  it('returns all elements with - +', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrangebylex(db, ['k', '-', '+']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b'), bulk('c'), bulk('d')],
    });
  });

  it('inclusive range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrangebylex(db, ['k', '[b', '[c']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('exclusive range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrangebylex(db, ['k', '(a', '(d']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrangebylex(db, ['k', '-', '+', 'LIMIT', '1', '2']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('empty result for nonexistent key', () => {
    const { db } = createDb();
    expect(zrangebylex(db, ['k', '-', '+'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('rejects invalid lex min', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a'], rng);
    expect(zrangebylex(db, ['k', 'invalid', '+'])).toEqual(
      err('ERR', 'min or max not valid string range item')
    );
  });

  it('WRONGTYPE error', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zrangebylex(db, ['k', '-', '+'])).toEqual(WRONGTYPE);
  });
});

// --- ZREVRANGEBYLEX ---

describe('ZREVRANGEBYLEX', () => {
  it('returns all elements in reverse with + -', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrevrangebylex(db, ['k', '+', '-']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('d'), bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('inclusive range (max first arg)', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrevrangebylex(db, ['k', '[c', '[b']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });

  it('exclusive range', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrevrangebylex(db, ['k', '(d', '(a']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrevrangebylex(db, ['k', '+', '-', 'LIMIT', '1', '2']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });
});

// --- ZRANGE ---

describe('ZRANGE', () => {
  it('returns range by rank (default)', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '0', '1'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('returns all with 0 -1', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '0', '-1'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b'), bulk('c')],
    });
  });

  it('negative indices', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '-2', '-1'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('out of range indices clamped', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = zrange(db, ['k', '-100', '100'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('start > end returns empty', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = zrange(db, ['k', '2', '0'], rng);
    expect(result).toEqual({ kind: 'array', value: [] });
  });

  it('WITHSCORES', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = zrange(db, ['k', '0', '-1', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('REV option reverses rank order', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '0', '-1', 'REV'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('BYSCORE option', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '1', '2', 'BYSCORE'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('BYSCORE with exclusive bounds', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '(1', '(3', 'BYSCORE'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b')],
    });
  });

  it('BYSCORE REV', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrange(db, ['k', '3', '1', 'BYSCORE', 'REV'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('BYSCORE LIMIT', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c', '4', 'd'], rng);
    const result = zrange(
      db,
      ['k', '-inf', '+inf', 'BYSCORE', 'LIMIT', '1', '2'],
      rng
    );
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('BYLEX option', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrange(db, ['k', '[b', '[c', 'BYLEX'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('BYLEX REV', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = zrange(db, ['k', '[c', '[a', 'BYLEX', 'REV'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('nonexistent key returns empty', () => {
    const { db, rng } = createDb();
    expect(zrange(db, ['k', '0', '-1'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('WRONGTYPE error', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zrange(db, ['k', '0', '-1'], rng)).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db, rng } = createDb();
    expect(zrange(db, ['k', '0'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zrange' command")
    );
  });

  it('LIMIT without BYSCORE or BYLEX returns error', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zrange(db, ['k', '0', '-1', 'LIMIT', '0', '2'], rng)).toEqual(
      err(
        'ERR',
        'syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX'
      )
    );
  });

  it('BYSCORE and BYLEX together returns syntax error', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrange(db, ['k', '0', '-1', 'BYSCORE', 'BYLEX'], rng)).toEqual(
      err('ERR', 'syntax error')
    );
  });
});

// --- ZRANGESTORE ---

describe('ZRANGESTORE', () => {
  it('stores rank range result', () => {
    const { db, rng } = createDb();
    zadd(db, ['src', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangestore(db, ['dst', 'src', '0', '1'], rng);
    expect(result).toEqual(integer(2));
    expect(zcard(db, ['dst'])).toEqual(integer(2));
  });

  it('stores BYSCORE range result', () => {
    const { db, rng } = createDb();
    zadd(db, ['src', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangestore(db, ['dst', 'src', '1', '2', 'BYSCORE'], rng);
    expect(result).toEqual(integer(2));
  });

  it('stores BYLEX range result', () => {
    const { db, rng } = createDb();
    zadd(db, ['src', '0', 'a', '0', 'b', '0', 'c'], rng);
    const result = zrangestore(db, ['dst', 'src', '[a', '[b', 'BYLEX'], rng);
    expect(result).toEqual(integer(2));
  });

  it('overwrites existing destination key', () => {
    const { db, rng } = createDb();
    zadd(db, ['dst', '10', 'x', '20', 'y'], rng);
    zadd(db, ['src', '1', 'a', '2', 'b'], rng);
    zrangestore(db, ['dst', 'src', '0', '-1'], rng);
    expect(zcard(db, ['dst'])).toEqual(integer(2));
  });

  it('deletes destination when result is empty', () => {
    const { db, rng } = createDb();
    zadd(db, ['dst', '10', 'x'], rng);
    zadd(db, ['src', '1', 'a'], rng);
    zrangestore(db, ['dst', 'src', '5', '10'], rng);
    expect(db.get('dst')).toBeNull();
  });

  it('returns 0 for nonexistent source', () => {
    const { db, rng } = createDb();
    expect(zrangestore(db, ['dst', 'src', '0', '-1'], rng)).toEqual(ZERO);
  });

  it('REV option', () => {
    const { db, rng } = createDb();
    zadd(db, ['src', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangestore(db, ['dst', 'src', '0', '1', 'REV'], rng);
    expect(result).toEqual(integer(2));
  });

  it('rejects WITHSCORES option', () => {
    const { db, rng } = createDb();
    zadd(db, ['src', '1', 'a', '2', 'b'], rng);
    expect(
      zrangestore(db, ['dst', 'src', '0', '-1', 'WITHSCORES'], rng)
    ).toEqual(err('ERR', 'syntax error'));
  });

  it('destination can be same as source', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrangestore(db, ['k', 'k', '0', '-1'], rng);
    expect(result).toEqual(integer(3));
    expect(zcard(db, ['k'])).toEqual(integer(3));
    expect(zscore(db, ['k', 'a'])).toEqual(bulk('1'));
    expect(zscore(db, ['k', 'c'])).toEqual(bulk('3'));
  });

  it('WRONGTYPE error on source', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['src', 'hello']);
    expect(zrangestore(db, ['dst', 'src', '0', '-1'], rng)).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db, rng } = createDb();
    expect(zrangestore(db, ['dst', 'src', '0'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zrangestore' command")
    );
  });
});
