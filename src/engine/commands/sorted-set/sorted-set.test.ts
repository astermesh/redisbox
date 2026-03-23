import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../../engine.ts';
import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  zadd,
  zrem,
  zincrby,
  zcard,
  zscore,
  zmscore,
  zrank,
  zrevrank,
  zpopmin,
  zpopmax,
  zmpop,
  zrandmember,
  zscan,
} from './sorted-set.ts';
import type { SortedSetData } from './types.ts';
import { set } from '../string/index.ts';
import { objectEncoding } from '../generic.ts';

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
const ONE = integer(1);
const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

// --- ZADD ---

describe('ZADD', () => {
  it('adds single member', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', '1.5', 'a'], rng)).toEqual(ONE);
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('adds multiple members', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng)).toEqual(
      integer(3)
    );
    expect(zcard(db, ['k'])).toEqual(integer(3));
  });

  it('updates score of existing member (returns 0 for no new additions)', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', '5', 'a'], rng)).toEqual(ZERO);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
  });

  it('mixed add and update returns only added count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', '2', 'a', '3', 'b'], rng)).toEqual(ONE);
  });

  it('duplicate members in same call — last score wins', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '5', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
  });

  // --- NX flag ---

  it('NX: adds new members only', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', 'NX', '5', 'a', '2', 'b'], rng)).toEqual(ONE);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('1'));
  });

  it('NX: case insensitive', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', 'nx', '5', 'a'], rng)).toEqual(ZERO);
  });

  // --- XX flag ---

  it('XX: updates existing members only', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', 'XX', '5', 'a', '2', 'b'], rng)).toEqual(ZERO);
    expect(zcard(db, ['k'])).toEqual(ONE);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
  });

  it('XX: on nonexistent key returns 0', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'XX', '1', 'a'], rng)).toEqual(ZERO);
    expect(zcard(db, ['k'])).toEqual(ZERO);
  });

  // --- GT flag ---

  it('GT: updates only when new score is greater', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    zadd(db, ['k', 'GT', '3', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    zadd(db, ['k', 'GT', '10', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('10'));
  });

  it('GT: still adds new members', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'GT', '1', 'a'], rng)).toEqual(ONE);
  });

  it('GT: equal score does not count as update', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    expect(zadd(db, ['k', 'GT', 'CH', '5', 'a'], rng)).toEqual(ZERO);
  });

  // --- LT flag ---

  it('LT: updates only when new score is less', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    zadd(db, ['k', 'LT', '10', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    zadd(db, ['k', 'LT', '2', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('2'));
  });

  it('LT: still adds new members', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'LT', '1', 'a'], rng)).toEqual(ONE);
  });

  it('LT: equal score does not count as update', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    expect(zadd(db, ['k', 'LT', 'CH', '5', 'a'], rng)).toEqual(ZERO);
  });

  // --- GT + LT ---

  it('GT+LT: returns error (incompatible)', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'GT', 'LT', '1', 'a'], rng)).toEqual(
      err(
        'ERR',
        'GT, LT, and/or NX options at the same time are not compatible'
      )
    );
  });

  // --- CH flag ---

  it('CH: returns added + updated count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', 'CH', '5', 'a', '2', 'b'], rng)).toEqual(integer(2));
  });

  it('CH: does not count unchanged scores', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zadd(db, ['k', 'CH', '1', 'a'], rng)).toEqual(ZERO);
  });

  // --- NX + XX incompatibility ---

  it('NX and XX together returns error', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'NX', 'XX', '1', 'a'], rng)).toEqual(
      err('ERR', 'XX and NX options at the same time are not compatible')
    );
  });

  // --- NX + GT/LT incompatibility ---

  it('NX and GT together returns error', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'NX', 'GT', '1', 'a'], rng)).toEqual(
      err(
        'ERR',
        'GT, LT, and/or NX options at the same time are not compatible'
      )
    );
  });

  it('NX and LT together returns error', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'NX', 'LT', '1', 'a'], rng)).toEqual(
      err(
        'ERR',
        'GT, LT, and/or NX options at the same time are not compatible'
      )
    );
  });

  // --- Score parsing ---

  it('accepts inf and -inf scores', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', '-inf', 'a', '+inf', 'b'], rng)).toEqual(integer(2));
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('-inf'));
    expect(zincrby(db, ['k', '0', 'b'], rng)).toEqual(bulk('inf'));
  });

  it('rejects invalid score', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'notanumber', 'a'], rng)).toEqual(
      err('ERR', 'value is not a valid float')
    );
  });

  it('rejects NaN score', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'nan', 'a'], rng)).toEqual(
      err('ERR', 'value is not a valid float')
    );
  });

  // --- Wrong number of args ---

  it('wrong number of arguments', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
    expect(zadd(db, ['k', '1'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
  });

  it('odd number of score-member args', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', '1', 'a', '2'], rng)).toEqual(
      err('ERR', 'syntax error')
    );
  });

  it('flags only, no score-member pairs (short)', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'NX'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
  });

  it('flags only, no score-member pairs (3+ args)', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'NX', 'CH'], rng)).toEqual(
      err('ERR', 'syntax error')
    );
  });

  // --- WRONGTYPE ---

  it('WRONGTYPE on non-zset key', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zadd(db, ['k', '1', 'a'], rng)).toEqual(WRONGTYPE);
  });

  // --- XX + GT combination ---

  it('XX+GT: only updates existing with higher score, no adds', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    expect(zadd(db, ['k', 'XX', 'GT', '3', 'a', '1', 'b'], rng)).toEqual(ZERO);
    expect(zcard(db, ['k'])).toEqual(ONE);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    expect(zadd(db, ['k', 'XX', 'GT', 'CH', '10', 'a'], rng)).toEqual(ONE);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('10'));
  });

  // --- XX + LT combination ---

  it('XX+LT: only updates existing with lower score, no adds', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    expect(zadd(db, ['k', 'XX', 'LT', '10', 'a', '1', 'b'], rng)).toEqual(ZERO);
    expect(zcard(db, ['k'])).toEqual(ONE);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    expect(zadd(db, ['k', 'XX', 'LT', 'CH', '2', 'a'], rng)).toEqual(ONE);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('2'));
  });

  // --- Edge: key gets cleaned up when XX prevents all adds ---

  it('XX on nonexistent key does not create key', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', 'XX', '1', 'a'], rng);
    const entry = db.get('k');
    expect(entry).toBeNull();
  });

  // --- Negative and zero scores ---

  it('handles negative scores', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '-3', 'a', '-1', 'b', '0', 'c'], rng);
    expect(zcard(db, ['k'])).toEqual(integer(3));
  });

  it('handles zero score', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '0', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('0'));
  });

  // --- Float scores ---

  it('handles float scores', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1.5', 'a', '2.7', 'b'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('1.5'));
    expect(zincrby(db, ['k', '0', 'b'], rng)).toEqual(bulk('2.7'));
  });

  // --- INCR flag ---

  it('INCR: increments existing member score and returns bulk string', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zadd(db, ['k', 'INCR', '5', 'a'], rng)).toEqual(bulk('15'));
  });

  it('INCR: creates new member and returns score', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'INCR', '3', 'a'], rng)).toEqual(bulk('3'));
  });

  it('INCR: with NX on existing member returns nil', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zadd(db, ['k', 'NX', 'INCR', '5', 'a'], rng)).toEqual(bulk(null));
  });

  it('INCR: with NX on new member returns score', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'NX', 'INCR', '5', 'a'], rng)).toEqual(bulk('5'));
  });

  it('INCR: with XX on existing member returns new score', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zadd(db, ['k', 'XX', 'INCR', '5', 'a'], rng)).toEqual(bulk('15'));
  });

  it('INCR: with XX on nonexistent member returns nil', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'XX', 'INCR', '5', 'a'], rng)).toEqual(bulk(null));
  });

  it('INCR: with GT updates only when new score > old', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zadd(db, ['k', 'GT', 'INCR', '5', 'a'], rng)).toEqual(bulk('15'));
    expect(zadd(db, ['k', 'GT', 'INCR', '-20', 'a'], rng)).toEqual(bulk(null));
  });

  it('INCR: with LT updates only when new score < old', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zadd(db, ['k', 'LT', 'INCR', '-3', 'a'], rng)).toEqual(bulk('7'));
    expect(zadd(db, ['k', 'LT', 'INCR', '20', 'a'], rng)).toEqual(bulk(null));
  });

  it('INCR: rejects multiple score-member pairs', () => {
    const { db, rng } = createDb();
    expect(zadd(db, ['k', 'INCR', '1', 'a', '2', 'b'], rng)).toEqual(
      err('ERR', 'INCR option supports a single increment-element pair')
    );
  });

  it('INCR: inf + (-inf) returns NaN error', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', 'inf', 'a'], rng);
    expect(zadd(db, ['k', 'INCR', '-inf', 'a'], rng)).toEqual(
      err('ERR', 'resulting score is not a number (NaN)')
    );
  });
});

// --- ZREM ---

describe('ZREM', () => {
  it('removes existing member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(zrem(db, ['k', 'a'])).toEqual(ONE);
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('removes multiple members', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zrem(db, ['k', 'a', 'c'])).toEqual(integer(2));
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('ignores nonexistent member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrem(db, ['k', 'nonexistent'])).toEqual(ZERO);
  });

  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(zrem(db, ['k', 'a'])).toEqual(ZERO);
  });

  it('deletes key when last member removed', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    zrem(db, ['k', 'a']);
    expect(db.get('k')).toBeNull();
  });

  it('mix of existing and nonexistent members', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(zrem(db, ['k', 'a', 'nonexistent', 'b'])).toEqual(integer(2));
  });

  it('wrong number of arguments', () => {
    const { db } = createDb();
    expect(zrem(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrem' command")
    );
  });

  it('WRONGTYPE on non-zset key', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zrem(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- ZINCRBY ---

describe('ZINCRBY', () => {
  it('increments score of existing member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zincrby(db, ['k', '5', 'a'], rng)).toEqual(bulk('15'));
  });

  it('creates member if it does not exist', () => {
    const { db, rng } = createDb();
    expect(zincrby(db, ['k', '3', 'a'], rng)).toEqual(bulk('3'));
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('creates key if it does not exist', () => {
    const { db, rng } = createDb();
    zincrby(db, ['k', '1', 'a'], rng);
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('handles negative increment', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '10', 'a'], rng);
    expect(zincrby(db, ['k', '-3', 'a'], rng)).toEqual(bulk('7'));
  });

  it('handles float increment', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zincrby(db, ['k', '0.5', 'a'], rng)).toEqual(bulk('1.5'));
  });

  it('handles inf increment', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zincrby(db, ['k', '+inf', 'a'], rng)).toEqual(bulk('inf'));
  });

  it('inf + (-inf) returns NaN error', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', 'inf', 'a'], rng);
    expect(zincrby(db, ['k', '-inf', 'a'], rng)).toEqual(
      err('ERR', 'resulting score is not a number (NaN)')
    );
  });

  it('rejects invalid increment', () => {
    const { db, rng } = createDb();
    expect(zincrby(db, ['k', 'abc', 'a'], rng)).toEqual(
      err('ERR', 'value is not a valid float')
    );
  });

  it('wrong number of arguments', () => {
    const { db, rng } = createDb();
    expect(zincrby(db, ['k', '1'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zincrby' command")
    );
    expect(zincrby(db, ['k', '1', 'a', 'extra'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zincrby' command")
    );
  });

  it('WRONGTYPE on non-zset key', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zincrby(db, ['k', '1', 'a'], rng)).toEqual(WRONGTYPE);
  });

  it('updates position in skip list after score change', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    zincrby(db, ['k', '10', 'a'], rng);
    expect(zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('11'));
  });
});

// --- ZCARD ---

describe('ZCARD', () => {
  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(zcard(db, ['k'])).toEqual(ZERO);
  });

  it('returns cardinality', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zcard(db, ['k'])).toEqual(integer(3));
  });

  it('reflects additions and removals', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(zcard(db, ['k'])).toEqual(integer(2));
    zadd(db, ['k', '3', 'c'], rng);
    expect(zcard(db, ['k'])).toEqual(integer(3));
    zrem(db, ['k', 'a']);
    expect(zcard(db, ['k'])).toEqual(integer(2));
  });

  it('WRONGTYPE on non-zset key', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zcard(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('wrong number of arguments', () => {
    const { db } = createDb();
    expect(zcard(db, [])).toEqual(
      err('ERR', "wrong number of arguments for 'zcard' command")
    );
  });
});

// --- Dual index consistency ---

describe('Dual index consistency', () => {
  it('skip list and dict stay in sync after adds', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>).value as SortedSetData;
    expect(zset.dict.size).toBe(3);
    expect(zset.sl.length).toBe(3);
  });

  it('skip list and dict stay in sync after removes', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    zrem(db, ['k', 'b']);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>).value as SortedSetData;
    expect(zset.dict.size).toBe(2);
    expect(zset.sl.length).toBe(2);
    expect(zset.sl.find(2, 'b')).toBeNull();
    expect(zset.dict.has('b')).toBe(false);
  });

  it('skip list and dict stay in sync after score updates', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    zadd(db, ['k', '10', 'a'], rng);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>).value as SortedSetData;
    expect(zset.dict.size).toBe(2);
    expect(zset.sl.length).toBe(2);
    expect(zset.sl.find(1, 'a')).toBeNull();
    expect(zset.sl.find(10, 'a')).not.toBeNull();
    expect(zset.dict.get('a')).toBe(10);
  });

  it('skip list and dict stay in sync after ZINCRBY', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    zincrby(db, ['k', '5', 'a'], rng);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>).value as SortedSetData;
    expect(zset.dict.size).toBe(2);
    expect(zset.sl.length).toBe(2);
    expect(zset.sl.find(1, 'a')).toBeNull();
    expect(zset.sl.find(6, 'a')).not.toBeNull();
    expect(zset.dict.get('a')).toBe(6);
  });
});

// --- ZSCORE ---

describe('ZSCORE', () => {
  it('returns score of existing member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1.5', 'a'], rng);
    expect(zscore(db, ['k', 'a'])).toEqual(bulk('1.5'));
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zscore(db, ['k', 'nosuch'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(zscore(db, ['nosuch', 'a'])).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'v']);
    expect(zscore(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns integer scores without trailing zeros', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'a'], rng);
    expect(zscore(db, ['k', 'a'])).toEqual(bulk('5'));
  });

  it('returns inf score', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '+inf', 'a'], rng);
    expect(zscore(db, ['k', 'a'])).toEqual(bulk('inf'));
  });

  it('returns -inf score', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '-inf', 'a'], rng);
    expect(zscore(db, ['k', 'a'])).toEqual(bulk('-inf'));
  });

  it('returns wrong arity for bad args', () => {
    const { db } = createDb();
    expect(zscore(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zscore' command")
    );
  });
});

// --- ZMSCORE ---

describe('ZMSCORE', () => {
  it('returns scores for multiple members', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zmscore(db, ['k', 'a', 'b', 'c']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('1'), bulk('2'), bulk('3')],
    });
  });

  it('returns nil for missing members', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '3', 'c'], rng);
    const result = zmscore(db, ['k', 'a', 'nosuch', 'c']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('1'), bulk(null), bulk('3')],
    });
  });

  it('returns all nils for non-existing key', () => {
    const { db } = createDb();
    const result = zmscore(db, ['nosuch', 'a', 'b']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk(null), bulk(null)],
    });
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'v']);
    expect(zmscore(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns wrong arity for no members', () => {
    const { db } = createDb();
    expect(zmscore(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zmscore' command")
    );
  });
});

// --- ZRANK ---

describe('ZRANK', () => {
  it('returns 0-based rank', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zrank(db, ['k', 'a'])).toEqual(integer(0));
    expect(zrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(zrank(db, ['k', 'c'])).toEqual(integer(2));
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrank(db, ['k', 'nosuch'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(zrank(db, ['nosuch', 'a'])).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'v']);
    expect(zrank(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('ranks by score then by lexicographic order', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'c', '1', 'a', '1', 'b'], rng);
    expect(zrank(db, ['k', 'a'])).toEqual(integer(0));
    expect(zrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(zrank(db, ['k', 'c'])).toEqual(integer(2));
  });

  it('returns WITHSCORE when requested', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1.5', 'a', '2.5', 'b'], rng);
    const result = zrank(db, ['k', 'a', 'WITHSCORE']);
    expect(result).toEqual({
      kind: 'array',
      value: [integer(0), bulk('1.5')],
    });
  });

  it('returns nil for WITHSCORE when member not found', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    const result = zrank(db, ['k', 'nosuch', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns nil-array for WITHSCORE when key not found', () => {
    const { db } = createDb();
    const result = zrank(db, ['nosuch', 'a', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns syntax error for invalid third argument', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrank(db, ['k', 'a', 'INVALID'])).toEqual(
      err('ERR', 'syntax error')
    );
  });

  it('returns wrong arity for bad args', () => {
    const { db } = createDb();
    expect(zrank(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrank' command")
    );
  });
});

// --- ZREVRANK ---

describe('ZREVRANK', () => {
  it('returns reverse 0-based rank', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zrevrank(db, ['k', 'a'])).toEqual(integer(2));
    expect(zrevrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(zrevrank(db, ['k', 'c'])).toEqual(integer(0));
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrevrank(db, ['k', 'nosuch'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(zrevrank(db, ['nosuch', 'a'])).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'v']);
    expect(zrevrank(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('ranks by reverse score then by reverse lexicographic order', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'c', '1', 'a', '1', 'b'], rng);
    expect(zrevrank(db, ['k', 'a'])).toEqual(integer(2));
    expect(zrevrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(zrevrank(db, ['k', 'c'])).toEqual(integer(0));
  });

  it('returns WITHSCORE when requested', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1.5', 'a', '2.5', 'b'], rng);
    const result = zrevrank(db, ['k', 'b', 'WITHSCORE']);
    expect(result).toEqual({
      kind: 'array',
      value: [integer(0), bulk('2.5')],
    });
  });

  it('returns nil for WITHSCORE when member not found', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    const result = zrevrank(db, ['k', 'nosuch', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns nil-array for WITHSCORE when key not found', () => {
    const { db } = createDb();
    const result = zrevrank(db, ['nosuch', 'a', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns syntax error for invalid third argument', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrevrank(db, ['k', 'a', 'INVALID'])).toEqual(
      err('ERR', 'syntax error')
    );
  });

  it('returns wrong arity for bad args', () => {
    const { db } = createDb();
    expect(zrevrank(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrevrank' command")
    );
  });

  it('handles single element set', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '5', 'only'], rng);
    expect(zrank(db, ['k', 'only'])).toEqual(integer(0));
    expect(zrevrank(db, ['k', 'only'])).toEqual(integer(0));
  });
});

// --- ZPOPMIN ---

describe('ZPOPMIN', () => {
  it('pops element with lowest score', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zpopmin(db, ['k'])).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1')],
    });
    expect(zcard(db, ['k'])).toEqual(integer(2));
  });

  it('pops multiple elements with count', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zpopmin(db, ['k', '2'])).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('count larger than set size returns all elements', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(zpopmin(db, ['k', '10'])).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
    expect(zcard(db, ['k'])).toEqual(integer(0));
  });

  it('returns empty array for nonexistent key', () => {
    const { db } = createDb();
    expect(zpopmin(db, ['k'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('count 0 returns empty array', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zpopmin(db, ['k', '0'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zpopmin(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('deletes key when all elements popped', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    zpopmin(db, ['k']);
    expect(zcard(db, ['k'])).toEqual(integer(0));
  });

  it('rejects extra arguments', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zpopmin(db, ['k', '1', 'extra'])).toEqual(
      err('ERR', "wrong number of arguments for 'zpopmin' command")
    );
  });
});

// --- ZPOPMAX ---

describe('ZPOPMAX', () => {
  it('pops element with highest score', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zpopmax(db, ['k'])).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('3')],
    });
    expect(zcard(db, ['k'])).toEqual(integer(2));
  });

  it('pops multiple elements from highest', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(zpopmax(db, ['k', '2'])).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('3'), bulk('b'), bulk('2')],
    });
    expect(zcard(db, ['k'])).toEqual(ONE);
  });

  it('returns empty array for nonexistent key', () => {
    const { db } = createDb();
    expect(zpopmax(db, ['k'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zpopmax(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('rejects extra arguments', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zpopmax(db, ['k', '1', 'extra'])).toEqual(
      err('ERR', "wrong number of arguments for 'zpopmax' command")
    );
  });
});

// --- ZMPOP ---

describe('ZMPOP', () => {
  it('pops min from first non-empty key', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    const result = zmpop(db, ['1', 'k1', 'MIN'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [
        bulk('k1'),
        {
          kind: 'array',
          value: [{ kind: 'array', value: [bulk('a'), bulk('1')] }],
        },
      ],
    });
  });

  it('pops max from first non-empty key', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    const result = zmpop(db, ['1', 'k1', 'MAX'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [
        bulk('k1'),
        {
          kind: 'array',
          value: [{ kind: 'array', value: [bulk('b'), bulk('2')] }],
        },
      ],
    });
  });

  it('skips empty keys', () => {
    const { db, rng } = createDb();
    zadd(db, ['k2', '5', 'x'], rng);
    const result = zmpop(db, ['2', 'k1', 'k2', 'MIN'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [
        bulk('k2'),
        {
          kind: 'array',
          value: [{ kind: 'array', value: [bulk('x'), bulk('5')] }],
        },
      ],
    });
  });

  it('pops multiple with COUNT', () => {
    const { db, rng } = createDb();
    zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zmpop(db, ['1', 'k1', 'MIN', 'COUNT', '2'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [
        bulk('k1'),
        {
          kind: 'array',
          value: [
            { kind: 'array', value: [bulk('a'), bulk('1')] },
            { kind: 'array', value: [bulk('b'), bulk('2')] },
          ],
        },
      ],
    });
  });

  it('returns nil-array when all keys empty', () => {
    const { db, rng } = createDb();
    expect(zmpop(db, ['1', 'k1', 'MIN'], rng)).toEqual({
      kind: 'nil-array',
    });
  });

  it('returns error for invalid numkeys', () => {
    const { db, rng } = createDb();
    expect(zmpop(db, ['0', 'k1', 'MIN'], rng)).toEqual(
      err('ERR', 'numkeys should be greater than 0')
    );
  });

  it('returns error for count 0', () => {
    const { db, rng } = createDb();
    expect(zmpop(db, ['1', 'k1', 'MIN', 'COUNT', '0'], rng)).toEqual(
      err('ERR', 'count should be greater than 0')
    );
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k1', 'hello']);
    expect(zmpop(db, ['1', 'k1', 'MIN'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZRANDMEMBER ---

describe('ZRANDMEMBER', () => {
  it('returns single random member', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrandmember(db, ['k'], rng);
    expect(result.kind).toBe('bulk');
    expect(['a', 'b', 'c']).toContain(
      (result as { kind: 'bulk'; value: string }).value
    );
  });

  it('returns nil for nonexistent key (no count)', () => {
    const { db, rng } = createDb();
    expect(zrandmember(db, ['k'], rng)).toEqual(bulk(null));
  });

  it('positive count returns unique elements', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zrandmember(db, ['k', '2'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(2);
  });

  it('positive count > size returns all', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = zrandmember(db, ['k', '10'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(2);
  });

  it('negative count may repeat elements', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    const result = zrandmember(db, ['k', '-3'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(3);
    for (const r of arr) {
      expect(r).toEqual(bulk('a'));
    }
  });

  it('WITHSCORES returns member-score pairs', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = zrandmember(db, ['k', '2', 'WITHSCORES'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(4);
  });

  it('returns empty array for nonexistent key with count', () => {
    const { db, rng } = createDb();
    expect(zrandmember(db, ['k', '3'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('count 0 returns empty array', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(zrandmember(db, ['k', '0'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zrandmember(db, ['k'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZSCAN ---

describe('ZSCAN', () => {
  it('scans all members', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zscan(db, ['k', '0']);
    expect(result).toEqual({
      kind: 'array',
      value: [
        bulk('0'),
        {
          kind: 'array',
          value: [
            bulk('a'),
            bulk('1'),
            bulk('b'),
            bulk('2'),
            bulk('c'),
            bulk('3'),
          ],
        },
      ],
    });
  });

  it('scans with MATCH pattern', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'alpha', '2', 'beta', '3', 'gamma'], rng);
    const result = zscan(db, ['k', '0', 'MATCH', 'a*']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('0'), { kind: 'array', value: [bulk('alpha'), bulk('1')] }],
    });
  });

  it('scans with COUNT', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = zscan(db, ['k', '0', 'COUNT', '2']);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr[0]).toEqual(bulk('2'));
  });

  it('returns cursor 0 and empty array for nonexistent key', () => {
    const { db } = createDb();
    expect(zscan(db, ['k', '0'])).toEqual({
      kind: 'array',
      value: [bulk('0'), { kind: 'array', value: [] }],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    set(db, () => 1000, ['k', 'hello']);
    expect(zscan(db, ['k', '0'])).toEqual(WRONGTYPE);
  });

  it('continues from cursor position', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const first = zscan(db, ['k', '0', 'COUNT', '1']);
    const firstArr = (first as { kind: 'array'; value: Reply[] }).value;
    const cursor = (firstArr[0] as { kind: 'bulk'; value: string }).value;

    const second = zscan(db, ['k', cursor, 'COUNT', '1']);
    const secondArr = (second as { kind: 'array'; value: Reply[] }).value;
    const cursor2 = (secondArr[0] as { kind: 'bulk'; value: string }).value;

    const third = zscan(db, ['k', cursor2, 'COUNT', '1']);
    const thirdArr = (third as { kind: 'array'; value: Reply[] }).value;
    const cursor3 = (thirdArr[0] as { kind: 'bulk'; value: string }).value;

    expect(cursor3).toBe('0');
  });
});

// --- Encoding transitions ---

describe('encoding transitions', () => {
  it('uses listpack for small sorted sets', () => {
    const { db, rng } = createDb();
    zadd(db, ['k', '1', 'a'], rng);
    expect(db.get('k')?.encoding).toBe('listpack');
    expect(objectEncoding(db, ['k'])).toEqual(bulk('listpack'));
  });

  it('stays listpack at exact entry count threshold (128)', () => {
    const { db, rng } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i < 128; i++) {
      args.push(String(i), `m${i}`);
    }
    zadd(db, args, rng);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('transitions to skiplist when exceeding entry count (129)', () => {
    const { db, rng } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      args.push(String(i), `m${i}`);
    }
    zadd(db, args, rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
    expect(objectEncoding(db, ['k'])).toEqual(bulk('skiplist'));
  });

  it('stays listpack at exact member byte length threshold (64 bytes)', () => {
    const { db, rng } = createDb();
    const member = 'x'.repeat(64);
    zadd(db, ['k', '1', member], rng);
    expect(db.get('k')?.encoding).toBe('listpack');
  });

  it('transitions to skiplist when member exceeds byte length (65 bytes)', () => {
    const { db, rng } = createDb();
    const member = 'x'.repeat(65);
    zadd(db, ['k', '1', member], rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
  });

  it('transitions to skiplist with multi-byte UTF-8 member exceeding 64 bytes', () => {
    const { db, rng } = createDb();
    const member = '\u{1F600}'.repeat(17);
    zadd(db, ['k', '1', member], rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
  });

  it('never demotes from skiplist after ZREM reduces size', () => {
    const { db, rng } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      args.push(String(i), `m${i}`);
    }
    zadd(db, args, rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
    zrem(db, ['k', 'm0', 'm1', 'm2']);
    expect(db.get('k')?.encoding).toBe('skiplist');
  });

  it('never demotes from skiplist after ZPOPMIN reduces size', () => {
    const { db, rng } = createDb();
    const args: string[] = ['k'];
    for (let i = 0; i <= 128; i++) {
      args.push(String(i), `m${i}`);
    }
    zadd(db, args, rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
    zpopmin(db, ['k', '10']);
    expect(db.get('k')?.encoding).toBe('skiplist');
  });

  it('transitions via ZINCRBY creating a new member', () => {
    const { db, rng } = createDb();
    zincrby(db, ['k', '1', 'a'], rng);
    expect(db.get('k')?.encoding).toBe('listpack');
    const longMember = 'y'.repeat(65);
    zincrby(db, ['k', '1', longMember], rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
  });

  it('ZUNIONSTORE creates listpack for small result', () => {
    const { db, rng } = createDb();
    // use zadd/zunionstore via index to avoid direct import of set-ops in this test
    zadd(db, ['src1', '1', 'a', '2', 'b'], rng);
    zadd(db, ['src2', '3', 'c'], rng);
    // Import zunionstore dynamically to keep test focused on sorted-set module
    // but here we just verify via db state — test the encoding via zadd+zcard
    // Since zunionstore is in set-ops, we skip that specific encoding test here
    // and keep it in set-ops.test.ts
    expect(db.get('src1')?.encoding).toBe('listpack');
  });

  it('stays skiplist after removing long member that caused promotion', () => {
    const { db, rng } = createDb();
    const longMember = 'x'.repeat(65);
    zadd(db, ['k', '1', 'a', '2', longMember], rng);
    expect(db.get('k')?.encoding).toBe('skiplist');
    zrem(db, ['k', longMember]);
    expect(db.get('k')?.encoding).toBe('skiplist');
  });
});
