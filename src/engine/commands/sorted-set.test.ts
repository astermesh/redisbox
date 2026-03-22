import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as sortedSet from './sorted-set.ts';
import * as string from './string.ts';

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
    expect(sortedSet.zadd(db, ['k', '1.5', 'a'], rng)).toEqual(ONE);
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
  });

  it('adds multiple members', () => {
    const { db, rng } = createDb();
    expect(
      sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng)
    ).toEqual(integer(3));
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(3));
  });

  it('updates score of existing member (returns 0 for no new additions)', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', '5', 'a'], rng)).toEqual(ZERO);
    // Verify score was updated via ZINCRBY with 0
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
  });

  it('mixed add and update returns only added count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', '2', 'a', '3', 'b'], rng)).toEqual(ONE);
  });

  it('duplicate members in same call — last score wins', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '5', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
  });

  // --- NX flag ---

  it('NX: adds new members only', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'NX', '5', 'a', '2', 'b'], rng)).toEqual(
      ONE
    );
    // 'a' should still have score 1
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('1'));
  });

  it('NX: case insensitive', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'nx', '5', 'a'], rng)).toEqual(ZERO);
  });

  // --- XX flag ---

  it('XX: updates existing members only', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'XX', '5', 'a', '2', 'b'], rng)).toEqual(
      ZERO
    );
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
  });

  it('XX: on nonexistent key returns 0', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'XX', '1', 'a'], rng)).toEqual(ZERO);
    // Key should not be created
    expect(sortedSet.zcard(db, ['k'])).toEqual(ZERO);
  });

  // --- GT flag ---

  it('GT: updates only when new score is greater', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    // Try to update with lower score
    sortedSet.zadd(db, ['k', 'GT', '3', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    // Update with higher score
    sortedSet.zadd(db, ['k', 'GT', '10', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('10'));
  });

  it('GT: still adds new members', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'GT', '1', 'a'], rng)).toEqual(ONE);
  });

  it('GT: equal score does not count as update', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'GT', 'CH', '5', 'a'], rng)).toEqual(ZERO);
  });

  // --- LT flag ---

  it('LT: updates only when new score is less', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    // Try to update with higher score
    sortedSet.zadd(db, ['k', 'LT', '10', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    // Update with lower score
    sortedSet.zadd(db, ['k', 'LT', '2', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('2'));
  });

  it('LT: still adds new members', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'LT', '1', 'a'], rng)).toEqual(ONE);
  });

  it('LT: equal score does not count as update', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'LT', 'CH', '5', 'a'], rng)).toEqual(ZERO);
  });

  // --- GT + LT ---

  it('GT+LT: returns error (incompatible)', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'GT', 'LT', '1', 'a'], rng)).toEqual(
      err(
        'ERR',
        'GT, LT, and/or NX options at the same time are not compatible'
      )
    );
  });

  // --- CH flag ---

  it('CH: returns added + updated count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    // Update a, add b → CH returns 2
    expect(sortedSet.zadd(db, ['k', 'CH', '5', 'a', '2', 'b'], rng)).toEqual(
      integer(2)
    );
  });

  it('CH: does not count unchanged scores', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    // Same score → not changed
    expect(sortedSet.zadd(db, ['k', 'CH', '1', 'a'], rng)).toEqual(ZERO);
  });

  // --- NX + XX incompatibility ---

  it('NX and XX together returns error', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'NX', 'XX', '1', 'a'], rng)).toEqual(
      err('ERR', 'XX and NX options at the same time are not compatible')
    );
  });

  // --- NX + GT/LT incompatibility ---

  it('NX and GT together returns error', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'NX', 'GT', '1', 'a'], rng)).toEqual(
      err(
        'ERR',
        'GT, LT, and/or NX options at the same time are not compatible'
      )
    );
  });

  it('NX and LT together returns error', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'NX', 'LT', '1', 'a'], rng)).toEqual(
      err(
        'ERR',
        'GT, LT, and/or NX options at the same time are not compatible'
      )
    );
  });

  // --- Score parsing ---

  it('accepts inf and -inf scores', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', '-inf', 'a', '+inf', 'b'], rng)).toEqual(
      integer(2)
    );
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('-inf'));
    expect(sortedSet.zincrby(db, ['k', '0', 'b'], rng)).toEqual(bulk('inf'));
  });

  it('rejects invalid score', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'notanumber', 'a'], rng)).toEqual(
      err('ERR', 'value is not a valid float')
    );
  });

  it('rejects NaN score', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'nan', 'a'], rng)).toEqual(
      err('ERR', 'value is not a valid float')
    );
  });

  // --- Wrong number of args ---

  it('wrong number of arguments', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
    expect(sortedSet.zadd(db, ['k', '1'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
  });

  it('odd number of score-member args', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', '1', 'a', '2'], rng)).toEqual(
      err('ERR', 'syntax error')
    );
  });

  it('flags only, no score-member pairs (short)', () => {
    const { db, rng } = createDb();
    // With only 2 args (key + flag), hits arity check first
    expect(sortedSet.zadd(db, ['k', 'NX'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
  });

  it('flags only, no score-member pairs (3+ args)', () => {
    const { db, rng } = createDb();
    // With 3 args (key + two flags), passes arity but fails syntax check
    expect(sortedSet.zadd(db, ['k', 'NX', 'CH'], rng)).toEqual(
      err('ERR', 'syntax error')
    );
  });

  // --- WRONGTYPE ---

  it('WRONGTYPE on non-zset key', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zadd(db, ['k', '1', 'a'], rng)).toEqual(WRONGTYPE);
  });

  // --- XX + GT combination ---

  it('XX+GT: only updates existing with higher score, no adds', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    // Try to add new member: should be ignored (XX)
    // Try to update 'a' with lower score: should be ignored (GT)
    expect(
      sortedSet.zadd(db, ['k', 'XX', 'GT', '3', 'a', '1', 'b'], rng)
    ).toEqual(ZERO);
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    // Update 'a' with higher score: should work
    expect(sortedSet.zadd(db, ['k', 'XX', 'GT', 'CH', '10', 'a'], rng)).toEqual(
      ONE
    );
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('10'));
  });

  // --- XX + LT combination ---

  it('XX+LT: only updates existing with lower score, no adds', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    expect(
      sortedSet.zadd(db, ['k', 'XX', 'LT', '10', 'a', '1', 'b'], rng)
    ).toEqual(ZERO);
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('5'));
    expect(sortedSet.zadd(db, ['k', 'XX', 'LT', 'CH', '2', 'a'], rng)).toEqual(
      ONE
    );
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('2'));
  });

  // --- Edge: key gets cleaned up when XX prevents all adds ---

  it('XX on nonexistent key does not create key', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', 'XX', '1', 'a'], rng);
    const entry = db.get('k');
    expect(entry).toBeNull();
  });

  // --- Negative and zero scores ---

  it('handles negative scores', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '-3', 'a', '-1', 'b', '0', 'c'], rng);
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(3));
  });

  it('handles zero score', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('0'));
  });

  // --- Float scores ---

  it('handles float scores', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1.5', 'a', '2.7', 'b'], rng);
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('1.5'));
    expect(sortedSet.zincrby(db, ['k', '0', 'b'], rng)).toEqual(bulk('2.7'));
  });

  // --- INCR flag ---

  it('INCR: increments existing member score and returns bulk string', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'INCR', '5', 'a'], rng)).toEqual(
      bulk('15')
    );
  });

  it('INCR: creates new member and returns score', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'INCR', '3', 'a'], rng)).toEqual(bulk('3'));
  });

  it('INCR: with NX on existing member returns nil', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'NX', 'INCR', '5', 'a'], rng)).toEqual(
      bulk(null)
    );
  });

  it('INCR: with NX on new member returns score', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'NX', 'INCR', '5', 'a'], rng)).toEqual(
      bulk('5')
    );
  });

  it('INCR: with XX on existing member returns new score', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'XX', 'INCR', '5', 'a'], rng)).toEqual(
      bulk('15')
    );
  });

  it('INCR: with XX on nonexistent member returns nil', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'XX', 'INCR', '5', 'a'], rng)).toEqual(
      bulk(null)
    );
  });

  it('INCR: with GT updates only when new score > old', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    // +5 → 15 > 10, should update
    expect(sortedSet.zadd(db, ['k', 'GT', 'INCR', '5', 'a'], rng)).toEqual(
      bulk('15')
    );
    // -20 → -5 < 15, should return nil
    expect(sortedSet.zadd(db, ['k', 'GT', 'INCR', '-20', 'a'], rng)).toEqual(
      bulk(null)
    );
  });

  it('INCR: with LT updates only when new score < old', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    // -3 → 7 < 10, should update
    expect(sortedSet.zadd(db, ['k', 'LT', 'INCR', '-3', 'a'], rng)).toEqual(
      bulk('7')
    );
    // +20 → 27 > 7, should return nil
    expect(sortedSet.zadd(db, ['k', 'LT', 'INCR', '20', 'a'], rng)).toEqual(
      bulk(null)
    );
  });

  it('INCR: rejects multiple score-member pairs', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'INCR', '1', 'a', '2', 'b'], rng)).toEqual(
      err('ERR', 'INCR option supports a single increment-element pair')
    );
  });

  it('INCR: inf + (-inf) returns NaN error', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', 'inf', 'a'], rng);
    expect(sortedSet.zadd(db, ['k', 'INCR', '-inf', 'a'], rng)).toEqual(
      err('ERR', 'resulting score is not a number (NaN)')
    );
  });
});

// --- ZREM ---

describe('ZREM', () => {
  it('removes existing member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(sortedSet.zrem(db, ['k', 'a'])).toEqual(ONE);
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
  });

  it('removes multiple members', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zrem(db, ['k', 'a', 'c'])).toEqual(integer(2));
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
  });

  it('ignores nonexistent member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zrem(db, ['k', 'nonexistent'])).toEqual(ZERO);
  });

  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zrem(db, ['k', 'a'])).toEqual(ZERO);
  });

  it('deletes key when last member removed', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    sortedSet.zrem(db, ['k', 'a']);
    expect(db.get('k')).toBeNull();
  });

  it('mix of existing and nonexistent members', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(sortedSet.zrem(db, ['k', 'a', 'nonexistent', 'b'])).toEqual(
      integer(2)
    );
  });

  it('wrong number of arguments', () => {
    const { db } = createDb();
    expect(sortedSet.zrem(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrem' command")
    );
  });

  it('WRONGTYPE on non-zset key', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zrem(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });
});

// --- ZINCRBY ---

describe('ZINCRBY', () => {
  it('increments score of existing member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '5', 'a'], rng)).toEqual(bulk('15'));
  });

  it('creates member if it does not exist', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zincrby(db, ['k', '3', 'a'], rng)).toEqual(bulk('3'));
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
  });

  it('creates key if it does not exist', () => {
    const { db, rng } = createDb();
    sortedSet.zincrby(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zcard(db, ['k'])).toEqual(ONE);
  });

  it('handles negative increment', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '10', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '-3', 'a'], rng)).toEqual(bulk('7'));
  });

  it('handles float increment', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '0.5', 'a'], rng)).toEqual(bulk('1.5'));
  });

  it('handles inf increment', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '+inf', 'a'], rng)).toEqual(bulk('inf'));
  });

  it('inf + (-inf) returns NaN error', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', 'inf', 'a'], rng);
    expect(sortedSet.zincrby(db, ['k', '-inf', 'a'], rng)).toEqual(
      err('ERR', 'resulting score is not a number (NaN)')
    );
  });

  it('rejects invalid increment', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zincrby(db, ['k', 'abc', 'a'], rng)).toEqual(
      err('ERR', 'value is not a valid float')
    );
  });

  it('wrong number of arguments', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zincrby(db, ['k', '1'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zincrby' command")
    );
    expect(sortedSet.zincrby(db, ['k', '1', 'a', 'extra'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zincrby' command")
    );
  });

  it('WRONGTYPE on non-zset key', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zincrby(db, ['k', '1', 'a'], rng)).toEqual(WRONGTYPE);
  });

  it('updates position in skip list after score change', () => {
    const { db, rng } = createDb();
    // Add a < b < c by score
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    // Increment 'a' so it has highest score
    sortedSet.zincrby(db, ['k', '10', 'a'], rng);
    // a now has score 11, should be last in order
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('11'));
  });
});

// --- ZCARD ---

describe('ZCARD', () => {
  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zcard(db, ['k'])).toEqual(ZERO);
  });

  it('returns cardinality', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(3));
  });

  it('reflects additions and removals', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(2));
    sortedSet.zadd(db, ['k', '3', 'c'], rng);
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(3));
    sortedSet.zrem(db, ['k', 'a']);
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(2));
  });

  it('WRONGTYPE on non-zset key', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zcard(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('wrong number of arguments', () => {
    const { db } = createDb();
    expect(sortedSet.zcard(db, [])).toEqual(
      err('ERR', "wrong number of arguments for 'zcard' command")
    );
  });
});

// --- ZCOUNT ---

describe('ZCOUNT', () => {
  it('counts all elements with -inf +inf', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcount(db, ['k', '-inf', '+inf'])).toEqual(integer(3));
  });

  it('counts elements in inclusive range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcount(db, ['k', '1', '2'])).toEqual(integer(2));
  });

  it('counts elements with exclusive min', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcount(db, ['k', '(1', '3'])).toEqual(integer(2));
  });

  it('counts elements with exclusive max', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcount(db, ['k', '1', '(3'])).toEqual(integer(2));
  });

  it('counts elements with both exclusive', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcount(db, ['k', '(1', '(3'])).toEqual(integer(1));
  });

  it('returns 0 for empty range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zcount(db, ['k', '(3', '+inf'])).toEqual(ZERO);
  });

  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zcount(db, ['k', '-inf', '+inf'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zcount(db, ['k', '-inf', '+inf'])).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db } = createDb();
    expect(sortedSet.zcount(db, ['k', '0'])).toEqual(
      err('ERR', "wrong number of arguments for 'zcount' command")
    );
  });

  it('rejects invalid min', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zcount(db, ['k', 'notanumber', '5'])).toEqual(
      err('ERR', 'min or max is not a float')
    );
  });

  it('rejects invalid max', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zcount(db, ['k', '0', 'notanumber'])).toEqual(
      err('ERR', 'min or max is not a float')
    );
  });
});

// --- ZLEXCOUNT ---

describe('ZLEXCOUNT', () => {
  it('counts all with - +', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    expect(sortedSet.zlexcount(db, ['k', '-', '+'])).toEqual(integer(4));
  });

  it('counts inclusive range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    expect(sortedSet.zlexcount(db, ['k', '[b', '[c'])).toEqual(integer(2));
  });

  it('counts exclusive range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    expect(sortedSet.zlexcount(db, ['k', '(a', '(d'])).toEqual(integer(2));
  });

  it('returns 0 for empty range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c'], rng);
    expect(sortedSet.zlexcount(db, ['k', '(c', '+'])).toEqual(ZERO);
  });

  it('returns 0 for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zlexcount(db, ['k', '-', '+'])).toEqual(ZERO);
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zlexcount(db, ['k', '-', '+'])).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db } = createDb();
    expect(sortedSet.zlexcount(db, ['k', '-'])).toEqual(
      err('ERR', "wrong number of arguments for 'zlexcount' command")
    );
  });

  it('rejects invalid min spec', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a'], rng);
    expect(sortedSet.zlexcount(db, ['k', 'a', '+'])).toEqual(
      err('ERR', 'min or max not valid string range item')
    );
  });
});

// --- ZRANGEBYSCORE ---

describe('ZRANGEBYSCORE', () => {
  it('returns elements in score range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangebyscore(db, ['k', '1', '2']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('returns all with -inf +inf', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangebyscore(db, ['k', '-inf', '+inf']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b'), bulk('c')],
    });
  });

  it('handles exclusive bounds', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangebyscore(db, ['k', '(1', '(3']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b')],
    });
  });

  it('WITHSCORES option', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zrangebyscore(db, [
      'k',
      '-inf',
      '+inf',
      'WITHSCORES',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(
      db,
      ['k', '1', 'a', '2', 'b', '3', 'c', '4', 'd', '5', 'e'],
      rng
    );
    const result = sortedSet.zrangebyscore(db, [
      'k',
      '-inf',
      '+inf',
      'LIMIT',
      '1',
      '2',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('LIMIT with WITHSCORES', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangebyscore(db, [
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
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangebyscore(db, [
      'k',
      '-inf',
      '+inf',
      'LIMIT',
      '1',
      '-1',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('empty result for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zrangebyscore(db, ['k', '-inf', '+inf'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('WRONGTYPE error', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zrangebyscore(db, ['k', '0', '1'])).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db } = createDb();
    expect(sortedSet.zrangebyscore(db, ['k', '0'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrangebyscore' command")
    );
  });
});

// --- ZREVRANGEBYSCORE ---

describe('ZREVRANGEBYSCORE', () => {
  it('returns elements in reverse score order (max first)', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrevrangebyscore(db, ['k', '3', '1']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('handles exclusive bounds', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrevrangebyscore(db, ['k', '(3', '(1']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b')],
    });
  });

  it('WITHSCORES', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrevrangebyscore(db, [
      'k',
      '+inf',
      '-inf',
      'WITHSCORES',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('3'), bulk('b'), bulk('2'), bulk('a'), bulk('1')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c', '4', 'd'], rng);
    const result = sortedSet.zrevrangebyscore(db, [
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
    expect(sortedSet.zrevrangebyscore(db, ['k', '+inf', '-inf'])).toEqual({
      kind: 'array',
      value: [],
    });
  });
});

// --- ZRANGEBYLEX ---

describe('ZRANGEBYLEX', () => {
  it('returns all elements with - +', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrangebylex(db, ['k', '-', '+']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b'), bulk('c'), bulk('d')],
    });
  });

  it('inclusive range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrangebylex(db, ['k', '[b', '[c']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('exclusive range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrangebylex(db, ['k', '(a', '(d']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrangebylex(db, [
      'k',
      '-',
      '+',
      'LIMIT',
      '1',
      '2',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('empty result for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zrangebylex(db, ['k', '-', '+'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('rejects invalid lex min', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a'], rng);
    expect(sortedSet.zrangebylex(db, ['k', 'invalid', '+'])).toEqual(
      err('ERR', 'min or max not valid string range item')
    );
  });

  it('WRONGTYPE error', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zrangebylex(db, ['k', '-', '+'])).toEqual(WRONGTYPE);
  });
});

// --- ZREVRANGEBYLEX ---

describe('ZREVRANGEBYLEX', () => {
  it('returns all elements in reverse with + -', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrevrangebylex(db, ['k', '+', '-']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('d'), bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('inclusive range (max first arg)', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrevrangebylex(db, ['k', '[c', '[b']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });

  it('exclusive range', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrevrangebylex(db, ['k', '(d', '(a']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });

  it('LIMIT offset count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrevrangebylex(db, [
      'k',
      '+',
      '-',
      'LIMIT',
      '1',
      '2',
    ]);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b')],
    });
  });
});

// --- ZRANGE (unified) ---

describe('ZRANGE', () => {
  it('returns range by rank (default)', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '0', '1'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('returns all with 0 -1', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '0', '-1'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b'), bulk('c')],
    });
  });

  it('negative indices', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '-2', '-1'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('out of range indices clamped', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zrange(db, ['k', '-100', '100'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('start > end returns empty', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zrange(db, ['k', '2', '0'], rng);
    expect(result).toEqual({ kind: 'array', value: [] });
  });

  it('WITHSCORES', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zrange(db, ['k', '0', '-1', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('REV option reverses rank order', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '0', '-1', 'REV'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('BYSCORE option', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '1', '2', 'BYSCORE'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('b')],
    });
  });

  it('BYSCORE with exclusive bounds', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '(1', '(3', 'BYSCORE'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b')],
    });
  });

  it('BYSCORE REV', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrange(db, ['k', '3', '1', 'BYSCORE', 'REV'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('BYSCORE LIMIT', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c', '4', 'd'], rng);
    const result = sortedSet.zrange(
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
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrange(db, ['k', '[b', '[c', 'BYLEX'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('c')],
    });
  });

  it('BYLEX REV', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '0', 'a', '0', 'b', '0', 'c', '0', 'd'], rng);
    const result = sortedSet.zrange(db, ['k', '[c', '[a', 'BYLEX', 'REV'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('b'), bulk('a')],
    });
  });

  it('nonexistent key returns empty', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zrange(db, ['k', '0', '-1'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('WRONGTYPE error', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zrange(db, ['k', '0', '-1'], rng)).toEqual(WRONGTYPE);
  });

  it('wrong arity', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zrange(db, ['k', '0'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zrange' command")
    );
  });

  it('LIMIT without BYSCORE or BYLEX returns error', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(
      sortedSet.zrange(db, ['k', '0', '-1', 'LIMIT', '0', '2'], rng)
    ).toEqual(
      err(
        'ERR',
        'syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX'
      )
    );
  });

  it('BYSCORE and BYLEX together returns syntax error', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(
      sortedSet.zrange(db, ['k', '0', '-1', 'BYSCORE', 'BYLEX'], rng)
    ).toEqual(err('ERR', 'syntax error'));
  });
});

// --- ZRANGESTORE ---

describe('ZRANGESTORE', () => {
  it('stores rank range result', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['src', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangestore(db, ['dst', 'src', '0', '1'], rng);
    expect(result).toEqual(integer(2));
    expect(sortedSet.zcard(db, ['dst'])).toEqual(integer(2));
  });

  it('stores BYSCORE range result', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['src', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangestore(
      db,
      ['dst', 'src', '1', '2', 'BYSCORE'],
      rng
    );
    expect(result).toEqual(integer(2));
  });

  it('stores BYLEX range result', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['src', '0', 'a', '0', 'b', '0', 'c'], rng);
    const result = sortedSet.zrangestore(
      db,
      ['dst', 'src', '[a', '[b', 'BYLEX'],
      rng
    );
    expect(result).toEqual(integer(2));
  });

  it('overwrites existing destination key', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['dst', '10', 'x', '20', 'y'], rng);
    sortedSet.zadd(db, ['src', '1', 'a', '2', 'b'], rng);
    sortedSet.zrangestore(db, ['dst', 'src', '0', '-1'], rng);
    expect(sortedSet.zcard(db, ['dst'])).toEqual(integer(2));
  });

  it('deletes destination when result is empty', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['dst', '10', 'x'], rng);
    sortedSet.zadd(db, ['src', '1', 'a'], rng);
    sortedSet.zrangestore(db, ['dst', 'src', '5', '10'], rng);
    expect(db.get('dst')).toBeNull();
  });

  it('returns 0 for nonexistent source', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zrangestore(db, ['dst', 'src', '0', '-1'], rng)).toEqual(
      ZERO
    );
  });

  it('REV option', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['src', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangestore(
      db,
      ['dst', 'src', '0', '1', 'REV'],
      rng
    );
    expect(result).toEqual(integer(2));
  });

  it('rejects WITHSCORES option', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['src', '1', 'a', '2', 'b'], rng);
    expect(
      sortedSet.zrangestore(db, ['dst', 'src', '0', '-1', 'WITHSCORES'], rng)
    ).toEqual(err('ERR', 'syntax error'));
  });

  it('destination can be same as source', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrangestore(db, ['k', 'k', '0', '-1'], rng);
    expect(result).toEqual(integer(3));
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(3));
    expect(sortedSet.zscore(db, ['k', 'a'])).toEqual(bulk('1'));
    expect(sortedSet.zscore(db, ['k', 'c'])).toEqual(bulk('3'));
  });

  it('WRONGTYPE error on source', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['src', 'hello']);
    expect(sortedSet.zrangestore(db, ['dst', 'src', '0', '-1'], rng)).toEqual(
      WRONGTYPE
    );
  });

  it('wrong arity', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zrangestore(db, ['dst', 'src', '0'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zrangestore' command")
    );
  });
});

// --- Dual index consistency ---

describe('Dual index consistency', () => {
  it('skip list and dict stay in sync after adds', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>)
      .value as sortedSet.SortedSetData;
    expect(zset.dict.size).toBe(3);
    expect(zset.sl.length).toBe(3);
  });

  it('skip list and dict stay in sync after removes', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    sortedSet.zrem(db, ['k', 'b']);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>)
      .value as sortedSet.SortedSetData;
    expect(zset.dict.size).toBe(2);
    expect(zset.sl.length).toBe(2);
    // Verify 'b' is not in skip list
    expect(zset.sl.find(2, 'b')).toBeNull();
    expect(zset.dict.has('b')).toBe(false);
  });

  it('skip list and dict stay in sync after score updates', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    sortedSet.zadd(db, ['k', '10', 'a'], rng); // Update a's score
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>)
      .value as sortedSet.SortedSetData;
    expect(zset.dict.size).toBe(2);
    expect(zset.sl.length).toBe(2);
    // Old score node should be gone
    expect(zset.sl.find(1, 'a')).toBeNull();
    // New score node should exist
    expect(zset.sl.find(10, 'a')).not.toBeNull();
    expect(zset.dict.get('a')).toBe(10);
  });

  it('skip list and dict stay in sync after ZINCRBY', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    sortedSet.zincrby(db, ['k', '5', 'a'], rng);
    const entry = db.get('k');
    expect(entry).not.toBeNull();
    const zset = (entry as NonNullable<typeof entry>)
      .value as sortedSet.SortedSetData;
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
    sortedSet.zadd(db, ['k', '1.5', 'a'], rng);
    expect(sortedSet.zscore(db, ['k', 'a'])).toEqual(bulk('1.5'));
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zscore(db, ['k', 'nosuch'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(sortedSet.zscore(db, ['nosuch', 'a'])).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'v']);
    expect(sortedSet.zscore(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns integer scores without trailing zeros', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    expect(sortedSet.zscore(db, ['k', 'a'])).toEqual(bulk('5'));
  });

  it('returns inf score', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '+inf', 'a'], rng);
    expect(sortedSet.zscore(db, ['k', 'a'])).toEqual(bulk('inf'));
  });

  it('returns -inf score', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '-inf', 'a'], rng);
    expect(sortedSet.zscore(db, ['k', 'a'])).toEqual(bulk('-inf'));
  });

  it('returns wrong arity for bad args', () => {
    const { db } = createDb();
    expect(sortedSet.zscore(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zscore' command")
    );
  });
});

// --- ZMSCORE ---

describe('ZMSCORE', () => {
  it('returns scores for multiple members', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zmscore(db, ['k', 'a', 'b', 'c']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('1'), bulk('2'), bulk('3')],
    });
  });

  it('returns nil for missing members', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '3', 'c'], rng);
    const result = sortedSet.zmscore(db, ['k', 'a', 'nosuch', 'c']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('1'), bulk(null), bulk('3')],
    });
  });

  it('returns all nils for non-existing key', () => {
    const { db } = createDb();
    const result = sortedSet.zmscore(db, ['nosuch', 'a', 'b']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk(null), bulk(null)],
    });
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'v']);
    expect(sortedSet.zmscore(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('returns wrong arity for no members', () => {
    const { db } = createDb();
    expect(sortedSet.zmscore(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zmscore' command")
    );
  });
});

// --- ZRANK ---

describe('ZRANK', () => {
  it('returns 0-based rank', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zrank(db, ['k', 'a'])).toEqual(integer(0));
    expect(sortedSet.zrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(sortedSet.zrank(db, ['k', 'c'])).toEqual(integer(2));
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zrank(db, ['k', 'nosuch'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(sortedSet.zrank(db, ['nosuch', 'a'])).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'v']);
    expect(sortedSet.zrank(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('ranks by score then by lexicographic order', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'c', '1', 'a', '1', 'b'], rng);
    expect(sortedSet.zrank(db, ['k', 'a'])).toEqual(integer(0));
    expect(sortedSet.zrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(sortedSet.zrank(db, ['k', 'c'])).toEqual(integer(2));
  });

  it('returns WITHSCORE when requested', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1.5', 'a', '2.5', 'b'], rng);
    const result = sortedSet.zrank(db, ['k', 'a', 'WITHSCORE']);
    expect(result).toEqual({
      kind: 'array',
      value: [integer(0), bulk('1.5')],
    });
  });

  it('returns nil for WITHSCORE when member not found', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    const result = sortedSet.zrank(db, ['k', 'nosuch', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns nil-array for WITHSCORE when key not found', () => {
    const { db } = createDb();
    const result = sortedSet.zrank(db, ['nosuch', 'a', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns syntax error for invalid third argument', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zrank(db, ['k', 'a', 'INVALID'])).toEqual(
      err('ERR', 'syntax error')
    );
  });

  it('returns wrong arity for bad args', () => {
    const { db } = createDb();
    expect(sortedSet.zrank(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrank' command")
    );
  });
});

// --- ZREVRANK ---

describe('ZREVRANK', () => {
  it('returns reverse 0-based rank', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zrevrank(db, ['k', 'a'])).toEqual(integer(2));
    expect(sortedSet.zrevrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(sortedSet.zrevrank(db, ['k', 'c'])).toEqual(integer(0));
  });

  it('returns nil for non-existing member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zrevrank(db, ['k', 'nosuch'])).toEqual(bulk(null));
  });

  it('returns nil for non-existing key', () => {
    const { db } = createDb();
    expect(sortedSet.zrevrank(db, ['nosuch', 'a'])).toEqual(bulk(null));
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'v']);
    expect(sortedSet.zrevrank(db, ['k', 'a'])).toEqual(WRONGTYPE);
  });

  it('ranks by reverse score then by reverse lexicographic order', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'c', '1', 'a', '1', 'b'], rng);
    expect(sortedSet.zrevrank(db, ['k', 'a'])).toEqual(integer(2));
    expect(sortedSet.zrevrank(db, ['k', 'b'])).toEqual(integer(1));
    expect(sortedSet.zrevrank(db, ['k', 'c'])).toEqual(integer(0));
  });

  it('returns WITHSCORE when requested', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1.5', 'a', '2.5', 'b'], rng);
    const result = sortedSet.zrevrank(db, ['k', 'b', 'WITHSCORE']);
    expect(result).toEqual({
      kind: 'array',
      value: [integer(0), bulk('2.5')],
    });
  });

  it('returns nil for WITHSCORE when member not found', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    const result = sortedSet.zrevrank(db, ['k', 'nosuch', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns nil-array for WITHSCORE when key not found', () => {
    const { db } = createDb();
    const result = sortedSet.zrevrank(db, ['nosuch', 'a', 'WITHSCORE']);
    expect(result).toEqual({ kind: 'nil-array' });
  });

  it('returns syntax error for invalid third argument', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zrevrank(db, ['k', 'a', 'INVALID'])).toEqual(
      err('ERR', 'syntax error')
    );
  });

  it('returns wrong arity for bad args', () => {
    const { db } = createDb();
    expect(sortedSet.zrevrank(db, ['k'])).toEqual(
      err('ERR', "wrong number of arguments for 'zrevrank' command")
    );
  });

  it('handles single element set', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'only'], rng);
    expect(sortedSet.zrank(db, ['k', 'only'])).toEqual(integer(0));
    expect(sortedSet.zrevrank(db, ['k', 'only'])).toEqual(integer(0));
  });
});

// --- ZPOPMIN ---

describe('ZPOPMIN', () => {
  it('pops element with lowest score', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zpopmin(db, ['k'])).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1')],
    });
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(2));
  });

  it('pops multiple elements with count', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zpopmin(db, ['k', '2'])).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(1));
  });

  it('count larger than set size returns all elements', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    expect(sortedSet.zpopmin(db, ['k', '10'])).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(0));
  });

  it('returns empty array for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zpopmin(db, ['k'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('count 0 returns empty array', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zpopmin(db, ['k', '0'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zpopmin(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('deletes key when all elements popped', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    sortedSet.zpopmin(db, ['k']);
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(0));
  });

  it('rejects extra arguments', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zpopmin(db, ['k', '1', 'extra'])).toEqual(
      err('ERR', "wrong number of arguments for 'zpopmin' command")
    );
  });
});

// --- ZPOPMAX ---

describe('ZPOPMAX', () => {
  it('pops element with highest score', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zpopmax(db, ['k'])).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('3')],
    });
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(2));
  });

  it('pops multiple elements from highest', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    expect(sortedSet.zpopmax(db, ['k', '2'])).toEqual({
      kind: 'array',
      value: [bulk('c'), bulk('3'), bulk('b'), bulk('2')],
    });
    expect(sortedSet.zcard(db, ['k'])).toEqual(integer(1));
  });

  it('returns empty array for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zpopmax(db, ['k'])).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zpopmax(db, ['k'])).toEqual(WRONGTYPE);
  });

  it('rejects extra arguments', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zpopmax(db, ['k', '1', 'extra'])).toEqual(
      err('ERR', "wrong number of arguments for 'zpopmax' command")
    );
  });
});

// --- ZMPOP ---

describe('ZMPOP', () => {
  it('pops min from first non-empty key', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zmpop(db, ['1', 'k1', 'MIN'], rng);
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
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zmpop(db, ['1', 'k1', 'MAX'], rng);
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
    sortedSet.zadd(db, ['k2', '5', 'x'], rng);
    const result = sortedSet.zmpop(db, ['2', 'k1', 'k2', 'MIN'], rng);
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
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zmpop(db, ['1', 'k1', 'MIN', 'COUNT', '2'], rng);
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
    expect(sortedSet.zmpop(db, ['1', 'k1', 'MIN'], rng)).toEqual({
      kind: 'nil-array',
    });
  });

  it('returns error for invalid numkeys', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zmpop(db, ['0', 'k1', 'MIN'], rng)).toEqual(
      err('ERR', 'numkeys should be greater than 0')
    );
  });

  it('returns error for count 0', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zmpop(db, ['1', 'k1', 'MIN', 'COUNT', '0'], rng)).toEqual(
      err('ERR', 'count should be greater than 0')
    );
  });

  it('returns WRONGTYPE for non-zset key', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zmpop(db, ['1', 'k1', 'MIN'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZRANDMEMBER ---

describe('ZRANDMEMBER', () => {
  it('returns single random member', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrandmember(db, ['k'], rng);
    expect(result.kind).toBe('bulk');
    expect(['a', 'b', 'c']).toContain(
      (result as { kind: 'bulk'; value: string }).value
    );
  });

  it('returns nil for nonexistent key (no count)', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zrandmember(db, ['k'], rng)).toEqual(bulk(null));
  });

  it('positive count returns unique elements', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zrandmember(db, ['k', '2'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(2);
  });

  it('positive count > size returns all', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zrandmember(db, ['k', '10'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(2);
  });

  it('negative count may repeat elements', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    const result = sortedSet.zrandmember(db, ['k', '-3'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(3);
    // All should be 'a' since it's the only element
    for (const r of arr) {
      expect(r).toEqual(bulk('a'));
    }
  });

  it('WITHSCORES returns member-score pairs', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zrandmember(db, ['k', '2', 'WITHSCORES'], rng);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    expect(arr.length).toBe(4); // 2 members * 2 (member + score)
  });

  it('returns empty array for nonexistent key with count', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zrandmember(db, ['k', '3'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('count 0 returns empty array', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a'], rng);
    expect(sortedSet.zrandmember(db, ['k', '0'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zrandmember(db, ['k'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZUNION ---

describe('ZUNION', () => {
  it('returns union of two sets', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = sortedSet.zunion(db, ['2', 'k1', 'k2'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('c'), bulk('b')],
    });
  });

  it('WITHSCORES returns scores', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = sortedSet.zunion(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('c'), bulk('4'), bulk('b'), bulk('5')],
    });
  });

  it('WEIGHTS applies multipliers', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = sortedSet.zunion(
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
    sortedSet.zadd(db, ['k1', '1', 'a', '5', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = sortedSet.zunion(
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
    sortedSet.zadd(db, ['k1', '1', 'a', '5', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    const result = sortedSet.zunion(
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
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    const result = sortedSet.zunion(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1')],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zunion(db, ['1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZINTER ---

describe('ZINTER', () => {
  it('returns intersection of two sets', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    sortedSet.zadd(db, ['k2', '4', 'b', '5', 'c', '6', 'd'], rng);
    const result = sortedSet.zinter(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('b'), bulk('6'), bulk('c'), bulk('8')],
    });
  });

  it('empty intersection when key missing', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    expect(sortedSet.zinter(db, ['2', 'k1', 'k2'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('WEIGHTS and AGGREGATE MIN', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '10', 'a', '20', 'b'], rng);
    sortedSet.zadd(db, ['k2', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zinter(
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
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zinter(db, ['1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZDIFF ---

describe('ZDIFF', () => {
  it('returns difference of two sets', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    sortedSet.zadd(db, ['k2', '4', 'b', '5', 'd'], rng);
    const result = sortedSet.zdiff(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('c'), bulk('3')],
    });
  });

  it('all elements when second key missing', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    const result = sortedSet.zdiff(db, ['2', 'k1', 'k2', 'WITHSCORES'], rng);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('a'), bulk('1'), bulk('b'), bulk('2')],
    });
  });

  it('empty result when first key missing', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k2', '1', 'a'], rng);
    expect(sortedSet.zdiff(db, ['2', 'k1', 'k2'], rng)).toEqual({
      kind: 'array',
      value: [],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zdiff(db, ['1', 'k1'], rng)).toEqual(WRONGTYPE);
  });
});

// --- ZUNIONSTORE ---

describe('ZUNIONSTORE', () => {
  it('stores union result', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    expect(sortedSet.zunionstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(
      integer(3)
    );
    expect(sortedSet.zscore(db, ['out', 'a'])).toEqual(bulk('1'));
    expect(sortedSet.zscore(db, ['out', 'b'])).toEqual(bulk('5'));
    expect(sortedSet.zscore(db, ['out', 'c'])).toEqual(bulk('4'));
  });

  it('with WEIGHTS', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    sortedSet.zadd(db, ['k2', '2', 'a'], rng);
    expect(
      sortedSet.zunionstore(
        db,
        ['out', '2', 'k1', 'k2', 'WEIGHTS', '10', '1'],
        rng
      )
    ).toEqual(integer(1));
    expect(sortedSet.zscore(db, ['out', 'a'])).toEqual(bulk('12'));
  });

  it('overwrites destination', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['out', '99', 'old'], rng);
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    sortedSet.zunionstore(db, ['out', '1', 'k1'], rng);
    expect(sortedSet.zscore(db, ['out', 'old'])).toEqual(bulk(null));
    expect(sortedSet.zscore(db, ['out', 'a'])).toEqual(bulk('1'));
  });

  it('destination can be a source key', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    expect(sortedSet.zunionstore(db, ['k1', '1', 'k1'], rng)).toEqual(
      integer(2)
    );
    expect(sortedSet.zscore(db, ['k1', 'a'])).toEqual(bulk('1'));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zunionstore(db, ['out', '1', 'k1'], rng)).toEqual(
      WRONGTYPE
    );
  });

  it('rejects WITHSCORES option', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    expect(
      sortedSet.zunionstore(db, ['out', '1', 'k1', 'WITHSCORES'], rng)
    ).toEqual(err('ERR', 'syntax error'));
  });
});

// --- ZINTERSTORE ---

describe('ZINTERSTORE', () => {
  it('stores intersection result', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b'], rng);
    sortedSet.zadd(db, ['k2', '3', 'b', '4', 'c'], rng);
    expect(sortedSet.zinterstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(
      integer(1)
    );
    expect(sortedSet.zscore(db, ['out', 'b'])).toEqual(bulk('5'));
  });

  it('empty result when no intersection', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    sortedSet.zadd(db, ['k2', '2', 'b'], rng);
    expect(sortedSet.zinterstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(
      integer(0)
    );
  });

  it('with AGGREGATE MAX', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '10', 'a', '20', 'b'], rng);
    sortedSet.zadd(db, ['k2', '1', 'a', '2', 'b'], rng);
    expect(
      sortedSet.zinterstore(
        db,
        ['out', '2', 'k1', 'k2', 'AGGREGATE', 'MAX'],
        rng
      )
    ).toEqual(integer(2));
    expect(sortedSet.zscore(db, ['out', 'a'])).toEqual(bulk('10'));
    expect(sortedSet.zscore(db, ['out', 'b'])).toEqual(bulk('20'));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zinterstore(db, ['out', '1', 'k1'], rng)).toEqual(
      WRONGTYPE
    );
  });

  it('rejects WITHSCORES option', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    sortedSet.zadd(db, ['k2', '2', 'a'], rng);
    expect(
      sortedSet.zinterstore(db, ['out', '2', 'k1', 'k2', 'WITHSCORES'], rng)
    ).toEqual(err('ERR', 'syntax error'));
  });
});

// --- ZDIFFSTORE ---

describe('ZDIFFSTORE', () => {
  it('stores difference result', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    sortedSet.zadd(db, ['k2', '4', 'b'], rng);
    expect(sortedSet.zdiffstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(
      integer(2)
    );
    expect(sortedSet.zscore(db, ['out', 'a'])).toEqual(bulk('1'));
    expect(sortedSet.zscore(db, ['out', 'c'])).toEqual(bulk('3'));
    expect(sortedSet.zscore(db, ['out', 'b'])).toEqual(bulk(null));
  });

  it('empty result when all elements in other sets', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    sortedSet.zadd(db, ['k2', '2', 'a'], rng);
    expect(sortedSet.zdiffstore(db, ['out', '2', 'k1', 'k2'], rng)).toEqual(
      integer(0)
    );
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    expect(sortedSet.zdiffstore(db, ['out', '1', 'k1'], rng)).toEqual(
      WRONGTYPE
    );
  });
});

// --- ZINTERCARD ---

describe('ZINTERCARD', () => {
  it('returns cardinality of intersection', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    sortedSet.zadd(db, ['k2', '4', 'b', '5', 'c', '6', 'd'], rng);
    expect(sortedSet.zintercard(db, ['2', 'k1', 'k2'])).toEqual(integer(2));
  });

  it('with LIMIT stops early', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a', '2', 'b', '3', 'c'], rng);
    sortedSet.zadd(db, ['k2', '4', 'a', '5', 'b', '6', 'c'], rng);
    expect(sortedSet.zintercard(db, ['2', 'k1', 'k2', 'LIMIT', '2'])).toEqual(
      integer(2)
    );
  });

  it('returns 0 when key missing', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    expect(sortedSet.zintercard(db, ['2', 'k1', 'k2'])).toEqual(integer(0));
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db, rng } = createDb();
    string.set(db, () => 1000, ['k1', 'hello']);
    sortedSet.zadd(db, ['k2', '1', 'a'], rng);
    expect(sortedSet.zintercard(db, ['2', 'k1', 'k2'])).toEqual(WRONGTYPE);
  });

  it('rejects negative LIMIT', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k1', '1', 'a'], rng);
    expect(sortedSet.zintercard(db, ['1', 'k1', 'LIMIT', '-1'])).toEqual(
      err('ERR', "LIMIT can't be negative")
    );
  });

  it('rejects numkeys 0', () => {
    const { db } = createDb();
    expect(sortedSet.zintercard(db, ['0', 'k1'])).toEqual(
      err('ERR', 'numkeys should be greater than 0')
    );
  });
});

// --- ZSCAN ---

describe('ZSCAN', () => {
  it('scans all members', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zscan(db, ['k', '0']);
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
    sortedSet.zadd(db, ['k', '1', 'alpha', '2', 'beta', '3', 'gamma'], rng);
    const result = sortedSet.zscan(db, ['k', '0', 'MATCH', 'a*']);
    expect(result).toEqual({
      kind: 'array',
      value: [bulk('0'), { kind: 'array', value: [bulk('alpha'), bulk('1')] }],
    });
  });

  it('scans with COUNT', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    const result = sortedSet.zscan(db, ['k', '0', 'COUNT', '2']);
    expect(result.kind).toBe('array');
    const arr = (result as { kind: 'array'; value: Reply[] }).value;
    // cursor should be non-zero since not all scanned
    expect(arr[0]).toEqual(bulk('2'));
  });

  it('returns cursor 0 and empty array for nonexistent key', () => {
    const { db } = createDb();
    expect(sortedSet.zscan(db, ['k', '0'])).toEqual({
      kind: 'array',
      value: [bulk('0'), { kind: 'array', value: [] }],
    });
  });

  it('returns WRONGTYPE for non-zset', () => {
    const { db } = createDb();
    string.set(db, () => 1000, ['k', 'hello']);
    expect(sortedSet.zscan(db, ['k', '0'])).toEqual(WRONGTYPE);
  });

  it('continues from cursor position', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '1', 'a', '2', 'b', '3', 'c'], rng);
    // First scan with count 1
    const first = sortedSet.zscan(db, ['k', '0', 'COUNT', '1']);
    const firstArr = (first as { kind: 'array'; value: Reply[] }).value;
    const cursor = (firstArr[0] as { kind: 'bulk'; value: string }).value;

    // Continue from cursor
    const second = sortedSet.zscan(db, ['k', cursor, 'COUNT', '1']);
    const secondArr = (second as { kind: 'array'; value: Reply[] }).value;
    const cursor2 = (secondArr[0] as { kind: 'bulk'; value: string }).value;

    // Third scan
    const third = sortedSet.zscan(db, ['k', cursor2, 'COUNT', '1']);
    const thirdArr = (third as { kind: 'array'; value: Reply[] }).value;
    const cursor3 = (thirdArr[0] as { kind: 'bulk'; value: string }).value;

    // After scanning all 3 elements, cursor should be 0
    expect(cursor3).toBe('0');
  });
});
