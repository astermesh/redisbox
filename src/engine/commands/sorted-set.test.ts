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

  it('GT+LT: updates when score differs in any direction', () => {
    const { db, rng } = createDb();
    sortedSet.zadd(db, ['k', '5', 'a'], rng);
    // Higher: should update
    expect(sortedSet.zadd(db, ['k', 'GT', 'LT', 'CH', '10', 'a'], rng)).toEqual(
      ONE
    );
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('10'));
    // Lower: should update
    expect(sortedSet.zadd(db, ['k', 'GT', 'LT', 'CH', '2', 'a'], rng)).toEqual(
      ONE
    );
    expect(sortedSet.zincrby(db, ['k', '0', 'a'], rng)).toEqual(bulk('2'));
    // Equal: should NOT update
    expect(sortedSet.zadd(db, ['k', 'GT', 'LT', 'CH', '2', 'a'], rng)).toEqual(
      ZERO
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
      err('ERR', 'GT, LT, and NX options at the same time are not compatible')
    );
  });

  it('NX and LT together returns error', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'NX', 'LT', '1', 'a'], rng)).toEqual(
      err('ERR', 'GT, LT, and NX options at the same time are not compatible')
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
      err('ERR', "wrong number of arguments for 'zadd' command")
    );
  });

  it('flags only, no score-member pairs', () => {
    const { db, rng } = createDb();
    expect(sortedSet.zadd(db, ['k', 'NX'], rng)).toEqual(
      err('ERR', "wrong number of arguments for 'zadd' command")
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
