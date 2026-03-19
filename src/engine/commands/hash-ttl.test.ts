import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import * as hashTtl from './hash-ttl.ts';
import * as hash from './hash.ts';

function createDb(time = 1000) {
  let now = time;
  const clock = () => now;
  const engine = new RedisEngine({ clock, rng: () => 0.5 });
  return {
    db: engine.db(0),
    clock,
    setTime: (t: number) => {
      now = t;
    },
  };
}

function integer(value: number): Reply {
  return { kind: 'integer', value };
}

function bulk(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function arr(...items: Reply[]): Reply {
  return { kind: 'array', value: items };
}

const WRONGTYPE: Reply = {
  kind: 'error',
  prefix: 'WRONGTYPE',
  message: 'Operation against a key holding the wrong kind of value',
};

function setupHash(db: Database): void {
  hash.hset(db, ['k', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3']);
}

// --- HEXPIRE ---

describe('HEXPIRE', () => {
  it('sets field expiry and returns 1 per field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'FIELDS', '2', 'f1', 'f2'])
    ).toEqual(arr(integer(1), integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(11000);
    expect(db.getFieldExpiry('k', 'f2')).toBe(11000);
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'FIELDS', '1', 'nofield'])
    ).toEqual(arr(integer(-2)));
  });

  it('returns -2 for all fields when key does not exist', () => {
    const { db, clock } = createDb(1000);
    expect(
      hashTtl.hexpire(db, clock, ['nokey', '10', 'FIELDS', '2', 'f1', 'f2'])
    ).toEqual(arr(integer(-2), integer(-2)));
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'val');
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'FIELDS', '1', 'f1'])
    ).toEqual(WRONGTYPE);
  });

  it('deletes field when seconds is 0 and returns 2', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.hexpire(db, clock, ['k', '0', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(2))
    );
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk(null));
  });

  it('deletes field when seconds is negative and returns 2', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpire(db, clock, ['k', '-5', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(2)));
    expect(hash.hexists(db, ['k', 'f1'])).toEqual(integer(0));
  });

  it('NX flag: sets only if no field expiry exists', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    // First set should succeed
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'NX', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    // Second set with NX should fail
    expect(
      hashTtl.hexpire(db, clock, ['k', '20', 'NX', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(11000);
  });

  it('XX flag: sets only if field expiry exists', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    // No expiry exists — XX should fail
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'XX', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
    // Set expiry first
    db.setFieldExpiry('k', 'f1', 5000);
    // Now XX should succeed
    expect(
      hashTtl.hexpire(db, clock, ['k', '20', 'XX', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(21000);
  });

  it('GT flag: sets only if new expiry is greater', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 15000);
    // 10s = 11000 < 15000 — GT should fail
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'GT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
    // 20s = 21000 > 15000 — GT should succeed
    expect(
      hashTtl.hexpire(db, clock, ['k', '20', 'GT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(21000);
  });

  it('LT flag: sets only if new expiry is less', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 15000);
    // 20s = 21000 > 15000 — LT should fail
    expect(
      hashTtl.hexpire(db, clock, ['k', '20', 'LT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
    // 5s = 6000 < 15000 — LT should succeed
    expect(
      hashTtl.hexpire(db, clock, ['k', '5', 'LT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(6000);
  });

  it('returns error for non-integer seconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.hexpire(db, clock, [
      'k',
      'abc',
      'FIELDS',
      '1',
      'f1',
    ]);
    expect(result.kind).toBe('error');
  });

  it('returns error for missing FIELDS keyword', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.hexpire(db, clock, [
      'k',
      '10',
      'NOTFIELDS',
      '1',
      'f1',
    ]);
    expect(result.kind).toBe('error');
  });

  it('returns error when numfields does not match actual fields', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.hexpire(db, clock, [
      'k',
      '10',
      'FIELDS',
      '3',
      'f1',
      'f2',
    ]);
    expect(result.kind).toBe('error');
  });

  it('returns error for numfields <= 0', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.hexpire(db, clock, ['k', '10', 'FIELDS', '0']);
    expect(result.kind).toBe('error');
  });

  it('handles mixed existing and non-existing fields', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpire(db, clock, [
        'k',
        '10',
        'FIELDS',
        '3',
        'f1',
        'nofield',
        'f3',
      ])
    ).toEqual(arr(integer(1), integer(-2), integer(1)));
  });

  it('GT with no existing expiry does not set (infinite > any finite)', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    // No expiry = infinite, GT should not set finite
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'GT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
  });

  it('LT with no existing expiry sets (any finite < infinite)', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    // No expiry = infinite, LT should set any finite
    expect(
      hashTtl.hexpire(db, clock, ['k', '10', 'LT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
  });

  it('deletes key when all fields are deleted by expiry', () => {
    const { db, clock } = createDb(1000);
    hash.hset(db, ['k', 'f1', 'v1']);
    hashTtl.hexpire(db, clock, ['k', '0', 'FIELDS', '1', 'f1']);
    expect(db.has('k')).toBe(false);
  });
});

// --- HPEXPIRE ---

describe('HPEXPIRE', () => {
  it('sets field expiry in milliseconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hpexpire(db, clock, ['k', '5000', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(6000);
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb(1000);
    expect(
      hashTtl.hpexpire(db, clock, ['nokey', '5000', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(-2)));
  });

  it('deletes field when ms is 0', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hpexpire(db, clock, ['k', '0', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(2)));
  });

  it('supports NX flag', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hpexpire(db, clock, ['k', '5000', 'NX', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(
      hashTtl.hpexpire(db, clock, ['k', '10000', 'NX', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
  });
});

// --- HEXPIREAT ---

describe('HEXPIREAT', () => {
  it('sets field expiry as unix timestamp in seconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpireat(db, clock, ['k', '20', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(20000);
  });

  it('deletes field when timestamp is in the past', () => {
    const { db, clock } = createDb(5000);
    setupHash(db);
    expect(
      hashTtl.hexpireat(db, clock, ['k', '1', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(2)));
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk(null));
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpireat(db, clock, ['k', '20', 'FIELDS', '1', 'nofield'])
    ).toEqual(arr(integer(-2)));
  });

  it('supports GT flag', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 15000);
    // timestamp 10 = 10000ms < 15000 — GT should fail
    expect(
      hashTtl.hexpireat(db, clock, ['k', '10', 'GT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
    // timestamp 20 = 20000ms > 15000 — GT should succeed
    expect(
      hashTtl.hexpireat(db, clock, ['k', '20', 'GT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
  });
});

// --- HPEXPIREAT ---

describe('HPEXPIREAT', () => {
  it('sets field expiry as unix timestamp in milliseconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hpexpireat(db, clock, ['k', '20000', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
    expect(db.getFieldExpiry('k', 'f1')).toBe(20000);
  });

  it('deletes field when timestamp is in the past', () => {
    const { db, clock } = createDb(5000);
    setupHash(db);
    expect(
      hashTtl.hpexpireat(db, clock, ['k', '1000', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(2)));
  });

  it('supports LT flag', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 15000);
    // 20000 > 15000 — LT should fail
    expect(
      hashTtl.hpexpireat(db, clock, ['k', '20000', 'LT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(0)));
    // 10000 < 15000 — LT should succeed
    expect(
      hashTtl.hpexpireat(db, clock, ['k', '10000', 'LT', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(1)));
  });
});

// --- HTTL ---

describe('HTTL', () => {
  it('returns TTL in seconds for field with expiry', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 11000);
    expect(hashTtl.httl(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(10))
    );
  });

  it('returns -1 for field without expiry', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.httl(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-1))
    );
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.httl(db, clock, ['k', 'FIELDS', '1', 'nofield'])).toEqual(
      arr(integer(-2))
    );
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb(1000);
    expect(hashTtl.httl(db, clock, ['nokey', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-2))
    );
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'val');
    expect(hashTtl.httl(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      WRONGTYPE
    );
  });

  it('returns multiple results for multiple fields', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 11000);
    expect(
      hashTtl.httl(db, clock, ['k', 'FIELDS', '3', 'f1', 'f2', 'nofield'])
    ).toEqual(arr(integer(10), integer(-1), integer(-2)));
  });

  it('rounds TTL like Redis (ceiling of remaining seconds)', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 11500); // 10.5s remaining
    // Redis rounds: ceil(10500/1000) = 11? No, Redis actually truncates.
    // TTL in Redis: floor((expiryMs - now) / 1000) when > 0
    // 10500 / 1000 = 10.5, floor = 10
    expect(hashTtl.httl(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(10))
    );
  });
});

// --- HPTTL ---

describe('HPTTL', () => {
  it('returns TTL in milliseconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 11000);
    expect(hashTtl.hpttl(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(10000))
    );
  });

  it('returns -1 for field without expiry', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.hpttl(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-1))
    );
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.hpttl(db, clock, ['k', 'FIELDS', '1', 'nofield'])).toEqual(
      arr(integer(-2))
    );
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb(1000);
    expect(hashTtl.hpttl(db, clock, ['nokey', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-2))
    );
  });
});

// --- HPERSIST ---

describe('HPERSIST', () => {
  it('removes field expiry and returns 1', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 11000);
    expect(hashTtl.hpersist(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(1))
    );
    expect(db.getFieldExpiry('k', 'f1')).toBeUndefined();
  });

  it('returns -1 for field without expiry', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.hpersist(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-1))
    );
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hpersist(db, clock, ['k', 'FIELDS', '1', 'nofield'])
    ).toEqual(arr(integer(-2)));
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb(1000);
    expect(hashTtl.hpersist(db, clock, ['nokey', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-2))
    );
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'val');
    expect(hashTtl.hpersist(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      WRONGTYPE
    );
  });

  it('handles multiple fields', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 11000);
    expect(
      hashTtl.hpersist(db, clock, ['k', 'FIELDS', '3', 'f1', 'f2', 'nofield'])
    ).toEqual(arr(integer(1), integer(-1), integer(-2)));
  });
});

// --- HEXPIRETIME ---

describe('HEXPIRETIME', () => {
  it('returns expiry as unix timestamp in seconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 20000);
    expect(hashTtl.hexpiretime(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(20))
    );
  });

  it('returns -1 for field without expiry', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.hexpiretime(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-1))
    );
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hexpiretime(db, clock, ['k', 'FIELDS', '1', 'nofield'])
    ).toEqual(arr(integer(-2)));
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb(1000);
    expect(
      hashTtl.hexpiretime(db, clock, ['nokey', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(-2)));
  });

  it('returns WRONGTYPE for non-hash key', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'val');
    expect(hashTtl.hexpiretime(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      WRONGTYPE
    );
  });

  it('converts ms to seconds correctly', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 20500);
    // floor(20500 / 1000) = 20
    expect(hashTtl.hexpiretime(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(20))
    );
  });
});

// --- HPEXPIRETIME ---

describe('HPEXPIRETIME', () => {
  it('returns expiry as unix timestamp in milliseconds', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 20000);
    expect(hashTtl.hpexpiretime(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(20000))
    );
  });

  it('returns -1 for field without expiry', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(hashTtl.hpexpiretime(db, clock, ['k', 'FIELDS', '1', 'f1'])).toEqual(
      arr(integer(-1))
    );
  });

  it('returns -2 for non-existent field', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    expect(
      hashTtl.hpexpiretime(db, clock, ['k', 'FIELDS', '1', 'nofield'])
    ).toEqual(arr(integer(-2)));
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb(1000);
    expect(
      hashTtl.hpexpiretime(db, clock, ['nokey', 'FIELDS', '1', 'f1'])
    ).toEqual(arr(integer(-2)));
  });
});

// --- Lazy field expiration in existing hash commands ---

describe('lazy field expiration', () => {
  it('HGET returns nil for expired field', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hget(db, ['k', 'f1'])).toEqual(bulk(null));
    // f2 and f3 still exist
    expect(hash.hget(db, ['k', 'f2'])).toEqual(bulk('v2'));
  });

  it('HEXISTS returns 0 for expired field', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hexists(db, ['k', 'f1'])).toEqual(integer(0));
  });

  it('HMGET returns nil for expired fields', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hmget(db, ['k', 'f1', 'f2'])).toEqual(
      arr(bulk(null), bulk('v2'))
    );
  });

  it('HGETALL skips expired fields', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    db.setFieldExpiry('k', 'f2', 2000);
    setTime(3000);
    expect(hash.hgetall(db, ['k'])).toEqual(arr(bulk('f3'), bulk('v3')));
  });

  it('HLEN excludes expired fields', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hlen(db, ['k'])).toEqual(integer(2));
  });

  it('HKEYS skips expired fields', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hkeys(db, ['k'])).toEqual(arr(bulk('f2'), bulk('f3')));
  });

  it('HVALS skips expired fields', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hvals(db, ['k'])).toEqual(arr(bulk('v2'), bulk('v3')));
  });

  it('deletes key when all fields expire via lazy expiration', () => {
    const { db, setTime } = createDb(1000);
    hash.hset(db, ['k', 'f1', 'v1']);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    expect(hash.hgetall(db, ['k'])).toEqual({ kind: 'array', value: [] });
    expect(db.has('k')).toBe(false);
  });

  it('HGET triggers lazy expiry and cleans up field expiry metadata', () => {
    const { db, setTime } = createDb(1000);
    setupHash(db);
    db.setFieldExpiry('k', 'f1', 2000);
    setTime(3000);
    hash.hget(db, ['k', 'f1']);
    expect(db.getFieldExpiry('k', 'f1')).toBeUndefined();
  });
});

// --- Error handling for FIELDS syntax ---

describe('FIELDS syntax parsing', () => {
  it('HTTL returns error for missing FIELDS keyword', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.httl(db, clock, ['k', 'NOTFIELDS', '1', 'f1']);
    expect(result.kind).toBe('error');
  });

  it('HTTL returns error when numfields does not match', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.httl(db, clock, ['k', 'FIELDS', '3', 'f1']);
    expect(result.kind).toBe('error');
  });

  it('HPERSIST returns error for non-integer numfields', () => {
    const { db, clock } = createDb(1000);
    setupHash(db);
    const result = hashTtl.hpersist(db, clock, ['k', 'FIELDS', 'abc', 'f1']);
    expect(result.kind).toBe('error');
  });
});
