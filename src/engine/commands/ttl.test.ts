import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import * as ttlCmd from './ttl.ts';

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

describe('EXPIRE', () => {
  it('sets expiry and returns 1', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expire(db, clock, ['k', '10'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    expect(db.getExpiry('k')).toBe(11000);
  });

  it('returns 0 for non-existent key', () => {
    const { db, clock } = createDb();
    expect(ttlCmd.expire(db, clock, ['missing', '10'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns error for non-integer', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expire(db, clock, ['k', 'abc']).kind).toBe('error');
  });

  it('NX flag: sets only if no expiry exists', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expire(db, clock, ['k', '10', 'NX'])).toEqual({
      kind: 'integer',
      value: 1,
    });
    // Already has expiry — NX should not overwrite
    expect(ttlCmd.expire(db, clock, ['k', '20', 'NX'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(db.getExpiry('k')).toBe(11000);
  });

  it('XX flag: sets only if expiry exists', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expire(db, clock, ['k', '10', 'XX'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    db.setExpiry('k', 5000);
    expect(ttlCmd.expire(db, clock, ['k', '10', 'XX'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('GT flag: sets only if new expiry is greater', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 20000);
    expect(ttlCmd.expire(db, clock, ['k', '10', 'GT'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(ttlCmd.expire(db, clock, ['k', '30', 'GT'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('LT flag: sets only if new expiry is less', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 5000);
    expect(ttlCmd.expire(db, clock, ['k', '10', 'LT'])).toEqual({
      kind: 'integer',
      value: 0,
    });
    expect(ttlCmd.expire(db, clock, ['k', '2', 'LT'])).toEqual({
      kind: 'integer',
      value: 1,
    });
  });

  it('NX and XX conflict returns error', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expire(db, clock, ['k', '10', 'NX', 'XX']).kind).toBe(
      'error'
    );
  });

  it('GT+LT conflict returns error', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expire(db, clock, ['k', '10', 'GT', 'LT']).kind).toBe(
      'error'
    );
  });

  it('negative timeout deletes the key', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    const reply = ttlCmd.expire(db, clock, ['k', '-1']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(db.has('k')).toBe(false);
  });

  it('zero timeout deletes the key', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    const reply = ttlCmd.expire(db, clock, ['k', '0']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(db.has('k')).toBe(false);
  });

  it('GT on key without TTL returns 0 (infinite > any finite)', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    const reply = ttlCmd.expire(db, clock, ['k', '10', 'GT']);
    expect(reply).toEqual({ kind: 'integer', value: 0 });
    expect(db.getExpiry('k')).toBeUndefined();
  });

  it('LT on key without TTL sets expiry (infinite > any finite)', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    const reply = ttlCmd.expire(db, clock, ['k', '10', 'LT']);
    expect(reply).toEqual({ kind: 'integer', value: 1 });
    expect(db.getExpiry('k')).toBe(11000);
  });
});

describe('PEXPIRE', () => {
  it('sets expiry in milliseconds', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    ttlCmd.pexpire(db, clock, ['k', '5000']);
    expect(db.getExpiry('k')).toBe(6000);
  });
});

describe('EXPIREAT', () => {
  it('sets expiry at unix timestamp', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    ttlCmd.expireat(db, clock, ['k', '100']);
    expect(db.getExpiry('k')).toBe(100000);
  });
});

describe('PEXPIREAT', () => {
  it('sets expiry at unix ms timestamp', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    ttlCmd.pexpireat(db, clock, ['k', '50000']);
    expect(db.getExpiry('k')).toBe(50000);
  });
});

describe('TTL', () => {
  it('returns seconds to expiry', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 11000);
    expect(ttlCmd.ttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: 10,
    });
  });

  it('rounds to nearest second (Redis formula: (ttl+500)/1000)', () => {
    const { db, clock, setTime } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 11000);
    // 9501ms remaining -> rounds to 10
    setTime(1499);
    expect(ttlCmd.ttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: 10,
    });
    // 9499ms remaining -> rounds to 9
    setTime(1501);
    expect(ttlCmd.ttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: 9,
    });
    // 499ms remaining -> rounds to 0
    setTime(10501);
    expect(ttlCmd.ttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: 0,
    });
  });

  it('returns -1 for key without TTL', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.ttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb();
    expect(ttlCmd.ttl(db, clock, ['missing'])).toEqual({
      kind: 'integer',
      value: -2,
    });
  });
});

describe('PTTL', () => {
  it('returns ms to expiry', () => {
    const { db, clock } = createDb(1000);
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 6000);
    expect(ttlCmd.pttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: 5000,
    });
  });

  it('returns -1 for key without TTL', () => {
    const { db, clock } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.pttl(db, clock, ['k'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('returns -2 for non-existent key', () => {
    const { db, clock } = createDb();
    expect(ttlCmd.pttl(db, clock, ['missing'])).toEqual({
      kind: 'integer',
      value: -2,
    });
  });
});

describe('EXPIRETIME', () => {
  it('returns unix timestamp of expiry', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 100000);
    expect(ttlCmd.expiretime(db, ['k'])).toEqual({
      kind: 'integer',
      value: 100,
    });
  });

  it('rounds to nearest second (Redis formula: (ms+500)/1000)', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    // 100500ms -> rounds to 101
    db.setExpiry('k', 100500);
    expect(ttlCmd.expiretime(db, ['k'])).toEqual({
      kind: 'integer',
      value: 101,
    });
    // 100499ms -> rounds to 100
    db.setExpiry('k', 100499);
    expect(ttlCmd.expiretime(db, ['k'])).toEqual({
      kind: 'integer',
      value: 100,
    });
  });

  it('returns -1 for key without TTL', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    expect(ttlCmd.expiretime(db, ['k'])).toEqual({
      kind: 'integer',
      value: -1,
    });
  });

  it('returns -2 for non-existent key', () => {
    const { db } = createDb();
    expect(ttlCmd.expiretime(db, ['missing'])).toEqual({
      kind: 'integer',
      value: -2,
    });
  });
});

describe('PEXPIRETIME', () => {
  it('returns ms timestamp of expiry', () => {
    const { db } = createDb();
    db.set('k', 'string', 'raw', 'v');
    db.setExpiry('k', 50000);
    expect(ttlCmd.pexpiretime(db, ['k'])).toEqual({
      kind: 'integer',
      value: 50000,
    });
  });
});
