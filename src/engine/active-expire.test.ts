import { describe, it, expect } from 'vitest';
import { Database } from './database.ts';
import {
  activeExpireCycle,
  fastActiveExpireCycle,
  createFastExpireCycleState,
  type FastExpireCycleState,
} from './active-expire.ts';

function createDb(time = 1000): {
  db: Database;
  setTime: (t: number) => void;
} {
  let now = time;
  const db = new Database(() => now);
  db.setRng(() => Math.random());
  return {
    db,
    setTime: (t: number) => {
      now = t;
    },
  };
}

function setKey(db: Database, key: string, expiryMs?: number): void {
  db.set(key, 'string', 'raw', `val-${key}`);
  if (expiryMs !== undefined) {
    db.setExpiry(key, expiryMs);
  }
}

/**
 * Create a clock that advances by `stepMs` on each call.
 * Useful for testing time-budget behavior.
 */
function advancingClock(start: number, stepMs: number): () => number {
  let time = start;
  return () => {
    const t = time;
    time += stepMs;
    return t;
  };
}

describe('activeExpireCycle', () => {
  describe('basic expiration', () => {
    it('deletes expired keys', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 10; i++) {
        setKey(db, `k${i}`, 2000);
      }
      expect(db.size).toBe(10);

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(10);
      expect(db.size).toBe(0);
    });

    it('does not delete non-expired keys', () => {
      const { db, setTime } = createDb(1000);

      setKey(db, 'alive', 5000);
      setKey(db, 'also-alive'); // no expiry

      setTime(2000);

      activeExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(db.size).toBe(2);
      expect(db.has('alive')).toBe(true);
      expect(db.has('also-alive')).toBe(true);
    });

    it('returns zero when no keys have TTL', () => {
      const { db } = createDb(1000);

      setKey(db, 'k1');
      setKey(db, 'k2');

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(0);
      expect(db.size).toBe(2);
    });

    it('skips databases with no expiring keys', () => {
      const { db: db0 } = createDb(1000);
      const { db: db1, setTime } = createDb(1000);

      setKey(db0, 'k1');
      setKey(db1, 'k2', 2000);
      setTime(2001);

      const result = activeExpireCycle({
        databases: [db0, db1],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(1);
      expect(db0.size).toBe(1);
      expect(db1.size).toBe(0);
    });
  });

  describe('sampling behavior', () => {
    it('stops when expired ratio falls below threshold', () => {
      const { db, setTime } = createDb(1000);

      // 1000 keys with TTL, only 5 expired (~0.5% < 9% threshold at effort=1)
      // So after one sample batch finds ~0 expired, the loop stops
      for (let i = 0; i < 1000; i++) {
        const expiry = i < 5 ? 2000 : 5000;
        setKey(db, `k${i}`, expiry);
      }

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      // May find some or none depending on random sampling
      expect(result.expired).toBeLessThanOrEqual(5);
    });

    it('repeats sampling when all keys are expired', () => {
      const { db, setTime } = createDb(1000);

      // All 100 keys expired — 100% expiry rate causes repeated sampling
      // until all are cleaned (expirySize drops to 0)
      for (let i = 0; i < 100; i++) {
        setKey(db, `k${i}`, 2000);
      }

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(100);
      expect(db.size).toBe(0);
    });

    it('continues looping when expired ratio is high', () => {
      const { db, setTime } = createDb(1000);

      // 200 expired keys — will take multiple sample batches of 25
      for (let i = 0; i < 200; i++) {
        setKey(db, `k${i}`, 2000);
      }

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      // All should be expired since they're all past TTL and ratio stays high
      expect(result.expired).toBe(200);
      expect(db.size).toBe(0);
    });
  });

  describe('time budget', () => {
    it('respects time budget and stops early', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 5000; i++) {
        setKey(db, `k${i}`, 2000);
      }

      setTime(2001);

      // Clock advances 5ms per call; budget at hz=10 effort=1 is 2.5ms
      // Should stop very quickly
      const result = activeExpireCycle({
        databases: [db],
        clock: advancingClock(2001, 5),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBeGreaterThan(0);
      expect(result.expired).toBeLessThan(5000);
      expect(result.timedOut).toBe(true);
    });

    it('time budget scales with hz', () => {
      // Higher hz = smaller time budget per cycle
      const makeDb = () => {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < 2000; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        return db;
      };

      const db10 = makeDb();
      activeExpireCycle({
        databases: [db10],
        clock: advancingClock(2001, 0.1),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      const db100 = makeDb();
      activeExpireCycle({
        databases: [db100],
        clock: advancingClock(2001, 0.1),
        rng: () => Math.random(),
        hz: 100,
        effort: 1,
      });

      // hz=10 has 2.5ms budget, hz=100 has 0.25ms budget
      const expired10 = 2000 - db10.size;
      const expired100 = 2000 - db100.size;
      expect(expired10).toBeGreaterThan(expired100);
    });
  });

  describe('effort scaling', () => {
    it('higher effort samples more keys per loop', () => {
      // effort=1 (adjusted=0): keys_per_loop = 20
      // effort=10 (adjusted=9): keys_per_loop = 20 + 5*9 = 65
      // With all keys expired, effort=10 samples more per iteration.
      // Use advancing clock so time budget limits how many iterations run.
      // Same clock speed means effort=10 deletes more keys per iteration.
      const keyCount = 5000;
      const makeDb = () => {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < keyCount; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        return db;
      };

      // Use same time budget (same hz) but different effort for keys_per_loop
      // Clock advances 1ms per call — at hz=10 effort=1 budget is 2.5ms
      // Both will time out, but effort=10 processes more keys per iteration
      const db1 = makeDb();
      const r1 = activeExpireCycle({
        databases: [db1],
        clock: advancingClock(2001, 1),
        rng: () => Math.random(),
        hz: 100, // small time budget to force time-out
        effort: 1,
      });

      const db10 = makeDb();
      const r10 = activeExpireCycle({
        databases: [db10],
        clock: advancingClock(2001, 1),
        rng: () => Math.random(),
        hz: 100, // same small time budget
        effort: 10,
      });

      // Both should time out
      expect(r1.timedOut).toBe(true);
      expect(r10.timedOut).toBe(true);

      // effort=10 samples 65 keys per loop vs 20 — more expired per iteration
      expect(r10.expired).toBeGreaterThan(r1.expired);
    });

    it('higher effort increases time budget percentage', () => {
      const keyCount = 10000;
      const makeDb = () => {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < keyCount; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        return db;
      };

      // Use a clock that advances fast enough to hit effort=1 budget
      // but not effort=10 budget
      // effort=1 (adjusted=0): budget 2.5ms, effort=10 (adjusted=9): budget 4.3ms
      const db1 = makeDb();
      const r1 = activeExpireCycle({
        databases: [db1],
        clock: advancingClock(2001, 0.5),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      const db10 = makeDb();
      const r10 = activeExpireCycle({
        databases: [db10],
        clock: advancingClock(2001, 0.5),
        rng: () => Math.random(),
        hz: 10,
        effort: 10,
      });

      // effort=1 should time out, effort=10 gets more budget
      expect(r1.timedOut).toBe(true);
      expect(r10.expired).toBeGreaterThan(r1.expired);
    });
  });

  describe('multi-database', () => {
    it('processes multiple databases in a cycle', () => {
      const { db: db0, setTime: setTime0 } = createDb(1000);
      const { db: db1, setTime: setTime1 } = createDb(1000);

      setKey(db0, 'a', 2000);
      setKey(db0, 'b', 2000);
      setKey(db1, 'c', 2000);

      setTime0(2001);
      setTime1(2001);

      const result = activeExpireCycle({
        databases: [db0, db1],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(3);
      expect(db0.size).toBe(0);
      expect(db1.size).toBe(0);
    });

    it('time budget applies across all databases', () => {
      const dbs: Database[] = [];
      for (let d = 0; d < 4; d++) {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < 1000; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        dbs.push(db);
      }

      // Very fast clock — should time out before processing all dbs
      const result = activeExpireCycle({
        databases: dbs,
        clock: advancingClock(2001, 10),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.timedOut).toBe(true);
      const totalRemaining = dbs.reduce((sum, db) => sum + db.size, 0);
      expect(totalRemaining).toBeGreaterThan(0);
    });
  });

  describe('Database.expirySize', () => {
    it('returns number of keys with TTL', () => {
      const { db } = createDb(1000);

      expect(db.expirySize).toBe(0);

      setKey(db, 'k1', 5000);
      expect(db.expirySize).toBe(1);

      setKey(db, 'k2', 6000);
      expect(db.expirySize).toBe(2);

      setKey(db, 'k3');
      expect(db.expirySize).toBe(2);
    });

    it('decreases when TTL key is deleted', () => {
      const { db } = createDb(1000);

      setKey(db, 'k1', 5000);
      setKey(db, 'k2', 5000);
      expect(db.expirySize).toBe(2);

      db.delete('k1');
      expect(db.expirySize).toBe(1);
    });
  });

  describe('Database.sampleExpiryKeys', () => {
    it('returns up to count random keys from expiry', () => {
      const { db } = createDb(1000);

      for (let i = 0; i < 50; i++) {
        setKey(db, `k${i}`, 5000);
      }

      const sample = db.sampleExpiryKeys(20, () => Math.random());
      expect(sample.length).toBe(20);

      for (const key of sample) {
        expect(db.getExpiry(key)).toBeDefined();
      }
    });

    it('returns all keys when count exceeds expiry size', () => {
      const { db } = createDb(1000);

      setKey(db, 'a', 5000);
      setKey(db, 'b', 5000);
      setKey(db, 'c', 5000);

      const sample = db.sampleExpiryKeys(20, () => Math.random());
      expect(sample.length).toBe(3);
    });

    it('returns empty array when no keys have TTL', () => {
      const { db } = createDb(1000);
      setKey(db, 'k1');
      expect(db.sampleExpiryKeys(20, () => Math.random()).length).toBe(0);
    });

    it('returns unique keys (no duplicates)', () => {
      const { db } = createDb(1000);

      for (let i = 0; i < 50; i++) {
        setKey(db, `k${i}`, 5000);
      }

      const sample = db.sampleExpiryKeys(20, () => Math.random());
      const unique = new Set(sample);
      expect(unique.size).toBe(sample.length);
    });
  });

  describe('hash field expiration', () => {
    function setHash(
      db: Database,
      key: string,
      fields: Record<string, string>,
      fieldExpiries?: Record<string, number>
    ): void {
      const map = new Map(Object.entries(fields));
      db.set(key, 'hash', 'hashtable', map);
      if (fieldExpiries) {
        for (const [field, expiry] of Object.entries(fieldExpiries)) {
          db.setFieldExpiry(key, field, expiry);
        }
      }
    }

    it('deletes expired hash fields', () => {
      const { db, setTime } = createDb(1000);

      setHash(db, 'h1', { f1: 'v1', f2: 'v2' }, { f1: 2000, f2: 2000 });
      setHash(db, 'h2', { f1: 'v1' }, { f1: 2000 });

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.fieldExpired).toBe(3);
      // All fields expired → keys should be deleted
      expect(db.has('h1')).toBe(false);
      expect(db.has('h2')).toBe(false);
    });

    it('preserves non-expired hash fields', () => {
      const { db, setTime } = createDb(1000);

      setHash(db, 'h', { f1: 'v1', f2: 'v2' }, { f1: 2000, f2: 5000 });

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.fieldExpired).toBe(1);
      expect(db.has('h')).toBe(true);
      const hash = db.get('h')?.value as Map<string, string>;
      expect(hash.has('f1')).toBe(false);
      expect(hash.has('f2')).toBe(true);
    });

    it('deletes empty hash after all fields expired', () => {
      const { db, setTime } = createDb(1000);

      setHash(db, 'h', { f1: 'v1' }, { f1: 2000 });

      setTime(2001);

      activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(db.has('h')).toBe(false);
      expect(db.size).toBe(0);
    });

    it('returns zero fieldExpired when no field expiry exists', () => {
      const { db } = createDb(1000);

      setKey(db, 'k1');
      setKey(db, 'k2');

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.fieldExpired).toBe(0);
    });

    it('skips databases with no field expiry keys', () => {
      const { db: db0 } = createDb(1000);
      const { db: db1, setTime: setTime1 } = createDb(1000);

      setKey(db0, 'k1');
      setHash(db1, 'h', { f1: 'v1' }, { f1: 2000 });
      setTime1(2001);

      const result = activeExpireCycle({
        databases: [db0, db1],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.fieldExpired).toBe(1);
    });

    it('handles both key and field expiration in the same cycle', () => {
      const { db, setTime } = createDb(1000);

      // Key-level expiry
      setKey(db, 'str1', 2000);
      setKey(db, 'str2', 2000);

      // Field-level expiry
      setHash(db, 'h', { f1: 'v1', f2: 'v2' }, { f1: 2000 });

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(2);
      expect(result.fieldExpired).toBe(1);
      expect(db.has('str1')).toBe(false);
      expect(db.has('str2')).toBe(false);
      expect(db.has('h')).toBe(true);
      const hash = db.get('h')?.value as Map<string, string>;
      expect(hash.has('f1')).toBe(false);
      expect(hash.has('f2')).toBe(true);
    });

    it('continues sampling fields when expired ratio is high', () => {
      const { db, setTime } = createDb(1000);

      // Create a hash with many expired fields
      const fields: Record<string, string> = {};
      const expiries: Record<string, number> = {};
      for (let i = 0; i < 200; i++) {
        fields[`f${i}`] = `v${i}`;
        expiries[`f${i}`] = 2000;
      }
      setHash(db, 'h', fields, expiries);

      setTime(2001);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.fieldExpired).toBe(200);
      expect(db.has('h')).toBe(false);
    });

    it('time budget applies to field expiration', () => {
      const { db, setTime } = createDb(1000);

      // Create many hashes with expired fields
      for (let i = 0; i < 500; i++) {
        setHash(db, `h${i}`, { f1: 'v1', f2: 'v2' }, { f1: 2000, f2: 2000 });
      }

      setTime(2001);

      // Very fast advancing clock — should time out
      const result = activeExpireCycle({
        databases: [db],
        clock: advancingClock(2001, 5),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.timedOut).toBe(true);
      // Some fields expired, but not all due to time budget
      expect(result.fieldExpired).toBeGreaterThan(0);
    });
  });

  describe('edge cases', () => {
    it('handles empty databases', () => {
      const { db } = createDb(1000);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(0);
    });

    it('handles key expiring exactly at current time', () => {
      const { db, setTime } = createDb(1000);

      setKey(db, 'k', 2000);
      setTime(2000);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      // Redis deletes keys where clock >= expiry
      expect(result.expired).toBe(1);
      expect(db.size).toBe(0);
    });

    it('does not delete keys that expire in the future', () => {
      const { db, setTime } = createDb(1000);

      setKey(db, 'k', 3000);
      setTime(2000);

      const result = activeExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(0);
      expect(db.size).toBe(1);
    });

    it('handles all databases empty', () => {
      const dbs = Array.from({ length: 16 }, () => {
        const { db } = createDb(1000);
        return db;
      });

      const result = activeExpireCycle({
        databases: dbs,
        clock: () => 2000,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(result.expired).toBe(0);
      expect(result.timedOut).toBe(false);
    });
  });
});

describe('fastActiveExpireCycle', () => {
  describe('conditional execution', () => {
    it('skips when last slow cycle did not time out', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 50; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: false,
        lastFastCycleTime: 0,
      };

      const result = fastActiveExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(true);
      expect(result.expired).toBe(0);
      // Keys should remain untouched
      expect(db.size).toBe(50);
    });

    it('runs when last slow cycle timed out', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 50; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      const result = fastActiveExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(false);
      expect(result.expired).toBe(50);
      expect(db.size).toBe(0);
    });

    it('skips when cooldown has not elapsed', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 50; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 2000, // ran 1ms ago, cooldown is 2ms
      };

      const result = fastActiveExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(true);
      expect(result.expired).toBe(0);
      expect(db.size).toBe(50);
    });

    it('runs when cooldown has elapsed', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 50; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 1998, // ran 3ms ago, cooldown is 2ms
      };

      const result = fastActiveExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(false);
      expect(result.expired).toBe(50);
    });
  });

  describe('time budget', () => {
    it('respects fixed 1ms time budget', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 5000; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      // Clock advances 0.5ms per call — budget is 1ms, so ~2 clock reads
      const result = fastActiveExpireCycle({
        databases: [db],
        clock: advancingClock(2001, 0.5),
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(false);
      expect(result.expired).toBeGreaterThan(0);
      expect(result.expired).toBeLessThan(5000);
      expect(result.timedOut).toBe(true);
    });

    it('time budget is fixed regardless of hz', () => {
      // Unlike slow cycle, fast cycle always has 1ms budget
      const makeDb = () => {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < 2000; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        return db;
      };

      const state1: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };
      const state2: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      const db1 = makeDb();
      const r1 = fastActiveExpireCycle({
        databases: [db1],
        clock: advancingClock(2001, 0.1),
        rng: () => Math.random(),
        effort: 1,
        state: state1,
      });

      const db2 = makeDb();
      const r2 = fastActiveExpireCycle({
        databases: [db2],
        clock: advancingClock(2001, 0.1),
        rng: () => Math.random(),
        effort: 1,
        state: state2,
      });

      // Both should expire approximately the same number of keys
      // since budget is the same (1ms) regardless of any other config
      expect(r1.expired).toBe(r2.expired);
    });

    it('budget is smaller than slow cycle budget', () => {
      const keyCount = 5000;
      const makeDb = () => {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < keyCount; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        return db;
      };

      // Slow cycle: at hz=10, effort=1, budget = 2.5ms
      const dbSlow = makeDb();
      const slowResult = activeExpireCycle({
        databases: [dbSlow],
        clock: advancingClock(2001, 0.1),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      // Fast cycle: fixed 1ms budget
      const dbFast = makeDb();
      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };
      const fastResult = fastActiveExpireCycle({
        databases: [dbFast],
        clock: advancingClock(2001, 0.1),
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      // Both should time out, but slow cycle deletes more
      expect(slowResult.timedOut).toBe(true);
      expect(fastResult.timedOut).toBe(true);
      expect(slowResult.expired).toBeGreaterThan(fastResult.expired);
    });
  });

  describe('state management', () => {
    it('updates lastFastCycleTime on run', () => {
      const { db, setTime } = createDb(1000);
      setKey(db, 'k1', 2000);
      setTime(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      fastActiveExpireCycle({
        databases: [db],
        clock: () => 5000,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(state.lastFastCycleTime).toBe(5000);
    });

    it('does not update lastFastCycleTime when skipped', () => {
      const { db } = createDb(1000);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: false,
        lastFastCycleTime: 100,
      };

      fastActiveExpireCycle({
        databases: [db],
        clock: () => 5000,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(state.lastFastCycleTime).toBe(100);
    });

    it('createFastExpireCycleState returns default state', () => {
      const state = createFastExpireCycleState();
      expect(state.lastSlowTimedOut).toBe(false);
      expect(state.lastFastCycleTime).toBe(0);
    });
  });

  describe('integration with slow cycle', () => {
    it('slow cycle timedOut triggers fast cycle', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 5000; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      // Slow cycle with tight time budget — will time out
      const slowResult = activeExpireCycle({
        databases: [db],
        clock: advancingClock(2001, 5),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(slowResult.timedOut).toBe(true);
      const afterSlow = db.size;

      // Feed slow result into fast cycle state
      const state: FastExpireCycleState = {
        lastSlowTimedOut: slowResult.timedOut,
        lastFastCycleTime: 0,
      };

      const fastResult = fastActiveExpireCycle({
        databases: [db],
        clock: () => 3000,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(fastResult.skipped).toBe(false);
      expect(fastResult.expired).toBeGreaterThan(0);
      expect(db.size).toBeLessThan(afterSlow);
    });

    it('slow cycle not timing out prevents fast cycle', () => {
      const { db, setTime } = createDb(1000);

      for (let i = 0; i < 10; i++) {
        setKey(db, `k${i}`, 2000);
      }
      setTime(2001);

      // Slow cycle with plenty of time — will not time out
      const slowResult = activeExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      expect(slowResult.timedOut).toBe(false);

      // Add more expired keys after slow cycle
      for (let i = 100; i < 110; i++) {
        setKey(db, `k${i}`, 2000);
      }

      const state: FastExpireCycleState = {
        lastSlowTimedOut: slowResult.timedOut,
        lastFastCycleTime: 0,
      };

      const fastResult = fastActiveExpireCycle({
        databases: [db],
        clock: () => 3000,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(fastResult.skipped).toBe(true);
      expect(fastResult.expired).toBe(0);
    });
  });

  describe('effort scaling', () => {
    it('higher effort samples more keys per loop', () => {
      const keyCount = 5000;
      const makeDb = () => {
        const { db, setTime } = createDb(1000);
        for (let i = 0; i < keyCount; i++) {
          setKey(db, `k${i}`, 2000);
        }
        setTime(2001);
        return db;
      };

      const state1: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };
      const state10: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      const db1 = makeDb();
      const r1 = fastActiveExpireCycle({
        databases: [db1],
        clock: advancingClock(2001, 1),
        rng: () => Math.random(),
        effort: 1,
        state: state1,
      });

      const db10 = makeDb();
      const r10 = fastActiveExpireCycle({
        databases: [db10],
        clock: advancingClock(2001, 1),
        rng: () => Math.random(),
        effort: 10,
        state: state10,
      });

      // Both time out with same 1ms budget, but effort=10 samples more per iteration
      expect(r1.timedOut).toBe(true);
      expect(r10.timedOut).toBe(true);
      expect(r10.expired).toBeGreaterThan(r1.expired);
    });
  });

  describe('edge cases', () => {
    it('handles empty databases', () => {
      const { db } = createDb(1000);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      const result = fastActiveExpireCycle({
        databases: [db],
        clock: () => 2000,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(false);
      expect(result.expired).toBe(0);
    });

    it('handles cooldown exactly at boundary', () => {
      const { db, setTime } = createDb(1000);
      setKey(db, 'k1', 2000);
      setTime(2001);

      // Cooldown is 2ms. lastFastCycleTime=1999, now=2001 → exactly 2ms elapsed
      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 1999,
      };

      const result = fastActiveExpireCycle({
        databases: [db],
        clock: () => 2001,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      // 2001 >= 1999 + 2 → should run
      expect(result.skipped).toBe(false);
      expect(result.expired).toBe(1);
    });

    it('handles multiple databases', () => {
      const { db: db0, setTime: setTime0 } = createDb(1000);
      const { db: db1, setTime: setTime1 } = createDb(1000);

      setKey(db0, 'a', 2000);
      setKey(db1, 'b', 2000);
      setTime0(2001);
      setTime1(2001);

      const state: FastExpireCycleState = {
        lastSlowTimedOut: true,
        lastFastCycleTime: 0,
      };

      const result = fastActiveExpireCycle({
        databases: [db0, db1],
        clock: () => 2001,
        rng: () => Math.random(),
        effort: 1,
        state,
      });

      expect(result.skipped).toBe(false);
      expect(result.expired).toBe(2);
      expect(db0.size).toBe(0);
      expect(db1.size).toBe(0);
    });
  });
});
