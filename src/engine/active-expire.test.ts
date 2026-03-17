import { describe, it, expect } from 'vitest';
import { Database } from './database.ts';
import { activeExpireCycle } from './active-expire.ts';

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

      // Clock advances 5ms per call; budget at hz=10 effort=1 is ~2.7ms
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

      // hz=10 has ~2.7ms budget, hz=100 has ~0.27ms budget
      const expired10 = 2000 - db10.size;
      const expired100 = 2000 - db100.size;
      expect(expired10).toBeGreaterThan(expired100);
    });
  });

  describe('effort scaling', () => {
    it('higher effort samples more keys per loop', () => {
      // effort=1: keys_per_loop = 25
      // effort=10: keys_per_loop = 70
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
      // Clock advances 1ms per call — at hz=10 effort=1 budget is ~2.7ms
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

      // effort=10 samples 70 keys per loop vs 25 — more expired per iteration
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
      // but not effort=100 budget
      // effort=1: budget ~2.7ms, effort=100: budget ~225ms
      const db1 = makeDb();
      const r1 = activeExpireCycle({
        databases: [db1],
        clock: advancingClock(2001, 0.5),
        rng: () => Math.random(),
        hz: 10,
        effort: 1,
      });

      const db100 = makeDb();
      const r100 = activeExpireCycle({
        databases: [db100],
        clock: advancingClock(2001, 0.5),
        rng: () => Math.random(),
        hz: 10,
        effort: 100,
      });

      // effort=1 should time out, effort=100 gets much more budget
      expect(r1.timedOut).toBe(true);
      expect(r100.expired).toBeGreaterThan(r1.expired);
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
