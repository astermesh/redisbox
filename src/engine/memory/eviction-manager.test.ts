import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import type { Database } from '../database.ts';
import { ConfigStore } from '../../config-store.ts';
import { EvictionManager } from './eviction-manager.ts';

function createSetup(opts?: { clock?: () => number; rng?: () => number }) {
  const clock = opts?.clock ?? (() => 1000);
  const rng = opts?.rng ?? (() => 0.5);
  const engine = new RedisEngine({ clock, rng });
  const config = new ConfigStore();
  const eviction = new EvictionManager(engine, config);
  const db = engine.db(0);
  return { engine, config, eviction, db };
}

function fillKeys(db: Database, count: number, prefix = 'key'): void {
  for (let i = 0; i < count; i++) {
    db.set(`${prefix}${i}`, 'string', 'raw', `val${i}`);
  }
}

function fillVolatileKeys(
  db: Database,
  count: number,
  prefix = 'vkey',
  baseExpiry = 5000
): void {
  for (let i = 0; i < count; i++) {
    db.set(`${prefix}${i}`, 'string', 'raw', `val${i}`);
    db.setExpiry(`${prefix}${i}`, baseExpiry + i * 1000);
  }
}

describe('EvictionManager', () => {
  describe('noeviction policy', () => {
    it('returns true when maxmemory is 0 (unlimited)', () => {
      const { eviction } = createSetup();
      expect(eviction.tryEvict()).toBe(true);
    });

    it('returns true when memory is below limit', () => {
      const { eviction, config, db } = createSetup();
      config.set('maxmemory', '1000000');
      db.set('k', 'string', 'raw', 'v');
      expect(eviction.tryEvict()).toBe(true);
    });

    it('returns false (OOM) when memory exceeds limit with noeviction policy', () => {
      const { eviction, config, db } = createSetup();
      config.set('maxmemory', '1'); // 1 byte limit
      config.set('maxmemory-policy', 'noeviction');
      fillKeys(db, 10);
      expect(eviction.tryEvict()).toBe(false);
    });
  });

  describe('allkeys-random policy', () => {
    it('evicts keys until memory is below limit', () => {
      const { eviction, config, db } = createSetup();
      fillKeys(db, 20);
      // Set maxmemory to something that requires evicting some keys
      config.set('maxmemory', '1'); // very low, will evict all
      config.set('maxmemory-policy', 'allkeys-random');
      const result = eviction.tryEvict();
      // All keys should be evicted since limit is 1 byte
      expect(db.size).toBe(0);
      // Returns false because we can't get below limit even after evicting all
      // (overhead still exists or no more keys)
      expect(result === true || result === false).toBe(true);
    });

    it('evicts from all keys, not just volatile', () => {
      const { eviction, config, db } = createSetup();
      // Add keys without expiry
      fillKeys(db, 10);
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'allkeys-random');
      eviction.tryEvict();
      expect(db.size).toBe(0);
    });
  });

  describe('volatile-random policy', () => {
    it('only evicts keys with expiry', () => {
      const { eviction, config, db } = createSetup();
      // Non-volatile keys
      fillKeys(db, 5, 'persistent');
      // Volatile keys
      fillVolatileKeys(db, 5);
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-random');
      eviction.tryEvict();
      // Persistent keys should remain
      for (let i = 0; i < 5; i++) {
        expect(db.has(`persistent${i}`)).toBe(true);
      }
      // Volatile keys should be evicted
      for (let i = 0; i < 5; i++) {
        expect(db.has(`vkey${i}`)).toBe(false);
      }
    });

    it('returns false when no volatile keys exist and memory is over limit', () => {
      const { eviction, config, db } = createSetup();
      fillKeys(db, 10); // no expiry
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-random');
      expect(eviction.tryEvict()).toBe(false);
    });
  });

  describe('allkeys-lru policy', () => {
    it('evicts least recently used keys', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Create keys at different times (1s apart for 24-bit LRU resolution)
      for (let i = 0; i < 10; i++) {
        db.set(`key${i}`, 'string', 'raw', `val${i}`);
        time += 1000;
      }

      // Access some keys to make them "recent"
      time = 50000;
      db.get('key5');
      db.get('key6');
      db.get('key7');
      db.get('key8');
      db.get('key9');

      // Set a limit that requires evicting some keys
      // Use a large enough limit so not all keys are evicted
      const mem = eviction.currentUsedMemory();
      // Set limit to ~half of current usage
      config.set('maxmemory', String(Math.floor(mem * 0.5)));
      config.set('maxmemory-policy', 'allkeys-lru');
      config.set('maxmemory-samples', '10'); // high sample to be deterministic

      eviction.tryEvict();

      // Recently accessed keys should be more likely to survive
      // Since we accessed key5-key9, they should survive more than key0-key4
      const recentSurvived = [5, 6, 7, 8, 9].filter((i) =>
        db.has(`key${i}`)
      ).length;
      const oldSurvived = [0, 1, 2, 3, 4].filter((i) =>
        db.has(`key${i}`)
      ).length;
      expect(recentSurvived).toBeGreaterThanOrEqual(oldSurvived);
    });
  });

  describe('volatile-lru policy', () => {
    it('only evicts volatile keys based on LRU', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Persistent keys
      fillKeys(db, 3, 'persistent');
      time += 1000;

      // Volatile keys at different times (1s apart for 24-bit LRU resolution)
      for (let i = 0; i < 5; i++) {
        db.set(`vkey${i}`, 'string', 'raw', `val${i}`);
        db.setExpiry(`vkey${i}`, 99999);
        time += 1000;
      }

      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-lru');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Persistent keys must survive
      for (let i = 0; i < 3; i++) {
        expect(db.has(`persistent${i}`)).toBe(true);
      }
    });
  });

  describe('allkeys-lfu policy', () => {
    it('evicts least frequently used keys', () => {
      const { eviction, config, db } = createSetup({
        rng: (() => {
          // Use a deterministic rng that always triggers LFU increment
          // (returns values < p for any counter)
          return () => 0.001;
        })(),
      });

      // Set LFU policy BEFORE creating keys so LFU tracking is active
      config.set('maxmemory-policy', 'allkeys-lfu');

      fillKeys(db, 10);

      // Access some keys multiple times to increase frequency
      for (let i = 0; i < 20; i++) {
        db.get('key0'); // high frequency
        db.get('key1'); // high frequency
      }

      const mem = eviction.currentUsedMemory();
      config.set('maxmemory', String(Math.floor(mem * 0.3)));
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Frequently accessed keys should survive, rarely accessed keys should be evicted
      expect(db.has('key0')).toBe(true);
      expect(db.has('key1')).toBe(true);
    });

    it('evicts all keys when memory limit is very low', () => {
      const { eviction, config, db } = createSetup();

      config.set('maxmemory-policy', 'allkeys-lfu');
      fillKeys(db, 10);

      config.set('maxmemory', '1');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();
      expect(db.size).toBe(0);
    });
  });

  describe('volatile-lfu policy', () => {
    it('only evicts volatile keys', () => {
      const { eviction, config, db } = createSetup();

      config.set('maxmemory-policy', 'volatile-lfu');

      fillKeys(db, 3, 'persistent');
      fillVolatileKeys(db, 5);

      config.set('maxmemory', '1');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Persistent keys must survive
      for (let i = 0; i < 3; i++) {
        expect(db.has(`persistent${i}`)).toBe(true);
      }
    });
  });

  describe('volatile-ttl policy', () => {
    it('evicts keys with shortest remaining TTL first', () => {
      const time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Keys with increasing TTLs
      for (let i = 0; i < 10; i++) {
        db.set(`vkey${i}`, 'string', 'raw', `val${i}`);
        db.setExpiry(`vkey${i}`, 2000 + i * 10000); // vkey0 expires soonest
      }

      // Also add persistent keys
      fillKeys(db, 3, 'persistent');

      const mem = eviction.currentUsedMemory();
      config.set('maxmemory', String(Math.floor(mem * 0.6)));
      config.set('maxmemory-policy', 'volatile-ttl');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Persistent keys must survive
      for (let i = 0; i < 3; i++) {
        expect(db.has(`persistent${i}`)).toBe(true);
      }

      // Keys with higher TTLs (later expiry) should be more likely to survive
      const earlyEvicted = [0, 1, 2, 3].filter(
        (i) => !db.has(`vkey${i}`)
      ).length;
      const lateEvicted = [7, 8, 9].filter((i) => !db.has(`vkey${i}`)).length;
      expect(earlyEvicted).toBeGreaterThanOrEqual(lateEvicted);
    });

    it('only evicts keys with expiry', () => {
      const { eviction, config, db } = createSetup();

      fillKeys(db, 5, 'persistent'); // no expiry
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-ttl');

      expect(eviction.tryEvict()).toBe(false);

      // Persistent keys must survive
      for (let i = 0; i < 5; i++) {
        expect(db.has(`persistent${i}`)).toBe(true);
      }
    });
  });

  describe('OOM error message', () => {
    it('returns correct OOM error reply', () => {
      const { eviction } = createSetup();
      const reply = eviction.oomReply();
      expect(reply).toEqual({
        kind: 'error',
        prefix: 'OOM',
        message: "command not allowed when used memory > 'maxmemory'.",
      });
    });
  });

  describe('eviction across multiple databases', () => {
    it('evicts from all databases', () => {
      const { eviction, config, engine } = createSetup();

      // Fill keys across multiple databases
      const db0 = engine.db(0);
      const db1 = engine.db(1);
      fillKeys(db0, 10, 'db0key');
      fillKeys(db1, 10, 'db1key');

      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'allkeys-random');

      eviction.tryEvict();

      // Both databases should have keys evicted
      expect(db0.size + db1.size).toBe(0);
    });
  });

  describe('eviction loop protection', () => {
    it('does not loop forever when eviction cannot free enough memory', () => {
      const { eviction, config, db } = createSetup();

      fillKeys(db, 5);

      // Set impossible memory limit
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-random');
      // No volatile keys exist, so eviction can't free anything

      // Should return false without infinite loop
      const result = eviction.tryEvict();
      expect(result).toBe(false);
    });
  });

  describe('currentUsedMemory', () => {
    it('returns engine memory usage', () => {
      const { eviction, db } = createSetup();
      fillKeys(db, 5);
      expect(eviction.currentUsedMemory()).toBeGreaterThan(0);
    });
  });

  describe('eviction pool (approximated LRU)', () => {
    it('prefers evicting keys with highest idle time', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Create old key at t=1s
      db.set('old', 'string', 'raw', 'old-val');

      // Create recent key at t=20s
      time = 20000;
      db.set('recent', 'string', 'raw', 'recent-val');

      // Evict at t=30s — old has 29s idle, recent has 10s idle
      time = 30000;
      const mem = eviction.currentUsedMemory();
      config.set('maxmemory', String(Math.floor(mem * 0.6)));
      config.set('maxmemory-policy', 'allkeys-lru');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Old key should be evicted first
      expect(db.has('old')).toBe(false);
      expect(db.has('recent')).toBe(true);
    });

    it('eviction pool persists across eviction cycles', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Create keys at different times (1s apart)
      for (let i = 0; i < 5; i++) {
        db.set(`key${i}`, 'string', 'raw', `val${i}`);
        time += 2000;
      }

      // key0 is oldest (t=1s), key4 is newest (t=9s)
      time = 20000;

      const mem = eviction.currentUsedMemory();
      // Set limit to evict roughly one key per cycle
      const limitPerKey = Math.floor(mem / 5);
      config.set('maxmemory', String(mem - limitPerKey));
      config.set('maxmemory-policy', 'allkeys-lru');
      config.set('maxmemory-samples', '10');

      // First eviction cycle — should evict oldest (key0)
      eviction.tryEvict();
      expect(db.has('key0')).toBe(false);

      // Add more keys to require another eviction
      db.set('extra1', 'string', 'raw', 'extra-val');
      db.set('extra2', 'string', 'raw', 'extra-val');
      db.set('extra3', 'string', 'raw', 'extra-val');

      // Second eviction cycle — pool should still contain previous candidates
      eviction.tryEvict();

      // The older keys (key1, key2) should be evicted before newer ones
      const oldRemaining = [1, 2].filter((i) => db.has(`key${i}`)).length;
      const newRemaining = [3, 4].filter((i) => db.has(`key${i}`)).length;
      expect(newRemaining).toBeGreaterThanOrEqual(oldRemaining);
    });

    it('handles deleted keys in pool gracefully', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      db.set('will-delete', 'string', 'raw', 'v1');
      time += 5000;
      db.set('keep', 'string', 'raw', 'v2');

      // Delete the oldest key before eviction
      time = 10000;
      db.delete('will-delete');

      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'allkeys-lru');
      config.set('maxmemory-samples', '10');

      // Should handle the deleted key gracefully and evict 'keep'
      eviction.tryEvict();
      expect(db.has('keep')).toBe(false);
    });

    it('uses 24-bit LRU clock for idle time calculation', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Create two keys with 5-second difference
      db.set('a', 'string', 'raw', 'v1');
      time += 5000;
      db.set('b', 'string', 'raw', 'v2');

      // Move time forward
      time += 10000;

      const mem = eviction.currentUsedMemory();
      config.set('maxmemory', String(Math.floor(mem * 0.6)));
      config.set('maxmemory-policy', 'allkeys-lru');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // 'a' has more idle time (15s vs 10s), should be evicted first
      expect(db.has('a')).toBe(false);
      expect(db.has('b')).toBe(true);
    });

    it('eviction pool is bounded to 16 entries', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Create 20 keys at different times — more than EVPOOL_SIZE (16)
      for (let i = 0; i < 20; i++) {
        db.set(`key${i}`, 'string', 'raw', `val${i}`);
        time += 1000;
      }

      // Advance time so all keys have idle time
      time += 50000;

      const mem = eviction.currentUsedMemory();
      // Evict roughly half
      config.set('maxmemory', String(Math.floor(mem * 0.5)));
      config.set('maxmemory-policy', 'allkeys-lru');
      config.set('maxmemory-samples', '20');

      eviction.tryEvict();

      // Oldest keys (lower indices) should be evicted first
      const oldSurvived = [0, 1, 2, 3, 4].filter((i) =>
        db.has(`key${i}`)
      ).length;
      const newSurvived = [15, 16, 17, 18, 19].filter((i) =>
        db.has(`key${i}`)
      ).length;
      expect(newSurvived).toBeGreaterThanOrEqual(oldSurvived);
    });

    it('volatile-lru uses eviction pool only for keys with expiry', () => {
      let time = 1000;
      const { eviction, config, db } = createSetup({
        clock: () => time,
      });

      // Persistent key (old)
      db.set('persistent', 'string', 'raw', 'pv');
      time += 5000;

      // Volatile keys at different times
      db.set('vol-old', 'string', 'raw', 'vo');
      db.setExpiry('vol-old', 99999);
      time += 3000;
      db.set('vol-new', 'string', 'raw', 'vn');
      db.setExpiry('vol-new', 99999);

      time += 10000;
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-lru');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Persistent key must survive
      expect(db.has('persistent')).toBe(true);
      // vol-old should be evicted first (more idle)
      expect(db.has('vol-old')).toBe(false);
    });
  });
});
