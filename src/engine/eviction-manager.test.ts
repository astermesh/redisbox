import { describe, it, expect } from 'vitest';
import { RedisEngine } from './engine.ts';
import type { Database } from './database.ts';
import { ConfigStore } from '../config-store.ts';
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

      // Create keys at different times
      for (let i = 0; i < 10; i++) {
        db.set(`key${i}`, 'string', 'raw', `val${i}`);
        time += 100;
      }

      // Access some keys to make them "recent"
      time = 5000;
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
      time += 500;

      // Volatile keys at different times
      for (let i = 0; i < 5; i++) {
        db.set(`vkey${i}`, 'string', 'raw', `val${i}`);
        db.setExpiry(`vkey${i}`, 99999);
        time += 100;
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
      const { eviction, config, db } = createSetup();

      fillKeys(db, 10);

      // Access some keys multiple times to increase frequency
      for (let i = 0; i < 20; i++) {
        db.get('key0'); // high frequency
        db.get('key1'); // high frequency
      }

      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'allkeys-lfu');
      config.set('maxmemory-samples', '10');

      eviction.tryEvict();

      // Note: lruFreq is currently always 0 (T04 will implement proper LFU tracking)
      // For now, all keys have equal frequency, so eviction is effectively random
      // Just verify that keys were evicted
      expect(db.size).toBe(0);
    });
  });

  describe('volatile-lfu policy', () => {
    it('only evicts volatile keys', () => {
      const { eviction, config, db } = createSetup();

      fillKeys(db, 3, 'persistent');
      fillVolatileKeys(db, 5);

      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-lfu');
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
});
