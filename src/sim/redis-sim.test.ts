import { describe, it, expect } from 'vitest';
import { RedisSim } from './redis-sim.ts';

describe('RedisSim', () => {
  describe('construction', () => {
    it('creates an engine with a virtual clock', () => {
      const sim = new RedisSim();
      const t = sim.engine.clock();
      expect(typeof t).toBe('number');
      expect(t).toBeGreaterThan(0);
    });

    it('accepts custom rng', () => {
      const sim = new RedisSim({ rng: () => 0.5 });
      expect(sim.engine.rng()).toBe(0.5);
    });

    it('exposes the virtual clock', () => {
      const sim = new RedisSim();
      expect(sim.clock.isFrozen).toBe(false);
    });
  });

  describe('time control propagation', () => {
    it('freezeTime stops engine clock', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const t1 = sim.engine.clock();
      const t2 = sim.engine.clock();
      expect(t2).toBe(t1);
    });

    it('advanceTime moves engine clock forward', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const t1 = sim.engine.clock();
      sim.advanceTime(1000);
      const t2 = sim.engine.clock();
      expect(t2).toBe(t1 + 1000);
    });

    it('setTime sets engine clock to specific value', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      sim.setTime(1700000000000);
      expect(sim.engine.clock()).toBe(1700000000000);
    });

    it('unfreezeTime resumes engine clock', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      sim.unfreezeTime();
      expect(sim.clock.isFrozen).toBe(false);
    });
  });

  describe('time control affects expiration', () => {
    it('keys expire when time advances past TTL', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const now = sim.engine.clock();

      const db = sim.engine.db(0);
      db.set('mykey', 'string', 'raw', 'hello');
      db.setExpiry('mykey', now + 1000);

      expect(db.get('mykey')).not.toBeNull();

      sim.advanceTime(999);
      expect(db.get('mykey')).not.toBeNull();

      sim.advanceTime(1);
      expect(db.get('mykey')).toBeNull();
    });

    it('frozen time prevents key expiration', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const now = sim.engine.clock();

      const db = sim.engine.db(0);
      db.set('mykey', 'string', 'raw', 'hello');
      db.setExpiry('mykey', now + 100);

      // Time is frozen — key should not expire
      expect(db.get('mykey')).not.toBeNull();
      expect(db.get('mykey')).not.toBeNull();
    });

    it('setTime past TTL causes expiration', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const now = sim.engine.clock();

      const db = sim.engine.db(0);
      db.set('mykey', 'string', 'raw', 'val');
      db.setExpiry('mykey', now + 500);

      sim.setTime(now + 500);
      expect(db.get('mykey')).toBeNull();
    });
  });

  describe('time control affects all databases', () => {
    it('expiration works across multiple databases', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const now = sim.engine.clock();

      const db0 = sim.engine.db(0);
      const db1 = sim.engine.db(1);

      db0.set('k0', 'string', 'raw', 'v0');
      db0.setExpiry('k0', now + 100);

      db1.set('k1', 'string', 'raw', 'v1');
      db1.setExpiry('k1', now + 200);

      sim.advanceTime(100);
      expect(db0.get('k0')).toBeNull();
      expect(db1.get('k1')).not.toBeNull();

      sim.advanceTime(100);
      expect(db1.get('k1')).toBeNull();
    });
  });

  describe('time control affects lruClock', () => {
    it('database entries get lruClock from virtual time', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      sim.setTime(5000);

      const db = sim.engine.db(0);
      db.set('k', 'string', 'raw', 'v');
      const entry = db.get('k');
      expect(entry?.lruClock).toBe(5000);

      sim.advanceTime(1000);
      db.get('k');
      const entry2 = db.get('k');
      expect(entry2?.lruClock).toBe(6000);
    });
  });

  describe('time control affects hash field expiry', () => {
    it('hash fields expire when time advances', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const now = sim.engine.clock();

      const db = sim.engine.db(0);
      const hash = new Map<string, string>();
      hash.set('f1', 'v1');
      hash.set('f2', 'v2');
      db.set('myhash', 'hash', 'hashtable', hash);

      db.setFieldExpiry('myhash', 'f1', now + 100);

      sim.advanceTime(100);
      const expired = db.expireHashFields('myhash');
      expect(expired).toBe(1);

      const entry = db.get('myhash');
      expect(entry).not.toBeNull();
      const remaining = (entry as { value: Map<string, string> }).value;
      expect(remaining.has('f1')).toBe(false);
      expect(remaining.has('f2')).toBe(true);
    });
  });

  describe('freeze-advance-unfreeze cycle', () => {
    it('correctly manages time through full cycle', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const t0 = sim.engine.clock();

      sim.advanceTime(5000);
      expect(sim.engine.clock()).toBe(t0 + 5000);

      sim.unfreezeTime();
      // After unfreezing, time should flow again
      expect(sim.clock.isFrozen).toBe(false);

      // Engine clock should be approximately t0 + 5000 + elapsed
      const t1 = sim.engine.clock();
      expect(t1).toBeGreaterThanOrEqual(t0 + 5000);
    });
  });
});
