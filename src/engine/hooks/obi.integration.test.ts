import { describe, it, expect } from 'vitest';
import { RedisEngine } from '../engine.ts';
import { RedisSim } from '../../sim/redis-sim.ts';
import {
  CommandDispatcher,
  createTransactionState,
} from '../command-dispatcher.ts';
import { createCommandTable } from '../command-registry.ts';
import type { CommandContext } from '../types.ts';

describe('OBI hooks integration with RedisEngine', () => {
  describe('redis:time hook through engine', () => {
    it('engine.clock() goes through OBI time hook', () => {
      const engine = new RedisEngine({ clock: () => 1000 });
      engine.obi.hook('redis:time').tap((_next) => 5000);
      expect(engine.clock()).toBe(5000);
    });

    it('time hook affects expiration', () => {
      let baseTime = 1000;
      const engine = new RedisEngine({ clock: () => baseTime });
      const db = engine.db(0);

      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);

      // Key should exist at time 1000
      expect(db.get('k')).not.toBeNull();

      // Advance base time past expiry
      baseTime = 2000;
      expect(db.get('k')).toBeNull();
    });

    it('time hook override affects expiration check', () => {
      const engine = new RedisEngine({ clock: () => 1000 });
      const db = engine.db(0);

      db.set('k', 'string', 'raw', 'v');
      db.setExpiry('k', 2000);

      // Key exists at base time 1000
      expect(db.get('k')).not.toBeNull();

      // Hook overrides time to 2000 — past expiry
      engine.obi.hook('redis:time').tap((_next) => 2000);
      expect(db.get('k')).toBeNull();
    });

    it('time hook affects LRU clock in database entries', () => {
      const engine = new RedisEngine({ clock: () => 5000 });
      const db = engine.db(0);

      db.set('k', 'string', 'raw', 'v');
      const entry = db.get('k');
      // getLruClock: 5000ms → 5s
      expect(entry?.lruClock).toBe(5);

      // Override time to 10000ms
      engine.obi.hook('redis:time').tap((_next) => 10000);
      db.get('k'); // Touch to update LRU
      const entry2 = db.get('k');
      // 10000ms → 10s
      expect(entry2?.lruClock).toBe(10);
    });

    it('time hook affects all databases', () => {
      const engine = new RedisEngine({ clock: () => 1000 });
      const db0 = engine.db(0);
      const db1 = engine.db(1);

      db0.set('k0', 'string', 'raw', 'v0');
      db0.setExpiry('k0', 1500);
      db1.set('k1', 'string', 'raw', 'v1');
      db1.setExpiry('k1', 1500);

      expect(db0.get('k0')).not.toBeNull();
      expect(db1.get('k1')).not.toBeNull();

      engine.obi.hook('redis:time').tap((_next) => 1500);
      expect(db0.get('k0')).toBeNull();
      expect(db1.get('k1')).toBeNull();
    });
  });

  describe('redis:random hook through engine', () => {
    it('engine.rng() goes through OBI random hook', () => {
      const engine = new RedisEngine({ rng: () => 0.5 });
      engine.obi.hook('redis:random').tap((_next) => 0.42);
      expect(engine.rng()).toBe(0.42);
    });

    it('deterministic rng produces repeatable RANDOMKEY results', () => {
      let idx = 0;
      const sequence = [0.0, 0.0, 0.0];
      const engine = new RedisEngine({
        rng: () => sequence[idx++ % sequence.length] ?? 0,
      });
      const db = engine.db(0);

      db.set('a', 'string', 'raw', '1');
      db.set('b', 'string', 'raw', '2');
      db.set('c', 'string', 'raw', '3');

      // With deterministic rng always returning 0, RANDOMKEY should
      // consistently pick the first key in iteration order
      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = { db, engine };

      const result1 = dispatcher.dispatch(state, ctx, ['RANDOMKEY']);
      const result2 = dispatcher.dispatch(state, ctx, ['RANDOMKEY']);
      expect(result1).toEqual(result2);
    });

    it('random hook override affects SRANDMEMBER', () => {
      // Always return 0 from rng to get deterministic member selection
      const engine = new RedisEngine({ rng: () => 0.5 });
      engine.obi.hook('redis:random').tap((_next) => 0);

      const db = engine.db(0);
      const set = new Set(['a', 'b', 'c']);
      db.set('myset', 'set', 'hashtable', set);

      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = { db, engine };

      const result1 = dispatcher.dispatch(state, ctx, ['SRANDMEMBER', 'myset']);
      const result2 = dispatcher.dispatch(state, ctx, ['SRANDMEMBER', 'myset']);
      // Deterministic rng → same member each time
      expect(result1).toEqual(result2);
    });
  });

  describe('redis:persist hook through persistence commands', () => {
    function setupDispatcher(engine: RedisEngine) {
      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = { db: engine.db(0), engine };
      return { dispatcher, state, ctx };
    }

    it('BGSAVE fires persist hook', () => {
      const engine = new RedisEngine();
      const { dispatcher, state, ctx } = setupDispatcher(engine);
      const signals: string[] = [];

      engine.obi.hook('redis:persist').tap((next) => {
        const result = next();
        signals.push(result.action);
        return result;
      });

      dispatcher.dispatch(state, ctx, ['BGSAVE']);
      expect(signals).toEqual(['bgsave']);
    });

    it('BGSAVE SCHEDULE fires persist hook', () => {
      const engine = new RedisEngine();
      const { dispatcher, state, ctx } = setupDispatcher(engine);
      const signals: string[] = [];

      engine.obi.hook('redis:persist').tap((next) => {
        const result = next();
        signals.push(result.action);
        return result;
      });

      dispatcher.dispatch(state, ctx, ['BGSAVE', 'SCHEDULE']);
      expect(signals).toEqual(['bgsave']);
    });

    it('BGREWRITEAOF fires persist hook', () => {
      const engine = new RedisEngine();
      const { dispatcher, state, ctx } = setupDispatcher(engine);
      const signals: string[] = [];

      engine.obi.hook('redis:persist').tap((next) => {
        const result = next();
        signals.push(result.action);
        return result;
      });

      dispatcher.dispatch(state, ctx, ['BGREWRITEAOF']);
      expect(signals).toEqual(['bgrewriteaof']);
    });

    it('SAVE fires persist hook', () => {
      const engine = new RedisEngine();
      const { dispatcher, state, ctx } = setupDispatcher(engine);
      const signals: string[] = [];

      engine.obi.hook('redis:persist').tap((next) => {
        const result = next();
        signals.push(result.action);
        return result;
      });

      dispatcher.dispatch(state, ctx, ['SAVE']);
      expect(signals).toEqual(['save']);
    });

    it('persist hook can reject and it still returns normal reply', () => {
      const engine = new RedisEngine();
      const { dispatcher, state, ctx } = setupDispatcher(engine);

      engine.obi.hook('redis:persist').tap((_next) => ({
        action: 'bgsave',
        accepted: false,
      }));

      // The command still returns its normal reply (stub behavior)
      const result = dispatcher.dispatch(state, ctx, ['BGSAVE']);
      expect(result).toEqual({
        kind: 'status',
        value: 'Background saving started',
      });
    });
  });

  describe('OBI hooks through RedisSim', () => {
    it('sim exposes OBI hooks', () => {
      const sim = new RedisSim();
      expect(sim.obi).toBe(sim.engine.obi);
    });

    it('sim time control works through OBI hooks', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      const t1 = sim.engine.clock();
      sim.advanceTime(1000);
      expect(sim.engine.clock()).toBe(t1 + 1000);
    });

    it('external sim can tap time hook on top of VirtualClock', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      sim.setTime(1000);

      // Additional hook adds 500ms offset on top of VirtualClock
      sim.obi.hook('redis:time').tap((next) => next() + 500);
      expect(sim.engine.clock()).toBe(1500);

      sim.advanceTime(1000);
      expect(sim.engine.clock()).toBe(2500);
    });

    it('external sim can tap random hook for determinism', () => {
      const sim = new RedisSim();
      let idx = 0;
      const sequence = [0.1, 0.2, 0.3];
      sim.obi.hook('redis:random').tap((_next) => {
        return sequence[idx++ % sequence.length] ?? 0;
      });

      expect(sim.engine.rng()).toBe(0.1);
      expect(sim.engine.rng()).toBe(0.2);
      expect(sim.engine.rng()).toBe(0.3);
      expect(sim.engine.rng()).toBe(0.1);
    });

    it('persist hook fires during sim command execution', () => {
      const sim = new RedisSim();
      const signals: string[] = [];
      sim.obi.hook('redis:persist').tap((next) => {
        const result = next();
        signals.push(result.action);
        return result;
      });

      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = { db: sim.engine.db(0), engine: sim.engine };

      dispatcher.dispatch(state, ctx, ['BGSAVE']);
      dispatcher.dispatch(state, ctx, ['SAVE']);
      dispatcher.dispatch(state, ctx, ['BGREWRITEAOF']);

      expect(signals).toEqual(['bgsave', 'save', 'bgrewriteaof']);
    });
  });

  describe('stream IDs use hooked time', () => {
    it('XADD auto-generated IDs use OBI time hook', () => {
      const engine = new RedisEngine({ clock: () => 1000 });
      engine.obi.hook('redis:time').tap((_next) => 5000);

      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = { db: engine.db(0), engine };

      const result = dispatcher.dispatch(state, ctx, [
        'XADD',
        'stream',
        '*',
        'f',
        'v',
      ]);
      expect(result.kind).toBe('bulk');
      expect((result as { value: string }).value).toBe('5000-0');
    });
  });

  describe('LASTSAVE uses hooked time', () => {
    it('LASTSAVE returns value from OBI time hook', () => {
      const engine = new RedisEngine({ clock: () => 1000 });
      engine.obi.hook('redis:time').tap((_next) => 1_700_000_500_123);

      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = { db: engine.db(0), engine };

      const result = dispatcher.dispatch(state, ctx, ['LASTSAVE']);
      expect(result).toEqual({ kind: 'integer', value: 1_700_000_500 });
    });
  });
});
