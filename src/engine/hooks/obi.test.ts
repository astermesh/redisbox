import { describe, it, expect, vi } from 'vitest';
import { ObiHookManager } from './obi.ts';
import type { SyncHookFn } from './hook.ts';
import { RedisEngine } from '../engine.ts';
import { RedisSim } from '../../sim/redis-sim.ts';
import {
  CommandDispatcher,
  createTransactionState,
} from '../command-dispatcher.ts';
import { createCommandTable } from '../command-registry.ts';
import type { CommandContext } from '../types.ts';

describe('ObiHookManager', () => {
  describe('construction', () => {
    it('creates manager with base clock and rng', () => {
      const obi = new ObiHookManager();
      // Default functions should work
      expect(typeof obi.clock()).toBe('number');
      expect(typeof obi.rng()).toBe('number');
    });

    it('accepts custom base clock', () => {
      const obi = new ObiHookManager({ clock: () => 42000 });
      expect(obi.clock()).toBe(42000);
    });

    it('accepts custom base rng', () => {
      const obi = new ObiHookManager({ rng: () => 0.5 });
      expect(obi.rng()).toBe(0.5);
    });
  });

  describe('hook access', () => {
    it('returns time hook by name', () => {
      const obi = new ObiHookManager();
      const hook = obi.hook('redis:time');
      expect(hook).toBeDefined();
    });

    it('returns random hook by name', () => {
      const obi = new ObiHookManager();
      const hook = obi.hook('redis:random');
      expect(hook).toBeDefined();
    });

    it('returns persist hook by name', () => {
      const obi = new ObiHookManager();
      const hook = obi.hook('redis:persist');
      expect(hook).toBeDefined();
    });

    it('returns same hook instance on repeated calls', () => {
      const obi = new ObiHookManager();
      expect(obi.hook('redis:time')).toBe(obi.hook('redis:time'));
      expect(obi.hook('redis:random')).toBe(obi.hook('redis:random'));
      expect(obi.hook('redis:persist')).toBe(obi.hook('redis:persist'));
    });

    it('throws on unknown hook name', () => {
      const obi = new ObiHookManager();
      expect(() => obi.hook('redis:unknown' as never)).toThrow(
        'Unknown OBI hook: redis:unknown'
      );
    });
  });

  describe('redis:time hook', () => {
    it('clock returns base clock value when no hooks tapped', () => {
      const obi = new ObiHookManager({ clock: () => 1000 });
      expect(obi.clock()).toBe(1000);
    });

    it('tapped hook can override time', () => {
      const obi = new ObiHookManager({ clock: () => 1000 });
      obi.hook('redis:time').tap((_next) => 9999);
      expect(obi.clock()).toBe(9999);
    });

    it('tapped hook can transform time from base', () => {
      const obi = new ObiHookManager({ clock: () => 1000 });
      obi.hook('redis:time').tap((next) => next() + 5000);
      expect(obi.clock()).toBe(6000);
    });

    it('multiple hooks compose as middleware', () => {
      const obi = new ObiHookManager({ clock: () => 100 });
      // First hook (outermost): add 1
      obi.hook('redis:time').tap((next) => next() + 1);
      // Second hook (inner): multiply by 2
      obi.hook('redis:time').tap((next) => next() * 2);
      // chain: hook1(hook2(base)) = (100 * 2) + 1 = 201
      expect(obi.clock()).toBe(201);
    });

    it('untapped hook no longer affects time', () => {
      const obi = new ObiHookManager({ clock: () => 1000 });
      const fn: SyncHookFn<number> = (_next) => 9999;
      obi.hook('redis:time').tap(fn);
      expect(obi.clock()).toBe(9999);
      obi.hook('redis:time').untap(fn);
      expect(obi.clock()).toBe(1000);
    });

    it('reflects changing base clock values', () => {
      let base = 1000;
      const obi = new ObiHookManager({ clock: () => base });
      expect(obi.clock()).toBe(1000);
      base = 2000;
      expect(obi.clock()).toBe(2000);
    });
  });

  describe('redis:random hook', () => {
    it('rng returns base rng value when no hooks tapped', () => {
      const obi = new ObiHookManager({ rng: () => 0.5 });
      expect(obi.rng()).toBe(0.5);
    });

    it('tapped hook can override random', () => {
      const obi = new ObiHookManager({ rng: () => 0.5 });
      obi.hook('redis:random').tap((_next) => 0.42);
      expect(obi.rng()).toBe(0.42);
    });

    it('tapped hook receives base value through next', () => {
      const obi = new ObiHookManager({ rng: () => 0.3 });
      const seen: number[] = [];
      obi.hook('redis:random').tap((next) => {
        const val = next();
        seen.push(val);
        return val;
      });
      obi.rng();
      expect(seen).toEqual([0.3]);
    });

    it('deterministic rng produces repeatable results', () => {
      let idx = 0;
      const sequence = [0.1, 0.7, 0.3, 0.9, 0.5];
      const obi = new ObiHookManager({ rng: () => Math.random() });
      obi.hook('redis:random').tap((_next) => {
        return sequence[idx++ % sequence.length] ?? 0;
      });

      const results = Array.from({ length: 5 }, () => obi.rng());
      expect(results).toEqual([0.1, 0.7, 0.3, 0.9, 0.5]);
    });

    it('untapped hook no longer affects random', () => {
      const obi = new ObiHookManager({ rng: () => 0.5 });
      const fn: SyncHookFn<number> = (_next) => 0.99;
      obi.hook('redis:random').tap(fn);
      expect(obi.rng()).toBe(0.99);
      obi.hook('redis:random').untap(fn);
      expect(obi.rng()).toBe(0.5);
    });
  });

  describe('redis:persist hook', () => {
    it('persist returns default result when no hooks tapped', () => {
      const obi = new ObiHookManager();
      const result = obi.persist('bgsave');
      expect(result).toEqual({ action: 'bgsave', accepted: true });
    });

    it('persist supports bgrewriteaof action', () => {
      const obi = new ObiHookManager();
      const result = obi.persist('bgrewriteaof');
      expect(result).toEqual({ action: 'bgrewriteaof', accepted: true });
    });

    it('persist supports save action', () => {
      const obi = new ObiHookManager();
      const result = obi.persist('save');
      expect(result).toEqual({ action: 'save', accepted: true });
    });

    it('tapped hook can reject persistence', () => {
      const obi = new ObiHookManager();
      obi.hook('redis:persist').tap((_next) => ({
        action: 'bgsave',
        accepted: false,
      }));
      const result = obi.persist('bgsave');
      expect(result.accepted).toBe(false);
    });

    it('tapped hook can intercept and modify persist signal', () => {
      const obi = new ObiHookManager();
      const signals: string[] = [];
      obi.hook('redis:persist').tap((next) => {
        const result = next();
        signals.push(result.action);
        return result;
      });
      obi.persist('bgsave');
      obi.persist('save');
      expect(signals).toEqual(['bgsave', 'save']);
    });

    it('untapped hook no longer affects persist', () => {
      const obi = new ObiHookManager();
      const fn: SyncHookFn<{ action: string; accepted: boolean }> = (
        _next
      ) => ({
        action: 'bgsave',
        accepted: false,
      });
      obi.hook('redis:persist').tap(fn);
      expect(obi.persist('bgsave').accepted).toBe(false);
      obi.hook('redis:persist').untap(fn);
      expect(obi.persist('bgsave').accepted).toBe(true);
    });
  });

  describe('hasHooks', () => {
    it('returns false when no hooks are tapped', () => {
      const obi = new ObiHookManager();
      expect(obi.hasHooks).toBe(false);
    });

    it('returns true when time hook is tapped', () => {
      const obi = new ObiHookManager();
      obi.hook('redis:time').tap((next) => next());
      expect(obi.hasHooks).toBe(true);
    });

    it('returns true when random hook is tapped', () => {
      const obi = new ObiHookManager();
      obi.hook('redis:random').tap((next) => next());
      expect(obi.hasHooks).toBe(true);
    });

    it('returns true when persist hook is tapped', () => {
      const obi = new ObiHookManager();
      obi.hook('redis:persist').tap((next) => next());
      expect(obi.hasHooks).toBe(true);
    });

    it('returns false after all hooks are untapped', () => {
      const obi = new ObiHookManager();
      const fn: SyncHookFn<number> = (next) => next();
      obi.hook('redis:time').tap(fn);
      expect(obi.hasHooks).toBe(true);
      obi.hook('redis:time').untap(fn);
      expect(obi.hasHooks).toBe(false);
    });
  });

  describe('isolation', () => {
    it('time and random hooks are independent', () => {
      const obi = new ObiHookManager({ clock: () => 1000, rng: () => 0.5 });
      obi.hook('redis:time').tap((_next) => 9999);
      // random should not be affected
      expect(obi.rng()).toBe(0.5);
      expect(obi.clock()).toBe(9999);
    });

    it('base functions are called each time', () => {
      const clockFn = vi.fn(() => 1000);
      const rngFn = vi.fn(() => 0.5);
      const obi = new ObiHookManager({ clock: clockFn, rng: rngFn });

      obi.clock();
      obi.clock();
      obi.rng();
      expect(clockFn).toHaveBeenCalledTimes(2);
      expect(rngFn).toHaveBeenCalledTimes(1);
    });
  });
});

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
