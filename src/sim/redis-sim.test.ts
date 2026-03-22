import { describe, it, expect } from 'vitest';
import { RedisSim } from './redis-sim.ts';
import {
  CommandDispatcher,
  createTransactionState,
} from '../engine/command-dispatcher.ts';
import { createCommandTable } from '../engine/command-registry.ts';
import type { CommandContext } from '../engine/types.ts';

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
      // getLruClock converts ms to seconds: 5000ms → 5s
      expect(entry?.lruClock).toBe(5);

      sim.advanceTime(1000);
      db.get('k');
      const entry2 = db.get('k');
      // 5000 + 1000 = 6000ms → 6s
      expect(entry2?.lruClock).toBe(6);
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

  describe('time control affects stream IDs', () => {
    it('XADD auto-generated IDs use virtual clock time', () => {
      const sim = new RedisSim();
      sim.freezeTime();
      sim.setTime(1000000);

      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = {
        db: sim.engine.db(0),
        engine: sim.engine,
      };

      // XADD with * ID — should use virtual clock (1000000ms = 1000000)
      const result1 = dispatcher.dispatch(state, ctx, [
        'XADD',
        'mystream',
        '*',
        'field1',
        'value1',
      ]);
      expect(result1.kind).toBe('bulk');
      expect((result1 as { value: string }).value).toBe('1000000-0');

      // Advance time and add another entry
      sim.advanceTime(5000);
      const result2 = dispatcher.dispatch(state, ctx, [
        'XADD',
        'mystream',
        '*',
        'field2',
        'value2',
      ]);
      expect(result2.kind).toBe('bulk');
      expect((result2 as { value: string }).value).toBe('1005000-0');
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

  describe('failure injection', () => {
    function setupDispatcher(sim: RedisSim) {
      const table = createCommandTable();
      const dispatcher = new CommandDispatcher(table);
      const state = createTransactionState();
      const ctx: CommandContext = {
        db: sim.engine.db(0),
        engine: sim.engine,
        ibi: sim.engine.ibi,
      };
      return { dispatcher, state, ctx };
    }

    describe('injectLatency', () => {
      it('adds delay before command execution', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        // Set a key first (no hooks yet)
        ctx.db.set('k', 'string', 'raw', 'hello');

        // Inject 10ms latency
        sim.injectLatency(10);

        const start = performance.now();
        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed = performance.now() - start;

        expect(result).toEqual({ kind: 'bulk', value: 'hello' });
        expect(elapsed).toBeGreaterThanOrEqual(8); // allow small timing variance
      });

      it('filters latency by command name', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        // Only add latency to SET commands
        sim.injectLatency(50, { commands: ['SET'] });

        // GET should be fast (no latency)
        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeLessThan(30);
      });

      it('applies latency to filtered commands', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectLatency(20, { commands: ['SET'] });

        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['SET', 'k', 'v']);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeGreaterThanOrEqual(15);
      });

      it('command filter is case-insensitive', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectLatency(20, { commands: ['set'] });

        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['SET', 'k', 'v']);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeGreaterThanOrEqual(15);
      });

      it('returns a disposer that removes the injection', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        const dispose = sim.injectLatency(50);
        dispose();

        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeLessThan(30);
      });

      it('multiple latency injections stack', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        sim.injectLatency(15);
        sim.injectLatency(15);

        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeGreaterThanOrEqual(25);
      });
    });

    describe('injectError', () => {
      it('returns error reply instead of executing command', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        sim.injectError('simulated failure');

        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        expect(result).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'simulated failure',
        });
      });

      it('filters errors by command name', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        sim.injectError('write error', { commands: ['SET'] });

        // GET should succeed
        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        expect(result).toEqual({ kind: 'bulk', value: 'v' });
      });

      it('returns error for filtered commands', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectError('write blocked', { commands: ['SET'] });

        const result = await dispatcher.dispatchAsync(state, ctx, [
          'SET',
          'k',
          'v',
        ]);
        expect(result).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'write blocked',
        });
      });

      it('command filter is case-insensitive', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectError('fail', { commands: ['get'] });

        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        expect(result).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'fail',
        });
      });

      it('probability 0 never injects errors', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        sim.injectError('fail', { probability: 0 });

        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        expect(result).toEqual({ kind: 'bulk', value: 'v' });
      });

      it('probability 1 always injects errors', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectError('fail', { probability: 1 });

        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        expect(result).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'fail',
        });
      });

      it('probability-based injection uses engine rng', async () => {
        let rngValue = 0.8;
        const sim = new RedisSim({ rng: () => rngValue });
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        sim.injectError('fail', { probability: 0.5 });

        // rng returns 0.8 >= 0.5 → no error
        const result1 = await dispatcher.dispatchAsync(state, ctx, [
          'GET',
          'k',
        ]);
        expect(result1).toEqual({ kind: 'bulk', value: 'v' });

        // rng returns 0.3 < 0.5 → error
        rngValue = 0.3;
        const result2 = await dispatcher.dispatchAsync(state, ctx, [
          'GET',
          'k',
        ]);
        expect(result2).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'fail',
        });
      });

      it('returns a disposer that removes the injection', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        const dispose = sim.injectError('fail');
        dispose();

        const result = await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        expect(result).toEqual({ kind: 'bulk', value: 'v' });
      });

      it('does not execute the command when error is injected', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectError('fail');

        // SET should be blocked — key should not be created
        await dispatcher.dispatchAsync(state, ctx, ['SET', 'k', 'v']);
        expect(ctx.db.get('k')).toBeNull();
      });
    });

    describe('simulateSlowCommand', () => {
      it('adds latency only to the specified command', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        sim.simulateSlowCommand('GET', 20);

        // GET should be slow
        const start1 = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed1 = performance.now() - start1;
        expect(elapsed1).toBeGreaterThanOrEqual(15);

        // SET should be fast
        const start2 = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['SET', 'k', 'v2']);
        const elapsed2 = performance.now() - start2;
        expect(elapsed2).toBeLessThan(15);
      });

      it('returns a disposer that removes the slow simulation', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        ctx.db.set('k', 'string', 'raw', 'v');

        const dispose = sim.simulateSlowCommand('GET', 50);
        dispose();

        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed = performance.now() - start;

        expect(elapsed).toBeLessThan(30);
      });
    });

    describe('combined injections', () => {
      it('latency and error can be injected simultaneously', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        sim.injectLatency(10, { commands: ['GET'] });
        sim.injectError('blocked', { commands: ['SET'] });

        ctx.db.set('k', 'string', 'raw', 'v');

        // GET should have latency but succeed
        const result1 = await dispatcher.dispatchAsync(state, ctx, [
          'GET',
          'k',
        ]);
        expect(result1).toEqual({ kind: 'bulk', value: 'v' });

        // SET should return error
        const result2 = await dispatcher.dispatchAsync(state, ctx, [
          'SET',
          'k',
          'v2',
        ]);
        expect(result2).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'blocked',
        });
      });

      it('disposing one injection does not affect others', async () => {
        const sim = new RedisSim();
        const { dispatcher, state, ctx } = setupDispatcher(sim);

        const disposeLatency = sim.injectLatency(10);
        sim.injectError('fail', { commands: ['SET'] });

        // Remove latency but keep error
        disposeLatency();

        ctx.db.set('k', 'string', 'raw', 'v');

        // GET should be fast and succeed
        const start = performance.now();
        await dispatcher.dispatchAsync(state, ctx, ['GET', 'k']);
        const elapsed = performance.now() - start;
        expect(elapsed).toBeLessThan(15);

        // SET should still error
        const result = await dispatcher.dispatchAsync(state, ctx, [
          'SET',
          'k2',
          'v',
        ]);
        expect(result).toEqual({
          kind: 'error',
          prefix: 'ERR',
          message: 'fail',
        });
      });
    });
  });
});
