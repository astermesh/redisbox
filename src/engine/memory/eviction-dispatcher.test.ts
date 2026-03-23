import { describe, it, expect, beforeEach } from 'vitest';
import {
  CommandDispatcher,
  createTransactionState,
} from '../command-dispatcher.ts';
import type { TransactionState } from '../command-dispatcher.ts';
import { createCommandTable } from '../command-registry.ts';
import { RedisEngine } from '../engine.ts';
import type { CommandContext } from '../types.ts';
import { ConfigStore } from '../../config-store.ts';
import { EvictionManager } from './eviction-manager.ts';

function createSetup() {
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  const config = new ConfigStore();
  const eviction = new EvictionManager(engine, config);
  const table = createCommandTable();
  const dispatcher = new CommandDispatcher(table);
  const state = createTransactionState();
  const ctx: CommandContext = {
    db: engine.db(0),
    engine,
    config,
    eviction,
  };
  return { engine, config, eviction, dispatcher, state, ctx };
}

describe('CommandDispatcher eviction integration', () => {
  let dispatcher: CommandDispatcher;
  let state: TransactionState;
  let ctx: CommandContext;
  let config: ConfigStore;

  beforeEach(() => {
    const setup = createSetup();
    dispatcher = setup.dispatcher;
    state = setup.state;
    ctx = setup.ctx;
    config = setup.config;
  });

  describe('denyoom commands', () => {
    it('allows SET when memory is below limit', () => {
      config.set('maxmemory', '1000000');
      config.set('maxmemory-policy', 'noeviction');
      const result = dispatcher.dispatch(state, ctx, ['SET', 'k', 'v']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
    });

    it('returns OOM error for SET when over limit with noeviction', () => {
      // Fill some data first
      for (let i = 0; i < 20; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `key${i}`, `val${i}`]);
      }
      // Set very low limit
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'noeviction');

      const result = dispatcher.dispatch(state, ctx, [
        'SET',
        'newkey',
        'newval',
      ]);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'OOM',
        message: "command not allowed when used memory > 'maxmemory'.",
      });
    });

    it('allows SET after eviction frees memory with allkeys-random', () => {
      // Fill data
      for (let i = 0; i < 20; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `key${i}`, `val${i}`]);
      }
      // Set a limit that allows some keys but not all
      const currentMem = ctx.eviction?.currentUsedMemory() ?? 0;
      config.set('maxmemory', String(Math.floor(currentMem * 0.8)));
      config.set('maxmemory-policy', 'allkeys-random');

      // This SET should succeed after eviction
      const result = dispatcher.dispatch(state, ctx, [
        'SET',
        'newkey',
        'newval',
      ]);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
      // Some keys should have been evicted
      expect(ctx.db.size).toBeLessThan(21);
    });

    it('returns OOM for LPUSH when over limit with noeviction', () => {
      for (let i = 0; i < 20; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `key${i}`, `val${i}`]);
      }
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'noeviction');

      const result = dispatcher.dispatch(state, ctx, ['LPUSH', 'list', 'val']);
      expect(result).toEqual({
        kind: 'error',
        prefix: 'OOM',
        message: "command not allowed when used memory > 'maxmemory'.",
      });
    });

    it('allows GET even when over limit (readonly, no denyoom)', () => {
      for (let i = 0; i < 20; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `key${i}`, `val${i}`]);
      }
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'noeviction');

      // GET should still work even when OOM
      const result = dispatcher.dispatch(state, ctx, ['GET', 'key0']);
      expect(result).toEqual({ kind: 'bulk', value: 'val0' });
    });

    it('allows DEL even when over limit (write but no denyoom)', () => {
      for (let i = 0; i < 20; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `key${i}`, `val${i}`]);
      }
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'noeviction');

      // DEL should still work even when OOM (it frees memory)
      const result = dispatcher.dispatch(state, ctx, ['DEL', 'key0']);
      expect(result).toEqual({ kind: 'integer', value: 1 });
    });
  });

  describe('MULTI/EXEC with eviction', () => {
    it('returns OOM for denyoom commands inside EXEC when over limit', () => {
      // Fill data
      for (let i = 0; i < 20; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `key${i}`, `val${i}`]);
      }

      // Start transaction
      dispatcher.dispatch(state, ctx, ['MULTI']);
      dispatcher.dispatch(state, ctx, ['SET', 'newkey', 'newval']);
      dispatcher.dispatch(state, ctx, ['GET', 'key0']);

      // Set OOM limit before EXEC
      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'noeviction');

      const result = dispatcher.dispatch(state, ctx, ['EXEC']);
      expect(result.kind).toBe('array');
      if (result.kind === 'array') {
        // SET should fail with OOM
        expect(result.value[0]).toEqual({
          kind: 'error',
          prefix: 'OOM',
          message: "command not allowed when used memory > 'maxmemory'.",
        });
        // GET should succeed (readonly, no denyoom)
        expect(result.value[1]).toEqual({ kind: 'bulk', value: 'val0' });
      }
    });
  });

  describe('eviction with volatile policies', () => {
    it('volatile-random only evicts keys with TTL', () => {
      // Set keys without TTL
      for (let i = 0; i < 5; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `persistent${i}`, `val${i}`]);
      }
      // Set keys with TTL
      for (let i = 0; i < 5; i++) {
        dispatcher.dispatch(state, ctx, ['SET', `volatile${i}`, `val${i}`]);
        dispatcher.dispatch(state, ctx, ['PEXPIREAT', `volatile${i}`, '99999']);
      }

      config.set('maxmemory', '1');
      config.set('maxmemory-policy', 'volatile-random');

      // This should fail since we can't evict enough
      // (persistent keys survive, volatile ones all gone, still over limit)
      const result = dispatcher.dispatch(state, ctx, [
        'SET',
        'newkey',
        'newval',
      ]);
      expect(result.kind).toBe('error');
      if (result.kind === 'error') {
        expect(result.prefix).toBe('OOM');
      }

      // Persistent keys must still be there
      for (let i = 0; i < 5; i++) {
        expect(ctx.db.has(`persistent${i}`)).toBe(true);
      }
    });
  });

  describe('no eviction manager', () => {
    it('allows denyoom commands when no eviction manager is configured', () => {
      // Create context without eviction manager
      const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
      const noEvictCtx: CommandContext = {
        db: engine.db(0),
        engine,
      };
      const result = dispatcher.dispatch(state, noEvictCtx, ['SET', 'k', 'v']);
      expect(result).toEqual({ kind: 'status', value: 'OK' });
    });
  });
});
