import { describe, it, expect } from 'vitest';
import { TimeoutManager } from './timeout-manager.ts';
import { BlockingManager } from './blocking-manager.ts';
import type { BlockedEntry } from './blocking-manager.ts';
import { NIL_ARRAY } from './types.ts';
import { RedisEngine } from './engine.ts';
import { VirtualClock } from '../sim/virtual-clock.ts';

function neverServe(): BlockedEntry['tryServe'] {
  return () => null;
}

describe('TimeoutManager', () => {
  describe('tick', () => {
    it('returns empty array when no clients are blocked', () => {
      const blocking = new BlockingManager();
      const now = 1000;
      const tm = new TimeoutManager(blocking, () => now);
      expect(tm.tick()).toEqual([]);
    });

    it('returns timed-out client with nil-array reply', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: neverServe(),
      });

      // Before timeout
      now = 1999;
      expect(tm.tick()).toEqual([]);
      expect(blocking.isBlocked(1)).toBe(true);

      // At timeout
      now = 2000;
      const results = tm.tick();
      expect(results).toHaveLength(1);
      expect(results[0]).toEqual({ clientId: 1, reply: NIL_ARRAY });
      expect(blocking.isBlocked(1)).toBe(false);
    });

    it('does not time out clients with timeout=0 (infinite)', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: neverServe(),
      });

      now = 999999;
      expect(tm.tick()).toEqual([]);
      expect(blocking.isBlocked(1)).toBe(true);
    });

    it('times out multiple clients independently', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1'],
        timeout: 2000,
        tryServe: neverServe(),
      });
      blocking.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k2'],
        timeout: 3000,
        tryServe: neverServe(),
      });

      now = 2500;
      const r1 = tm.tick();
      expect(r1).toHaveLength(1);
      expect(r1[0]).toMatchObject({ clientId: 1 });
      expect(blocking.isBlocked(2)).toBe(true);

      now = 3000;
      const r2 = tm.tick();
      expect(r2).toHaveLength(1);
      expect(r2[0]).toMatchObject({ clientId: 2 });
    });

    it('times out multiple clients in the same tick', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: neverServe(),
      });
      blocking.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2500,
        tryServe: neverServe(),
      });

      now = 3000;
      const results = tm.tick();
      expect(results).toHaveLength(2);
      const ids = results.map((r) => r.clientId).sort();
      expect(ids).toEqual([1, 2]);
    });

    it('uses virtual clock for timeout evaluation', () => {
      const blocking = new BlockingManager();
      let now = 0;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 5000,
        tryServe: neverServe(),
      });

      // Simulate frozen time - no advancement
      now = 1000;
      expect(tm.tick()).toEqual([]);

      // Advance past timeout
      now = 5000;
      const results = tm.tick();
      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({ clientId: 1 });
    });

    it('does not return already-unblocked clients', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: neverServe(),
      });

      // Manually unblock before timeout
      blocking.unblockClient(1);
      now = 3000;
      expect(tm.tick()).toEqual([]);
    });
  });

  describe('disconnectClient', () => {
    it('cleans up blocking state on disconnect', () => {
      const blocking = new BlockingManager();
      const tm = new TimeoutManager(blocking, () => 1000);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1', 'k2'],
        timeout: 5000,
        tryServe: neverServe(),
      });
      expect(blocking.isBlocked(1)).toBe(true);

      tm.disconnectClient(1);
      expect(blocking.isBlocked(1)).toBe(false);
      expect(blocking.blockedCount).toBe(0);
    });

    it('is a no-op for non-blocked clients', () => {
      const blocking = new BlockingManager();
      const tm = new TimeoutManager(blocking, () => 1000);

      // Should not throw
      tm.disconnectClient(99);
      expect(blocking.blockedCount).toBe(0);
    });

    it('disconnected client does not appear in subsequent tick', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: neverServe(),
      });

      tm.disconnectClient(1);
      now = 3000;
      expect(tm.tick()).toEqual([]);
    });

    it('cleans up one client without affecting others', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: neverServe(),
      });
      blocking.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 3000,
        tryServe: neverServe(),
      });

      tm.disconnectClient(1);
      expect(blocking.isBlocked(1)).toBe(false);
      expect(blocking.isBlocked(2)).toBe(true);

      now = 3000;
      const results = tm.tick();
      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({ clientId: 2 });
    });
  });

  describe('virtual time integration', () => {
    it('works with advancing virtual clock', () => {
      const blocking = new BlockingManager();
      let frozenTime = 1000;
      const tm = new TimeoutManager(blocking, () => frozenTime);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 5000,
        tryServe: neverServe(),
      });

      // Simulate VirtualClock.advanceTime
      frozenTime = 3000;
      expect(tm.tick()).toEqual([]);

      frozenTime = 5000;
      const results = tm.tick();
      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({ clientId: 1 });
    });

    it('frozen clock prevents timeouts from firing', () => {
      const blocking = new BlockingManager();
      const frozenTime = 1000;
      const tm = new TimeoutManager(blocking, () => frozenTime);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: neverServe(),
      });

      // Clock is frozen at 1000, timeout at 2000 — should never fire
      expect(tm.tick()).toEqual([]);
      expect(tm.tick()).toEqual([]);
      expect(tm.tick()).toEqual([]);
      expect(blocking.isBlocked(1)).toBe(true);
    });

    it('time jump causes immediate timeout', () => {
      const blocking = new BlockingManager();
      let now = 1000;
      const tm = new TimeoutManager(blocking, () => now);

      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 100_000,
        tryServe: neverServe(),
      });

      // Simulate VirtualClock.setTime jumping far ahead
      now = 200_000;
      const results = tm.tick();
      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({ clientId: 1 });
    });
  });

  describe('hasBlockedClients', () => {
    it('returns false when no clients are blocked', () => {
      const blocking = new BlockingManager();
      const tm = new TimeoutManager(blocking, () => 1000);
      expect(tm.hasBlockedClients()).toBe(false);
    });

    it('returns true when clients are blocked', () => {
      const blocking = new BlockingManager();
      const tm = new TimeoutManager(blocking, () => 1000);
      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 5000,
        tryServe: neverServe(),
      });
      expect(tm.hasBlockedClients()).toBe(true);
    });

    it('returns false after all clients are disconnected', () => {
      const blocking = new BlockingManager();
      const tm = new TimeoutManager(blocking, () => 1000);
      blocking.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 5000,
        tryServe: neverServe(),
      });
      tm.disconnectClient(1);
      expect(tm.hasBlockedClients()).toBe(false);
    });
  });
});

describe('TimeoutManager integration with RedisEngine', () => {
  it('engine exposes timeouts property', () => {
    const engine = new RedisEngine({ clock: () => 1000 });
    expect(engine.timeouts).toBeInstanceOf(TimeoutManager);
  });

  it('engine.timeouts uses engine.clock for timeout evaluation', () => {
    const clock = new VirtualClock();
    clock.freezeTime();
    clock.setTime(1000);

    const engine = new RedisEngine({ clock: () => clock.now() });

    engine.blocking.blockClient({
      clientId: 1,
      dbIndex: 0,
      keys: ['k'],
      timeout: 5000,
      tryServe: neverServe(),
    });

    // Clock frozen at 1000, timeout at 5000
    expect(engine.timeouts.tick()).toEqual([]);

    // Advance clock past timeout
    clock.setTime(5000);
    const results = engine.timeouts.tick();
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual({ clientId: 1, reply: NIL_ARRAY });
  });

  it('VirtualClock.advanceTime triggers timeout through engine', () => {
    const clock = new VirtualClock();
    clock.freezeTime();
    clock.setTime(1000);

    const engine = new RedisEngine({ clock: () => clock.now() });

    engine.blocking.blockClient({
      clientId: 1,
      dbIndex: 0,
      keys: ['k'],
      timeout: 5000,
      tryServe: neverServe(),
    });

    // 1000 + 1500 = 2500, not yet at 5000
    clock.advanceTime(1500);
    expect(engine.timeouts.tick()).toEqual([]);

    // 2500 + 2000 = 4500, still not at 5000
    clock.advanceTime(2000);
    expect(engine.timeouts.tick()).toEqual([]);

    // 4500 + 500 = 5000, exactly at timeout
    clock.advanceTime(500);
    const results = engine.timeouts.tick();
    expect(results).toHaveLength(1);
    expect(results[0]).toMatchObject({ clientId: 1 });
  });

  it('disconnect cleanup works through engine.timeouts', () => {
    const engine = new RedisEngine({ clock: () => 1000 });

    engine.blocking.blockClient({
      clientId: 42,
      dbIndex: 0,
      keys: ['mykey'],
      timeout: 5000,
      tryServe: neverServe(),
    });

    expect(engine.blocking.isBlocked(42)).toBe(true);
    engine.timeouts.disconnectClient(42);
    expect(engine.blocking.isBlocked(42)).toBe(false);
  });

  it('processReadyKeys and tick work together correctly', () => {
    const clock = new VirtualClock();
    clock.freezeTime();
    clock.setTime(1000);

    const engine = new RedisEngine({ clock: () => clock.now() });
    const reply = { kind: 'bulk' as const, value: 'served' };

    // Client 1: will be served by processReadyKeys
    engine.blocking.blockClient({
      clientId: 1,
      dbIndex: 0,
      keys: ['k'],
      timeout: 5000,
      tryServe: () => reply,
    });

    // Client 2: will time out
    engine.blocking.blockClient({
      clientId: 2,
      dbIndex: 0,
      keys: ['other'],
      timeout: 3000,
      tryServe: neverServe(),
    });

    // Signal key ready and process
    engine.blocking.signalKeyAsReady(0, 'k');
    const served = engine.blocking.processReadyKeys();
    expect(served).toHaveLength(1);
    expect(served[0]).toMatchObject({ clientId: 1 });

    // Advance time past client 2's timeout
    clock.setTime(3000);
    const timedOut = engine.timeouts.tick();
    expect(timedOut).toHaveLength(1);
    expect(timedOut[0]).toMatchObject({ clientId: 2 });
  });
});
