import { describe, it, expect, vi } from 'vitest';
import { ObiHookManager } from './obi.ts';
import type { SyncHookFn } from './hook.ts';

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
