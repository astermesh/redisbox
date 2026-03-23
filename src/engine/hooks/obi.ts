/**
 * OBI (Outbound Box Interface) hooks for SimBox integration.
 *
 * Controls external dependencies that RedisBox relies on:
 * - `redis:time` — virtual clock (SyncHook, WASM-boundary safe)
 * - `redis:random` — deterministic randomness (SyncHook, WASM-boundary safe)
 * - `redis:persist` — persistence signals (SyncHook)
 */

import { SyncHook } from './hook.ts';

/** Persistence signal passed through the redis:persist hook. */
export interface PersistSignal {
  action: string;
  accepted: boolean;
}

/** All OBI hook event names. */
export type ObiHookName = 'redis:time' | 'redis:random' | 'redis:persist';

/** Dependencies for OBI hook manager (base functions before hook wrapping). */
export interface ObiDeps {
  clock: () => number;
  rng: () => number;
}

const defaultObiDeps: ObiDeps = {
  clock: () => Date.now(),
  rng: () => Math.random(),
};

/**
 * OBI Hook Manager — holds all outbound dependency hooks.
 *
 * Usage:
 * 1. `manager.hook('redis:time').tap(fn)` — intercept time access
 * 2. `manager.hook('redis:random').tap(fn)` — intercept randomness
 * 3. `manager.hook('redis:persist').tap(fn)` — intercept persistence signals
 * 4. `manager.clock()` — get current time (through hook chain)
 * 5. `manager.rng()` — get random value (through hook chain)
 * 6. `manager.persist(action)` — emit persistence signal (through hook chain)
 */
export class ObiHookManager {
  private readonly timeHook = new SyncHook<number>();
  private readonly randomHook = new SyncHook<number>();
  private readonly persistHook = new SyncHook<PersistSignal>();
  private readonly baseClock: () => number;
  private readonly baseRng: () => number;

  constructor(deps?: Partial<ObiDeps>) {
    const resolved = { ...defaultObiDeps, ...deps };
    this.baseClock = resolved.clock;
    this.baseRng = resolved.rng;
  }

  /** Get a specific hook by name for tapping/untapping. */
  hook(name: 'redis:time' | 'redis:random'): SyncHook<number>;
  hook(name: 'redis:persist'): SyncHook<PersistSignal>;
  hook(name: ObiHookName): SyncHook<number> | SyncHook<PersistSignal> {
    switch (name) {
      case 'redis:time':
        return this.timeHook;
      case 'redis:random':
        return this.randomHook;
      case 'redis:persist':
        return this.persistHook;
      default:
        throw new Error(`Unknown OBI hook: ${name as string}`);
    }
  }

  /** Check if any hooks are registered. */
  get hasHooks(): boolean {
    return (
      this.timeHook.size > 0 ||
      this.randomHook.size > 0 ||
      this.persistHook.size > 0
    );
  }

  /** Get current time through the redis:time hook chain. */
  clock(): number {
    return this.timeHook.execute(this.baseClock);
  }

  /** Get random value through the redis:random hook chain. */
  rng(): number {
    return this.randomHook.execute(this.baseRng);
  }

  /** Emit persistence signal through the redis:persist hook chain. */
  persist(action: string): PersistSignal {
    return this.persistHook.execute(() => ({ action, accepted: true }));
  }
}
