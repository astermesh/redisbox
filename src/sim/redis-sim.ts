/**
 * RedisSim — Sim-side API for controlling RedisBox behavior.
 *
 * Provides time control methods that affect all time-dependent
 * subsystems: expiration, OBJECT IDLETIME, stream IDs, blocking timeouts.
 */

import { RedisEngine } from '../engine/engine.ts';
import type { EngineDeps } from '../engine/types.ts';
import { VirtualClock } from './virtual-clock.ts';

export class RedisSim {
  readonly engine: RedisEngine;
  readonly clock: VirtualClock;

  constructor(deps?: Partial<Omit<EngineDeps, 'clock'>>) {
    this.clock = new VirtualClock();
    this.engine = new RedisEngine({
      clock: () => this.clock.now(),
      ...deps,
    });
  }

  advanceTime(ms: number): void {
    this.clock.advanceTime(ms);
  }

  freezeTime(): void {
    this.clock.freezeTime();
  }

  setTime(timestamp: number): void {
    this.clock.setTime(timestamp);
  }

  unfreezeTime(): void {
    this.clock.unfreezeTime();
  }
}
