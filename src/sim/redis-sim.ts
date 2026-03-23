/**
 * RedisSim — Sim-side API for controlling RedisBox behavior.
 *
 * Provides time control, deterministic randomness, and failure injection.
 * Time control (via OBI redis:time hook) affects all time-dependent subsystems:
 * expiration, OBJECT IDLETIME, stream IDs, blocking timeouts.
 * Randomness control (via OBI redis:random hook) enables deterministic behavior.
 * Failure injection uses IBI hooks to inject latency and errors.
 */

import { RedisEngine } from '../engine/engine.ts';
import type { EngineDeps } from '../engine/types.ts';
import { errorReply } from '../engine/types.ts';
import type { CommandHookCtx } from '../engine/hooks/ibi.ts';
import type { ObiHookManager } from '../engine/hooks/obi.ts';
import type { Reply } from '../engine/types.ts';
import { PreDecision } from '../engine/hooks/hook.ts';
import { VirtualClock } from './virtual-clock.ts';

export interface LatencyOptions {
  commands?: string[];
}

export interface ErrorOptions {
  commands?: string[];
  probability?: number;
}

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

  /** Access OBI hooks for direct Sim attachment. */
  get obi(): ObiHookManager {
    return this.engine.obi;
  }

  // --- Time Control ---

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

  // --- Failure Injection ---

  /**
   * Inject latency before command execution via IBI pre-phase delay.
   * Returns a disposer function to remove the injection.
   */
  injectLatency(ms: number, options?: LatencyOptions): () => void {
    const upperCommands = options?.commands?.map((c) => c.toUpperCase());

    const hookFn = async (
      ctx: CommandHookCtx,
      next: () => Promise<Reply>
    ): Promise<Reply> => {
      if (!upperCommands || upperCommands.includes(ctx.command)) {
        await PreDecision.delay(ms);
      }
      return next();
    };

    this.engine.ibi.hook('redis:command').tap(hookFn);
    return () => this.engine.ibi.hook('redis:command').untap(hookFn);
  }

  /**
   * Inject errors for commands via IBI pre-phase fail.
   * Returns a disposer function to remove the injection.
   */
  injectError(error: string, options?: ErrorOptions): () => void {
    const upperCommands = options?.commands?.map((c) => c.toUpperCase());
    const probability = options?.probability ?? 1;

    const hookFn = async (
      ctx: CommandHookCtx,
      next: () => Promise<Reply>
    ): Promise<Reply> => {
      if (upperCommands && !upperCommands.includes(ctx.command)) {
        return next();
      }
      if (probability < 1 && this.engine.rng() >= probability) {
        return next();
      }
      return errorReply('ERR', error);
    };

    this.engine.ibi.hook('redis:command').tap(hookFn);
    return () => this.engine.ibi.hook('redis:command').untap(hookFn);
  }

  /**
   * Simulate a slow command by injecting latency for a specific command.
   * Returns a disposer function to remove the injection.
   */
  simulateSlowCommand(command: string, durationMs: number): () => void {
    return this.injectLatency(durationMs, { commands: [command] });
  }
}
