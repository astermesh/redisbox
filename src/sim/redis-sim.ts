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
import { bulkReply, errorReply } from '../engine/types.ts';
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

  // --- Behavioral Modification ---

  /**
   * Simulate cache misses for GET/MGET at the given rate (0–1).
   * Intercepts string read commands via IBI hook and returns nil
   * instead of the actual value at the configured probability.
   * Returns a disposer function to remove the simulation.
   */
  setCacheMissRate(rate: number): () => void {
    const NIL: Reply = bulkReply(null);

    const hookFn = async (
      ctx: CommandHookCtx,
      next: () => Promise<Reply>
    ): Promise<Reply> => {
      const cmd = ctx.command;
      if (cmd !== 'GET' && cmd !== 'MGET') {
        return next();
      }

      const result = await next();

      if (rate <= 0) return result;

      if (cmd === 'GET') {
        // Only turn hits into misses — if already nil, leave it
        if (result.kind === 'bulk' && result.value !== null) {
          if (rate >= 1 || this.engine.rng() < rate) {
            return NIL;
          }
        }
        return result;
      }

      // MGET — process each element independently
      if (result.kind === 'array') {
        const values = result.value.map((item) => {
          if (item.kind === 'bulk' && item.value !== null) {
            if (rate >= 1 || this.engine.rng() < rate) {
              return NIL;
            }
          }
          return item;
        });
        return { kind: 'array', value: values };
      }

      return result;
    };

    this.engine.ibi.hook('redis:string:read').tap(hookFn);
    return () => this.engine.ibi.hook('redis:string:read').untap(hookFn);
  }

  /**
   * Drop pub/sub messages at the given rate (0–1).
   * Affects both channel and pattern subscription deliveries.
   * Returns a disposer function to remove the simulation.
   */
  setMessageDropRate(rate: number): () => void {
    const filter = (_clientId: number, _channel: string): boolean => {
      if (rate <= 0) return true;
      if (rate >= 1) return false;
      return this.engine.rng() >= rate;
    };

    this.engine.pubsub.setMessageFilter(filter);
    return () => this.engine.pubsub.setMessageFilter(null);
  }

  /**
   * Simulate eviction of specific keys across all databases.
   * Immediately deletes the specified keys, similar to Redis memory eviction.
   * @returns total number of keys actually evicted
   */
  injectEviction(keys: string[]): number {
    let count = 0;
    for (const db of this.engine.databases) {
      for (const key of keys) {
        if (db.delete(key)) {
          count++;
        }
      }
    }
    return count;
  }
}
