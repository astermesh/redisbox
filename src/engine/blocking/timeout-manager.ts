/**
 * Timeout management for blocked clients.
 *
 * Integrates the BlockingManager's timeout checking with a virtual-time-aware
 * clock. On each tick, checks for timed-out clients and produces nil-array
 * replies. Also handles cleanup when clients disconnect.
 *
 * The clock function is injected, allowing virtual time control via
 * VirtualClock (freeze, advance, set). Timeouts fire based on the clock's
 * current value, so frozen time prevents timeouts and time jumps cause
 * immediate expiration.
 */

import type { BlockingManager } from './blocking-manager.ts';
import type { Reply } from '../types.ts';
import { NIL_ARRAY } from '../types.ts';

export interface TimeoutResult {
  clientId: number;
  reply: Reply;
}

export class TimeoutManager {
  private readonly blocking: BlockingManager;
  private readonly clock: () => number;

  constructor(blocking: BlockingManager, clock: () => number) {
    this.blocking = blocking;
    this.clock = clock;
  }

  /**
   * Check for timed-out blocked clients and return timeout replies.
   * Should be called in the beforeSleep / event-loop phase.
   *
   * Returns a list of {clientId, reply} for each timed-out client.
   * The reply is NIL_ARRAY (matching real Redis behavior for BLPOP/BRPOP etc.).
   */
  tick(): TimeoutResult[] {
    const timedOut = this.blocking.checkTimeouts(this.clock());
    if (timedOut.length === 0) return [];

    const results: TimeoutResult[] = [];
    for (const entry of timedOut) {
      results.push({ clientId: entry.clientId, reply: NIL_ARRAY });
    }
    return results;
  }

  /**
   * Clean up blocking state when a client disconnects.
   * Removes the client from all blocking queues.
   */
  disconnectClient(clientId: number): void {
    this.blocking.unblockClient(clientId);
  }

  /**
   * Check if there are any blocked clients.
   */
  hasBlockedClients(): boolean {
    return this.blocking.hasBlockedClients();
  }
}
