/**
 * Blocking command infrastructure.
 *
 * Manages per-key blocking queues for commands like BLPOP, BRPOP,
 * BZPOPMIN, BZPOPMAX, BLMOVE, BLMPOP, BZMPOP.
 *
 * Design follows Redis blocking model:
 * - Clients register on one or more keys with a tryServe callback
 * - Mutations (LPUSH, ZADD, XADD etc.) call signalKeyAsReady
 * - processReadyKeys (called in beforeSleep phase) iterates ready keys
 *   and attempts to serve blocked clients in FIFO order
 * - tryServe re-evaluates the blocking condition before serving,
 *   because data may have been consumed by a previous client
 */

import type { Reply } from '../types.ts';

/**
 * Callback that attempts to serve a blocked client.
 * Called during processReadyKeys with the database index and key that became ready.
 *
 * Returns a Reply if the client should be unblocked (data was consumed),
 * or null if the blocking condition is still not met (data was consumed
 * by a previous client in the same cycle).
 */
export type TryServeCallback = (dbIndex: number, key: string) => Reply | null;

export interface BlockedEntry {
  clientId: number;
  dbIndex: number;
  keys: string[];
  /** Absolute timestamp (ms) when the block expires. 0 = no timeout. */
  timeout: number;
  tryServe: TryServeCallback;
}

export interface UnblockedResult {
  clientId: number;
  reply: Reply;
}

/**
 * Composite key for the per-key blocking queue.
 * Includes database index to keep keys in different databases separate.
 */
function compositeKey(dbIndex: number, key: string): string {
  return `${dbIndex}:${key}`;
}

export class BlockingManager {
  /** composite key → list of blocked client IDs (FIFO order) */
  private readonly keyQueues = new Map<string, number[]>();

  /** clientId → blocked entry */
  private readonly clients = new Map<number, BlockedEntry>();

  /**
   * Set of composite keys that have been signaled as ready
   * since the last processReadyKeys call.
   */
  private readonly readyKeys = new Set<string>();

  /**
   * Register a client as blocked on one or more keys.
   */
  blockClient(entry: BlockedEntry): void {
    this.clients.set(entry.clientId, entry);

    for (const key of entry.keys) {
      const ck = compositeKey(entry.dbIndex, key);
      let queue = this.keyQueues.get(ck);
      if (!queue) {
        queue = [];
        this.keyQueues.set(ck, queue);
      }
      queue.push(entry.clientId);
    }
  }

  /**
   * Remove a client from all blocking queues.
   * Returns true if the client was blocked.
   */
  unblockClient(clientId: number): boolean {
    const entry = this.clients.get(clientId);
    if (!entry) return false;

    for (const key of entry.keys) {
      const ck = compositeKey(entry.dbIndex, key);
      const queue = this.keyQueues.get(ck);
      if (queue) {
        const idx = queue.indexOf(clientId);
        if (idx !== -1) {
          queue.splice(idx, 1);
        }
        if (queue.length === 0) {
          this.keyQueues.delete(ck);
        }
      }
    }

    this.clients.delete(clientId);
    return true;
  }

  /**
   * Signal that a key has new data and blocked clients should be checked.
   * Called after mutations (LPUSH, ZADD, XADD, etc.).
   */
  signalKeyAsReady(dbIndex: number, key: string): void {
    const ck = compositeKey(dbIndex, key);
    if (this.keyQueues.has(ck)) {
      this.readyKeys.add(ck);
    }
  }

  /**
   * Process all ready keys and attempt to serve blocked clients.
   * Called in the "beforeSleep" phase of the event loop.
   *
   * For each ready key, iterates the blocked client queue in FIFO order.
   * Calls tryServe to re-evaluate the blocking condition. If tryServe
   * returns a Reply, the client is unblocked. If it returns null,
   * the client stays blocked (data was consumed by a previous client).
   *
   * Returns the list of clients that were unblocked with their replies.
   */
  processReadyKeys(): UnblockedResult[] {
    const results: UnblockedResult[] = [];

    // Snapshot and clear ready keys to avoid re-entrancy issues
    const keys = [...this.readyKeys];
    this.readyKeys.clear();

    for (const ck of keys) {
      const queue = this.keyQueues.get(ck);
      if (!queue || queue.length === 0) continue;

      // Parse composite key back to dbIndex and key
      const sepIdx = ck.indexOf(':');
      const dbIndex = Number(ck.substring(0, sepIdx));
      const key = ck.substring(sepIdx + 1);

      // Process queue in FIFO order
      // We iterate a snapshot since unblockClient modifies the queue
      const snapshot = [...queue];
      for (const clientId of snapshot) {
        // Client may have been unblocked already (by a previous key in this cycle)
        const entry = this.clients.get(clientId);
        if (!entry) continue;

        const reply = entry.tryServe(dbIndex, key);
        if (reply !== null) {
          this.unblockClient(clientId);
          results.push({ clientId, reply });
        }
      }
    }

    return results;
  }

  /**
   * Check for timed-out blocked clients.
   * Returns entries that have timed out so the caller can send
   * the appropriate timeout reply (typically NIL or EMPTY_ARRAY).
   *
   * @param now Current timestamp in milliseconds.
   */
  checkTimeouts(now: number): BlockedEntry[] {
    const timedOut: BlockedEntry[] = [];

    for (const entry of this.clients.values()) {
      if (entry.timeout > 0 && now >= entry.timeout) {
        timedOut.push(entry);
      }
    }

    // Unblock after iteration to avoid modifying the map during iteration
    for (const entry of timedOut) {
      this.unblockClient(entry.clientId);
    }

    return timedOut;
  }

  /**
   * Get the blocked entry for a client.
   */
  getBlockedEntry(clientId: number): BlockedEntry | undefined {
    return this.clients.get(clientId);
  }

  /**
   * Check if a client is currently blocked.
   */
  isBlocked(clientId: number): boolean {
    return this.clients.has(clientId);
  }

  /**
   * Total number of blocked clients.
   */
  get blockedCount(): number {
    return this.clients.size;
  }

  /**
   * Check if there are any blocked clients at all.
   */
  hasBlockedClients(): boolean {
    return this.clients.size > 0;
  }
}
