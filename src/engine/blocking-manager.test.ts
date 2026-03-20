import { describe, it, expect } from 'vitest';
import { BlockingManager } from './blocking-manager.ts';
import type { BlockedEntry } from './blocking-manager.ts';
import type { Reply } from './types.ts';

function bulkReply(value: string | null): Reply {
  return { kind: 'bulk', value };
}

function arrayReply(...items: Reply[]): Reply {
  return { kind: 'array', value: items };
}

/**
 * A simple tryServe callback that always succeeds with a fixed reply.
 */
function alwaysServe(reply: Reply): BlockedEntry['tryServe'] {
  return () => reply;
}

/**
 * A tryServe callback that always fails (data was consumed).
 */
function neverServe(): BlockedEntry['tryServe'] {
  return () => null;
}

describe('BlockingManager', () => {
  describe('blockClient', () => {
    it('registers a client as blocked on a key', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['mylist'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      expect(mgr.isBlocked(1)).toBe(true);
      expect(mgr.blockedCount).toBe(1);
    });

    it('registers a client blocked on multiple keys', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1', 'k2', 'k3'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      expect(mgr.isBlocked(1)).toBe(true);
      expect(mgr.blockedCount).toBe(1);
    });

    it('supports multiple clients blocked on same key', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('a')),
      });
      mgr.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('b')),
      });
      expect(mgr.blockedCount).toBe(2);
    });
  });

  describe('unblockClient', () => {
    it('removes a blocked client', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const removed = mgr.unblockClient(1);
      expect(removed).toBe(true);
      expect(mgr.isBlocked(1)).toBe(false);
      expect(mgr.blockedCount).toBe(0);
    });

    it('returns false for non-blocked client', () => {
      const mgr = new BlockingManager();
      expect(mgr.unblockClient(99)).toBe(false);
    });

    it('cleans up all key queues when unblocking multi-key client', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1', 'k2'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      mgr.unblockClient(1);
      // Signal both keys — should produce no results since client was removed
      mgr.signalKeyAsReady(0, 'k1');
      mgr.signalKeyAsReady(0, 'k2');
      const results = mgr.processReadyKeys();
      expect(results).toEqual([]);
    });
  });

  describe('signalKeyAsReady + processReadyKeys', () => {
    it('wakes up a blocked client when key is signaled', () => {
      const mgr = new BlockingManager();
      const reply = arrayReply(bulkReply('k'), bulkReply('val'));
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply),
      });
      mgr.signalKeyAsReady(0, 'k');
      const results = mgr.processReadyKeys();
      expect(results).toEqual([{ clientId: 1, reply }]);
      expect(mgr.isBlocked(1)).toBe(false);
    });

    it('maintains FIFO order for multiple clients on same key', () => {
      const mgr = new BlockingManager();
      const reply1 = bulkReply('first');
      const reply2 = bulkReply('second');
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply1),
      });
      mgr.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply2),
      });
      mgr.signalKeyAsReady(0, 'k');
      const results = mgr.processReadyKeys();
      // Both should be served in FIFO order
      expect(results[0]).toEqual({ clientId: 1, reply: reply1 });
      expect(results[1]).toEqual({ clientId: 2, reply: reply2 });
    });

    it('re-evaluates blocking condition: skips client if tryServe returns null', () => {
      const mgr = new BlockingManager();
      // First client always fails (data consumed by someone else)
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: neverServe(),
      });
      const reply2 = bulkReply('got-it');
      mgr.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply2),
      });
      mgr.signalKeyAsReady(0, 'k');
      const results = mgr.processReadyKeys();
      // Client 1 stays blocked (tryServe returned null), client 2 gets served
      expect(results).toEqual([{ clientId: 2, reply: reply2 }]);
      expect(mgr.isBlocked(1)).toBe(true);
      expect(mgr.isBlocked(2)).toBe(false);
    });

    it('does not signal clients in wrong database', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      // Signal key in different database
      mgr.signalKeyAsReady(1, 'k');
      const results = mgr.processReadyKeys();
      expect(results).toEqual([]);
      expect(mgr.isBlocked(1)).toBe(true);
    });

    it('client blocked on multiple keys wakes on first signaled key', () => {
      const mgr = new BlockingManager();
      let servedKey = '';
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1', 'k2'],
        timeout: 0,
        tryServe: (_dbIndex: number, key: string) => {
          servedKey = key;
          return bulkReply(key);
        },
      });
      mgr.signalKeyAsReady(0, 'k2');
      const results = mgr.processReadyKeys();
      expect(results).toHaveLength(1);
      expect(servedKey).toBe('k2');
      expect(mgr.isBlocked(1)).toBe(false);
    });

    it('does nothing when no keys are signaled', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const results = mgr.processReadyKeys();
      expect(results).toEqual([]);
      expect(mgr.isBlocked(1)).toBe(true);
    });

    it('clears ready keys after processing', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      mgr.signalKeyAsReady(0, 'k');
      mgr.processReadyKeys();
      // Second call should return nothing
      const results2 = mgr.processReadyKeys();
      expect(results2).toEqual([]);
    });

    it('handles signaling same key multiple times', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      mgr.signalKeyAsReady(0, 'k');
      mgr.signalKeyAsReady(0, 'k');
      mgr.signalKeyAsReady(0, 'k');
      const results = mgr.processReadyKeys();
      // Client should only be served once
      expect(results).toHaveLength(1);
    });
  });

  describe('checkTimeouts', () => {
    it('times out a blocked client', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 1000,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const timedOut = mgr.checkTimeouts(1001);
      expect(timedOut).toHaveLength(1);
      expect(timedOut[0]).toMatchObject({ clientId: 1 });
      expect(mgr.isBlocked(1)).toBe(false);
    });

    it('does not time out client with timeout=0 (infinite)', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const timedOut = mgr.checkTimeouts(999999);
      expect(timedOut).toEqual([]);
      expect(mgr.isBlocked(1)).toBe(true);
    });

    it('does not time out client before timeout expires', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 5000,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const timedOut = mgr.checkTimeouts(4999);
      expect(timedOut).toEqual([]);
      expect(mgr.isBlocked(1)).toBe(true);
    });

    it('times out at exact timeout value', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 5000,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const timedOut = mgr.checkTimeouts(5000);
      expect(timedOut).toHaveLength(1);
      expect(timedOut[0]).toMatchObject({ clientId: 1 });
    });

    it('times out multiple clients independently', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 1000,
        tryServe: alwaysServe(bulkReply('a')),
      });
      mgr.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 2000,
        tryServe: alwaysServe(bulkReply('b')),
      });
      const timedOut1 = mgr.checkTimeouts(1500);
      expect(timedOut1).toHaveLength(1);
      expect(timedOut1[0]).toMatchObject({ clientId: 1 });
      expect(mgr.isBlocked(2)).toBe(true);

      const timedOut2 = mgr.checkTimeouts(2500);
      expect(timedOut2).toHaveLength(1);
      expect(timedOut2[0]).toMatchObject({ clientId: 2 });
    });
  });

  describe('getBlockedEntry', () => {
    it('returns blocked entry for a blocked client', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1', 'k2'],
        timeout: 5000,
        tryServe: alwaysServe(bulkReply('val')),
      });
      const entry = mgr.getBlockedEntry(1);
      expect(entry).toMatchObject({
        clientId: 1,
        keys: ['k1', 'k2'],
        dbIndex: 0,
        timeout: 5000,
      });
    });

    it('returns undefined for non-blocked client', () => {
      const mgr = new BlockingManager();
      expect(mgr.getBlockedEntry(99)).toBeUndefined();
    });
  });

  describe('edge cases', () => {
    it('client unblocked between signal and process is skipped', () => {
      const mgr = new BlockingManager();
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(bulkReply('val')),
      });
      mgr.signalKeyAsReady(0, 'k');
      // Unblock before processing
      mgr.unblockClient(1);
      const results = mgr.processReadyKeys();
      expect(results).toEqual([]);
    });

    it('signal for key with no blocked clients is harmless', () => {
      const mgr = new BlockingManager();
      mgr.signalKeyAsReady(0, 'nonexistent');
      const results = mgr.processReadyKeys();
      expect(results).toEqual([]);
    });

    it('blocked client on same key in different databases are independent', () => {
      const mgr = new BlockingManager();
      const reply0 = bulkReply('db0');
      const reply1 = bulkReply('db1');
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply0),
      });
      mgr.blockClient({
        clientId: 2,
        dbIndex: 1,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply1),
      });
      // Only signal db 0
      mgr.signalKeyAsReady(0, 'k');
      const results = mgr.processReadyKeys();
      expect(results).toEqual([{ clientId: 1, reply: reply0 }]);
      expect(mgr.isBlocked(1)).toBe(false);
      expect(mgr.isBlocked(2)).toBe(true);
    });

    it('processReadyKeys handles mixed serve/fail across clients', () => {
      const mgr = new BlockingManager();
      // Client 1: will fail to serve
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: neverServe(),
      });
      // Client 2: will succeed
      const reply2 = bulkReply('c2');
      mgr.blockClient({
        clientId: 2,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: alwaysServe(reply2),
      });
      // Client 3: will fail
      mgr.blockClient({
        clientId: 3,
        dbIndex: 0,
        keys: ['k'],
        timeout: 0,
        tryServe: neverServe(),
      });
      mgr.signalKeyAsReady(0, 'k');
      const results = mgr.processReadyKeys();
      expect(results).toEqual([{ clientId: 2, reply: reply2 }]);
      expect(mgr.isBlocked(1)).toBe(true);
      expect(mgr.isBlocked(2)).toBe(false);
      expect(mgr.isBlocked(3)).toBe(true);
    });

    it('already unblocked client is not processed twice from multiple key signals', () => {
      const mgr = new BlockingManager();
      const reply = bulkReply('val');
      let serveCount = 0;
      mgr.blockClient({
        clientId: 1,
        dbIndex: 0,
        keys: ['k1', 'k2'],
        timeout: 0,
        tryServe: () => {
          serveCount++;
          return reply;
        },
      });
      // Signal both keys
      mgr.signalKeyAsReady(0, 'k1');
      mgr.signalKeyAsReady(0, 'k2');
      const results = mgr.processReadyKeys();
      // Client should only be served once (unblocked on first key)
      expect(results).toHaveLength(1);
      expect(serveCount).toBe(1);
    });
  });
});
