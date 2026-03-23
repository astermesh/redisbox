import { describe, it, expect } from 'vitest';
import { LatencyManager } from './latency.ts';

describe('LatencyManager', () => {
  describe('record', () => {
    it('does not record when threshold is 0 (disabled)', () => {
      const lm = new LatencyManager();
      lm.record('command', 500, 0, 1000);
      expect(lm.latest()).toEqual([]);
    });

    it('does not record when threshold is negative', () => {
      const lm = new LatencyManager();
      lm.record('command', 500, -1, 1000);
      expect(lm.latest()).toEqual([]);
    });

    it('does not record when latency is below threshold', () => {
      const lm = new LatencyManager();
      lm.record('command', 50, 100, 1000);
      expect(lm.latest()).toEqual([]);
    });

    it('records when latency equals threshold', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 100, 1000);
      expect(lm.latest()).toHaveLength(1);
    });

    it('records when latency exceeds threshold', () => {
      const lm = new LatencyManager();
      lm.record('command', 200, 100, 1000);
      expect(lm.latest()).toHaveLength(1);
      expect(lm.latest()[0]).toEqual({
        event: 'command',
        timestamp: 1000,
        latest: 200,
        max: 200,
      });
    });

    it('evicts oldest samples beyond 160', () => {
      const lm = new LatencyManager();
      for (let i = 0; i < 170; i++) {
        lm.record('command', 100, 50, 1000 + i);
      }
      const history = lm.history('command');
      expect(history).toHaveLength(160);
      // Oldest should have been evicted — first sample starts at offset 10
      expect(history[0]?.timestamp).toBe(1010);
      expect(history[159]?.timestamp).toBe(1169);
    });

    it('tracks all-time max correctly', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('command', 500, 50, 1001);
      lm.record('command', 200, 50, 1002);

      const entry = lm.latest()[0];
      expect(entry?.latest).toBe(200); // most recent
      expect(entry?.max).toBe(500); // all-time max
    });

    it('records multiple event types independently', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('fast-command', 200, 50, 1001);

      expect(lm.latest()).toHaveLength(2);
      expect(lm.history('command')).toHaveLength(1);
      expect(lm.history('fast-command')).toHaveLength(1);
    });
  });

  describe('latest', () => {
    it('returns empty array when no events recorded', () => {
      const lm = new LatencyManager();
      expect(lm.latest()).toEqual([]);
    });

    it('returns latest sample per event', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('command', 200, 50, 1001);

      const entries = lm.latest();
      expect(entries).toHaveLength(1);
      expect(entries[0]?.timestamp).toBe(1001);
      expect(entries[0]?.latest).toBe(200);
    });
  });

  describe('history', () => {
    it('returns empty array for unknown event', () => {
      const lm = new LatencyManager();
      expect(lm.history('unknown')).toEqual([]);
    });

    it('returns all samples for an event', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('command', 200, 50, 1001);
      lm.record('command', 150, 50, 1002);

      const history = lm.history('command');
      expect(history).toHaveLength(3);
      expect(history[0]).toEqual({ timestamp: 1000, latency: 100 });
      expect(history[1]).toEqual({ timestamp: 1001, latency: 200 });
      expect(history[2]).toEqual({ timestamp: 1002, latency: 150 });
    });

    it('returns a copy (not the internal array)', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      const h1 = lm.history('command');
      const h2 = lm.history('command');
      expect(h1).not.toBe(h2);
      expect(h1).toEqual(h2);
    });
  });

  describe('reset', () => {
    it('clears all events when called with no args', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('fast-command', 200, 50, 1001);

      const count = lm.reset();
      expect(count).toBe(2);
      expect(lm.latest()).toEqual([]);
    });

    it('clears all events when called with empty array', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);

      const count = lm.reset([]);
      expect(count).toBe(1);
      expect(lm.latest()).toEqual([]);
    });

    it('clears only specified events', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('fast-command', 200, 50, 1001);
      lm.record('expire-cycle', 150, 50, 1002);

      const count = lm.reset(['command', 'expire-cycle']);
      expect(count).toBe(2);
      expect(lm.latest()).toHaveLength(1);
      expect(lm.latest()[0]?.event).toBe('fast-command');
    });

    it('returns 0 when resetting non-existent events', () => {
      const lm = new LatencyManager();
      const count = lm.reset(['nonexistent']);
      expect(count).toBe(0);
    });
  });

  describe('eventNames', () => {
    it('returns empty array when no events', () => {
      const lm = new LatencyManager();
      expect(lm.eventNames()).toEqual([]);
    });

    it('returns all event names', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      lm.record('fast-command', 200, 50, 1001);
      expect(lm.eventNames()).toEqual(['command', 'fast-command']);
    });
  });

  describe('has', () => {
    it('returns false for unknown event', () => {
      const lm = new LatencyManager();
      expect(lm.has('unknown')).toBe(false);
    });

    it('returns true for event with samples', () => {
      const lm = new LatencyManager();
      lm.record('command', 100, 50, 1000);
      expect(lm.has('command')).toBe(true);
    });
  });

  describe('allTimeMax', () => {
    it('returns 0 for unknown event', () => {
      const lm = new LatencyManager();
      expect(lm.allTimeMax('unknown')).toBe(0);
    });

    it('returns all-time max even after samples evicted', () => {
      const lm = new LatencyManager();
      lm.record('command', 999, 50, 1000);
      // Fill up to evict the first sample
      for (let i = 1; i <= 160; i++) {
        lm.record('command', 100, 50, 1000 + i);
      }
      // The 999ms sample was evicted but all-time max is preserved
      const history = lm.history('command');
      expect(history.every((s) => s.latency <= 100)).toBe(true);
      expect(lm.allTimeMax('command')).toBe(999);
    });
  });
});
