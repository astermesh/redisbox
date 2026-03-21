import { describe, it, expect } from 'vitest';
import {
  getLruClock,
  estimateIdleTime,
  LRU_CLOCK_MAX,
  LRU_CLOCK_RESOLUTION,
} from './lru.ts';

describe('LRU clock utilities', () => {
  describe('getLruClock', () => {
    it('converts milliseconds to seconds', () => {
      expect(getLruClock(0)).toBe(0);
      expect(getLruClock(1000)).toBe(1);
      expect(getLruClock(5000)).toBe(5);
      expect(getLruClock(60000)).toBe(60);
    });

    it('truncates sub-second precision', () => {
      expect(getLruClock(1500)).toBe(1);
      expect(getLruClock(999)).toBe(0);
      expect(getLruClock(2999)).toBe(2);
    });

    it('wraps around at 24-bit boundary', () => {
      const maxSeconds = LRU_CLOCK_MAX; // 2^24 - 1
      expect(getLruClock(maxSeconds * 1000)).toBe(maxSeconds);
      // One more second wraps to 0
      expect(getLruClock((maxSeconds + 1) * 1000)).toBe(0);
      // Two more wraps to 1
      expect(getLruClock((maxSeconds + 2) * 1000)).toBe(1);
    });

    it('masks to 24 bits for large values', () => {
      const val = getLruClock((LRU_CLOCK_MAX + 100) * 1000);
      expect(val).toBe(99);
    });
  });

  describe('estimateIdleTime', () => {
    it('returns 0 when clocks are equal', () => {
      expect(estimateIdleTime(100, 100)).toBe(0);
    });

    it('returns difference in milliseconds without wraparound', () => {
      expect(estimateIdleTime(110, 100)).toBe(10 * LRU_CLOCK_RESOLUTION);
      expect(estimateIdleTime(200, 100)).toBe(100 * LRU_CLOCK_RESOLUTION);
    });

    it('handles wraparound correctly', () => {
      // Current clock wrapped around, entry was near the max
      const entryClock = LRU_CLOCK_MAX - 5; // near the end
      const currentClock = 10; // wrapped around
      // Idle = 10 + (LRU_CLOCK_MAX + 1) - (LRU_CLOCK_MAX - 5) = 10 + 6 = 16
      expect(estimateIdleTime(currentClock, entryClock)).toBe(
        16 * LRU_CLOCK_RESOLUTION
      );
    });

    it('returns correct idle time at the wrap boundary', () => {
      expect(estimateIdleTime(0, LRU_CLOCK_MAX)).toBe(1 * LRU_CLOCK_RESOLUTION);
    });

    it('handles large idle times', () => {
      // Half the clock range
      const half = Math.floor(LRU_CLOCK_MAX / 2);
      expect(estimateIdleTime(half, 0)).toBe(half * LRU_CLOCK_RESOLUTION);
    });
  });
});
