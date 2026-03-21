import { describe, it, expect } from 'vitest';
import { VirtualClock } from './virtual-clock.ts';

describe('VirtualClock', () => {
  describe('default behavior', () => {
    it('returns base clock time when no modifications', () => {
      let base = 1000;
      const clock = new VirtualClock(() => base);
      expect(clock.now()).toBe(1000);
      base = 2000;
      expect(clock.now()).toBe(2000);
    });

    it('is not frozen by default', () => {
      const clock = new VirtualClock(() => 1000);
      expect(clock.isFrozen).toBe(false);
    });
  });

  describe('advanceTime', () => {
    it('advances time by given milliseconds', () => {
      const clock = new VirtualClock(() => 1000);
      clock.advanceTime(500);
      expect(clock.now()).toBe(1500);
    });

    it('accumulates multiple advances', () => {
      const clock = new VirtualClock(() => 1000);
      clock.advanceTime(100);
      clock.advanceTime(200);
      expect(clock.now()).toBe(1300);
    });

    it('advances frozen time when frozen', () => {
      const clock = new VirtualClock(() => 1000);
      clock.freezeTime();
      expect(clock.now()).toBe(1000);
      clock.advanceTime(500);
      expect(clock.now()).toBe(1500);
      expect(clock.isFrozen).toBe(true);
    });

    it('throws on negative advance', () => {
      const clock = new VirtualClock(() => 1000);
      expect(() => clock.advanceTime(-1)).toThrow(
        'Cannot advance time by a negative amount'
      );
    });

    it('accepts zero advance', () => {
      const clock = new VirtualClock(() => 1000);
      clock.advanceTime(0);
      expect(clock.now()).toBe(1000);
    });
  });

  describe('freezeTime', () => {
    it('freezes time at current value', () => {
      let base = 1000;
      const clock = new VirtualClock(() => base);
      clock.freezeTime();
      expect(clock.now()).toBe(1000);
      base = 5000;
      expect(clock.now()).toBe(1000);
    });

    it('sets isFrozen to true', () => {
      const clock = new VirtualClock(() => 1000);
      clock.freezeTime();
      expect(clock.isFrozen).toBe(true);
    });

    it('is idempotent when already frozen', () => {
      const clock = new VirtualClock(() => 1000);
      clock.freezeTime();
      clock.advanceTime(500);
      clock.freezeTime();
      expect(clock.now()).toBe(1500);
    });

    it('captures offset in frozen value', () => {
      const clock = new VirtualClock(() => 1000);
      clock.advanceTime(200);
      clock.freezeTime();
      expect(clock.now()).toBe(1200);
    });
  });

  describe('setTime', () => {
    it('sets absolute time when not frozen', () => {
      const clock = new VirtualClock(() => 1000);
      clock.setTime(5000);
      expect(clock.now()).toBe(5000);
    });

    it('sets absolute time when frozen', () => {
      const clock = new VirtualClock(() => 1000);
      clock.freezeTime();
      clock.setTime(9999);
      expect(clock.now()).toBe(9999);
      expect(clock.isFrozen).toBe(true);
    });

    it('adjusts offset relative to base clock', () => {
      let base = 1000;
      const clock = new VirtualClock(() => base);
      clock.setTime(5000);
      base = 1100;
      expect(clock.now()).toBe(5100);
    });
  });

  describe('unfreezeTime', () => {
    it('resumes time from frozen point', () => {
      let base = 1000;
      const clock = new VirtualClock(() => base);
      clock.freezeTime();
      clock.advanceTime(500);
      expect(clock.now()).toBe(1500);

      base = 1200;
      clock.unfreezeTime();
      expect(clock.isFrozen).toBe(false);
      // offset = frozenAt(1500) - base(1200) = 300
      expect(clock.now()).toBe(1500);

      base = 1300;
      expect(clock.now()).toBe(1600);
    });

    it('is a no-op when not frozen', () => {
      const clock = new VirtualClock(() => 1000);
      clock.advanceTime(100);
      clock.unfreezeTime();
      expect(clock.now()).toBe(1100);
      expect(clock.isFrozen).toBe(false);
    });
  });

  describe('freeze-advance-unfreeze cycle', () => {
    it('correctly handles full cycle', () => {
      let base = 1000;
      const clock = new VirtualClock(() => base);

      clock.freezeTime();
      clock.advanceTime(2000);
      expect(clock.now()).toBe(3000);

      base = 1050;
      clock.unfreezeTime();
      // offset = 3000 - 1050 = 1950
      expect(clock.now()).toBe(3000);

      base = 1100;
      expect(clock.now()).toBe(3050);
    });

    it('supports multiple freeze-unfreeze cycles', () => {
      let base = 0;
      const clock = new VirtualClock(() => base);

      clock.freezeTime();
      clock.advanceTime(100);
      clock.unfreezeTime();
      expect(clock.now()).toBe(100);

      base = 50;
      expect(clock.now()).toBe(150);

      clock.freezeTime();
      clock.advanceTime(200);
      clock.unfreezeTime();

      base = 100;
      expect(clock.now()).toBe(400);
    });
  });
});
