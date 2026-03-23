import { describe, it, expect } from 'vitest';
import {
  LFU_INIT_VAL,
  lfuGetTimeInMinutes,
  lfuTimeElapsed,
  lfuDecrAndReturn,
  lfuLogIncr,
} from './lfu.ts';

describe('lfuGetTimeInMinutes', () => {
  it('converts milliseconds to minutes', () => {
    expect(lfuGetTimeInMinutes(0)).toBe(0);
    expect(lfuGetTimeInMinutes(60000)).toBe(1);
    expect(lfuGetTimeInMinutes(120000)).toBe(2);
    expect(lfuGetTimeInMinutes(3600000)).toBe(60); // 1 hour
  });

  it('truncates partial minutes', () => {
    expect(lfuGetTimeInMinutes(30000)).toBe(0); // 30s
    expect(lfuGetTimeInMinutes(90000)).toBe(1); // 1m30s
  });

  it('wraps at 16-bit boundary', () => {
    // 2^16 = 65536 minutes
    const maxMs = 65536 * 60000;
    expect(lfuGetTimeInMinutes(maxMs)).toBe(0); // wraps to 0
    expect(lfuGetTimeInMinutes(maxMs + 60000)).toBe(1);
  });
});

describe('lfuTimeElapsed', () => {
  it('computes elapsed time without wraparound', () => {
    expect(lfuTimeElapsed(10, 20)).toBe(10);
    expect(lfuTimeElapsed(0, 100)).toBe(100);
    expect(lfuTimeElapsed(5, 5)).toBe(0);
  });

  it('handles 16-bit wraparound', () => {
    // Clock wrapped: now < lastDecrTime
    // Matches Redis: 65535 - ldt + now
    expect(lfuTimeElapsed(65530, 5)).toBe(10); // 65535 - 65530 + 5
    expect(lfuTimeElapsed(65535, 0)).toBe(0); // 65535 - 65535 + 0
  });
});

describe('lfuDecrAndReturn', () => {
  it('decays counter by elapsed periods', () => {
    // 10 minutes elapsed, decay time 1 minute → decay by 10
    expect(lfuDecrAndReturn(15, 0, 10, 1)).toBe(5);
  });

  it('does not decay below 0', () => {
    expect(lfuDecrAndReturn(3, 0, 10, 1)).toBe(0);
  });

  it('does not decay when decayTime is 0', () => {
    expect(lfuDecrAndReturn(10, 0, 100, 0)).toBe(10);
  });

  it('handles partial periods (floors)', () => {
    // 5 minutes elapsed, decay time 3 → 1 period → decay by 1
    expect(lfuDecrAndReturn(10, 0, 5, 3)).toBe(9);
  });

  it('does not decay when insufficient time has passed', () => {
    // 2 minutes elapsed, decay time 5 → 0 periods
    expect(lfuDecrAndReturn(10, 0, 2, 5)).toBe(10);
  });

  it('handles wraparound in minutes clock', () => {
    // lastDecr=65530, now=5 → elapsed=10 (Redis: 65535-65530+5), decayTime=2 → 5 periods
    expect(lfuDecrAndReturn(10, 65530, 5, 2)).toBe(5);
  });
});

describe('lfuLogIncr', () => {
  it('always increments counter 0 (rng < 1.0)', () => {
    // p = 1.0 / (0 * 10 + 1) = 1.0, so any rng < 1.0 increments
    expect(lfuLogIncr(0, 10, () => 0.5)).toBe(1);
    expect(lfuLogIncr(0, 10, () => 0.99)).toBe(1);
  });

  it('increments with high probability for counters below LFU_INIT_VAL', () => {
    // For counter <= LFU_INIT_VAL, baseval = 0, p = 1.0
    for (let c = 0; c <= LFU_INIT_VAL; c++) {
      expect(lfuLogIncr(c, 10, () => 0.5)).toBe(c + 1);
    }
  });

  it('increments with lower probability for higher counters', () => {
    // counter=15, logFactor=10: baseval=10, p=1/(10*10+1)≈0.0099
    // rng=0.5 > 0.0099 → no increment
    expect(lfuLogIncr(15, 10, () => 0.5)).toBe(15);
    // rng=0.001 < 0.0099 → increment
    expect(lfuLogIncr(15, 10, () => 0.001)).toBe(16);
  });

  it('never exceeds 255', () => {
    expect(lfuLogIncr(255, 10, () => 0)).toBe(255);
    expect(lfuLogIncr(255, 0, () => 0)).toBe(255);
  });

  it('logFactor 0 makes increment very likely', () => {
    // p = 1.0 / (baseval * 0 + 1) = 1.0 for any baseval
    expect(lfuLogIncr(100, 0, () => 0.5)).toBe(101);
    expect(lfuLogIncr(200, 0, () => 0.99)).toBe(201);
  });

  it('higher logFactor reduces increment probability', () => {
    // counter=10, logFactor=1: baseval=5, p=1/6≈0.167
    // counter=10, logFactor=100: baseval=5, p=1/501≈0.002
    expect(lfuLogIncr(10, 1, () => 0.1)).toBe(11); // 0.1 < 0.167
    expect(lfuLogIncr(10, 100, () => 0.1)).toBe(10); // 0.1 > 0.002
  });
});

describe('LFU_INIT_VAL', () => {
  it('equals 5 matching Redis', () => {
    expect(LFU_INIT_VAL).toBe(5);
  });
});
