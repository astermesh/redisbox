/**
 * LFU (Least Frequently Used) utilities for approximated LFU eviction.
 *
 * Redis uses a Morris counter-based LFU with:
 * - 8-bit logarithmic frequency counter (0-255)
 * - 16-bit last-decrement-time (minutes resolution, wraps at 2^16)
 *
 * On each key access:
 * 1. Decay the counter based on elapsed time since last decrement
 * 2. Probabilistically increment the counter (higher counter = lower probability)
 *
 * Config parameters:
 * - lfu-log-factor (default 10): controls increment probability curve
 * - lfu-decay-time (default 1): minutes between each counter decrement
 */

/** Initial frequency counter value for new keys (matches Redis LFU_INIT_VAL). */
export const LFU_INIT_VAL = 5;

/** Maximum value of the 16-bit minutes clock. */
const LFU_MINUTES_MAX = 0xffff;

/**
 * Convert a millisecond timestamp to a 16-bit minutes clock.
 * Matches Redis `LFUGetTimeInMinutes()`.
 */
export function lfuGetTimeInMinutes(msTime: number): number {
  return Math.floor(msTime / 60000) & LFU_MINUTES_MAX;
}

/**
 * Compute elapsed time in minutes between two 16-bit minute timestamps.
 * Handles wraparound. Matches Redis `LFUTimeElapsed()`.
 */
export function lfuTimeElapsed(
  lastDecrTime: number,
  nowMinutes: number
): number {
  if (nowMinutes >= lastDecrTime) {
    return nowMinutes - lastDecrTime;
  }
  return LFU_MINUTES_MAX - lastDecrTime + nowMinutes;
}

/**
 * Decay the frequency counter based on elapsed time.
 * Each `decayTime` minutes that pass, the counter is decremented by 1.
 * If decayTime is 0, no decay occurs.
 * Matches Redis `LFUDecrAndReturn()`.
 *
 * Returns the decayed counter value.
 */
export function lfuDecrAndReturn(
  counter: number,
  lastDecrTime: number,
  nowMinutes: number,
  decayTime: number
): number {
  if (decayTime === 0) return counter;
  const elapsed = lfuTimeElapsed(lastDecrTime, nowMinutes);
  const numPeriods = Math.floor(elapsed / decayTime);
  if (numPeriods > 0) {
    return Math.max(0, counter - numPeriods);
  }
  return counter;
}

/**
 * Probabilistically increment the frequency counter using Morris counter logic.
 * Higher counter values have lower probability of incrementing.
 * Matches Redis `LFULogIncr()`.
 *
 * @param counter - Current counter value (0-255)
 * @param logFactor - lfu-log-factor config value (default 10)
 * @param rng - Random number generator returning [0, 1)
 * @returns New counter value
 */
export function lfuLogIncr(
  counter: number,
  logFactor: number,
  rng: () => number
): number {
  if (counter === 255) return 255;
  const baseval = counter > LFU_INIT_VAL ? counter - LFU_INIT_VAL : 0;
  const p = 1.0 / (baseval * logFactor + 1);
  if (rng() < p) {
    return counter + 1;
  }
  return counter;
}
