/**
 * LRU clock utilities for approximated LRU eviction.
 *
 * Redis uses a 24-bit clock with 1-second resolution for LRU tracking.
 * The clock wraps around every 2^24 seconds (~194 days). Each key stores
 * a snapshot of this clock at its last access time. Idle time is computed
 * by comparing the current clock with the stored value, handling wraparound.
 */

/** Number of bits used for the LRU clock. */
export const LRU_BITS = 24;

/** Maximum value of the 24-bit LRU clock. */
export const LRU_CLOCK_MAX = (1 << LRU_BITS) - 1;

/** LRU clock resolution in milliseconds (1 second). */
export const LRU_CLOCK_RESOLUTION = 1000;

/**
 * Convert a millisecond timestamp to a 24-bit LRU clock value.
 * Matches Redis `getLRUClock()`.
 */
export function getLruClock(msTime: number): number {
  return Math.floor(msTime / LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/**
 * Estimate the idle time of an object in milliseconds.
 * Handles 24-bit clock wraparound.
 * Matches Redis `estimateObjectIdleTime()`.
 */
export function estimateIdleTime(
  currentClock: number,
  entryClock: number
): number {
  if (currentClock >= entryClock) {
    return (currentClock - entryClock) * LRU_CLOCK_RESOLUTION;
  }
  return (currentClock + LRU_CLOCK_MAX + 1 - entryClock) * LRU_CLOCK_RESOLUTION;
}
