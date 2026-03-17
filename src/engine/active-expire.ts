import type { Database } from './database.ts';

/**
 * Redis active expiration cycle (slow variant).
 *
 * Mirrors the real Redis `activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW)` algorithm
 * from expire.c. Runs at `hz` frequency, sampling random keys with TTL and
 * deleting expired ones. Repeats sampling for a database when the expired
 * ratio exceeds the acceptable-stale threshold.
 *
 * Config parameters (matching Redis 7.x):
 * - hz: cycles per second (default 10)
 * - active-expire-effort: 1..100, scales sampling size and time budget
 */

const ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP = 20;
const ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC = 25;
const ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE = 10;

// Check time budget every N iterations to avoid excessive clock calls
const TIMELIMIT_CHECK_INTERVAL = 16;

export interface ActiveExpireCycleOpts {
  databases: Database[];
  clock: () => number;
  rng: () => number;
  hz: number;
  effort: number;
}

export interface ActiveExpireCycleResult {
  /** Total number of keys expired in this cycle. */
  expired: number;
  /** Whether the cycle stopped early due to time budget. */
  timedOut: boolean;
}

export function activeExpireCycle(
  opts: ActiveExpireCycleOpts
): ActiveExpireCycleResult {
  const { databases, clock, rng, hz, effort } = opts;

  // Scale parameters by effort (matches Redis expire.c)
  const configKeysPerLoop =
    ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP +
    Math.floor((ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP / 4) * effort);
  const configCycleSlowTimePerc =
    ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC + 2 * effort;
  const configCycleAcceptableStale =
    ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE - effort;

  // Time limit in milliseconds
  // Redis: timelimit = 1000000 * perc / hz / 100 (in microseconds)
  // We use ms: timelimit_ms = 1000 * perc / hz / 100
  const timeLimitMs = (1000 * configCycleSlowTimePerc) / hz / 100;
  const startTime = clock();

  let totalExpired = 0;
  let timedOut = false;
  let iteration = 0;

  for (const db of databases) {
    if (db.expirySize === 0) continue;

    // Inner loop: keep sampling while expired ratio exceeds threshold
    for (;;) {
      const sampled = db.sampleExpiryKeys(configKeysPerLoop, rng);
      if (sampled.length === 0) break;

      let expired = 0;
      for (const key of sampled) {
        if (db.tryExpire(key)) {
          expired++;
        }
      }
      totalExpired += expired;

      iteration++;
      // Check time limit periodically
      if (iteration % TIMELIMIT_CHECK_INTERVAL === 0) {
        const elapsed = clock() - startTime;
        if (elapsed >= timeLimitMs) {
          timedOut = true;
          break;
        }
      }

      // If expired ratio is at or below acceptable stale threshold, move to next db
      // Redis: if (expired <= sampled / (100/acceptable_stale)) break
      const threshold = sampled.length * (configCycleAcceptableStale / 100);
      if (expired <= threshold) break;

      // Also check if no more expiring keys remain
      if (db.expirySize === 0) break;

      // Quick time check when we know we'll loop again
      const elapsed = clock() - startTime;
      if (elapsed >= timeLimitMs) {
        timedOut = true;
        break;
      }
    }

    if (timedOut) break;
  }

  return { expired: totalExpired, timedOut };
}
