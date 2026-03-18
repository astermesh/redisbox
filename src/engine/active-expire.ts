import type { Database } from './database.ts';

/**
 * Redis active expiration cycles (slow and fast variants).
 *
 * Mirrors the real Redis `activeExpireCycle()` algorithm from expire.c.
 *
 * Slow cycle: runs at `hz` frequency from serverCron, with a time budget
 * scaled by hz and effort.
 *
 * Fast cycle: runs before processing events (beforeSleep equivalent),
 * with a fixed 1ms time budget. Only runs if the last slow cycle timed out
 * (indicating high expired key ratio). Has a cooldown of 2ms between runs.
 *
 * Config parameters (matching Redis 7.x):
 * - hz: cycles per second (default 10)
 * - active-expire-effort: 1..10, scales sampling size and time budget
 */

const ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP = 20;
const ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC = 25;
const ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE = 10;

// Check time budget every N iterations to avoid excessive clock calls
const TIMELIMIT_CHECK_INTERVAL = 16;

// Fast cycle: fixed 1ms budget (Redis: ACTIVE_EXPIRE_CYCLE_FAST_DURATION = 1000us)
const ACTIVE_EXPIRE_CYCLE_FAST_DURATION_MS = 1;

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

/**
 * Core expiration loop shared by both slow and fast cycles.
 */
function expireCycleCore(
  databases: Database[],
  clock: () => number,
  rng: () => number,
  configKeysPerLoop: number,
  configCycleAcceptableStale: number,
  timeLimitMs: number
): ActiveExpireCycleResult {
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

      // If expired ratio is below acceptable stale threshold, move to next db
      // Redis: if ((expired*100/sampled) < config_cycle_acceptable_stale) break
      const threshold = sampled.length * (configCycleAcceptableStale / 100);
      if (expired < threshold) break;

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

/**
 * Slow active expiration cycle.
 *
 * Called from the main timer (serverCron equivalent) at `hz` frequency.
 * Time budget is scaled by hz and effort.
 */
export function activeExpireCycle(
  opts: ActiveExpireCycleOpts
): ActiveExpireCycleResult {
  const { databases, clock, rng, hz, effort } = opts;

  // Rescale effort from 1-10 config range to 0-9 (matches Redis expire.c)
  const adjustedEffort = effort - 1;

  // Scale parameters by effort (matches Redis expire.c)
  const configKeysPerLoop =
    ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP +
    (ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP / 4) * adjustedEffort;
  const configCycleSlowTimePerc =
    ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC + 2 * adjustedEffort;
  const configCycleAcceptableStale =
    ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE - adjustedEffort;

  // Time limit in milliseconds
  // Redis: timelimit = 1000000 * perc / hz / 100 (in microseconds)
  // We use ms: timelimit_ms = 1000 * perc / hz / 100
  const timeLimitMs = (1000 * configCycleSlowTimePerc) / hz / 100;

  return expireCycleCore(
    databases,
    clock,
    rng,
    configKeysPerLoop,
    configCycleAcceptableStale,
    timeLimitMs
  );
}

/**
 * Mutable state shared between slow and fast expiration cycles.
 *
 * Mirrors Redis's static variables `timelimit_exit`, `stat_expired_stale_perc`,
 * and `last_fast_cycle` in expire.c.
 */
export interface FastExpireCycleState {
  /** Whether the last slow cycle timed out. */
  lastSlowTimedOut: boolean;
  /** Timestamp (ms) of the last fast cycle run. */
  lastFastCycleTime: number;
}

export function createFastExpireCycleState(): FastExpireCycleState {
  return {
    lastSlowTimedOut: false,
    lastFastCycleTime: 0,
  };
}

export interface FastActiveExpireCycleOpts {
  databases: Database[];
  clock: () => number;
  rng: () => number;
  effort: number;
  state: FastExpireCycleState;
}

export interface FastActiveExpireCycleResult extends ActiveExpireCycleResult {
  /** Whether the cycle was skipped because conditions were not met. */
  skipped: boolean;
}

/**
 * Fast active expiration cycle.
 *
 * Called before processing events (beforeSleep equivalent).
 * Fixed 1ms time budget. Only runs if the last slow cycle timed out
 * and at least 2ms have passed since the last fast cycle.
 */
export function fastActiveExpireCycle(
  opts: FastActiveExpireCycleOpts
): FastActiveExpireCycleResult {
  const { databases, clock, rng, effort, state } = opts;

  const now = clock();

  // Condition 1: last slow cycle must have timed out (high expired ratio)
  if (!state.lastSlowTimedOut) {
    return { expired: 0, timedOut: false, skipped: true };
  }

  // Condition 2: cooldown — at least 2 * FAST_DURATION since last fast cycle
  // Redis: if (start < last_fast_cycle + ACTIVE_EXPIRE_CYCLE_FAST_DURATION*2)
  const cooldownMs = ACTIVE_EXPIRE_CYCLE_FAST_DURATION_MS * 2;
  if (now < state.lastFastCycleTime + cooldownMs) {
    return { expired: 0, timedOut: false, skipped: true };
  }

  state.lastFastCycleTime = now;

  // Rescale effort from 1-10 config range to 0-9 (matches Redis expire.c)
  const adjustedEffort = effort - 1;

  // Scale sampling parameters by effort (same as slow cycle)
  const configKeysPerLoop =
    ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP +
    (ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP / 4) * adjustedEffort;
  const configCycleAcceptableStale =
    ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE - adjustedEffort;

  // Fixed 1ms time budget (not scaled by hz)
  const result = expireCycleCore(
    databases,
    clock,
    rng,
    configKeysPerLoop,
    configCycleAcceptableStale,
    ACTIVE_EXPIRE_CYCLE_FAST_DURATION_MS
  );

  return { ...result, skipped: false };
}
