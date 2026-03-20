import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  errorReply,
  wrongArityError,
  ZERO,
  WRONGTYPE_ERR,
  NOT_FLOAT_ERR,
} from '../types.ts';
import { SkipList } from '../skip-list.ts';
import { parseFloat64, formatFloat } from './incr.ts';

function formatScore(n: number): string {
  if (n === Infinity) return 'inf';
  if (n === -Infinity) return '-inf';
  return formatFloat(n);
}

export interface SortedSetData {
  sl: SkipList;
  dict: Map<string, number>;
}

/**
 * Get or create a sorted set entry.
 */
function getOrCreateZset(
  db: Database,
  key: string,
  rng: () => number
): { zset: SortedSetData; error: null } | { zset: null; error: Reply } {
  const entry = db.get(key);
  if (entry) {
    if (entry.type !== 'zset') return { zset: null, error: WRONGTYPE_ERR };
    return { zset: entry.value as SortedSetData, error: null };
  }
  const zset: SortedSetData = {
    sl: new SkipList(rng),
    dict: new Map(),
  };
  db.set(key, 'zset', 'skiplist', zset);
  return { zset, error: null };
}

/**
 * Get an existing sorted set entry.
 */
function getExistingZset(
  db: Database,
  key: string
): { zset: SortedSetData | null; error: Reply | null } {
  const entry = db.get(key);
  if (!entry) return { zset: null, error: null };
  if (entry.type !== 'zset') return { zset: null, error: WRONGTYPE_ERR };
  return { zset: entry.value as SortedSetData, error: null };
}

/**
 * Remove empty sorted set key from database.
 */
function removeIfEmpty(db: Database, key: string, zset: SortedSetData): void {
  if (zset.dict.size === 0) {
    db.delete(key);
  }
}

// --- ZADD ---

export function zadd(db: Database, args: string[], rng: () => number): Reply {
  if (args.length < 3) {
    return wrongArityError('zadd');
  }

  const key = args[0] as string;

  // Parse flags
  let nx = false;
  let xx = false;
  let gt = false;
  let lt = false;
  let ch = false;
  let i = 1;

  while (i < args.length) {
    const flag = (args[i] as string).toUpperCase();
    if (flag === 'NX') {
      nx = true;
      i++;
    } else if (flag === 'XX') {
      xx = true;
      i++;
    } else if (flag === 'GT') {
      gt = true;
      i++;
    } else if (flag === 'LT') {
      lt = true;
      i++;
    } else if (flag === 'CH') {
      ch = true;
      i++;
    } else {
      break;
    }
  }

  // NX and XX are mutually exclusive
  if (nx && xx) {
    return errorReply(
      'ERR',
      'XX and NX options at the same time are not compatible'
    );
  }

  // NX and GT/LT are incompatible
  if (nx && (gt || lt)) {
    return errorReply(
      'ERR',
      'GT, LT, and NX options at the same time are not compatible'
    );
  }

  // Remaining args must be score-member pairs
  const remaining = args.length - i;
  if (remaining < 2 || remaining % 2 !== 0) {
    return wrongArityError('zadd');
  }

  // Parse all score-member pairs first (validate before mutating)
  const pairs: { score: number; member: string }[] = [];
  for (; i < args.length; i += 2) {
    const scoreStr = args[i] as string;
    const member = args[i + 1] as string;

    const parsed = parseFloat64(scoreStr);
    if (!parsed) {
      return NOT_FLOAT_ERR;
    }
    if (isNaN(parsed.value)) {
      return NOT_FLOAT_ERR;
    }

    pairs.push({ score: parsed.value, member });
  }

  const { zset, error } = getOrCreateZset(db, key, rng);
  if (error) return error;

  let added = 0;
  let updated = 0;

  for (const { score, member } of pairs) {
    const existing = zset.dict.get(member);

    if (existing !== undefined) {
      // Member exists — update logic
      if (nx) continue; // NX: never update

      let doUpdate = true;
      if (gt && lt) {
        // GT+LT: update only if score differs
        doUpdate = score !== existing;
      } else if (gt) {
        doUpdate = score > existing;
      } else if (lt) {
        doUpdate = score < existing;
      }

      if (doUpdate && score !== existing) {
        // Remove old, insert new in skip list
        zset.sl.delete(existing, member);
        zset.sl.insert(score, member);
        zset.dict.set(member, score);
        updated++;
      }
    } else {
      // Member doesn't exist — add logic
      if (xx) continue; // XX: never add

      zset.sl.insert(score, member);
      zset.dict.set(member, score);
      added++;
    }
  }

  // Clean up if nothing was added to a newly created empty set
  removeIfEmpty(db, key, zset);

  return integerReply(ch ? added + updated : added);
}

// --- ZREM ---

export function zrem(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return wrongArityError('zrem');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return ZERO;

  let removed = 0;
  for (let i = 1; i < args.length; i++) {
    const member = args[i] as string;
    const score = zset.dict.get(member);
    if (score !== undefined) {
      zset.sl.delete(score, member);
      zset.dict.delete(member);
      removed++;
    }
  }

  removeIfEmpty(db, key, zset);

  return integerReply(removed);
}

// --- ZINCRBY ---

export function zincrby(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  if (args.length !== 3) {
    return wrongArityError('zincrby');
  }

  const key = args[0] as string;
  const incrStr = args[1] as string;
  const member = args[2] as string;

  const parsed = parseFloat64(incrStr);
  if (!parsed) {
    return NOT_FLOAT_ERR;
  }
  if (isNaN(parsed.value)) {
    return NOT_FLOAT_ERR;
  }
  const increment = parsed.value;

  const { zset, error } = getOrCreateZset(db, key, rng);
  if (error) return error;

  const existing = zset.dict.get(member);
  let newScore: number;

  if (existing !== undefined) {
    newScore = existing + increment;
    if (isNaN(newScore)) {
      return errorReply('ERR', 'resulting score is not a number (NaN)');
    }
    zset.sl.delete(existing, member);
    zset.sl.insert(newScore, member);
    zset.dict.set(member, newScore);
  } else {
    newScore = increment;
    if (isNaN(newScore)) {
      return errorReply('ERR', 'resulting score is not a number (NaN)');
    }
    zset.sl.insert(newScore, member);
    zset.dict.set(member, newScore);
  }

  return bulkReply(formatScore(newScore));
}

// --- ZCARD ---

export function zcard(db: Database, args: string[]): Reply {
  if (args.length !== 1) {
    return wrongArityError('zcard');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return ZERO;

  return integerReply(zset.dict.size);
}
