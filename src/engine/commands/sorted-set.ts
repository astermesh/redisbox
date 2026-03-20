import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  errorReply,
  wrongArityError,
  ZERO,
  NIL,
  WRONGTYPE_ERR,
  NOT_FLOAT_ERR,
} from '../types.ts';
import { SkipList } from '../skip-list.ts';
import { parseFloat64, formatFloat } from './incr.ts';
import type { CommandSpec } from '../command-table.ts';

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
  let incr = false;
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
    } else if (flag === 'INCR') {
      incr = true;
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

  // NX+GT, NX+LT, GT+LT are all incompatible
  if ((nx && (gt || lt)) || (gt && lt)) {
    return errorReply(
      'ERR',
      'GT, LT, and/or NX options at the same time are not compatible'
    );
  }

  // Remaining args must be score-member pairs
  const remaining = args.length - i;
  if (remaining < 2 || remaining % 2 !== 0) {
    return errorReply('ERR', 'syntax error');
  }

  // INCR mode: only one score-member pair allowed
  if (incr && remaining !== 2) {
    return errorReply(
      'ERR',
      'INCR option supports a single increment-element pair'
    );
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
  let incrResult: number | null = null;

  for (const { score, member } of pairs) {
    const existing = zset.dict.get(member);

    if (existing !== undefined) {
      // Member exists — update logic
      if (nx) {
        if (incr) incrResult = null;
        continue; // NX: never update
      }

      if (incr) {
        // INCR mode: add increment to existing score
        const newScore = existing + score;
        if (isNaN(newScore)) {
          return errorReply('ERR', 'resulting score is not a number (NaN)');
        }

        let doUpdate = true;
        if (gt) {
          doUpdate = newScore > existing;
        } else if (lt) {
          doUpdate = newScore < existing;
        }

        if (doUpdate) {
          zset.sl.delete(existing, member);
          zset.sl.insert(newScore, member);
          zset.dict.set(member, newScore);
          updated++;
          incrResult = newScore;
        } else {
          incrResult = null;
        }
      } else {
        let doUpdate = true;
        if (gt) {
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
      }
    } else {
      // Member doesn't exist — add logic
      if (xx) {
        if (incr) incrResult = null;
        continue; // XX: never add
      }

      if (incr) {
        if (isNaN(score)) {
          return errorReply('ERR', 'resulting score is not a number (NaN)');
        }
        zset.sl.insert(score, member);
        zset.dict.set(member, score);
        added++;
        incrResult = score;
      } else {
        zset.sl.insert(score, member);
        zset.dict.set(member, score);
        added++;
      }
    }
  }

  // Clean up if nothing was added to a newly created empty set
  removeIfEmpty(db, key, zset);

  if (incr) {
    return incrResult !== null ? bulkReply(formatScore(incrResult)) : NIL;
  }

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

export const specs: CommandSpec[] = [
  {
    name: 'zadd',
    handler: (ctx, args) => zadd(ctx.db, args, ctx.engine.rng),
    arity: -4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zrem',
    handler: (ctx, args) => zrem(ctx.db, args),
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zincrby',
    handler: (ctx, args) => zincrby(ctx.db, args, ctx.engine.rng),
    arity: 4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zcard',
    handler: (ctx, args) => zcard(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
  },
];
