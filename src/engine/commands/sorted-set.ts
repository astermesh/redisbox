import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  wrongArityError,
  ZERO,
  NIL,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_FLOAT_ERR,
  SYNTAX_ERR,
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

// --- Score range parsing ---

const MIN_MAX_ERR = errorReply('ERR', 'min or max is not a float');
const LEX_RANGE_ERR = errorReply(
  'ERR',
  'min or max not valid string range item'
);

interface ScoreRange {
  min: number;
  max: number;
  minExclusive: boolean;
  maxExclusive: boolean;
}

function parseScoreBound(
  s: string
): { value: number; exclusive: boolean } | null {
  if (s.startsWith('(')) {
    const rest = s.slice(1);
    const parsed = parseFloat64(rest);
    if (!parsed) return null;
    return { value: parsed.value, exclusive: true };
  }
  const parsed = parseFloat64(s);
  if (!parsed) return null;
  return { value: parsed.value, exclusive: false };
}

function parseScoreRange(minStr: string, maxStr: string): ScoreRange | null {
  const min = parseScoreBound(minStr);
  const max = parseScoreBound(maxStr);
  if (!min || !max) return null;
  return {
    min: min.value,
    max: max.value,
    minExclusive: min.exclusive,
    maxExclusive: max.exclusive,
  };
}

// --- Lex range parsing ---

interface LexBound {
  value: string;
  exclusive: boolean;
  isNegInf: boolean;
  isPosInf: boolean;
}

interface LexRange {
  min: LexBound;
  max: LexBound;
}

function parseLexBound(s: string): LexBound | null {
  if (s === '-')
    return { value: '', exclusive: false, isNegInf: true, isPosInf: false };
  if (s === '+')
    return { value: '', exclusive: false, isNegInf: false, isPosInf: true };
  if (s.startsWith('['))
    return {
      value: s.slice(1),
      exclusive: false,
      isNegInf: false,
      isPosInf: false,
    };
  if (s.startsWith('('))
    return {
      value: s.slice(1),
      exclusive: true,
      isNegInf: false,
      isPosInf: false,
    };
  return null;
}

function parseLexRange(minStr: string, maxStr: string): LexRange | null {
  const min = parseLexBound(minStr);
  const max = parseLexBound(maxStr);
  if (!min || !max) return null;
  return { min, max };
}

/**
 * Check if element passes lex min bound.
 */
function lexGteMin(element: string, bound: LexBound): boolean {
  if (bound.isNegInf) return true;
  if (bound.isPosInf) return false;
  return bound.exclusive ? element > bound.value : element >= bound.value;
}

/**
 * Check if element passes lex max bound.
 */
function lexLteMax(element: string, bound: LexBound): boolean {
  if (bound.isPosInf) return true;
  if (bound.isNegInf) return false;
  return bound.exclusive ? element < bound.value : element <= bound.value;
}

// --- LIMIT parsing helper ---

function parseLimit(
  args: string[],
  startIdx: number
): { offset: number; count: number } | Reply {
  if (startIdx + 2 > args.length) {
    return errorReply('ERR', 'syntax error');
  }
  const offset = Number(args[startIdx]);
  const count = Number(args[startIdx + 1]);
  if (!Number.isInteger(offset) || !Number.isInteger(count)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  return { offset, count };
}

// --- ZCOUNT ---

export function zcount(db: Database, args: string[]): Reply {
  if (args.length !== 3) {
    return wrongArityError('zcount');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return ZERO;

  const range = parseScoreRange(args[1] as string, args[2] as string);
  if (!range) return MIN_MAX_ERR;

  return integerReply(
    zset.sl.countInRange(
      range.min,
      range.max,
      range.minExclusive,
      range.maxExclusive
    )
  );
}

// --- ZLEXCOUNT ---

export function zlexcount(db: Database, args: string[]): Reply {
  if (args.length !== 3) {
    return wrongArityError('zlexcount');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return ZERO;

  const range = parseLexRange(args[1] as string, args[2] as string);
  if (!range) return LEX_RANGE_ERR;

  let count = 0;
  let node = zset.sl.head.lvl(0).forward;
  while (node) {
    if (!lexLteMax(node.element, range.max)) break;
    if (lexGteMin(node.element, range.min)) {
      count++;
    }
    node = node.lvl(0).forward;
  }

  return integerReply(count);
}

// --- Score-based range collection helper ---

function collectByScore(
  zset: SortedSetData,
  range: ScoreRange,
  reverse: boolean,
  withScores: boolean,
  offset: number,
  count: number
): Reply[] {
  const results: Reply[] = [];

  if (reverse) {
    // For reverse, we get the last node in range and walk backward
    const last = zset.sl.getLastInRange(
      range.min,
      range.max,
      range.minExclusive,
      range.maxExclusive
    );
    if (!last) return results;

    let node: typeof last | null = last;
    let skipped = 0;
    let collected = 0;

    while (node) {
      // Check if still in range
      const s = node.score;
      if (range.minExclusive ? s <= range.min : s < range.min) break;

      if (skipped < offset) {
        skipped++;
        node = node.backward;
        continue;
      }

      if (count >= 0 && collected >= count) break;

      results.push(bulkReply(node.element));
      if (withScores) results.push(bulkReply(formatScore(node.score)));
      collected++;
      node = node.backward;
    }
  } else {
    const first = zset.sl.getFirstInRange(
      range.min,
      range.max,
      range.minExclusive,
      range.maxExclusive
    );
    if (!first) return results;

    let node: typeof first | null = first;
    let skipped = 0;
    let collected = 0;

    while (node) {
      const s = node.score;
      if (range.maxExclusive ? s >= range.max : s > range.max) break;

      if (skipped < offset) {
        skipped++;
        node = node.lvl(0).forward;
        continue;
      }

      if (count >= 0 && collected >= count) break;

      results.push(bulkReply(node.element));
      if (withScores) results.push(bulkReply(formatScore(node.score)));
      collected++;
      node = node.lvl(0).forward;
    }
  }

  return results;
}

// --- Lex-based range collection helper ---

function collectByLex(
  zset: SortedSetData,
  range: LexRange,
  reverse: boolean,
  withScores: boolean,
  offset: number,
  count: number
): Reply[] {
  const results: Reply[] = [];

  if (reverse) {
    // Walk backward from tail
    let node = zset.sl.tail;
    let skipped = 0;
    let collected = 0;

    while (node) {
      if (!lexGteMin(node.element, range.min)) break;
      if (lexLteMax(node.element, range.max)) {
        if (skipped < offset) {
          skipped++;
        } else {
          if (count >= 0 && collected >= count) break;
          results.push(bulkReply(node.element));
          if (withScores) results.push(bulkReply(formatScore(node.score)));
          collected++;
        }
      }
      node = node.backward;
    }
  } else {
    let node = zset.sl.head.lvl(0).forward;
    let skipped = 0;
    let collected = 0;

    while (node) {
      if (!lexLteMax(node.element, range.max)) break;
      if (lexGteMin(node.element, range.min)) {
        if (skipped < offset) {
          skipped++;
        } else {
          if (count >= 0 && collected >= count) break;
          results.push(bulkReply(node.element));
          if (withScores) results.push(bulkReply(formatScore(node.score)));
          collected++;
        }
      }
      node = node.lvl(0).forward;
    }
  }

  return results;
}

// --- ZRANGEBYSCORE ---

export function zrangebyscore(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return wrongArityError('zrangebyscore');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return EMPTY_ARRAY;

  const range = parseScoreRange(args[1] as string, args[2] as string);
  if (!range) return MIN_MAX_ERR;

  let withScores = false;
  let offset = 0;
  let count = -1;
  let i = 3;

  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'WITHSCORES') {
      withScores = true;
      i++;
    } else if (opt === 'LIMIT') {
      i++;
      const lim = parseLimit(args, i);
      if ('kind' in lim) return lim;
      offset = lim.offset;
      count = lim.count;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  return arrayReply(
    collectByScore(zset, range, false, withScores, offset, count)
  );
}

// --- ZREVRANGEBYSCORE ---

export function zrevrangebyscore(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return wrongArityError('zrevrangebyscore');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return EMPTY_ARRAY;

  // Note: ZREVRANGEBYSCORE takes max first, then min
  const range = parseScoreRange(args[2] as string, args[1] as string);
  if (!range) return MIN_MAX_ERR;

  let withScores = false;
  let offset = 0;
  let count = -1;
  let i = 3;

  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'WITHSCORES') {
      withScores = true;
      i++;
    } else if (opt === 'LIMIT') {
      i++;
      const lim = parseLimit(args, i);
      if ('kind' in lim) return lim;
      offset = lim.offset;
      count = lim.count;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  return arrayReply(
    collectByScore(zset, range, true, withScores, offset, count)
  );
}

// --- ZRANGEBYLEX ---

export function zrangebylex(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return wrongArityError('zrangebylex');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return EMPTY_ARRAY;

  const range = parseLexRange(args[1] as string, args[2] as string);
  if (!range) return LEX_RANGE_ERR;

  let offset = 0;
  let count = -1;
  let i = 3;

  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'LIMIT') {
      i++;
      const lim = parseLimit(args, i);
      if ('kind' in lim) return lim;
      offset = lim.offset;
      count = lim.count;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  return arrayReply(collectByLex(zset, range, false, false, offset, count));
}

// --- ZREVRANGEBYLEX ---

export function zrevrangebylex(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return wrongArityError('zrevrangebylex');
  }

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return EMPTY_ARRAY;

  // Note: ZREVRANGEBYLEX takes max first, then min
  const range = parseLexRange(args[2] as string, args[1] as string);
  if (!range) return LEX_RANGE_ERR;

  let offset = 0;
  let count = -1;
  let i = 3;

  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'LIMIT') {
      i++;
      const lim = parseLimit(args, i);
      if ('kind' in lim) return lim;
      offset = lim.offset;
      count = lim.count;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  return arrayReply(collectByLex(zset, range, true, false, offset, count));
}

// --- ZRANGE (unified, Redis 6.2+) ---

export function zrange(
  db: Database,
  args: string[],
  _rng: () => number
): Reply {
  if (args.length < 3) {
    return wrongArityError('zrange');
  }

  const key = args[0] as string;

  // Parse options
  let byScore = false;
  let byLex = false;
  let rev = false;
  let withScores = false;
  let hasLimit = false;
  let offset = 0;
  let count = -1;
  let i = 3;

  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'BYSCORE') {
      byScore = true;
      i++;
    } else if (opt === 'BYLEX') {
      byLex = true;
      i++;
    } else if (opt === 'REV') {
      rev = true;
      i++;
    } else if (opt === 'WITHSCORES') {
      withScores = true;
      i++;
    } else if (opt === 'LIMIT') {
      hasLimit = true;
      i++;
      const lim = parseLimit(args, i);
      if ('kind' in lim) return lim;
      offset = lim.offset;
      count = lim.count;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  // Validate incompatible options
  if (byScore && byLex) {
    return SYNTAX_ERR;
  }

  if (hasLimit && !byScore && !byLex) {
    return errorReply(
      'ERR',
      'syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX'
    );
  }

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return EMPTY_ARRAY;

  if (byScore) {
    // When REV, min and max are swapped (max first in args)
    const minStr = rev ? (args[2] as string) : (args[1] as string);
    const maxStr = rev ? (args[1] as string) : (args[2] as string);
    const range = parseScoreRange(minStr, maxStr);
    if (!range) return MIN_MAX_ERR;
    return arrayReply(
      collectByScore(zset, range, rev, withScores, offset, count)
    );
  }

  if (byLex) {
    const minStr = rev ? (args[2] as string) : (args[1] as string);
    const maxStr = rev ? (args[1] as string) : (args[2] as string);
    const range = parseLexRange(minStr, maxStr);
    if (!range) return LEX_RANGE_ERR;
    return arrayReply(
      collectByLex(zset, range, rev, withScores, offset, count)
    );
  }

  // Default: by rank
  const len = zset.dict.size;
  let start = Number(args[1]);
  let end = Number(args[2]);

  if (!Number.isInteger(start) || !Number.isInteger(end)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  // Normalize negative indices
  if (start < 0) start = Math.max(len + start, 0);
  if (end < 0) end = len + end;

  // Clamp
  if (start > len - 1) return EMPTY_ARRAY;
  if (end > len - 1) end = len - 1;
  if (start > end) return EMPTY_ARRAY;

  const results: Reply[] = [];

  if (rev) {
    // Reverse: rank 0 is the last element
    const revStart = len - 1 - end;
    const revEnd = len - 1 - start;
    for (let r = revEnd; r >= revStart; r--) {
      const node = zset.sl.getElementByRank(r + 1); // 1-based
      if (!node) break;
      results.push(bulkReply(node.element));
      if (withScores) results.push(bulkReply(formatScore(node.score)));
    }
  } else {
    for (let r = start; r <= end; r++) {
      const node = zset.sl.getElementByRank(r + 1); // 1-based
      if (!node) break;
      results.push(bulkReply(node.element));
      if (withScores) results.push(bulkReply(formatScore(node.score)));
    }
  }

  return arrayReply(results);
}

// --- ZRANGESTORE ---

export function zrangestore(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  if (args.length < 4) {
    return wrongArityError('zrangestore');
  }

  const dst = args[0] as string;
  // ZRANGESTORE does not support WITHSCORES
  const rangeArgs = args.slice(1);
  if (rangeArgs.some((a) => a.toUpperCase() === 'WITHSCORES')) {
    return SYNTAX_ERR;
  }
  const rangeResult = zrange(db, rangeArgs, rng);

  if (rangeResult.kind === 'error') return rangeResult;
  if (rangeResult.kind !== 'array') return ZERO;

  const elements = rangeResult.value as Reply[];
  if (elements.length === 0) {
    // Delete destination if it exists
    db.delete(dst);
    return ZERO;
  }

  // Delete existing destination
  db.delete(dst);

  // Create new sorted set at destination
  // We need to get scores from source, so re-collect with scores
  const rangeArgsWithScores = [...rangeArgs];
  // Check if WITHSCORES is already there
  const hasWithScores = rangeArgs.some((a) => a.toUpperCase() === 'WITHSCORES');
  if (!hasWithScores) {
    // Insert WITHSCORES after min max (index 2) but before options
    rangeArgsWithScores.push('WITHSCORES');
  }

  const withScoresResult = zrange(db, rangeArgsWithScores, rng);
  if (withScoresResult.kind !== 'array') return ZERO;

  const items = withScoresResult.value as Reply[];
  const zset: SortedSetData = {
    sl: new SkipList(rng),
    dict: new Map(),
  };

  for (let j = 0; j < items.length; j += 2) {
    const member = (items[j] as { kind: 'bulk'; value: string }).value;
    const score = parseFloat64(
      (items[j + 1] as { kind: 'bulk'; value: string }).value
    );
    if (score) {
      zset.sl.insert(score.value, member);
      zset.dict.set(member, score.value);
    }
  }

  if (zset.dict.size > 0) {
    db.set(dst, 'zset', 'skiplist', zset);
  }

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
  {
    name: 'zcount',
    handler: (ctx, args) => zcount(ctx.db, args),
    arity: 4,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
  },
  {
    name: 'zlexcount',
    handler: (ctx, args) => zlexcount(ctx.db, args),
    arity: 4,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
  },
  {
    name: 'zrangebyscore',
    handler: (ctx, args) => zrangebyscore(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zrevrangebyscore',
    handler: (ctx, args) => zrevrangebyscore(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zrangebylex',
    handler: (ctx, args) => zrangebylex(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zrevrangebylex',
    handler: (ctx, args) => zrevrangebylex(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zrange',
    handler: (ctx, args) => zrange(ctx.db, args, ctx.engine.rng),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zrangestore',
    handler: (ctx, args) => zrangestore(ctx.db, args, ctx.engine.rng),
    arity: -5,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
];
