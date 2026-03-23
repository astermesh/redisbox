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
  NIL_ARRAY,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
  NOT_INTEGER_ERR,
  NOT_FLOAT_ERR,
  SYNTAX_ERR,
} from '../types.ts';
import { SkipList } from '../skip-list.ts';
import { parseFloat64, parseInteger, formatFloat } from './incr.ts';
import { matchGlob } from '../glob-pattern.ts';
import { partialShuffle, strByteLength } from '../utils.ts';
import type { CommandSpec } from '../command-table.ts';

// Default thresholds — match Redis defaults.
// TODO: read from ConfigStore when config is wired into CommandContext.
const DEFAULT_MAX_LISTPACK_ENTRIES = 128;
const DEFAULT_MAX_LISTPACK_VALUE = 64;

/**
 * Check if a sorted set should use listpack encoding based on current state.
 * Returns true if the set is small enough for listpack.
 */
function fitsListpack(
  dict: Map<string, number>,
  maxEntries: number = DEFAULT_MAX_LISTPACK_ENTRIES,
  maxValue: number = DEFAULT_MAX_LISTPACK_VALUE
): boolean {
  if (dict.size > maxEntries) return false;
  for (const member of dict.keys()) {
    if (strByteLength(member) > maxValue) return false;
  }
  return true;
}

/**
 * Choose initial encoding for a new sorted set based on its contents.
 */
export function chooseEncoding(
  dict: Map<string, number>
): 'listpack' | 'skiplist' {
  return fitsListpack(dict) ? 'listpack' : 'skiplist';
}

/**
 * Promote encoding from listpack to skiplist if needed.
 * Redis only transitions in one direction: listpack → skiplist.
 * Once promoted, it never reverts back — even if the set shrinks.
 */
export function updateEncoding(db: Database, key: string): void {
  const entry = db.get(key);
  if (!entry || entry.type !== 'zset') return;
  if (entry.encoding === 'skiplist') return; // never demote
  const zset = entry.value as SortedSetData;
  if (!fitsListpack(zset.dict)) {
    entry.encoding = 'skiplist';
  }
}

export function formatScore(n: number): string {
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
  db.set(key, 'zset', 'listpack', zset);
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
  updateEncoding(db, key);

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

  updateEncoding(db, key);
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

  // Collect with scores in a single pass before any mutation (handles dst = src)
  const rangeArgsWithScores = [...rangeArgs, 'WITHSCORES'];
  const withScoresResult = zrange(db, rangeArgsWithScores, rng);

  if (withScoresResult.kind === 'error') return withScoresResult;
  if (withScoresResult.kind !== 'array') return ZERO;

  const items = withScoresResult.value as Reply[];
  if (items.length === 0) {
    db.delete(dst);
    return ZERO;
  }

  // Now safe to delete and recreate destination
  db.delete(dst);

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
    db.set(dst, 'zset', chooseEncoding(zset.dict), zset);
  }

  return integerReply(zset.dict.size);
}

// --- ZSCORE ---

export function zscore(db: Database, args: string[]): Reply {
  if (args.length !== 2) {
    return wrongArityError('zscore');
  }

  const key = args[0] as string;
  const member = args[1] as string;

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset) return NIL;

  const score = zset.dict.get(member);
  if (score === undefined) return NIL;

  return bulkReply(formatScore(score));
}

// --- ZMSCORE ---

export function zmscore(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return wrongArityError('zmscore');
  }

  const key = args[0] as string;

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  const results: Reply[] = [];
  for (let i = 1; i < args.length; i++) {
    const member = args[i] as string;
    if (!zset) {
      results.push(NIL);
      continue;
    }
    const score = zset.dict.get(member);
    results.push(score === undefined ? NIL : bulkReply(formatScore(score)));
  }

  return arrayReply(results);
}

// --- ZRANK ---

export function zrank(db: Database, args: string[]): Reply {
  if (args.length < 2 || args.length > 3) {
    return wrongArityError('zrank');
  }

  const key = args[0] as string;
  const member = args[1] as string;

  let withScore = false;
  if (args.length === 3) {
    if ((args[2] as string).toUpperCase() === 'WITHSCORE') {
      withScore = true;
    } else {
      return SYNTAX_ERR;
    }
  }

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  if (!zset) {
    return withScore ? NIL_ARRAY : NIL;
  }

  const score = zset.dict.get(member);
  if (score === undefined) {
    return withScore ? NIL_ARRAY : NIL;
  }

  const rank = zset.sl.getRank(score, member);
  if (rank < 0) {
    return withScore ? NIL_ARRAY : NIL;
  }

  if (withScore) {
    return arrayReply([integerReply(rank), bulkReply(formatScore(score))]);
  }
  return integerReply(rank);
}

// --- ZREVRANK ---

export function zrevrank(db: Database, args: string[]): Reply {
  if (args.length < 2 || args.length > 3) {
    return wrongArityError('zrevrank');
  }

  const key = args[0] as string;
  const member = args[1] as string;

  let withScore = false;
  if (args.length === 3) {
    if ((args[2] as string).toUpperCase() === 'WITHSCORE') {
      withScore = true;
    } else {
      return SYNTAX_ERR;
    }
  }

  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  if (!zset) {
    return withScore ? NIL_ARRAY : NIL;
  }

  const score = zset.dict.get(member);
  if (score === undefined) {
    return withScore ? NIL_ARRAY : NIL;
  }

  const rank = zset.sl.getRank(score, member);
  if (rank < 0) {
    return withScore ? NIL_ARRAY : NIL;
  }

  const revRank = zset.dict.size - 1 - rank;

  if (withScore) {
    return arrayReply([integerReply(revRank), bulkReply(formatScore(score))]);
  }
  return integerReply(revRank);
}

// --- ZPOPMIN ---

export function zpopmin(db: Database, args: string[]): Reply {
  if (args.length > 2) return wrongArityError('zpopmin');

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset || zset.dict.size === 0) return EMPTY_ARRAY;

  let count = 1;
  if (args.length === 2) {
    const parsed = parseInteger(args[1] as string);
    if (parsed === null) return NOT_INTEGER_ERR;
    count = Number(parsed);
    if (count < 0) return NOT_INTEGER_ERR;
  }

  if (count === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  const actual = Math.min(count, zset.dict.size);

  for (let i = 0; i < actual; i++) {
    const node = zset.sl.getElementByRank(1);
    if (!node) break;
    results.push(bulkReply(node.element));
    results.push(bulkReply(formatScore(node.score)));
    zset.sl.delete(node.score, node.element);
    zset.dict.delete(node.element);
  }

  removeIfEmpty(db, key, zset);
  return arrayReply(results);
}

// --- ZPOPMAX ---

export function zpopmax(db: Database, args: string[]): Reply {
  if (args.length > 2) return wrongArityError('zpopmax');

  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset || zset.dict.size === 0) return EMPTY_ARRAY;

  let count = 1;
  if (args.length === 2) {
    const parsed = parseInteger(args[1] as string);
    if (parsed === null) return NOT_INTEGER_ERR;
    count = Number(parsed);
    if (count < 0) return NOT_INTEGER_ERR;
  }

  if (count === 0) return EMPTY_ARRAY;

  const results: Reply[] = [];
  const actual = Math.min(count, zset.dict.size);

  for (let i = 0; i < actual; i++) {
    const node = zset.sl.tail;
    if (!node) break;
    results.push(bulkReply(node.element));
    results.push(bulkReply(formatScore(node.score)));
    zset.sl.delete(node.score, node.element);
    zset.dict.delete(node.element);
  }

  removeIfEmpty(db, key, zset);
  return arrayReply(results);
}

// --- ZMPOP ---

export function zmpop(db: Database, args: string[], _rng: () => number): Reply {
  // ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
  if (args.length < 2) return wrongArityError('zmpop');

  const numkeysStr = args[0] as string;
  const numkeysParsed = parseInteger(numkeysStr);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply('ERR', 'numkeys should be greater than 0');
  }

  if (args.length < numkeys + 2) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  let i = 1;
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[i] as string);
    i++;
  }

  // Parse MIN|MAX
  const direction = (args[i] as string).toUpperCase();
  if (direction !== 'MIN' && direction !== 'MAX') {
    return SYNTAX_ERR;
  }
  i++;

  let count = 1;
  if (i < args.length) {
    const flag = (args[i] as string).toUpperCase();
    if (flag !== 'COUNT') return SYNTAX_ERR;
    i++;
    if (i >= args.length) return SYNTAX_ERR;
    const countParsed = parseInteger(args[i] as string);
    if (countParsed === null) return NOT_INTEGER_ERR;
    count = Number(countParsed);
    if (count < 0) return NOT_INTEGER_ERR;
    if (count === 0) {
      return errorReply('ERR', 'count should be greater than 0');
    }
    i++;
  }

  if (i < args.length) return SYNTAX_ERR;

  // Find first non-empty sorted set
  for (const key of keys) {
    const { zset, error } = getExistingZset(db, key);
    if (error) return error;
    if (!zset || zset.dict.size === 0) continue;

    const actual = Math.min(count, zset.dict.size);
    const elements: Reply[] = [];

    for (let j = 0; j < actual; j++) {
      const node =
        direction === 'MIN' ? zset.sl.getElementByRank(1) : zset.sl.tail;
      if (!node) break;
      elements.push(
        arrayReply([
          bulkReply(node.element),
          bulkReply(formatScore(node.score)),
        ])
      );
      zset.sl.delete(node.score, node.element);
      zset.dict.delete(node.element);
    }

    removeIfEmpty(db, key, zset);
    return arrayReply([bulkReply(key), arrayReply(elements)]);
  }

  return NIL_ARRAY;
}

// --- ZRANDMEMBER ---

export function zrandmember(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  const key = args[0] as string;
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;

  // No count argument — single element or nil
  if (args.length === 1) {
    if (!zset || zset.dict.size === 0) return NIL;
    const members = Array.from(zset.dict.keys());
    const idx = Math.floor(rng() * members.length);
    return bulkReply(members[idx] as string);
  }

  // Parse count
  const countParsed = parseInteger(args[1] as string);
  if (countParsed === null) return NOT_INTEGER_ERR;
  const count = Number(countParsed);

  let withScores = false;
  if (args.length >= 3) {
    if ((args[2] as string).toUpperCase() === 'WITHSCORES') {
      withScores = true;
    } else {
      return SYNTAX_ERR;
    }
  }

  if (args.length > 3) return SYNTAX_ERR;

  if (!zset || zset.dict.size === 0) return EMPTY_ARRAY;
  if (count === 0) return EMPTY_ARRAY;

  const members = Array.from(zset.dict.keys());
  const results: Reply[] = [];

  if (count > 0) {
    // Positive count: unique elements, at most set size
    const actual = Math.min(count, members.length);
    const shuffled = partialShuffle([...members], actual, rng);
    for (let j = 0; j < actual; j++) {
      const member = shuffled[j] as string;
      results.push(bulkReply(member));
      if (withScores) {
        results.push(bulkReply(formatScore(zset.dict.get(member) as number)));
      }
    }
  } else {
    // Negative count: |count| elements, may repeat
    const absCount = Math.abs(count);
    for (let j = 0; j < absCount; j++) {
      const idx = Math.floor(rng() * members.length);
      const member = members[idx] as string;
      results.push(bulkReply(member));
      if (withScores) {
        results.push(bulkReply(formatScore(zset.dict.get(member) as number)));
      }
    }
  }

  return arrayReply(results);
}

// --- Aggregate helpers for ZUNION/ZINTER/ZDIFF ---

type AggregateFunc = 'SUM' | 'MIN' | 'MAX';

function collectZsets(
  db: Database,
  keys: string[]
): { zsets: (SortedSetData | null)[]; error: Reply | null } {
  const zsets: (SortedSetData | null)[] = [];
  for (const key of keys) {
    const { zset, error } = getExistingZset(db, key);
    if (error) return { zsets: [], error };
    zsets.push(zset);
  }
  return { zsets, error: null };
}

function aggregateScore(a: number, b: number, func: AggregateFunc): number {
  switch (func) {
    case 'SUM':
      return a + b;
    case 'MIN':
      return Math.min(a, b);
    case 'MAX':
      return Math.max(a, b);
  }
}

function parseWeightsAndAggregate(
  args: string[],
  startIdx: number,
  numkeys: number,
  allowWithScores: boolean
): {
  weights: number[];
  aggregate: AggregateFunc;
  error: Reply | null;
} {
  const weights = new Array<number>(numkeys).fill(1);
  let aggregate: AggregateFunc = 'SUM';
  let i = startIdx;

  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'WEIGHTS') {
      i++;
      for (let k = 0; k < numkeys; k++) {
        if (i >= args.length) {
          return {
            weights,
            aggregate,
            error: SYNTAX_ERR,
          };
        }
        const parsed = parseFloat64(args[i] as string);
        if (!parsed) {
          return { weights, aggregate, error: NOT_FLOAT_ERR };
        }
        weights[k] = parsed.value;
        i++;
      }
    } else if (opt === 'AGGREGATE') {
      i++;
      if (i >= args.length) {
        return { weights, aggregate, error: SYNTAX_ERR };
      }
      const agg = (args[i] as string).toUpperCase();
      if (agg !== 'SUM' && agg !== 'MIN' && agg !== 'MAX') {
        return { weights, aggregate, error: SYNTAX_ERR };
      }
      aggregate = agg;
      i++;
    } else if (opt === 'WITHSCORES' && allowWithScores) {
      // handled by caller, skip
      i++;
    } else {
      return { weights, aggregate, error: SYNTAX_ERR };
    }
  }

  return { weights, aggregate, error: null };
}

function computeZunion(
  zsets: (SortedSetData | null)[],
  weights: number[],
  aggregate: AggregateFunc
): Map<string, number> {
  const result = new Map<string, number>();

  for (let k = 0; k < zsets.length; k++) {
    const zs = zsets[k];
    if (!zs) continue;
    const w = weights[k] as number;
    for (const [member, score] of zs.dict) {
      const weighted = score * w;
      const existing = result.get(member);
      if (existing !== undefined) {
        result.set(member, aggregateScore(existing, weighted, aggregate));
      } else {
        result.set(member, weighted);
      }
    }
  }

  return result;
}

function computeZinter(
  zsets: (SortedSetData | null)[],
  weights: number[],
  aggregate: AggregateFunc
): Map<string, number> {
  const result = new Map<string, number>();

  // Find the smallest non-null set
  let smallestIdx = -1;
  let smallestSize = Infinity;
  for (let k = 0; k < zsets.length; k++) {
    const zs = zsets[k];
    if (!zs) return result; // empty intersection
    if (zs.dict.size < smallestSize) {
      smallestSize = zs.dict.size;
      smallestIdx = k;
    }
  }

  if (smallestIdx < 0) return result;

  const smallest = zsets[smallestIdx] as SortedSetData;
  for (const [member, score] of smallest.dict) {
    let combined = score * (weights[smallestIdx] as number);
    let inAll = true;

    for (let k = 0; k < zsets.length; k++) {
      if (k === smallestIdx) continue;
      const zs = zsets[k] as SortedSetData;
      const otherScore = zs.dict.get(member);
      if (otherScore === undefined) {
        inAll = false;
        break;
      }
      combined = aggregateScore(
        combined,
        otherScore * (weights[k] as number),
        aggregate
      );
    }

    if (inAll) {
      result.set(member, combined);
    }
  }

  return result;
}

function computeZdiff(zsets: (SortedSetData | null)[]): Map<string, number> {
  const result = new Map<string, number>();

  const first = zsets[0];
  if (!first) return result;

  for (const [member, score] of first.dict) {
    let inOther = false;
    for (let k = 1; k < zsets.length; k++) {
      const zs = zsets[k];
      if (zs && zs.dict.has(member)) {
        inOther = true;
        break;
      }
    }
    if (!inOther) {
      result.set(member, score);
    }
  }

  return result;
}

function resultMapToSortedReply(
  resultMap: Map<string, number>,
  withScores: boolean
): Reply {
  if (resultMap.size === 0) return EMPTY_ARRAY;

  // Sort by score, then by member lexicographically
  const entries = Array.from(resultMap.entries());
  entries.sort((a, b) => {
    if (a[1] !== b[1]) return a[1] - b[1];
    return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
  });

  const results: Reply[] = [];
  for (const [member, score] of entries) {
    results.push(bulkReply(member));
    if (withScores) {
      results.push(bulkReply(formatScore(score)));
    }
  }

  return arrayReply(results);
}

// --- ZUNION ---

export function zunion(
  db: Database,
  args: string[],
  _rng: () => number
): Reply {
  // ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
  const numkeysParsed = parseInteger(args[0] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply(
      'ERR',
      "at least 1 input key is needed for 'zunion' command"
    );
  }

  if (args.length < numkeys + 1) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[k + 1] as string);
  }

  let withScores = false;
  // Check for WITHSCORES before parsing weights/aggregate
  for (let j = numkeys + 1; j < args.length; j++) {
    if ((args[j] as string).toUpperCase() === 'WITHSCORES') {
      withScores = true;
    }
  }

  const {
    weights,
    aggregate,
    error: parseErr,
  } = parseWeightsAndAggregate(args, numkeys + 1, numkeys, true);
  if (parseErr) return parseErr;

  const { zsets, error } = collectZsets(db, keys);
  if (error) return error;

  const resultMap = computeZunion(zsets, weights, aggregate);
  return resultMapToSortedReply(resultMap, withScores);
}

// --- ZINTER ---

export function zinter(
  db: Database,
  args: string[],
  _rng: () => number
): Reply {
  const numkeysParsed = parseInteger(args[0] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply(
      'ERR',
      "at least 1 input key is needed for 'zinter' command"
    );
  }

  if (args.length < numkeys + 1) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[k + 1] as string);
  }

  let withScores = false;
  for (let j = numkeys + 1; j < args.length; j++) {
    if ((args[j] as string).toUpperCase() === 'WITHSCORES') {
      withScores = true;
    }
  }

  const {
    weights,
    aggregate,
    error: parseErr,
  } = parseWeightsAndAggregate(args, numkeys + 1, numkeys, true);
  if (parseErr) return parseErr;

  const { zsets, error } = collectZsets(db, keys);
  if (error) return error;

  const resultMap = computeZinter(zsets, weights, aggregate);
  return resultMapToSortedReply(resultMap, withScores);
}

// --- ZDIFF ---

export function zdiff(db: Database, args: string[], _rng: () => number): Reply {
  const numkeysParsed = parseInteger(args[0] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply(
      'ERR',
      "at least 1 input key is needed for 'zdiff' command"
    );
  }

  if (args.length < numkeys + 1) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[k + 1] as string);
  }

  let withScores = false;
  for (let j = numkeys + 1; j < args.length; j++) {
    if ((args[j] as string).toUpperCase() === 'WITHSCORES') {
      withScores = true;
    }
  }

  // ZDIFF doesn't support WEIGHTS or AGGREGATE, only WITHSCORES
  let i = numkeys + 1;
  while (i < args.length) {
    const opt = (args[i] as string).toUpperCase();
    if (opt === 'WITHSCORES') {
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const { zsets, error } = collectZsets(db, keys);
  if (error) return error;

  const resultMap = computeZdiff(zsets);
  return resultMapToSortedReply(resultMap, withScores);
}

// --- Store helper for sorted set operations ---

function storeZsetResult(
  db: Database,
  destination: string,
  resultMap: Map<string, number>,
  rng: () => number
): Reply {
  // Delete existing destination
  db.delete(destination);

  if (resultMap.size === 0) {
    return ZERO;
  }

  const zset: SortedSetData = {
    sl: new SkipList(rng),
    dict: new Map(),
  };

  for (const [member, score] of resultMap) {
    zset.sl.insert(score, member);
    zset.dict.set(member, score);
  }

  db.set(destination, 'zset', chooseEncoding(zset.dict), zset);
  return integerReply(zset.dict.size);
}

// --- ZUNIONSTORE ---

export function zunionstore(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  // ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
  const destination = args[0] as string;
  const numkeysParsed = parseInteger(args[1] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply(
      'ERR',
      "at least 1 input key is needed for 'zunionstore' command"
    );
  }

  if (args.length < numkeys + 2) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[k + 2] as string);
  }

  const {
    weights,
    aggregate,
    error: parseErr,
  } = parseWeightsAndAggregate(args, numkeys + 2, numkeys, false);
  if (parseErr) return parseErr;

  // Read all source sets before mutation (destination may be a source)
  const { zsets, error } = collectZsets(db, keys);
  if (error) return error;

  const resultMap = computeZunion(zsets, weights, aggregate);
  return storeZsetResult(db, destination, resultMap, rng);
}

// --- ZINTERSTORE ---

export function zinterstore(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  const destination = args[0] as string;
  const numkeysParsed = parseInteger(args[1] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply(
      'ERR',
      "at least 1 input key is needed for 'zinterstore' command"
    );
  }

  if (args.length < numkeys + 2) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[k + 2] as string);
  }

  const {
    weights,
    aggregate,
    error: parseErr,
  } = parseWeightsAndAggregate(args, numkeys + 2, numkeys, false);
  if (parseErr) return parseErr;

  const { zsets, error } = collectZsets(db, keys);
  if (error) return error;

  const resultMap = computeZinter(zsets, weights, aggregate);
  return storeZsetResult(db, destination, resultMap, rng);
}

// --- ZDIFFSTORE ---

export function zdiffstore(
  db: Database,
  args: string[],
  rng: () => number
): Reply {
  const destination = args[0] as string;
  const numkeysParsed = parseInteger(args[1] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply(
      'ERR',
      "at least 1 input key is needed for 'zdiffstore' command"
    );
  }

  if (args.length < numkeys + 2) {
    return SYNTAX_ERR;
  }

  const keys: string[] = [];
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[k + 2] as string);
  }

  // ZDIFFSTORE doesn't support WEIGHTS or AGGREGATE
  if (args.length > numkeys + 2) {
    return SYNTAX_ERR;
  }

  const { zsets, error } = collectZsets(db, keys);
  if (error) return error;

  const resultMap = computeZdiff(zsets);
  return storeZsetResult(db, destination, resultMap, rng);
}

// --- ZINTERCARD ---

export function zintercard(db: Database, args: string[]): Reply {
  // ZINTERCARD numkeys key [key ...] [LIMIT limit]
  const numkeysParsed = parseInteger(args[0] as string);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply('ERR', 'numkeys should be greater than 0');
  }

  const remaining = args.length - 1;
  if (numkeys > remaining) {
    return errorReply(
      'ERR',
      "Number of keys can't be greater than number of args"
    );
  }

  const keys: string[] = [];
  let i = 1;
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[i] as string);
    i++;
  }

  let limit = 0;
  if (i < args.length) {
    const flag = (args[i] as string).toUpperCase();
    if (flag !== 'LIMIT') return SYNTAX_ERR;
    i++;
    if (i >= args.length) return SYNTAX_ERR;
    const limitParsed = parseInteger(args[i] as string);
    if (limitParsed === null) return NOT_INTEGER_ERR;
    limit = Number(limitParsed);
    if (limit < 0) {
      return errorReply('ERR', "LIMIT can't be negative");
    }
    i++;
  }

  if (i < args.length) return SYNTAX_ERR;

  // Collect zsets
  const zsets: (SortedSetData | null)[] = [];
  for (const key of keys) {
    const { zset, error } = getExistingZset(db, key);
    if (error) return error;
    zsets.push(zset);
  }

  // If any key doesn't exist, intersection is empty
  for (const zs of zsets) {
    if (!zs) return ZERO;
  }

  const nonNull = zsets as SortedSetData[];

  // Find smallest
  let smallestIdx = 0;
  for (let k = 1; k < nonNull.length; k++) {
    if (
      (nonNull[k] as SortedSetData).dict.size <
      (nonNull[smallestIdx] as SortedSetData).dict.size
    ) {
      smallestIdx = k;
    }
  }

  const smallest = nonNull[smallestIdx] as SortedSetData;
  let count = 0;

  for (const [member] of smallest.dict) {
    let inAll = true;
    for (let k = 0; k < nonNull.length; k++) {
      if (k === smallestIdx) continue;
      if (!(nonNull[k] as SortedSetData).dict.has(member)) {
        inAll = false;
        break;
      }
    }
    if (inAll) {
      count++;
      if (limit > 0 && count >= limit) break;
    }
  }

  return integerReply(count);
}

// --- ZSCAN ---

export function zscan(db: Database, args: string[]): Reply {
  const key = args[0] as string;
  const cursorStr = args[1] as string;

  const cursor = parseInt(cursorStr, 10);
  if (isNaN(cursor) || cursor < 0) {
    return errorReply('ERR', 'invalid cursor');
  }

  // Check key type before parsing options
  const entry = db.get(key);
  if (entry && entry.type !== 'zset') return WRONGTYPE_ERR;

  let matchPattern: string | null = null;
  let count = 10;

  let i = 2;
  while (i < args.length) {
    const flag = (args[i] as string).toUpperCase();
    if (flag === 'MATCH') {
      i++;
      matchPattern = (args[i] as string) ?? '*';
    } else if (flag === 'COUNT') {
      i++;
      count = parseInt((args[i] as string) ?? '10', 10);
      if (isNaN(count)) {
        return NOT_INTEGER_ERR;
      }
      if (count < 1) {
        return SYNTAX_ERR;
      }
    } else {
      return SYNTAX_ERR;
    }
    i++;
  }

  if (!entry) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const zset = entry.value as SortedSetData;
  const allMembers = Array.from(zset.dict.keys());

  if (allMembers.length === 0) {
    return arrayReply([bulkReply('0'), EMPTY_ARRAY]);
  }

  const results: Reply[] = [];
  let position = cursor;
  let scanned = 0;

  while (position < allMembers.length && scanned < count) {
    const member = allMembers[position] as string;
    position++;
    scanned++;

    if (matchPattern && !matchGlob(matchPattern, member)) continue;

    results.push(bulkReply(member));
    results.push(bulkReply(formatScore(zset.dict.get(member) as number)));
  }

  const nextCursor = position >= allMembers.length ? 0 : position;

  return arrayReply([bulkReply(String(nextCursor)), arrayReply(results)]);
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
    name: 'zscore',
    handler: (ctx, args) => zscore(ctx.db, args),
    arity: 3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
  },
  {
    name: 'zmscore',
    handler: (ctx, args) => zmscore(ctx.db, args),
    arity: -3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
  },
  {
    name: 'zrank',
    handler: (ctx, args) => zrank(ctx.db, args),
    arity: -3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
  },
  {
    name: 'zrevrank',
    handler: (ctx, args) => zrevrank(ctx.db, args),
    arity: -3,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset', '@fast'],
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
  {
    name: 'zpopmin',
    handler: (ctx, args) => zpopmin(ctx.db, args),
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zpopmax',
    handler: (ctx, args) => zpopmax(ctx.db, args),
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zmpop',
    handler: (ctx, args) => zmpop(ctx.db, args, ctx.engine.rng),
    arity: -4,
    flags: ['write', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@write', '@sortedset', '@slow'],
  },
  {
    name: 'zrandmember',
    handler: (ctx, args) => zrandmember(ctx.db, args, ctx.engine.rng),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zunion',
    handler: (ctx, args) => zunion(ctx.db, args, ctx.engine.rng),
    arity: -3,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zinter',
    handler: (ctx, args) => zinter(ctx.db, args, ctx.engine.rng),
    arity: -3,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zdiff',
    handler: (ctx, args) => zdiff(ctx.db, args, ctx.engine.rng),
    arity: -3,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zunionstore',
    handler: (ctx, args) => zunionstore(ctx.db, args, ctx.engine.rng),
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
  {
    name: 'zinterstore',
    handler: (ctx, args) => zinterstore(ctx.db, args, ctx.engine.rng),
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
  {
    name: 'zdiffstore',
    handler: (ctx, args) => zdiffstore(ctx.db, args, ctx.engine.rng),
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
  {
    name: 'zintercard',
    handler: (ctx, args) => zintercard(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@sortedset'],
  },
  {
    name: 'zscan',
    handler: (ctx, args) => zscan(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@sortedset'],
  },
];
