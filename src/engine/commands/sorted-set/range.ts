import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  wrongArityError,
  ZERO,
  EMPTY_ARRAY,
  SYNTAX_ERR,
} from '../../types.ts';
import { SkipList } from '../../skip-list.ts';
import { parseFloat64 } from '../incr.ts';
import type { CommandSpec } from '../../command-table.ts';
import { notify, EVENT_FLAGS } from '../../notify.ts';
import type { SortedSetData } from './types.ts';
import { formatScore, getExistingZset, chooseEncoding } from './types.ts';

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

function lexGteMin(element: string, bound: LexBound): boolean {
  if (bound.isNegInf) return true;
  if (bound.isPosInf) return false;
  return bound.exclusive ? element > bound.value : element >= bound.value;
}

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

  if (start < 0) start = Math.max(len + start, 0);
  if (end < 0) end = len + end;

  if (start > len - 1) return EMPTY_ARRAY;
  if (end > len - 1) end = len - 1;
  if (start > end) return EMPTY_ARRAY;

  const results: Reply[] = [];

  if (rev) {
    const revStart = len - 1 - end;
    const revEnd = len - 1 - start;
    for (let r = revEnd; r >= revStart; r--) {
      const node = zset.sl.getElementByRank(r + 1);
      if (!node) break;
      results.push(bulkReply(node.element));
      if (withScores) results.push(bulkReply(formatScore(node.score)));
    }
  } else {
    for (let r = start; r <= end; r++) {
      const node = zset.sl.getElementByRank(r + 1);
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

export const specs: CommandSpec[] = [
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
    handler: (ctx, args) => {
      const reply = zrangestore(ctx.db, args, ctx.engine.rng);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zrangestore', args[0] ?? '');
      }
      return reply;
    },
    arity: -5,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
];
