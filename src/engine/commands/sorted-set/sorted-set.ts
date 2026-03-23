import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
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
} from '../../types.ts';
import { parseFloat64, parseInteger } from '../incr.ts';
import { matchGlob } from '../../glob-pattern.ts';
import { partialShuffle } from '../../utils.ts';
import type { CommandSpec } from '../../command-table.ts';
import type { ConfigStore } from '../../../config-store.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import { parseScanCursor, parseScanOptions } from '../scan-utils.ts';
import type { SortedSetData } from './types.ts';
import {
  formatScore,
  getOrCreateZset,
  getExistingZset,
  updateEncoding,
} from './types.ts';
import { specs as rangeSpecs } from './range.ts';
import { specs as opsSpecs } from './ops.ts';

function removeIfEmpty(db: Database, key: string, zset: SortedSetData): void {
  if (zset.dict.size === 0) {
    db.delete(key);
  }
}

// --- ZADD ---

export function zadd(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
): Reply {
  if (args.length < 3) {
    return wrongArityError('zadd');
  }

  const key = args[0] as string;

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

  if (nx && xx) {
    return errorReply(
      'ERR',
      'XX and NX options at the same time are not compatible'
    );
  }

  if ((nx && (gt || lt)) || (gt && lt)) {
    return errorReply(
      'ERR',
      'GT, LT, and/or NX options at the same time are not compatible'
    );
  }

  const remaining = args.length - i;
  if (remaining < 2 || remaining % 2 !== 0) {
    return errorReply('ERR', 'syntax error');
  }

  if (incr && remaining !== 2) {
    return errorReply(
      'ERR',
      'INCR option supports a single increment-element pair'
    );
  }

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
      if (nx) {
        if (incr) incrResult = null;
        continue;
      }

      if (incr) {
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
          zset.sl.delete(existing, member);
          zset.sl.insert(score, member);
          zset.dict.set(member, score);
          updated++;
        }
      }
    } else {
      if (xx) {
        if (incr) incrResult = null;
        continue;
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

  removeIfEmpty(db, key, zset);
  updateEncoding(db, key, config);

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
  rng: () => number,
  config?: ConfigStore
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

  updateEncoding(db, key, config);
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

  if (args.length === 1) {
    if (!zset || zset.dict.size === 0) return NIL;
    const members = Array.from(zset.dict.keys());
    const idx = Math.floor(rng() * members.length);
    return bulkReply(members[idx] as string);
  }

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

// --- ZSCAN ---

export function zscan(db: Database, args: string[]): Reply {
  const key = args[0] as string;
  const { cursor, error: cursorErr } = parseScanCursor(args[1] as string);
  if (cursorErr) return cursorErr;

  const entry = db.get(key);
  if (entry && entry.type !== 'zset') return WRONGTYPE_ERR;

  const { options, error: optErr } = parseScanOptions(args, 2);
  if (optErr) return optErr;

  const { matchPattern, count } = options;

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
  ...rangeSpecs,
  ...opsSpecs,
  {
    name: 'zadd',
    handler: (ctx, args) => {
      const reply = zadd(ctx.db, args, ctx.engine.rng, ctx.config);
      if (
        reply.kind === 'integer' ||
        (reply.kind === 'bulk' && reply.value !== null)
      ) {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zadd', args[0] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zrem',
    handler: (ctx, args) => {
      const reply = zrem(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zrem', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zincrby',
    handler: (ctx, args) => {
      const reply = zincrby(ctx.db, args, ctx.engine.rng, ctx.config);
      if (reply.kind === 'bulk' && reply.value !== null) {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zincrby', args[0] ?? '');
      }
      return reply;
    },
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
    name: 'zpopmin',
    handler: (ctx, args) => {
      const reply = zpopmin(ctx.db, args);
      if (reply !== EMPTY_ARRAY && reply.kind === 'array') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zpopmin', args[0] ?? '');
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zpopmax',
    handler: (ctx, args) => {
      const reply = zpopmax(ctx.db, args);
      if (reply !== EMPTY_ARRAY && reply.kind === 'array') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zpopmax', args[0] ?? '');
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@fast'],
  },
  {
    name: 'zmpop',
    handler: (ctx, args) => {
      const reply = zmpop(ctx.db, args, ctx.engine.rng);
      if (reply.kind === 'array' && reply !== NIL_ARRAY) {
        const parts = reply.value as Reply[];
        if (parts[0] && parts[0].kind === 'bulk') {
          const key = parts[0].value as string;
          const numkeys = parseInt(args[0] ?? '0', 10);
          const dirArg = (args[1 + numkeys] ?? '').toUpperCase();
          notify(
            ctx,
            EVENT_FLAGS.SORTEDSET,
            dirArg === 'MIN' ? 'zpopmin' : 'zpopmax',
            key
          );
        }
      }
      return reply;
    },
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
