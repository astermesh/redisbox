import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  ZERO,
  EMPTY_ARRAY,
  NOT_INTEGER_ERR,
  NOT_FLOAT_ERR,
  SYNTAX_ERR,
} from '../../types.ts';
import { SkipList } from '../../skip-list.ts';
import { parseFloat64, parseInteger } from '../incr.ts';
import type { CommandSpec } from '../../command-table.ts';
import type { ConfigStore } from '../../../config-store.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import type { SortedSetData } from './types.ts';
import { formatScore, getExistingZset, chooseEncoding } from './types.ts';

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

function storeZsetResult(
  db: Database,
  destination: string,
  resultMap: Map<string, number>,
  rng: () => number,
  config?: ConfigStore
): Reply {
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

  db.set(destination, 'zset', chooseEncoding(zset.dict, config), zset);
  return integerReply(zset.dict.size);
}

// --- ZUNION ---

export function zunion(
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

// --- ZUNIONSTORE ---

export function zunionstore(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
): Reply {
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
  return storeZsetResult(db, destination, resultMap, rng, config);
}

// --- ZINTERSTORE ---

export function zinterstore(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
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
  return storeZsetResult(db, destination, resultMap, rng, config);
}

// --- ZDIFFSTORE ---

export function zdiffstore(
  db: Database,
  args: string[],
  rng: () => number,
  config?: ConfigStore
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
  return storeZsetResult(db, destination, resultMap, rng, config);
}

// --- ZINTERCARD ---

export function zintercard(db: Database, args: string[]): Reply {
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

  const zsets: (SortedSetData | null)[] = [];
  for (const key of keys) {
    const { zset, error } = getExistingZset(db, key);
    if (error) return error;
    zsets.push(zset);
  }

  for (const zs of zsets) {
    if (!zs) return ZERO;
  }

  const nonNull = zsets as SortedSetData[];

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

export const specs: CommandSpec[] = [
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
    handler: (ctx, args) => {
      const reply = zunionstore(ctx.db, args, ctx.engine.rng, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zunionstore', args[0] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
  {
    name: 'zinterstore',
    handler: (ctx, args) => {
      const reply = zinterstore(ctx.db, args, ctx.engine.rng, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zinterstore', args[0] ?? '');
      }
      return reply;
    },
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@sortedset'],
  },
  {
    name: 'zdiffstore',
    handler: (ctx, args) => {
      const reply = zdiffstore(ctx.db, args, ctx.engine.rng, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SORTEDSET, 'zdiffstore', args[0] ?? '');
      }
      return reply;
    },
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
];
