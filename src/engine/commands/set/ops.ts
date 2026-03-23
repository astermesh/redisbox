import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  integerReply,
  bulkReply,
  arrayReply,
  errorReply,
  ZERO,
  EMPTY_ARRAY,
  SYNTAX_ERR,
  NOT_INTEGER_ERR,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import type { ConfigStore } from '../../../config-store.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';
import { parseInteger } from '../incr.ts';
import {
  collectSets,
  findSmallest,
  computeIntersection,
  computeDifference,
  storeSetResult,
} from './utils.ts';

// --- SUNION ---

export function sunion(db: Database, args: string[]): Reply {
  const { sets, error } = collectSets(db, args);
  if (error) return error;

  const result = new Set<string>();
  for (const s of sets) {
    if (s) {
      for (const member of s) {
        result.add(member);
      }
    }
  }

  if (result.size === 0) return EMPTY_ARRAY;
  const replies: Reply[] = [];
  for (const member of result) {
    replies.push(bulkReply(member));
  }
  return arrayReply(replies);
}

// --- SINTER ---

export function sinter(db: Database, args: string[]): Reply {
  const { sets, error } = collectSets(db, args);
  if (error) return error;

  const result = computeIntersection(sets);
  if (!result || result.size === 0) return EMPTY_ARRAY;

  const replies: Reply[] = [];
  for (const member of result) {
    replies.push(bulkReply(member));
  }
  return arrayReply(replies);
}

// --- SDIFF ---

export function sdiff(db: Database, args: string[]): Reply {
  const { sets, error } = collectSets(db, args);
  if (error) return error;

  const result = computeDifference(sets);
  if (result.size === 0) return EMPTY_ARRAY;

  const replies: Reply[] = [];
  for (const member of result) {
    replies.push(bulkReply(member));
  }
  return arrayReply(replies);
}

// --- SUNIONSTORE ---

export function sunionstore(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const destination = args[0] ?? '';
  const keys = args.slice(1);

  // Read all source sets first (destination may be one of them)
  const { sets, error } = collectSets(db, keys);
  if (error) return error;

  const result = new Set<string>();
  for (const s of sets) {
    if (s) {
      for (const member of s) {
        result.add(member);
      }
    }
  }

  return storeSetResult(db, destination, result, config);
}

// --- SINTERSTORE ---

export function sinterstore(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const destination = args[0] ?? '';
  const keys = args.slice(1);

  const { sets, error } = collectSets(db, keys);
  if (error) return error;

  const result = computeIntersection(sets) ?? new Set<string>();
  return storeSetResult(db, destination, result, config);
}

// --- SDIFFSTORE ---

export function sdiffstore(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const destination = args[0] ?? '';
  const keys = args.slice(1);

  const { sets, error } = collectSets(db, keys);
  if (error) return error;

  const result = computeDifference(sets);
  return storeSetResult(db, destination, result, config);
}

// --- SINTERCARD ---

export function sintercard(db: Database, args: string[]): Reply {
  // SINTERCARD numkeys key [key ...] [LIMIT limit]
  const numkeysStr = args[0] ?? '';
  const numkeysParsed = parseInteger(numkeysStr);
  if (numkeysParsed === null) return NOT_INTEGER_ERR;
  const numkeys = Number(numkeysParsed);

  if (numkeys <= 0) {
    return errorReply('ERR', 'numkeys should be greater than 0');
  }

  // Check that enough args remain for numkeys keys
  const remaining = args.length - 1;
  if (numkeys > remaining) {
    return errorReply(
      'ERR',
      "Number of keys can't be greater than number of args"
    );
  }

  // Parse keys
  const keys: string[] = [];
  let i = 1;
  for (let k = 0; k < numkeys; k++) {
    keys.push(args[i] ?? '');
    i++;
  }

  // Parse optional LIMIT
  let limit = 0;
  if (i < args.length) {
    const flag = (args[i] ?? '').toUpperCase();
    if (flag !== 'LIMIT') {
      return SYNTAX_ERR;
    }
    i++;
    if (i >= args.length) return SYNTAX_ERR;
    const limitParsed = parseInteger(args[i] ?? '');
    if (limitParsed === null) return NOT_INTEGER_ERR;
    limit = Number(limitParsed);
    if (limit < 0) {
      return errorReply('ERR', "LIMIT can't be negative");
    }
    i++;
  }

  // Extra trailing args
  if (i < args.length) {
    return SYNTAX_ERR;
  }

  const { sets, error } = collectSets(db, keys);
  if (error) return error;

  // If any key doesn't exist, intersection is empty
  for (const s of sets) {
    if (!s) return ZERO;
  }

  const nonNull = sets as Set<string>[];
  const smallest = findSmallest(nonNull);

  let count = 0;
  for (const member of smallest) {
    let inAll = true;
    for (const s of nonNull) {
      if (s !== smallest && !s.has(member)) {
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
    name: 'sunion',
    handler: (ctx, args) => sunion(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@read', '@set', '@slow'],
  },
  {
    name: 'sinter',
    handler: (ctx, args) => sinter(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@read', '@set', '@slow'],
  },
  {
    name: 'sdiff',
    handler: (ctx, args) => sdiff(ctx.db, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@read', '@set', '@slow'],
  },
  {
    name: 'sunionstore',
    handler: (ctx, args) => {
      const reply = sunionstore(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SET, 'sunionstore', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@write', '@set', '@slow'],
  },
  {
    name: 'sinterstore',
    handler: (ctx, args) => {
      const reply = sinterstore(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SET, 'sinterstore', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@write', '@set', '@slow'],
  },
  {
    name: 'sdiffstore',
    handler: (ctx, args) => {
      const reply = sdiffstore(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.SET, 'sdiffstore', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@write', '@set', '@slow'],
  },
  {
    name: 'sintercard',
    handler: (ctx, args) => sintercard(ctx.db, args),
    arity: -3,
    flags: ['readonly', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@set', '@slow'],
  },
];
