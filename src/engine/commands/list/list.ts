import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  NIL,
  OK,
  ZERO,
  EMPTY_ARRAY,
  SYNTAX_ERR,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import type { ConfigStore } from '../../../config-store.ts';
import { notify, EVENT_FLAGS } from '../../pubsub/notify.ts';

import {
  getOrCreateList,
  getExistingList,
  updateEncoding,
  deleteIfEmpty,
  parseCount,
  parseInteger,
  resolveIndex,
} from './utils.ts';
import { lpos, lmove, lmpop, rpoplpush } from './operations.ts';
import { specs as operationSpecs } from './operations.ts';

// --- LPUSH ---

export function lpush(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const { list, error } = getOrCreateList(db, key);
  if (error) return error;

  for (let i = 1; i < args.length; i++) {
    list.unshift(args[i] ?? '');
  }

  updateEncoding(db, key, config);
  return integerReply(list.length);
}

// --- RPUSH ---

export function rpush(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const { list, error } = getOrCreateList(db, key);
  if (error) return error;

  for (let i = 1; i < args.length; i++) {
    list.push(args[i] ?? '');
  }

  updateEncoding(db, key, config);
  return integerReply(list.length);
}

// --- LPUSHX ---

export function lpushx(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  for (let i = 1; i < args.length; i++) {
    list.unshift(args[i] ?? '');
  }

  updateEncoding(db, key, config);
  return integerReply(list.length);
}

// --- RPUSHX ---

export function rpushx(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  for (let i = 1; i < args.length; i++) {
    list.push(args[i] ?? '');
  }

  updateEncoding(db, key, config);
  return integerReply(list.length);
}

// --- LPOP ---

export function lpop(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { count, error: countErr } = parseCount(args[1]);
  if (countErr) return countErr;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NIL;

  // No count argument — single pop, return bulk
  if (count === null) {
    const value = list.shift() ?? '';
    deleteIfEmpty(db, key, list);
    return bulkReply(value);
  }

  // Count = 0 — return empty array
  if (count === 0) return EMPTY_ARRAY;

  // Pop up to count elements
  const popped = list.splice(0, count);
  deleteIfEmpty(db, key, list);
  return arrayReply(popped.map((v) => bulkReply(v)));
}

// --- RPOP ---

export function rpop(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const { count, error: countErr } = parseCount(args[1]);
  if (countErr) return countErr;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NIL;

  // No count argument — single pop, return bulk
  if (count === null) {
    const value = list.pop() ?? '';
    deleteIfEmpty(db, key, list);
    return bulkReply(value);
  }

  // Count = 0 — return empty array
  if (count === 0) return EMPTY_ARRAY;

  // Pop up to count elements from the tail
  const actualCount = Math.min(count, list.length);
  const popped = list.splice(list.length - actualCount, actualCount);
  popped.reverse();
  deleteIfEmpty(db, key, list);
  return arrayReply(popped.map((v) => bulkReply(v)));
}

// --- LLEN ---

export function llen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  return integerReply(list.length);
}

// --- LRANGE ---

export function lrange(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const startParsed = parseInteger(args[1] ?? '');
  if (startParsed.error) return startParsed.error;
  const stopParsed = parseInteger(args[2] ?? '');
  if (stopParsed.error) return stopParsed.error;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return EMPTY_ARRAY;

  let start = resolveIndex(startParsed.value, list.length);
  let stop = resolveIndex(stopParsed.value, list.length);

  if (start < 0) start = 0;
  if (stop >= list.length) stop = list.length - 1;

  if (start > stop) return EMPTY_ARRAY;

  const result: Reply[] = [];
  for (let i = start; i <= stop; i++) {
    result.push(bulkReply(list[i] ?? ''));
  }
  return arrayReply(result);
}

// --- LINDEX ---

export function lindex(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const indexParsed = parseInteger(args[1] ?? '');
  if (indexParsed.error) return indexParsed.error;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return NIL;

  const idx = resolveIndex(indexParsed.value, list.length);
  if (idx < 0 || idx >= list.length) return NIL;

  return bulkReply(list[idx] ?? '');
}

// --- LSET ---

export function lset(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const indexParsed = parseInteger(args[1] ?? '');
  if (indexParsed.error) return indexParsed.error;
  const value = args[2] ?? '';

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return errorReply('ERR', 'no such key');

  const idx = resolveIndex(indexParsed.value, list.length);
  if (idx < 0 || idx >= list.length) {
    return errorReply('ERR', 'index out of range');
  }

  list[idx] = value;
  updateEncoding(db, key, config);
  return OK;
}

// --- LINSERT ---

export function linsert(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const key = args[0] ?? '';
  const direction = (args[1] ?? '').toUpperCase();
  const pivot = args[2] ?? '';
  const value = args[3] ?? '';

  if (direction !== 'BEFORE' && direction !== 'AFTER') {
    return SYNTAX_ERR;
  }

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  const pivotIndex = list.indexOf(pivot);
  if (pivotIndex === -1) return integerReply(-1);

  const insertIndex = direction === 'BEFORE' ? pivotIndex : pivotIndex + 1;
  list.splice(insertIndex, 0, value);
  updateEncoding(db, key, config);
  return integerReply(list.length);
}

// --- LREM ---

export function lrem(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const countParsed = parseInteger(args[1] ?? '');
  if (countParsed.error) return countParsed.error;
  const element = args[2] ?? '';
  const count = countParsed.value;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return ZERO;

  let removed = 0;

  if (count > 0) {
    for (let i = 0; i < list.length && removed < count; ) {
      if (list[i] === element) {
        list.splice(i, 1);
        removed++;
      } else {
        i++;
      }
    }
  } else if (count < 0) {
    const toRemove = -count;
    for (let i = list.length - 1; i >= 0 && removed < toRemove; i--) {
      if (list[i] === element) {
        list.splice(i, 1);
        removed++;
      }
    }
  } else {
    for (let i = 0; i < list.length; ) {
      if (list[i] === element) {
        list.splice(i, 1);
        removed++;
      } else {
        i++;
      }
    }
  }

  deleteIfEmpty(db, key, list);
  return integerReply(removed);
}

// --- LTRIM ---

export function ltrim(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const startParsed = parseInteger(args[1] ?? '');
  if (startParsed.error) return startParsed.error;
  const stopParsed = parseInteger(args[2] ?? '');
  if (stopParsed.error) return stopParsed.error;

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) return OK;

  let start = resolveIndex(startParsed.value, list.length);
  let stop = resolveIndex(stopParsed.value, list.length);

  if (start < 0) start = 0;
  if (stop >= list.length) stop = list.length - 1;

  if (start > stop || start >= list.length) {
    list.length = 0;
    deleteIfEmpty(db, key, list);
    return OK;
  }

  const kept = list.slice(start, stop + 1);
  list.length = 0;
  list.push(...kept);
  deleteIfEmpty(db, key, list);
  return OK;
}

// Re-export operations for consumers that import from this module
export { lpos, lmove, lmpop, rpoplpush };

export const specs: CommandSpec[] = [
  ...operationSpecs,
  {
    name: 'lpush',
    handler: (ctx, args) => {
      const reply = lpush(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.LIST, 'lpush', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@fast'],
  },
  {
    name: 'rpush',
    handler: (ctx, args) => {
      const reply = rpush(ctx.db, args, ctx.config);
      if (reply.kind === 'integer') {
        notify(ctx, EVENT_FLAGS.LIST, 'rpush', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@fast'],
  },
  {
    name: 'lpushx',
    handler: (ctx, args) => {
      const reply = lpushx(ctx.db, args, ctx.config);
      if (reply.kind === 'integer' && reply !== ZERO) {
        notify(ctx, EVENT_FLAGS.LIST, 'lpush', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@fast'],
  },
  {
    name: 'rpushx',
    handler: (ctx, args) => {
      const reply = rpushx(ctx.db, args, ctx.config);
      if (reply.kind === 'integer' && reply !== ZERO) {
        notify(ctx, EVENT_FLAGS.LIST, 'rpush', args[0] ?? '');
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@fast'],
  },
  {
    name: 'lpop',
    handler: (ctx, args) => {
      const reply = lpop(ctx.db, args);
      if (reply !== NIL && reply.kind !== 'error') {
        notify(ctx, EVENT_FLAGS.LIST, 'lpop', args[0] ?? '');
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@fast'],
  },
  {
    name: 'rpop',
    handler: (ctx, args) => {
      const reply = rpop(ctx.db, args);
      if (reply !== NIL && reply.kind !== 'error') {
        notify(ctx, EVENT_FLAGS.LIST, 'rpop', args[0] ?? '');
      }
      return reply;
    },
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@fast'],
  },
  {
    name: 'llen',
    handler: (ctx, args) => llen(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@list', '@fast'],
  },
  {
    name: 'lrange',
    handler: (ctx, args) => lrange(ctx.db, args),
    arity: 4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@list', '@slow'],
  },
  {
    name: 'lindex',
    handler: (ctx, args) => lindex(ctx.db, args),
    arity: 3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@list', '@slow'],
  },
  {
    name: 'lset',
    handler: (ctx, args) => {
      const reply = lset(ctx.db, args, ctx.config);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.LIST, 'lset', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@slow'],
  },
  {
    name: 'linsert',
    handler: (ctx, args) => {
      const reply = linsert(ctx.db, args, ctx.config);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.LIST, 'linsert', args[0] ?? '');
      }
      return reply;
    },
    arity: 5,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@slow'],
  },
  {
    name: 'lrem',
    handler: (ctx, args) => {
      const reply = lrem(ctx.db, args);
      if (reply.kind === 'integer' && (reply.value as number) > 0) {
        notify(ctx, EVENT_FLAGS.LIST, 'lrem', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@slow'],
  },
  {
    name: 'ltrim',
    handler: (ctx, args) => {
      const reply = ltrim(ctx.db, args);
      if (reply === OK) {
        notify(ctx, EVENT_FLAGS.LIST, 'ltrim', args[0] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@list', '@slow'],
  },
];
