/**
 * Blocking list commands: BLPOP, BRPOP, BLMOVE, BLMPOP, BRPOPLPUSH.
 *
 * Non-blocking path: if data is available immediately, return the result.
 * Blocking path: return NIL_ARRAY as a signal to the caller (server layer)
 * that the client should be registered with the BlockingManager.
 *
 * Inside MULTI, these always execute the non-blocking path — blocking
 * is not possible in a transaction (matches real Redis behavior).
 */

import type { Database } from '../../database.ts';
import type { Reply, CommandContext } from '../../types.ts';
import {
  bulkReply,
  arrayReply,
  errorReply,
  NIL_ARRAY,
  NOT_INTEGER_ERR,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import { lmove, getExistingList } from './index.ts';
import { notify, EVENT_FLAGS } from '../../notify.ts';

/**
 * Parse a blocking timeout (seconds, may be float).
 * Returns timeout in seconds or an error reply.
 * Redis error: "timeout is not a float or out of range"
 */
function parseTimeout(
  s: string
): { timeout: number; error: null } | { timeout: null; error: Reply } {
  const n = Number(s);
  if (!Number.isFinite(n) || isNaN(n)) {
    return {
      timeout: null,
      error: errorReply('ERR', 'timeout is not a float or out of range'),
    };
  }
  if (n < 0) {
    return {
      timeout: null,
      error: errorReply('ERR', 'timeout is not a float or out of range'),
    };
  }
  return { timeout: n, error: null };
}

/**
 * Delete a key if the list is empty.
 */
function deleteIfEmpty(db: Database, key: string, items: string[]): void {
  if (items.length === 0) {
    db.delete(key);
  }
}

// --- BLPOP ---

export function blpop(ctx: CommandContext, args: string[]): Reply {
  // Last argument is timeout
  const timeoutStr = args[args.length - 1] ?? '';
  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  const keys = args.slice(0, -1);

  // Try non-blocking path: find first non-empty list
  for (const key of keys) {
    const { list, error } = getExistingList(ctx.db, key);
    if (error) return error;
    if (!list || list.length === 0) continue;

    const element = list.shift() ?? '';
    deleteIfEmpty(ctx.db, key, list);
    return arrayReply([bulkReply(key), bulkReply(element)]);
  }

  // Blocking path: no data available
  return NIL_ARRAY;
}

// --- BRPOP ---

export function brpop(ctx: CommandContext, args: string[]): Reply {
  const timeoutStr = args[args.length - 1] ?? '';
  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  const keys = args.slice(0, -1);

  for (const key of keys) {
    const { list, error } = getExistingList(ctx.db, key);
    if (error) return error;
    if (!list || list.length === 0) continue;

    const element = list.pop() ?? '';
    deleteIfEmpty(ctx.db, key, list);
    return arrayReply([bulkReply(key), bulkReply(element)]);
  }

  return NIL_ARRAY;
}

// --- BLMOVE ---

export function blmove(ctx: CommandContext, args: string[]): Reply {
  const source = args[0] ?? '';
  const destination = args[1] ?? '';
  const wherefrom = (args[2] ?? '').toUpperCase();
  const whereto = (args[3] ?? '').toUpperCase();
  const timeoutStr = args[4] ?? '';

  if (
    (wherefrom !== 'LEFT' && wherefrom !== 'RIGHT') ||
    (whereto !== 'LEFT' && whereto !== 'RIGHT')
  ) {
    return SYNTAX_ERR;
  }

  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  // Check if source exists and has data
  const { list: srcList, error } = getExistingList(ctx.db, source);
  if (error) return error;

  if (!srcList || srcList.length === 0) {
    // Blocking path
    return NIL_ARRAY;
  }

  // Check destination type before modifying (same as lmove)
  if (source !== destination) {
    const dstEntry = ctx.db.get(destination);
    if (dstEntry && dstEntry.type !== 'list') return WRONGTYPE_ERR;
  }

  // Non-blocking path: delegate to lmove
  return lmove(ctx.db, [source, destination, wherefrom, whereto]);
}

// --- BLMPOP ---

export function blmpop(ctx: CommandContext, args: string[]): Reply {
  // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
  const timeoutStr = args[0] ?? '';
  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  // Parse numkeys (same logic as LMPOP: non-integer → NOT_INTEGER_ERR, ≤0 → specific message)
  const numkeysStr = args[1] ?? '';
  if (!/^-?\d+$/.test(numkeysStr)) {
    return NOT_INTEGER_ERR;
  }
  const numkeys = Number(numkeysStr);
  if (!Number.isInteger(numkeys)) {
    return NOT_INTEGER_ERR;
  }
  if (numkeys <= 0) {
    return errorReply('ERR', "numkeys can't be non-positive value");
  }

  // Parse direction
  const directionIndex = 2 + numkeys;
  const direction = (args[directionIndex] ?? '').toUpperCase();
  if (direction !== 'LEFT' && direction !== 'RIGHT') {
    return SYNTAX_ERR;
  }

  // Parse optional COUNT
  let count = 1;
  let i = directionIndex + 1;
  while (i < args.length) {
    const option = (args[i] ?? '').toUpperCase();
    if (option === 'COUNT') {
      const countStr = args[i + 1];
      if (countStr === undefined) return SYNTAX_ERR;
      if (!/^-?\d+$/.test(countStr)) {
        return NOT_INTEGER_ERR;
      }
      const c = Number(countStr);
      if (!Number.isInteger(c) || c < 0) {
        return NOT_INTEGER_ERR;
      }
      if (c === 0) {
        return errorReply('ERR', 'count should be greater than 0');
      }
      count = c;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  // Try non-blocking path: find first non-empty list
  for (let ki = 0; ki < numkeys; ki++) {
    const key = args[2 + ki] ?? '';
    const { list, error } = getExistingList(ctx.db, key);
    if (error) return error;
    if (!list || list.length === 0) continue;

    const actualCount = Math.min(count, list.length);
    let popped: string[];
    if (direction === 'LEFT') {
      popped = list.splice(0, actualCount);
    } else {
      popped = list.splice(list.length - actualCount, actualCount);
      popped.reverse();
    }

    deleteIfEmpty(ctx.db, key, list);
    return arrayReply([
      bulkReply(key),
      arrayReply(popped.map((v) => bulkReply(v))),
    ]);
  }

  // Blocking path
  return NIL_ARRAY;
}

// --- BRPOPLPUSH (deprecated since 6.2, replaced by BLMOVE src dst RIGHT LEFT) ---

export function brpoplpush(ctx: CommandContext, args: string[]): Reply {
  const source = args[0] ?? '';
  const destination = args[1] ?? '';
  const timeoutStr = args[2] ?? '';

  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  // Check if source exists and has data
  const { list: srcList, error } = getExistingList(ctx.db, source);
  if (error) return error;

  if (!srcList || srcList.length === 0) {
    return NIL_ARRAY;
  }

  // Check destination type
  if (source !== destination) {
    const dstEntry = ctx.db.get(destination);
    if (dstEntry && dstEntry.type !== 'list') return WRONGTYPE_ERR;
  }

  // Non-blocking path: delegate to lmove with RIGHT LEFT
  return lmove(ctx.db, [source, destination, 'RIGHT', 'LEFT']);
}

export const specs: CommandSpec[] = [
  {
    name: 'blpop',
    handler: (ctx, args) => {
      const reply = blpop(ctx, args);
      if (reply.kind === 'array' && reply !== NIL_ARRAY) {
        const parts = reply.value as Reply[];
        if (parts[0] && parts[0].kind === 'bulk') {
          notify(ctx, EVENT_FLAGS.LIST, 'lpop', parts[0].value as string);
        }
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'blocking'],
    firstKey: 1,
    lastKey: -2,
    keyStep: 1,
    categories: ['@write', '@list', '@slow', '@blocking'],
  },
  {
    name: 'brpop',
    handler: (ctx, args) => {
      const reply = brpop(ctx, args);
      if (reply.kind === 'array' && reply !== NIL_ARRAY) {
        const parts = reply.value as Reply[];
        if (parts[0] && parts[0].kind === 'bulk') {
          notify(ctx, EVENT_FLAGS.LIST, 'rpop', parts[0].value as string);
        }
      }
      return reply;
    },
    arity: -3,
    flags: ['write', 'denyoom', 'blocking'],
    firstKey: 1,
    lastKey: -2,
    keyStep: 1,
    categories: ['@write', '@list', '@slow', '@blocking'],
  },
  {
    name: 'blmove',
    handler: (ctx, args) => {
      const reply = blmove(ctx, args);
      if (reply.kind === 'bulk' && reply.value !== null) {
        const source = args[0] ?? '';
        const destination = args[1] ?? '';
        const wherefrom = (args[2] ?? '').toUpperCase();
        const whereto = (args[3] ?? '').toUpperCase();
        notify(
          ctx,
          EVENT_FLAGS.LIST,
          wherefrom === 'LEFT' ? 'lpop' : 'rpop',
          source
        );
        notify(
          ctx,
          EVENT_FLAGS.LIST,
          whereto === 'LEFT' ? 'lpush' : 'rpush',
          destination
        );
      }
      return reply;
    },
    arity: 6,
    flags: ['write', 'denyoom', 'blocking'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@list', '@slow', '@blocking'],
  },
  {
    name: 'blmpop',
    handler: (ctx, args) => {
      const reply = blmpop(ctx, args);
      if (reply.kind === 'array' && reply !== NIL_ARRAY) {
        const parts = reply.value as Reply[];
        if (parts[0] && parts[0].kind === 'bulk') {
          const key = parts[0].value as string;
          const numkeys = parseInt(args[1] ?? '0', 10);
          const dirArg = (args[2 + numkeys] ?? '').toUpperCase();
          notify(
            ctx,
            EVENT_FLAGS.LIST,
            dirArg === 'LEFT' ? 'lpop' : 'rpop',
            key
          );
        }
      }
      return reply;
    },
    arity: -5,
    flags: ['write', 'denyoom', 'blocking', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@write', '@list', '@slow', '@blocking'],
  },
  {
    name: 'brpoplpush',
    handler: (ctx, args) => {
      const reply = brpoplpush(ctx, args);
      if (reply.kind === 'bulk' && reply.value !== null) {
        notify(ctx, EVENT_FLAGS.LIST, 'rpop', args[0] ?? '');
        notify(ctx, EVENT_FLAGS.LIST, 'lpush', args[1] ?? '');
      }
      return reply;
    },
    arity: 4,
    flags: ['write', 'denyoom', 'blocking'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@list', '@slow', '@blocking'],
  },
];
