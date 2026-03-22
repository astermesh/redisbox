/**
 * Blocking sorted set commands: BZPOPMIN, BZPOPMAX, BZMPOP.
 *
 * Non-blocking path: if data is available immediately, return the result.
 * Blocking path: return NIL_ARRAY as a signal to the caller (server layer)
 * that the client should be registered with the BlockingManager.
 *
 * Inside MULTI, these always execute the non-blocking path — blocking
 * is not possible in a transaction (matches real Redis behavior).
 */

import type { Database } from '../database.ts';
import type { Reply, CommandContext } from '../types.ts';
import {
  bulkReply,
  arrayReply,
  errorReply,
  NIL_ARRAY,
  NOT_INTEGER_ERR,
  WRONGTYPE_ERR,
  SYNTAX_ERR,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import { formatScore } from './sorted-set.ts';
import type { SortedSetData } from './sorted-set.ts';

/**
 * Parse a blocking timeout (seconds, may be float).
 * Returns timeout in seconds or an error reply.
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
 * Get an existing sorted set. Returns the set or null/error.
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

/**
 * Pop a single element with the lowest score from the sorted set at `key`.
 * Returns [key, element, score] or null if empty/not found.
 */
function popMin(db: Database, key: string): Reply | null {
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset || zset.dict.size === 0) return null;

  const node = zset.sl.head.lvl(0).forward;
  if (!node) return null;

  const element = node.element;
  const score = node.score;
  zset.dict.delete(element);
  zset.sl.delete(score, element);
  removeIfEmpty(db, key, zset);

  return arrayReply([
    bulkReply(key),
    bulkReply(element),
    bulkReply(formatScore(score)),
  ]);
}

/**
 * Pop a single element with the highest score from the sorted set at `key`.
 * Returns [key, element, score] or null if empty/not found.
 */
function popMax(db: Database, key: string): Reply | null {
  const { zset, error } = getExistingZset(db, key);
  if (error) return error;
  if (!zset || zset.dict.size === 0) return null;

  const node = zset.sl.tail;
  if (!node) return null;

  const element = node.element;
  const score = node.score;
  zset.dict.delete(element);
  zset.sl.delete(score, element);
  removeIfEmpty(db, key, zset);

  return arrayReply([
    bulkReply(key),
    bulkReply(element),
    bulkReply(formatScore(score)),
  ]);
}

// --- BZPOPMIN ---

export function bzpopmin(ctx: CommandContext, args: string[]): Reply {
  // BZPOPMIN key [key ...] timeout
  const timeoutStr = args[args.length - 1] ?? '';
  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  const keys = args.slice(0, -1);

  // Try non-blocking path: find first non-empty sorted set
  for (const key of keys) {
    const result = popMin(ctx.db, key);
    if (result !== null) return result;
  }

  // Blocking path: no data available
  return NIL_ARRAY;
}

// --- BZPOPMAX ---

export function bzpopmax(ctx: CommandContext, args: string[]): Reply {
  // BZPOPMAX key [key ...] timeout
  const timeoutStr = args[args.length - 1] ?? '';
  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  const keys = args.slice(0, -1);

  for (const key of keys) {
    const result = popMax(ctx.db, key);
    if (result !== null) return result;
  }

  return NIL_ARRAY;
}

// --- BZMPOP ---

export function bzmpop(ctx: CommandContext, args: string[]): Reply {
  // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
  const timeoutStr = args[0] ?? '';
  const { error: timeoutErr } = parseTimeout(timeoutStr);
  if (timeoutErr) return timeoutErr;

  const numkeysStr = args[1] ?? '';
  if (!/^-?\d+$/.test(numkeysStr)) {
    return NOT_INTEGER_ERR;
  }
  const numkeys = Number(numkeysStr);
  if (!Number.isInteger(numkeys) || numkeys <= 0) {
    return errorReply('ERR', "numkeys can't be non-positive value");
  }

  const directionIndex = 2 + numkeys;
  const direction = (args[directionIndex] ?? '').toUpperCase();
  if (direction !== 'MIN' && direction !== 'MAX') {
    return SYNTAX_ERR;
  }

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

  for (let ki = 0; ki < numkeys; ki++) {
    const key = args[2 + ki] ?? '';
    const { zset, error } = getExistingZset(ctx.db, key);
    if (error) return error;
    if (!zset || zset.dict.size === 0) continue;

    const actual = Math.min(count, zset.dict.size);
    const elements: Reply[] = [];

    for (let j = 0; j < actual; j++) {
      const node =
        direction === 'MIN' ? zset.sl.head.lvl(0).forward : zset.sl.tail;
      if (!node) break;
      elements.push(
        arrayReply([
          bulkReply(node.element),
          bulkReply(formatScore(node.score)),
        ])
      );
      zset.dict.delete(node.element);
      zset.sl.delete(node.score, node.element);
    }

    removeIfEmpty(ctx.db, key, zset);
    return arrayReply([bulkReply(key), arrayReply(elements)]);
  }

  // Blocking path
  return NIL_ARRAY;
}

export const specs: CommandSpec[] = [
  {
    name: 'bzpopmin',
    handler: (ctx, args) => bzpopmin(ctx, args),
    arity: -3,
    flags: ['write', 'denyoom', 'blocking'],
    firstKey: 1,
    lastKey: -2,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@slow', '@blocking'],
  },
  {
    name: 'bzpopmax',
    handler: (ctx, args) => bzpopmax(ctx, args),
    arity: -3,
    flags: ['write', 'denyoom', 'blocking'],
    firstKey: 1,
    lastKey: -2,
    keyStep: 1,
    categories: ['@write', '@sortedset', '@slow', '@blocking'],
  },
  {
    name: 'bzmpop',
    handler: (ctx, args) => bzmpop(ctx, args),
    arity: -5,
    flags: ['write', 'denyoom', 'blocking', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@write', '@sortedset', '@slow', '@blocking'],
  },
];
