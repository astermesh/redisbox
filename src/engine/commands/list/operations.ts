import type { Database } from '../../database.ts';
import type { Reply } from '../../types.ts';
import {
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  NIL,
  EMPTY_ARRAY,
  WRONGTYPE_ERR,
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
} from './utils.ts';

// --- LPOS ---

export function lpos(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const element = args[1] ?? '';

  let rank = 1;
  let count: number | null = null;
  let maxlen = 0;

  for (let i = 2; i < args.length; i += 2) {
    const option = (args[i] ?? '').toUpperCase();
    const valStr = args[i + 1];
    if (valStr === undefined) return SYNTAX_ERR;

    const parsed = parseInteger(valStr);
    if (parsed.error) return parsed.error;

    if (option === 'RANK') {
      if (parsed.value === 0) {
        return errorReply(
          'ERR',
          "RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the last match"
        );
      }
      rank = parsed.value;
    } else if (option === 'COUNT') {
      if (parsed.value < 0) {
        return errorReply('ERR', "COUNT can't be negative");
      }
      count = parsed.value;
    } else if (option === 'MAXLEN') {
      if (parsed.value < 0) {
        return errorReply('ERR', "MAXLEN can't be negative");
      }
      maxlen = parsed.value;
    } else {
      return SYNTAX_ERR;
    }
  }

  const { list, error } = getExistingList(db, key);
  if (error) return error;
  if (!list) {
    return count !== null ? EMPTY_ARRAY : NIL;
  }

  const results: number[] = [];
  const wantCount = count === 0 ? Infinity : (count ?? 1);
  let matchesSkipped = 0;
  const absRank = Math.abs(rank);
  const forward = rank > 0;

  if (forward) {
    const limit = maxlen > 0 ? Math.min(list.length, maxlen) : list.length;
    for (let i = 0; i < limit && results.length < wantCount; i++) {
      if (list[i] === element) {
        matchesSkipped++;
        if (matchesSkipped >= absRank) {
          results.push(i);
        }
      }
    }
  } else {
    const startIdx = list.length - 1;
    const limit = maxlen > 0 ? maxlen : list.length;
    let scanned = 0;
    for (
      let i = startIdx;
      i >= 0 && scanned < limit && results.length < wantCount;
      i--
    ) {
      scanned++;
      if (list[i] === element) {
        matchesSkipped++;
        if (matchesSkipped >= absRank) {
          results.push(i);
        }
      }
    }
  }

  if (count !== null) {
    return arrayReply(results.map((pos) => integerReply(pos)));
  }

  return results.length > 0 ? integerReply(results[0] ?? 0) : NIL;
}

// --- LMOVE ---

export function lmove(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const source = args[0] ?? '';
  const destination = args[1] ?? '';
  const wherefrom = (args[2] ?? '').toUpperCase();
  const whereto = (args[3] ?? '').toUpperCase();

  if (
    (wherefrom !== 'LEFT' && wherefrom !== 'RIGHT') ||
    (whereto !== 'LEFT' && whereto !== 'RIGHT')
  ) {
    return SYNTAX_ERR;
  }

  // Check source exists and is a list
  const srcResult = getExistingList(db, source);
  if (srcResult.error) return srcResult.error;
  if (!srcResult.list) return NIL;

  // Check destination type before modifying anything (even if same key)
  if (source !== destination) {
    const dstEntry = db.get(destination);
    if (dstEntry && dstEntry.type !== 'list') return WRONGTYPE_ERR;
  }

  const srcList = srcResult.list;

  // Pop from source
  const element =
    wherefrom === 'LEFT' ? (srcList.shift() ?? '') : (srcList.pop() ?? '');

  // Clean up source if empty
  deleteIfEmpty(db, source, srcList);

  // Push to destination
  const pushTo = (targetList: string[]): void => {
    if (whereto === 'LEFT') {
      targetList.unshift(element);
    } else {
      targetList.push(element);
    }
  };

  if (source === destination && srcList.length > 0) {
    // Same key, list still exists — push directly
    pushTo(srcList);
    updateEncoding(db, source, config);
  } else if (source === destination && srcList.length === 0) {
    // Same key, was deleted — recreate
    const { list: newList } = getOrCreateList(db, destination);
    if (newList) pushTo(newList);
    updateEncoding(db, destination, config);
  } else {
    // Different keys
    const dstResult = getOrCreateList(db, destination);
    if (dstResult.error) return dstResult.error;
    if (dstResult.list) pushTo(dstResult.list);
    updateEncoding(db, destination, config);
  }

  return bulkReply(element);
}

// --- LMPOP ---

export function lmpop(db: Database, args: string[]): Reply {
  // Parse numkeys
  const numkeysParsed = parseInteger(args[0] ?? '');
  if (numkeysParsed.error) return numkeysParsed.error;
  const numkeys = numkeysParsed.value;

  if (numkeys <= 0) {
    return errorReply('ERR', "numkeys can't be non-positive value");
  }

  // Parse direction (comes after numkeys keys)
  const directionIndex = 1 + numkeys;
  const direction = (args[directionIndex] ?? '').toUpperCase();

  if (direction !== 'LEFT' && direction !== 'RIGHT') {
    return SYNTAX_ERR;
  }

  // Parse optional COUNT (only allowed once, like real Redis)
  let count = 1;
  let countSeen = false;
  let i = directionIndex + 1;
  while (i < args.length) {
    const option = (args[i] ?? '').toUpperCase();
    if (option === 'COUNT' && !countSeen) {
      const countStr = args[i + 1];
      if (countStr === undefined) return SYNTAX_ERR;
      const countParsed = parseCount(countStr);
      if (countParsed.error) return countParsed.error;
      if (countParsed.count === 0) {
        return errorReply('ERR', 'count should be greater than 0');
      }
      count = countParsed.count ?? 1;
      countSeen = true;
      i += 2;
    } else {
      return SYNTAX_ERR;
    }
  }

  // Find first non-empty list
  for (let ki = 0; ki < numkeys; ki++) {
    const key = args[1 + ki] ?? '';
    const { list, error } = getExistingList(db, key);
    if (error) return error;
    if (!list || list.length === 0) continue;

    // Pop elements
    const actualCount = Math.min(count, list.length);
    let popped: string[];
    if (direction === 'LEFT') {
      popped = list.splice(0, actualCount);
    } else {
      popped = list.splice(list.length - actualCount, actualCount);
      popped.reverse();
    }

    deleteIfEmpty(db, key, list);
    return arrayReply([
      bulkReply(key),
      arrayReply(popped.map((v) => bulkReply(v))),
    ]);
  }

  return NIL;
}

// --- RPOPLPUSH (deprecated since 6.2, replaced by LMOVE RIGHT LEFT) ---

export function rpoplpush(
  db: Database,
  args: string[],
  config?: ConfigStore
): Reply {
  const source = args[0] ?? '';
  const destination = args[1] ?? '';
  return lmove(db, [source, destination, 'RIGHT', 'LEFT'], config);
}

export const specs: CommandSpec[] = [
  {
    name: 'lpos',
    handler: (ctx, args) => lpos(ctx.db, args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@list', '@slow'],
  },
  {
    name: 'lmove',
    handler: (ctx, args) => {
      const reply = lmove(ctx.db, args, ctx.config);
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
    arity: 5,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@list', '@slow'],
  },
  {
    name: 'lmpop',
    handler: (ctx, args) => {
      const reply = lmpop(ctx.db, args);
      if (reply.kind === 'array' && reply.value !== null) {
        // LMPOP returns [key, [elements]] — extract key from response
        const parts = reply.value as Reply[];
        if (parts[0] && parts[0].kind === 'bulk') {
          const key = parts[0].value as string;
          const numkeys = parseInt(args[0] ?? '0', 10);
          const dirArg = (args[1 + numkeys] ?? '').toUpperCase();
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
    arity: -4,
    flags: ['write', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@write', '@list', '@slow'],
  },
  {
    name: 'rpoplpush',
    handler: (ctx, args) => {
      const reply = rpoplpush(ctx.db, args, ctx.config);
      if (reply.kind === 'bulk' && reply.value !== null) {
        notify(ctx, EVENT_FLAGS.LIST, 'rpop', args[0] ?? '');
        notify(ctx, EVENT_FLAGS.LIST, 'lpush', args[1] ?? '');
      }
      return reply;
    },
    arity: 3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@list', '@slow'],
  },
];
