import type { Database } from '../database.ts';
import type { RedisEngine } from '../engine.ts';
import type { Reply } from '../types.ts';
import {
  integerReply,
  bulkReply,
  statusReply,
  arrayReply,
  errorReply,
  OK,
  ZERO,
  ONE,
  NIL,
  SYNTAX_ERR,
  NO_SUCH_KEY_ERR,
  wrongArityError,
  unknownSubcommandError,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import { getLruClock, estimateIdleTime } from '../lru.ts';

export function del(db: Database, args: string[]): Reply {
  let count = 0;
  for (const key of args) {
    if (db.delete(key)) count++;
  }
  return integerReply(count);
}

export const unlink = del;

export function exists(db: Database, args: string[]): Reply {
  let count = 0;
  for (const key of args) {
    if (db.has(key)) count++;
  }
  return integerReply(count);
}

export function type(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  return statusReply(entry ? entry.type : 'none');
}

export function rename(db: Database, args: string[]): Reply {
  const src = args[0] ?? '';
  const dst = args[1] ?? '';
  const entry = db.getWithoutTouch(src);
  if (!entry) {
    return NO_SUCH_KEY_ERR;
  }
  db.rename(src, dst);
  return OK;
}

export function renamenx(db: Database, args: string[]): Reply {
  const src = args[0] ?? '';
  const dst = args[1] ?? '';
  const srcEntry = db.getWithoutTouch(src);
  if (!srcEntry) {
    return NO_SUCH_KEY_ERR;
  }
  if (src === dst) return ZERO;
  if (db.has(dst)) return ZERO;
  db.rename(src, dst);
  return ONE;
}

export function persist(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  return db.removeExpiry(key) ? ONE : ZERO;
}

export function randomkey(db: Database): Reply {
  const key = db.randomKey();
  return key === null ? NIL : bulkReply(key);
}

export function touch(db: Database, args: string[]): Reply {
  let count = 0;
  for (const key of args) {
    if (db.touch(key)) count++;
  }
  return integerReply(count);
}

export function copy(
  engine: RedisEngine,
  srcDb: Database,
  args: string[]
): Reply {
  const src = args[0] ?? '';
  const dst = args[1] ?? '';
  let destDb = srcDb;
  let replace = false;

  let i = 2;
  while (i < args.length) {
    const flag = (args[i] ?? '').toUpperCase();
    if (flag === 'DB') {
      i++;
      const dbIdx = parseInt(args[i] ?? '', 10);
      if (isNaN(dbIdx) || dbIdx < 0 || dbIdx > 15) {
        return errorReply('ERR', 'invalid DB index');
      }
      destDb = engine.db(dbIdx);
    } else if (flag === 'REPLACE') {
      replace = true;
    } else {
      return SYNTAX_ERR;
    }
    i++;
  }

  if (srcDb === destDb && src === dst) {
    return errorReply('ERR', 'source and destination objects are the same');
  }

  const copiedEntry = srcDb.copyEntry(src);
  if (!copiedEntry) return ZERO;

  if (!replace && destDb.has(dst)) return ZERO;

  const srcExpiry = srcDb.getExpiry(src);
  destDb.setEntry(dst, copiedEntry);
  if (srcExpiry !== undefined) {
    destDb.setExpiry(dst, srcExpiry);
  } else {
    destDb.removeExpiry(dst);
  }
  return ONE;
}

export function objectEncoding(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return NIL;
  return bulkReply(entry.encoding);
}

export function objectRefcount(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return NIL;
  return integerReply(1);
}

export function objectIdletimeWithClock(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const entry = db.getWithoutTouch(key);
  if (!entry) return NIL;
  const idle = estimateIdleTime(getLruClock(clock()), entry.lruClock);
  return integerReply(Math.floor(idle / 1000));
}

export function objectFreq(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return NIL;
  return integerReply(entry.lruFreq);
}

export function objectHelp(): Reply {
  return arrayReply([
    bulkReply(
      'OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
    ),
    bulkReply('ENCODING <key>'),
    bulkReply(
      '    Return the kind of internal representation the Redis object stored at <key> is using.'
    ),
    bulkReply('FREQ <key>'),
    bulkReply(
      '    Return the logarithmic access frequency counter of a Redis object stored at <key>.'
    ),
    bulkReply('HELP'),
    bulkReply('    Return subcommand help summary.'),
    bulkReply('IDLETIME <key>'),
    bulkReply('    Return the idle time of a Redis object stored at <key>.'),
    bulkReply('REFCOUNT <key>'),
    bulkReply('    Return the reference count of the object stored at <key>.'),
  ]);
}

export function dump(): Reply {
  return errorReply('ERR', 'DUMP is not supported in this engine');
}

export function restore(): Reply {
  return errorReply('ERR', 'RESTORE is not supported in this engine');
}

export function object(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  if (args.length === 0) {
    return wrongArityError('object');
  }

  const subcommand = (args[0] ?? '').toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'ENCODING':
      if (subArgs.length !== 1) {
        return wrongArityError('object|encoding');
      }
      return objectEncoding(db, subArgs);
    case 'REFCOUNT':
      if (subArgs.length !== 1) {
        return wrongArityError('object|refcount');
      }
      return objectRefcount(db, subArgs);
    case 'IDLETIME':
      if (subArgs.length !== 1) {
        return wrongArityError('object|idletime');
      }
      return objectIdletimeWithClock(db, clock, subArgs);
    case 'FREQ':
      if (subArgs.length !== 1) {
        return wrongArityError('object|freq');
      }
      return objectFreq(db, subArgs);
    case 'HELP':
      return objectHelp();
    default:
      return unknownSubcommandError('object', (args[0] ?? '').toLowerCase());
  }
}

export const specs: CommandSpec[] = [
  {
    name: 'del',
    handler: (ctx, args) => del(ctx.db, args),
    arity: -2,
    flags: ['write'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'unlink',
    handler: (ctx, args) => unlink(ctx.db, args),
    arity: -2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'exists',
    handler: (ctx, args) => exists(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'type',
    handler: (ctx, args) => type(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'rename',
    handler: (ctx, args) => rename(ctx.db, args),
    arity: 3,
    flags: ['write'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'renamenx',
    handler: (ctx, args) => renamenx(ctx.db, args),
    arity: 3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'persist',
    handler: (ctx, args) => persist(ctx.db, args),
    arity: 2,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'randomkey',
    handler: (ctx) => randomkey(ctx.db),
    arity: 1,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'touch',
    handler: (ctx, args) => touch(ctx.db, args),
    arity: -2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'copy',
    handler: (ctx, args) => copy(ctx.engine, ctx.db, args),
    arity: -3,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'object',
    handler: (ctx, args) => object(ctx.db, ctx.engine.clock, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
    subcommands: [
      {
        name: 'encoding',
        handler: (ctx, args) => objectEncoding(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'refcount',
        handler: (ctx, args) => objectRefcount(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'idletime',
        handler: (ctx, args) =>
          objectIdletimeWithClock(ctx.db, ctx.engine.clock, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'freq',
        handler: (ctx, args) => objectFreq(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@keyspace', '@read'],
      },
      {
        name: 'help',
        handler: () => objectHelp(),
        arity: 2,
        flags: ['readonly'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@keyspace', '@read'],
      },
    ],
  },
  {
    name: 'dump',
    handler: () => dump(),
    arity: 2,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@read'],
  },
  {
    name: 'restore',
    handler: () => restore(),
    arity: -4,
    flags: ['write', 'denyoom'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@keyspace', '@write'],
  },
];
