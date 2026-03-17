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
} from '../types.ts';

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
    return errorReply('ERR', 'no such key');
  }
  db.rename(src, dst);
  return OK;
}

export function renamenx(db: Database, args: string[]): Reply {
  const src = args[0] ?? '';
  const dst = args[1] ?? '';
  const srcEntry = db.getWithoutTouch(src);
  if (!srcEntry) {
    return errorReply('ERR', 'no such key');
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
  let dst = args[1] ?? '';
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
    } else if (flag === 'DESTINATION') {
      i++;
      dst = args[i] ?? '';
    }
    i++;
  }

  const copiedEntry = srcDb.copyEntry(src);
  if (!copiedEntry) return ZERO;

  if (!replace && destDb.has(dst)) return ZERO;

  const srcExpiry = srcDb.getExpiry(src);
  destDb.setEntry(dst, copiedEntry);
  if (srcExpiry !== undefined) {
    destDb.setExpiry(dst, srcExpiry);
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

export function objectIdletime(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.getWithoutTouch(key);
  if (!entry) return NIL;
  const clock = Date.now();
  return integerReply(Math.floor((clock - entry.lruClock) / 1000));
}

export function objectIdletimeWithClock(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const entry = db.getWithoutTouch(key);
  if (!entry) return NIL;
  return integerReply(Math.floor((clock() - entry.lruClock) / 1000));
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

export function wait(): Reply {
  return integerReply(0);
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
    return errorReply('ERR', "wrong number of arguments for 'object' command");
  }

  const subcommand = (args[0] ?? '').toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'ENCODING':
      if (subArgs.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'object|encoding' command"
        );
      }
      return objectEncoding(db, subArgs);
    case 'REFCOUNT':
      if (subArgs.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'object|refcount' command"
        );
      }
      return objectRefcount(db, subArgs);
    case 'IDLETIME':
      if (subArgs.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'object|idletime' command"
        );
      }
      return objectIdletimeWithClock(db, clock, subArgs);
    case 'FREQ':
      if (subArgs.length !== 1) {
        return errorReply(
          'ERR',
          "wrong number of arguments for 'object|freq' command"
        );
      }
      return objectFreq(db, subArgs);
    case 'HELP':
      return objectHelp();
    default:
      return errorReply(
        'ERR',
        `unknown subcommand or wrong number of arguments for 'object|${(args[0] ?? '').toLowerCase()}' command`
      );
  }
}
