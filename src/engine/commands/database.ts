import type { Database } from '../database.ts';
import type { RedisEngine } from '../engine.ts';
import type { Reply, CommandContext } from '../types.ts';
import {
  integerReply,
  errorReply,
  wrongArityError,
  OK,
  NOT_INTEGER_ERR,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

const DB_OUT_OF_RANGE_ERR = errorReply('ERR', 'DB index is out of range');
const INVALID_DB_INDEX_ERR = errorReply('ERR', 'invalid DB index');
const FLUSH_ARG_ERR = errorReply(
  'ERR',
  'FLUSHALL can call with no argument or a single argument ASYNC|SYNC'
);

function parseDbIndex(value: string): number | null {
  if (!/^-?\d+$/.test(value)) return null;
  const idx = parseInt(value, 10);
  if (!Number.isFinite(idx)) return null;
  return idx;
}

/**
 * Validate optional ASYNC|SYNC flag for FLUSHDB/FLUSHALL.
 * Returns null on success, error reply on failure.
 */
function validateFlushArgs(commandName: string, args: string[]): Reply | null {
  if (args.length === 0) return null;
  if (args.length > 1) return wrongArityError(commandName);
  const flag = (args[0] ?? '').toUpperCase();
  if (flag === 'ASYNC' || flag === 'SYNC') return null;
  return FLUSH_ARG_ERR;
}

export function select(ctx: CommandContext, args: string[]): Reply {
  const raw = args[0] ?? '';
  const idx = parseDbIndex(raw);
  if (idx === null) {
    return NOT_INTEGER_ERR;
  }
  if (idx < 0 || idx > 15) {
    return DB_OUT_OF_RANGE_ERR;
  }
  if (ctx.client) {
    ctx.client.dbIndex = idx;
    ctx.db = ctx.engine.db(idx);
  }
  return OK;
}

export function dbsize(db: Database): Reply {
  return integerReply(db.size);
}

export function flushdb(ctx: CommandContext, args: string[]): Reply {
  const err = validateFlushArgs('flushdb', args);
  if (err) return err;
  ctx.db.flush();
  return OK;
}

export function flushall(ctx: CommandContext, args: string[]): Reply {
  const err = validateFlushArgs('flushall', args);
  if (err) return err;
  for (const db of ctx.engine.databases) {
    db.flush();
  }
  return OK;
}

export function swapdb(engine: RedisEngine, args: string[]): Reply {
  const rawIdx1 = args[0] ?? '';
  const rawIdx2 = args[1] ?? '';

  const idx1 = parseDbIndex(rawIdx1);
  if (idx1 === null) return NOT_INTEGER_ERR;

  const idx2 = parseDbIndex(rawIdx2);
  if (idx2 === null) return NOT_INTEGER_ERR;

  if (idx1 < 0 || idx1 > 15 || idx2 < 0 || idx2 > 15) {
    return INVALID_DB_INDEX_ERR;
  }

  if (idx1 !== idx2) {
    const db1 = engine.db(idx1);
    const db2 = engine.db(idx2);
    engine.databases[idx1] = db2;
    engine.databases[idx2] = db1;
  }

  return OK;
}

export const specs: CommandSpec[] = [
  {
    name: 'select',
    handler: (ctx, args) => select(ctx, args),
    arity: 2,
    flags: ['fast', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@connection'],
  },
  {
    name: 'dbsize',
    handler: (ctx) => dbsize(ctx.db),
    arity: 1,
    flags: ['readonly', 'fast', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@read', '@fast'],
  },
  {
    name: 'flushdb',
    handler: (ctx, args) => flushdb(ctx, args),
    arity: -1,
    flags: ['write', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'flushall',
    handler: (ctx, args) => flushall(ctx, args),
    arity: -1,
    flags: ['write', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@write'],
  },
  {
    name: 'swapdb',
    handler: (ctx, args) => swapdb(ctx.engine, args),
    arity: 3,
    flags: ['write', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@keyspace', '@write', '@fast'],
  },
];
