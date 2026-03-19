import type { Database } from '../database.ts';
import type { RedisEngine } from '../engine.ts';
import type { Reply, CommandContext } from '../types.ts';
import {
  integerReply,
  errorReply,
  OK,
  NOT_INTEGER_ERR,
  SYNTAX_ERR,
} from '../types.ts';

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
function validateFlushArgs(args: string[]): Reply | null {
  if (args.length === 0) return null;
  if (args.length > 1) return SYNTAX_ERR;
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
  const err = validateFlushArgs(args);
  if (err) return err;
  ctx.db.flush();
  return OK;
}

export function flushall(ctx: CommandContext, args: string[]): Reply {
  const err = validateFlushArgs(args);
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
