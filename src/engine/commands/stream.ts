import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  integerReply,
  errorReply,
  EMPTY_ARRAY,
  NIL_ARRAY,
  WRONGTYPE_ERR,
  ZERO,
  SYNTAX_ERR,
} from '../types.ts';
import { RedisStream, parseStreamId } from '../stream.ts';
import type { StreamEntry, StreamId } from '../stream.ts';
import type { CommandSpec } from '../command-table.ts';

const INVALID_STREAM_ID_ERR = errorReply(
  'ERR',
  'Invalid stream ID specified as stream command argument'
);

const MAX_ID: StreamId = {
  ms: Number.MAX_SAFE_INTEGER,
  seq: Number.MAX_SAFE_INTEGER,
};
const MIN_ID: StreamId = { ms: 0, seq: 0 };

function getStream(
  db: Database,
  key: string
):
  | { stream: RedisStream; error: null; exists: true }
  | { stream: null; error: Reply; exists: false }
  | { stream: null; error: null; exists: false } {
  const entry = db.get(key);
  if (!entry) return { stream: null, error: null, exists: false };
  if (entry.type !== 'stream')
    return { stream: null, error: WRONGTYPE_ERR, exists: false };
  return { stream: entry.value as RedisStream, error: null, exists: true };
}

interface TrimOptions {
  strategy: 'maxlen' | 'minid';
  approximate: boolean;
  threshold: string;
}

/**
 * Parse trim options from XADD args starting at position `i`.
 * Returns the parsed options and the new position, or an error.
 */
function parseTrimOptions(
  args: string[],
  i: number
): { options: TrimOptions; nextIdx: number } | { error: Reply } {
  const strategyArg = (args[i] ?? '').toUpperCase();
  let strategy: 'maxlen' | 'minid';

  if (strategyArg === 'MAXLEN') {
    strategy = 'maxlen';
  } else if (strategyArg === 'MINID') {
    strategy = 'minid';
  } else {
    return { error: SYNTAX_ERR };
  }
  i++;

  let approximate = false;
  const nextArg = args[i] ?? '';
  if (nextArg === '~') {
    approximate = true;
    i++;
  } else if (nextArg === '=') {
    approximate = false;
    i++;
  }

  const threshold = args[i];
  if (threshold === undefined) {
    return { error: SYNTAX_ERR };
  }
  i++;

  // Optional LIMIT count (only meaningful with ~, but always accepted)
  if (i < args.length && (args[i] as string).toUpperCase() === 'LIMIT') {
    i++;
    const limitVal = args[i];
    if (limitVal === undefined) {
      return { error: SYNTAX_ERR };
    }
    const n = Number(limitVal);
    if (!Number.isInteger(n) || n < 0) {
      return {
        error: errorReply('ERR', 'value is not an integer or out of range'),
      };
    }
    i++;
  }

  return { options: { strategy, approximate, threshold }, nextIdx: i };
}

/**
 * Apply trim options to a stream.
 */
function applyTrim(stream: RedisStream, options: TrimOptions): Reply | null {
  if (options.strategy === 'maxlen') {
    const maxlen = Number(options.threshold);
    if (!Number.isInteger(maxlen) || maxlen < 0) {
      return errorReply('ERR', 'value is not an integer or out of range');
    }
    stream.trimByMaxlen(maxlen, options.approximate);
  } else {
    // MINID
    const minId = parseStreamId(options.threshold);
    if (!minId) {
      return errorReply(
        'ERR',
        'Invalid stream ID specified as stream command argument'
      );
    }
    stream.trimByMinid(minId, options.approximate);
  }
  return null;
}

/**
 * XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
 */
export function xadd(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 4) {
    return errorReply('ERR', "wrong number of arguments for 'xadd' command");
  }

  const key = args[0] as string;
  let i = 1;
  let nomkstream = false;
  let trimOptions: TrimOptions | null = null;

  // Parse optional flags before the ID
  while (i < args.length) {
    const arg = (args[i] as string).toUpperCase();

    if (arg === 'NOMKSTREAM') {
      nomkstream = true;
      i++;
      continue;
    }

    if (arg === 'MAXLEN' || arg === 'MINID') {
      const result = parseTrimOptions(args, i);
      if ('error' in result) return result.error;
      trimOptions = result.options;
      i = result.nextIdx;
      continue;
    }

    // Not a recognized option — must be the ID
    break;
  }

  // args[i] is the ID
  const idArg = args[i];
  if (idArg === undefined) {
    return errorReply('ERR', "wrong number of arguments for 'xadd' command");
  }
  i++;

  // Remaining args are field-value pairs
  const fieldArgs = args.slice(i);
  if (fieldArgs.length === 0 || fieldArgs.length % 2 !== 0) {
    return errorReply('ERR', "wrong number of arguments for 'xadd' command");
  }

  // Check existing key type
  const existing = getStream(db, key);
  if (existing.error) return existing.error;

  let stream = existing.stream;

  // NOMKSTREAM: if key doesn't exist, don't create it
  if (!stream && nomkstream) {
    return bulkReply(null);
  }

  // Create stream if needed
  if (!stream) {
    stream = new RedisStream();
    db.set(key, 'stream', 'stream', stream);
  }

  // Resolve the ID
  const resolved = stream.resolveNextId(idArg, clockMs);
  if ('error' in resolved) {
    const errMsg = resolved.error;
    const dashIdx = errMsg.indexOf(' ');
    const prefix = errMsg.substring(0, dashIdx);
    const message = errMsg.substring(dashIdx + 1);
    return errorReply(prefix, message);
  }

  // Build fields array
  const fields: [string, string][] = [];
  for (let j = 0; j < fieldArgs.length; j += 2) {
    fields.push([fieldArgs[j] as string, fieldArgs[j + 1] as string]);
  }

  // Add entry
  const idStr = stream.addEntry(resolved, fields);

  // Apply trimming if specified
  if (trimOptions) {
    const trimErr = applyTrim(stream, trimOptions);
    if (trimErr) return trimErr;
  }

  return bulkReply(idStr);
}

/**
 * XLEN key
 */
export function xlen(db: Database, args: string[]): Reply {
  const key = args[0] ?? '';
  const entry = db.get(key);
  if (!entry) return ZERO;
  if (entry.type !== 'stream') return WRONGTYPE_ERR;
  const stream = entry.value as RedisStream;
  return integerReply(stream.length);
}

/**
 * Parse a range boundary ID (for XRANGE/XREVRANGE).
 * Handles special IDs: - (min), + (max), and incomplete IDs (ms only).
 */
function parseRangeId(id: string, mode: 'start' | 'end'): StreamId | null {
  if (id === '-') return MIN_ID;
  if (id === '+') return MAX_ID;

  const dashIdx = id.indexOf('-');
  if (dashIdx === -1) {
    // Incomplete ID — just ms part
    const ms = Number(id);
    if (!Number.isInteger(ms) || ms < 0) return null;
    return { ms, seq: mode === 'start' ? 0 : Number.MAX_SAFE_INTEGER };
  }

  return parseStreamId(id);
}

/**
 * Format a stream entry as a Reply: [id, [field1, val1, field2, val2, ...]]
 */
function entryToReply(entry: StreamEntry): Reply {
  const fields: Reply[] = [];
  for (const [f, v] of entry.fields) {
    fields.push(bulkReply(f));
    fields.push(bulkReply(v));
  }
  return arrayReply([bulkReply(entry.id), arrayReply(fields)]);
}

/**
 * Parse optional COUNT from args at position i.
 * Returns { count, nextIdx } or { error }.
 */
function parseCount(
  args: string[],
  i: number
): { count: number; nextIdx: number } | { error: Reply } {
  const countStr = args[i + 1];
  if (countStr === undefined) return { error: SYNTAX_ERR };
  const n = Number(countStr);
  if (!Number.isInteger(n) || n < 0) {
    return {
      error: errorReply('ERR', 'value is not an integer or out of range'),
    };
  }
  return { count: n, nextIdx: i + 2 };
}

/**
 * XRANGE key start end [COUNT count]
 */
export function xrange(db: Database, args: string[]): Reply {
  const key = args[0] as string;
  const startArg = args[1] as string;
  const endArg = args[2] as string;

  const start = parseRangeId(startArg, 'start');
  if (!start) return INVALID_STREAM_ID_ERR;
  const end = parseRangeId(endArg, 'end');
  if (!end) return INVALID_STREAM_ID_ERR;

  let count: number | undefined;
  if (args.length > 3) {
    const upper = (args[3] as string).toUpperCase();
    if (upper === 'COUNT') {
      const result = parseCount(args, 3);
      if ('error' in result) return result.error;
      count = result.count;
    } else {
      return SYNTAX_ERR;
    }
  }

  const result = getStream(db, key);
  if (result.error) return result.error;
  if (!result.stream) return EMPTY_ARRAY;

  const entries = result.stream.range(start, end, count);
  return arrayReply(entries.map(entryToReply));
}

/**
 * XREVRANGE key end start [COUNT count]
 */
export function xrevrange(db: Database, args: string[]): Reply {
  const key = args[0] as string;
  const endArg = args[1] as string; // note: first arg is the higher ID
  const startArg = args[2] as string;

  const end = parseRangeId(endArg, 'end');
  if (!end) return INVALID_STREAM_ID_ERR;
  const start = parseRangeId(startArg, 'start');
  if (!start) return INVALID_STREAM_ID_ERR;

  let count: number | undefined;
  if (args.length > 3) {
    const upper = (args[3] as string).toUpperCase();
    if (upper === 'COUNT') {
      const result = parseCount(args, 3);
      if ('error' in result) return result.error;
      count = result.count;
    } else {
      return SYNTAX_ERR;
    }
  }

  const result = getStream(db, key);
  if (result.error) return result.error;
  if (!result.stream) return EMPTY_ARRAY;

  // revrange takes (higher, lower) — the stream method handles reverse iteration
  const entries = result.stream.revrange(end, start, count);
  return arrayReply(entries.map(entryToReply));
}

/**
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
 */
export function xread(db: Database, args: string[]): Reply {
  let i = 0;
  let count: number | undefined;
  let streamsIdx = -1;

  // Parse options before STREAMS keyword
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();

    if (upper === 'COUNT') {
      const result = parseCount(args, i);
      if ('error' in result) return result.error;
      count = result.count;
      i = result.nextIdx;
      continue;
    }

    if (upper === 'BLOCK') {
      // Accept BLOCK syntax but don't actually block
      i++;
      const blockMs = args[i];
      if (blockMs === undefined) return SYNTAX_ERR;
      const n = Number(blockMs);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      i++;
      continue;
    }

    if (upper === 'STREAMS') {
      streamsIdx = i + 1;
      break;
    }

    // Unknown option
    return SYNTAX_ERR;
  }

  if (streamsIdx === -1) {
    return errorReply(
      'ERR',
      "Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified."
    );
  }

  // Everything after STREAMS: first half are keys, second half are IDs
  const remaining = args.slice(streamsIdx);
  if (remaining.length === 0 || remaining.length % 2 !== 0) {
    return errorReply(
      'ERR',
      "Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified."
    );
  }

  const numStreams = remaining.length / 2;
  const keys = remaining.slice(0, numStreams);
  const ids = remaining.slice(numStreams);

  const resultStreams: Reply[] = [];

  for (let j = 0; j < numStreams; j++) {
    const key = keys[j] as string;
    const idArg = ids[j] as string;

    const lookup = getStream(db, key);
    if (lookup.error) return lookup.error;
    if (!lookup.stream) continue;

    // Handle $ special ID — means "last ID at read time"
    // In non-blocking mode, this will always return empty
    if (idArg === '$') continue;

    const afterId = parseStreamId(idArg);
    if (!afterId) return INVALID_STREAM_ID_ERR;

    const entries = lookup.stream.entriesAfter(afterId, count);
    if (entries.length === 0) continue;

    resultStreams.push(
      arrayReply([bulkReply(key), arrayReply(entries.map(entryToReply))])
    );
  }

  if (resultStreams.length === 0) return NIL_ARRAY;
  return arrayReply(resultStreams);
}

export const specs: CommandSpec[] = [
  {
    name: 'xadd',
    handler: (ctx, args) => xadd(ctx.db, ctx.engine.clock(), args),
    arity: -5,
    flags: ['write', 'denyoom', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xlen',
    handler: (ctx, args) => xlen(ctx.db, args),
    arity: 2,
    flags: ['readonly', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@fast'],
  },
  {
    name: 'xrange',
    handler: (ctx, args) => xrange(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
  },
  {
    name: 'xrevrange',
    handler: (ctx, args) => xrevrange(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
  },
  {
    name: 'xread',
    handler: (ctx, args) => xread(ctx.db, args),
    arity: -4,
    flags: ['readonly'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@stream', '@slow'],
  },
];
