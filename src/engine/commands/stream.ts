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
  OK,
  SYNTAX_ERR,
} from '../types.ts';
import {
  RedisStream,
  parseStreamId,
  compareStreamIds,
  streamIdToString,
} from '../stream.ts';
import type {
  StreamEntry,
  StreamId,
  PendingEntry,
  ConsumerGroup,
  StreamConsumer,
} from '../stream.ts';

/**
 * Parse an entry ID that is known to be valid (was validated on insert).
 * Falls back to 0-0 if somehow invalid (should never happen).
 */
function safeParseId(id: string): StreamId {
  return parseStreamId(id) ?? { ms: 0, seq: 0 };
}
import type { CommandSpec } from '../command-table.ts';
import type { CommandContext } from '../types.ts';

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
 * Increment a stream ID to the next possible value.
 * Used for exclusive start ranges: (id → id+1
 */
function streamIncrId(id: StreamId): StreamId | null {
  if (id.seq < Number.MAX_SAFE_INTEGER) {
    return { ms: id.ms, seq: id.seq + 1 };
  }
  if (id.ms < Number.MAX_SAFE_INTEGER) {
    return { ms: id.ms + 1, seq: 0 };
  }
  return null; // overflow
}

/**
 * Decrement a stream ID to the previous possible value.
 * Used for exclusive end ranges: (id → id-1
 */
function streamDecrId(id: StreamId): StreamId | null {
  if (id.seq > 0) {
    return { ms: id.ms, seq: id.seq - 1 };
  }
  if (id.ms > 0) {
    return { ms: id.ms - 1, seq: Number.MAX_SAFE_INTEGER };
  }
  return null; // underflow
}

const INVALID_START_RANGE_ERR = errorReply(
  'ERR',
  'invalid start ID for the interval'
);
const INVALID_END_RANGE_ERR = errorReply(
  'ERR',
  'invalid end ID for the interval'
);

/**
 * Parse a range boundary ID (for XRANGE/XREVRANGE).
 * Handles special IDs: - (min), + (max), exclusive ( prefix, and incomplete IDs (ms only).
 */
function parseRangeId(
  id: string,
  mode: 'start' | 'end'
): StreamId | { error: Reply } | null {
  if (id === '-') return MIN_ID;
  if (id === '+') return MAX_ID;

  // Exclusive range with ( prefix (Redis 6.2+)
  const exclusive = id.startsWith('(');
  const rawId = exclusive ? id.substring(1) : id;

  let parsed: StreamId | null;
  const dashIdx = rawId.indexOf('-');
  if (dashIdx === -1) {
    // Incomplete ID — just ms part
    const ms = Number(rawId);
    if (!Number.isInteger(ms) || ms < 0) return null;
    parsed = { ms, seq: mode === 'start' ? 0 : Number.MAX_SAFE_INTEGER };
  } else {
    parsed = parseStreamId(rawId);
  }

  if (!parsed) return null;

  if (exclusive) {
    if (mode === 'start') {
      const incr = streamIncrId(parsed);
      if (!incr) return { error: INVALID_START_RANGE_ERR };
      return incr;
    } else {
      const decr = streamDecrId(parsed);
      if (!decr) return { error: INVALID_END_RANGE_ERR };
      return decr;
    }
  }

  return parsed;
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
  if ('error' in start) return start.error;
  const end = parseRangeId(endArg, 'end');
  if (!end) return INVALID_STREAM_ID_ERR;
  if ('error' in end) return end.error;

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
  if ('error' in end) return end.error;
  const start = parseRangeId(startArg, 'start');
  if (!start) return INVALID_STREAM_ID_ERR;
  if ('error' in start) return start.error;

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

// ─── XGROUP ─────────────────────────────────────────────────────────

function xgroupCreate(db: Database, args: string[]): Reply {
  // XGROUP CREATE key groupname id-or-$ [MKSTREAM] [ENTRIESREAD entries-read]
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|create' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const idArg = args[2] as string;
  let mkstream = false;
  let entriesRead = -1;

  let i = 3;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'MKSTREAM') {
      mkstream = true;
      i++;
    } else if (upper === 'ENTRIESREAD') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const n = Number(val);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      entriesRead = n;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const existing = getStream(db, key);
  if (existing.error) return existing.error;

  let stream = existing.stream;

  if (!stream) {
    if (!mkstream) {
      return errorReply(
        'ERR',
        'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
      );
    }
    stream = new RedisStream();
    db.set(key, 'stream', 'stream', stream);
  }

  // Parse the ID
  let lastDeliveredId: StreamId;
  if (idArg === '$') {
    lastDeliveredId = stream.lastId;
  } else if (idArg === '0') {
    lastDeliveredId = { ms: 0, seq: 0 };
  } else {
    const parsed = parseStreamId(idArg);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    lastDeliveredId = parsed;
  }

  const created = stream.createGroup(
    groupName,
    lastDeliveredId,
    entriesRead >= 0 ? entriesRead : 0
  );
  if (!created) {
    return errorReply('BUSYGROUP', 'Consumer Group name already exists');
  }

  return OK;
}

function xgroupSetid(db: Database, args: string[]): Reply {
  // XGROUP SETID key groupname id-or-$ [ENTRIESREAD entries-read]
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|setid' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const idArg = args[2] as string;
  let entriesRead = -1;

  let i = 3;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'ENTRIESREAD') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const n = Number(val);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      entriesRead = n;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const stream = existing.stream;
  const group = stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }

  let newId: StreamId;
  if (idArg === '$') {
    newId = stream.lastId;
  } else if (idArg === '0') {
    newId = { ms: 0, seq: 0 };
  } else {
    const parsed = parseStreamId(idArg);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    newId = parsed;
  }

  stream.setGroupId(groupName, newId, entriesRead >= 0 ? entriesRead : 0);
  return OK;
}

function xgroupDestroy(db: Database, args: string[]): Reply {
  // XGROUP DESTROY key groupname
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|destroy' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const destroyed = existing.stream.destroyGroup(groupName);
  return integerReply(destroyed ? 1 : 0);
}

function xgroupDelconsumer(db: Database, args: string[]): Reply {
  // XGROUP DELCONSUMER key groupname consumername
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|delconsumer' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const group = existing.stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }

  const pendingCount = existing.stream.deleteConsumer(groupName, consumerName);
  return integerReply(pendingCount ?? 0);
}

function xgroupCreateconsumer(db: Database, args: string[]): Reply {
  // XGROUP CREATECONSUMER key groupname consumername
  if (args.length < 3) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xgroup|createconsumer' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;

  const existing = getStream(db, key);
  if (existing.error) return existing.error;
  if (!existing.stream) {
    return errorReply(
      'ERR',
      'The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.'
    );
  }

  const result = existing.stream.createConsumer(groupName, consumerName);
  if (result === null) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }
  return integerReply(result);
}

function xgroup(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'xgroup' command");
  }

  const subcommand = (args[0] as string).toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'CREATE':
      return xgroupCreate(ctx.db, subArgs);
    case 'SETID':
      return xgroupSetid(ctx.db, subArgs);
    case 'DESTROY':
      return xgroupDestroy(ctx.db, subArgs);
    case 'DELCONSUMER':
      return xgroupDelconsumer(ctx.db, subArgs);
    case 'CREATECONSUMER':
      return xgroupCreateconsumer(ctx.db, subArgs);
    default:
      return errorReply(
        'ERR',
        `unknown subcommand or wrong number of arguments for 'xgroup|${args[0]}' command`
      );
  }
}

function ensureConsumer(
  group: ConsumerGroup,
  name: string,
  clockMs: number
): StreamConsumer {
  let consumer = group.consumers.get(name);
  if (!consumer) {
    consumer = { name, seenTime: clockMs, pending: new Map() };
    group.consumers.set(name, consumer);
  }
  consumer.seenTime = clockMs;
  return consumer;
}

// ─── XREADGROUP ──────────────────────────────────────────────────────

/**
 * XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
 */
export function xreadgroup(
  db: Database,
  clockMs: number,
  args: string[]
): Reply {
  let i = 0;

  // Must start with GROUP keyword
  if (args.length < 4 || (args[i] as string).toUpperCase() !== 'GROUP') {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xreadgroup' command"
    );
  }
  i++;

  const groupName = args[i++] as string;
  const consumerName = args[i++] as string;

  let count: number | undefined;
  let noack = false;
  let streamsIdx = -1;

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

    if (upper === 'NOACK') {
      noack = true;
      i++;
      continue;
    }

    if (upper === 'STREAMS') {
      streamsIdx = i + 1;
      break;
    }

    return SYNTAX_ERR;
  }

  if (streamsIdx === -1) {
    return errorReply(
      'ERR',
      "Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
    );
  }

  const remaining = args.slice(streamsIdx);
  if (remaining.length === 0 || remaining.length % 2 !== 0) {
    return errorReply(
      'ERR',
      "Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
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
    if (!lookup.stream) {
      return errorReply(
        'NOGROUP',
        "No such key '" +
          key +
          "' or consumer group '" +
          groupName +
          "' in XREADGROUP with GROUP option"
      );
    }

    const stream = lookup.stream;
    const group = stream.getGroup(groupName);
    if (!group) {
      return errorReply(
        'NOGROUP',
        "No such key '" +
          key +
          "' or consumer group '" +
          groupName +
          "' in XREADGROUP with GROUP option"
      );
    }

    // Ensure consumer exists
    const consumer = ensureConsumer(group, consumerName, clockMs);

    if (idArg === '$') {
      return errorReply(
        'ERR',
        'The $ ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.'
      );
    }

    if (idArg === '>') {
      // Read new (undelivered) messages
      const entries = stream.entriesAfter(group.lastDeliveredId, count);
      if (entries.length === 0) continue;

      // Update lastDeliveredId to the last entry we're delivering
      const lastEntry = entries[entries.length - 1] as StreamEntry;
      group.lastDeliveredId = { ...safeParseId(lastEntry.id) };
      group.entriesRead += entries.length;

      if (!noack) {
        // Add to PEL
        for (const entry of entries) {
          const pe: PendingEntry = {
            entryId: entry.id,
            consumer: consumerName,
            deliveryTime: clockMs,
            deliveryCount: 1,
          };
          group.pel.set(entry.id, pe);
          consumer.pending.set(entry.id, pe);
        }
      }

      resultStreams.push(
        arrayReply([bulkReply(key), arrayReply(entries.map(entryToReply))])
      );
    } else {
      // Read pending entries for this consumer (entries in consumer's PEL with ID > idArg)
      const afterId = parseStreamId(idArg);
      if (!afterId) return INVALID_STREAM_ID_ERR;

      // Collect pending replies with ID > afterId
      const pendingReplies: Reply[] = [];
      // We need to iterate in order, so sort by entry ID
      const sortedPending = [...consumer.pending.keys()].sort((a, b) =>
        compareStreamIds(safeParseId(a), safeParseId(b))
      );

      for (const entryId of sortedPending) {
        const eid = safeParseId(entryId);
        if (compareStreamIds(eid, afterId) <= 0) continue;

        // Update delivery time and count (matches real Redis behavior)
        const pe = consumer.pending.get(entryId);
        if (pe) {
          pe.deliveryTime = clockMs;
          pe.deliveryCount++;
        }

        // Find the actual entry in the stream
        const entryData = stream.range(eid, eid, 1);
        if (entryData.length > 0) {
          pendingReplies.push(entryToReply(entryData[0] as StreamEntry));
        } else {
          // Entry was deleted (XDEL/XTRIM) — return [id, null]
          pendingReplies.push(
            arrayReply([bulkReply(entryId), bulkReply(null)])
          );
        }
        if (count !== undefined && pendingReplies.length >= count) break;
      }

      resultStreams.push(
        arrayReply([bulkReply(key), arrayReply(pendingReplies)])
      );
    }
  }

  if (resultStreams.length === 0) return NIL_ARRAY;
  return arrayReply(resultStreams);
}

// ─── XACK ────────────────────────────────────────────────────────────

/**
 * XACK key group id [id ...]
 */
export function xack(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return errorReply('ERR', "wrong number of arguments for 'xack' command");
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) return ZERO;

  const group = lookup.stream.getGroup(groupName);
  if (!group) return ZERO;

  let acked = 0;
  for (let i = 2; i < args.length; i++) {
    const idStr = args[i] as string;
    const parsed = parseStreamId(idStr);
    if (!parsed) return INVALID_STREAM_ID_ERR;

    const entryId = `${parsed.ms}-${parsed.seq}`;
    const pe = group.pel.get(entryId);
    if (pe) {
      // Remove from group PEL
      group.pel.delete(entryId);
      // Remove from consumer's pending
      const consumer = group.consumers.get(pe.consumer);
      if (consumer) {
        consumer.pending.delete(entryId);
      }
      acked++;
    }
  }

  return integerReply(acked);
}

// ─── XPENDING ────────────────────────────────────────────────────────

/**
 * XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
 */
export function xpending(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xpending' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply(
      'NOGROUP',
      "No such key '" + key + "' or consumer group '" + groupName + "'"
    );
  }

  const group = lookup.stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such key '" + key + "' or consumer group '" + groupName + "'"
    );
  }

  // Summary form: XPENDING key group
  if (args.length === 2) {
    return xpendingSummary(group);
  }

  // Detail form: XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
  let i = 2;
  let minIdle = 0;

  if ((args[i] as string).toUpperCase() === 'IDLE') {
    i++;
    const idleStr = args[i];
    if (idleStr === undefined) return SYNTAX_ERR;
    const n = Number(idleStr);
    if (!Number.isInteger(n) || n < 0) {
      return errorReply('ERR', 'value is not an integer or out of range');
    }
    minIdle = n;
    i++;
  }

  const startArg = args[i++];
  const endArg = args[i++];
  const countArg = args[i++];

  if (
    startArg === undefined ||
    endArg === undefined ||
    countArg === undefined
  ) {
    return SYNTAX_ERR;
  }

  const start = parseRangeId(startArg, 'start');
  if (!start) return INVALID_STREAM_ID_ERR;
  if ('error' in start) return start.error;

  const end = parseRangeId(endArg, 'end');
  if (!end) return INVALID_STREAM_ID_ERR;
  if ('error' in end) return end.error;

  const countN = Number(countArg);
  if (!Number.isInteger(countN) || countN < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  const consumerFilter = args[i] as string | undefined;

  return xpendingDetail(
    group,
    start,
    end,
    countN,
    consumerFilter,
    minIdle,
    clockMs
  );
}

function xpendingSummary(group: ConsumerGroup): Reply {
  if (group.pel.size === 0) {
    return arrayReply([
      integerReply(0),
      bulkReply(null),
      bulkReply(null),
      NIL_ARRAY,
    ]);
  }

  // Find min and max IDs, and count per consumer
  let minId: StreamId | null = null;
  let maxId: StreamId | null = null;
  const consumerCounts = new Map<string, number>();

  for (const [entryId, pe] of group.pel) {
    const eid = safeParseId(entryId);
    if (minId === null || compareStreamIds(eid, minId) < 0) minId = eid;
    if (maxId === null || compareStreamIds(eid, maxId) > 0) maxId = eid;
    consumerCounts.set(pe.consumer, (consumerCounts.get(pe.consumer) ?? 0) + 1);
  }

  // Build consumer list sorted by name
  const consumerList: Reply[] = [...consumerCounts.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, cnt]) =>
      arrayReply([bulkReply(name), bulkReply(String(cnt))])
    );

  return arrayReply([
    integerReply(group.pel.size),
    bulkReply(minId ? streamIdToString(minId) : null),
    bulkReply(maxId ? streamIdToString(maxId) : null),
    arrayReply(consumerList),
  ]);
}

function xpendingDetail(
  group: ConsumerGroup,
  start: StreamId,
  end: StreamId,
  count: number,
  consumerFilter: string | undefined,
  minIdle: number,
  clockMs: number
): Reply {
  // Collect and sort all PEL entries
  const entries: PendingEntry[] = [];

  for (const [entryId, pe] of group.pel) {
    if (consumerFilter && pe.consumer !== consumerFilter) continue;

    const eid = safeParseId(entryId);
    if (compareStreamIds(eid, start) < 0) continue;
    if (compareStreamIds(eid, end) > 0) continue;

    if (minIdle > 0) {
      const idle = clockMs - pe.deliveryTime;
      if (idle < minIdle) continue;
    }

    entries.push(pe);
  }

  // Sort by entry ID
  entries.sort((a, b) =>
    compareStreamIds(safeParseId(a.entryId), safeParseId(b.entryId))
  );

  // Apply count limit
  const limited = entries.slice(0, count);

  // Format: [id, consumer, idle-time-ms, delivery-count]
  const result: Reply[] = limited.map((pe) => {
    const idle = clockMs - pe.deliveryTime;
    return arrayReply([
      bulkReply(pe.entryId),
      bulkReply(pe.consumer),
      integerReply(idle),
      integerReply(pe.deliveryCount),
    ]);
  });

  return arrayReply(result);
}

// ─── XDEL ─────────────────────────────────────────────────────────────

/**
 * XDEL key id [id ...]
 */
export function xdel(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply('ERR', "wrong number of arguments for 'xdel' command");
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) return ZERO;

  const ids: StreamId[] = [];
  for (let i = 1; i < args.length; i++) {
    const parsed = parseStreamId(args[i] as string);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    ids.push(parsed);
  }

  const deleted = lookup.stream.deleteEntries(ids);
  return integerReply(deleted);
}

// ─── XTRIM ────────────────────────────────────────────────────────────

/**
 * XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
 */
export function xtrim(db: Database, args: string[]): Reply {
  if (args.length < 3) {
    return errorReply('ERR', "wrong number of arguments for 'xtrim' command");
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) return ZERO;

  const result = parseTrimOptions(args, 1);
  if ('error' in result) return result.error;

  // Ensure no extra args after trim options
  if (result.nextIdx < args.length) {
    return SYNTAX_ERR;
  }

  const stream = lookup.stream;
  const lengthBefore = stream.length;
  const trimErr = applyTrim(stream, result.options);
  if (trimErr) return trimErr;
  return integerReply(lengthBefore - stream.length);
}

// ─── XSETID ──────────────────────────────────────────────────────────

/**
 * XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
 */
export function xsetid(db: Database, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply('ERR', "wrong number of arguments for 'xsetid' command");
  }

  const key = args[0] as string;
  const idArg = args[1] as string;

  const newId = parseStreamId(idArg);
  if (!newId) return INVALID_STREAM_ID_ERR;

  let entriesAdded = -1;
  let maxDeletedId: StreamId | null = null;

  let i = 2;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'ENTRIESADDED') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const n = Number(val);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      entriesAdded = n;
      i++;
    } else if (upper === 'MAXDELETEDID') {
      i++;
      const val = args[i];
      if (val === undefined) return SYNTAX_ERR;
      const parsed = parseStreamId(val);
      if (!parsed) return INVALID_STREAM_ID_ERR;
      maxDeletedId = parsed;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;

  let stream = lookup.stream;
  if (!stream) {
    stream = new RedisStream();
    db.set(key, 'stream', 'stream', stream);
  }

  // The new ID must be >= current last ID
  if (compareStreamIds(newId, stream.lastId) < 0) {
    return errorReply(
      'ERR',
      'The ID specified in XSETID is smaller than the target stream top item'
    );
  }

  stream.setLastId(newId);

  if (entriesAdded >= 0) {
    stream.setEntriesAdded(entriesAdded);
  }
  if (maxDeletedId) {
    stream.setMaxDeletedEntryId(maxDeletedId);
  }

  return OK;
}

// ─── XCLAIM ──────────────────────────────────────────────────────────

/**
 * XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]
 */
export function xclaim(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 5) {
    return errorReply('ERR', "wrong number of arguments for 'xclaim' command");
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;
  const minIdleStr = args[3] as string;

  const minIdleTime = Number(minIdleStr);
  if (!Number.isInteger(minIdleTime) || minIdleTime < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply(
      'ERR',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XCLAIM"
    );
  }

  const stream = lookup.stream;
  const group = stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XCLAIM"
    );
  }

  // Parse IDs and options
  const claimIds: StreamId[] = [];
  let idle: number | null = null;
  let timeMs: number | null = null;
  let retrycount: number | null = null;
  let force = false;
  let justid = false;
  let _lastid: StreamId | null = null;

  let i = 4;
  // First parse IDs until we hit an option keyword
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (
      upper === 'IDLE' ||
      upper === 'TIME' ||
      upper === 'RETRYCOUNT' ||
      upper === 'FORCE' ||
      upper === 'JUSTID' ||
      upper === 'LASTID'
    ) {
      break;
    }
    const parsed = parseStreamId(args[i] as string);
    if (!parsed) return INVALID_STREAM_ID_ERR;
    claimIds.push(parsed);
    i++;
  }

  if (claimIds.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'xclaim' command");
  }

  // Parse options
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'IDLE') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      idle = n;
      i++;
    } else if (upper === 'TIME') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      timeMs = n;
      i++;
    } else if (upper === 'RETRYCOUNT') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      retrycount = n;
      i++;
    } else if (upper === 'FORCE') {
      force = true;
      i++;
    } else if (upper === 'JUSTID') {
      justid = true;
      i++;
    } else if (upper === 'LASTID') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const parsed = parseStreamId(args[i] as string);
      if (!parsed) return INVALID_STREAM_ID_ERR;
      _lastid = parsed;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  // Determine delivery time for claimed entries
  let deliveryTime: number;
  if (timeMs !== null) {
    deliveryTime = timeMs;
  } else if (idle !== null) {
    deliveryTime = clockMs - idle;
  } else {
    deliveryTime = clockMs;
  }

  // Ensure consumer exists
  const consumer = ensureConsumer(group, consumerName, clockMs);

  // Update LASTID if provided
  if (_lastid !== null) {
    if (compareStreamIds(_lastid, group.lastDeliveredId) > 0) {
      group.lastDeliveredId = { ..._lastid };
    }
  }

  const result: Reply[] = [];

  for (const claimId of claimIds) {
    const entryIdStr = streamIdToString(claimId);
    const pe = group.pel.get(entryIdStr);

    if (!pe) {
      // Not in PEL — only claim if FORCE and entry exists in stream
      if (force && stream.hasEntry(entryIdStr)) {
        const newPe: PendingEntry = {
          entryId: entryIdStr,
          consumer: consumerName,
          deliveryTime,
          deliveryCount: retrycount !== null ? retrycount : 1,
        };
        group.pel.set(entryIdStr, newPe);
        consumer.pending.set(entryIdStr, newPe);

        if (justid) {
          result.push(bulkReply(entryIdStr));
        } else {
          const entries = stream.range(claimId, claimId, 1);
          if (entries.length > 0) {
            result.push(entryToReply(entries[0] as StreamEntry));
          }
        }
      }
      // If not in PEL and not FORCE, skip silently
      continue;
    }

    // Transfer ownership: remove from old consumer
    const oldConsumer = group.consumers.get(pe.consumer);
    if (oldConsumer) {
      oldConsumer.pending.delete(entryIdStr);
    }

    // Update PEL entry
    pe.consumer = consumerName;
    pe.deliveryTime = deliveryTime;
    pe.deliveryCount = retrycount !== null ? retrycount : pe.deliveryCount + 1;

    // Add to new consumer's pending
    consumer.pending.set(entryIdStr, pe);

    if (justid) {
      result.push(bulkReply(entryIdStr));
    } else {
      // Try to find actual entry data
      const entries = stream.range(claimId, claimId, 1);
      if (entries.length > 0) {
        result.push(entryToReply(entries[0] as StreamEntry));
      } else {
        // Entry was deleted — return [id, null]
        result.push(arrayReply([bulkReply(entryIdStr), bulkReply(null)]));
      }
    }
  }

  return arrayReply(result);
}

// ─── XAUTOCLAIM ──────────────────────────────────────────────────────

/**
 * XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
 */
export function xautoclaim(
  db: Database,
  clockMs: number,
  args: string[]
): Reply {
  if (args.length < 5) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xautoclaim' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;
  const consumerName = args[2] as string;
  const minIdleStr = args[3] as string;
  const startArg = args[4] as string;

  const minIdleTime = Number(minIdleStr);
  if (!Number.isInteger(minIdleTime) || minIdleTime < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  // Parse start ID — "0-0" means start from beginning
  const startId = parseStreamId(startArg);
  if (!startId) return INVALID_STREAM_ID_ERR;

  let count = 100; // default
  let justid = false;

  let i = 5;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'COUNT') {
      i++;
      if (i >= args.length) return SYNTAX_ERR;
      const n = Number(args[i]);
      if (!Number.isInteger(n) || n < 0) {
        return errorReply('ERR', 'value is not an integer or out of range');
      }
      count = n;
      i++;
    } else if (upper === 'JUSTID') {
      justid = true;
      i++;
    } else {
      return SYNTAX_ERR;
    }
  }

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply(
      'NOGROUP',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XAUTOCLAIM"
    );
  }

  const stream = lookup.stream;
  const group = stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such key '" +
        key +
        "' or consumer group '" +
        groupName +
        "' in XAUTOCLAIM"
    );
  }

  // Ensure consumer exists
  const consumer = ensureConsumer(group, consumerName, clockMs);

  // Collect eligible PEL entries: ID >= startId and idle >= minIdleTime
  const eligible: PendingEntry[] = [];
  for (const [entryId, pe] of group.pel) {
    const eid = safeParseId(entryId);
    if (compareStreamIds(eid, startId) < 0) continue;
    const idleTime = clockMs - pe.deliveryTime;
    if (idleTime < minIdleTime) continue;
    eligible.push(pe);
  }

  // Sort by entry ID
  eligible.sort((a, b) =>
    compareStreamIds(safeParseId(a.entryId), safeParseId(b.entryId))
  );

  // Apply count limit — take count entries, compute next cursor
  const claimed = eligible.slice(0, count);
  let nextCursor = '0-0';
  if (eligible.length > count && claimed.length > 0) {
    // Next cursor is the ID after the last claimed entry
    const lastClaimed = claimed[claimed.length - 1] as PendingEntry;
    const lastId = safeParseId(lastClaimed.entryId);
    // Increment to get next cursor
    if (lastId.seq < Number.MAX_SAFE_INTEGER) {
      nextCursor = streamIdToString({ ms: lastId.ms, seq: lastId.seq + 1 });
    } else {
      nextCursor = streamIdToString({ ms: lastId.ms + 1, seq: 0 });
    }
  }

  const claimedEntries: Reply[] = [];
  const deletedIds: Reply[] = [];

  for (const pe of claimed) {
    const entryIdStr = pe.entryId;
    const entryExists = stream.hasEntry(entryIdStr);

    if (!entryExists) {
      // Entry was deleted — add to deleted IDs list and remove from PEL
      deletedIds.push(bulkReply(entryIdStr));
      // Remove from old consumer
      const oldConsumer = group.consumers.get(pe.consumer);
      if (oldConsumer) {
        oldConsumer.pending.delete(entryIdStr);
      }
      group.pel.delete(entryIdStr);
      continue;
    }

    // Transfer ownership
    const oldConsumer = group.consumers.get(pe.consumer);
    if (oldConsumer) {
      oldConsumer.pending.delete(entryIdStr);
    }

    pe.consumer = consumerName;
    pe.deliveryTime = clockMs;
    pe.deliveryCount++;

    consumer.pending.set(entryIdStr, pe);

    if (justid) {
      claimedEntries.push(bulkReply(entryIdStr));
    } else {
      const eid = safeParseId(entryIdStr);
      const entries = stream.range(eid, eid, 1);
      if (entries.length > 0) {
        claimedEntries.push(entryToReply(entries[0] as StreamEntry));
      }
    }
  }

  return arrayReply([
    bulkReply(nextCursor),
    arrayReply(claimedEntries),
    arrayReply(deletedIds),
  ]);
}

// ─── XINFO ───────────────────────────────────────────────────────────

function xinfoStream(db: Database, args: string[]): Reply {
  if (args.length < 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xinfo|stream' command"
    );
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply('ERR', 'no such key');
  }

  const stream = lookup.stream;

  // Check for FULL option
  let full = false;
  let fullCount = 10; // default for FULL
  let i = 1;
  while (i < args.length) {
    const upper = (args[i] as string).toUpperCase();
    if (upper === 'FULL') {
      full = true;
      i++;
      if (i < args.length && (args[i] as string).toUpperCase() === 'COUNT') {
        i++;
        if (i >= args.length) return SYNTAX_ERR;
        const n = Number(args[i]);
        if (!Number.isInteger(n) || n < 0) {
          return errorReply('ERR', 'value is not an integer or out of range');
        }
        fullCount = n;
        i++;
      }
    } else {
      return SYNTAX_ERR;
    }
  }

  if (full) {
    return xinfoStreamFull(stream, fullCount);
  }

  // Standard XINFO STREAM response
  const firstEntry = stream.firstEntry();
  const lastEntry = stream.lastEntry();

  const result: Reply[] = [
    bulkReply('length'),
    integerReply(stream.length),
    bulkReply('radix-tree-keys'),
    integerReply(1),
    bulkReply('radix-tree-nodes'),
    integerReply(2),
    bulkReply('last-generated-id'),
    bulkReply(stream.lastIdString),
    bulkReply('max-deleted-entry-id'),
    bulkReply(streamIdToString(stream.maxDeletedEntryId)),
    bulkReply('entries-added'),
    integerReply(stream.entriesAdded),
    bulkReply('recorded-first-entry-id'),
    bulkReply(firstEntry ? firstEntry.id : '0-0'),
    bulkReply('groups'),
    integerReply(stream.groups.size),
    bulkReply('first-entry'),
    firstEntry ? entryToReply(firstEntry) : bulkReply(null),
    bulkReply('last-entry'),
    lastEntry ? entryToReply(lastEntry) : bulkReply(null),
  ];

  return arrayReply(result);
}

function xinfoStreamFull(stream: RedisStream, count: number): Reply {
  const entries = stream.getEntries();
  const limitedEntries = count === 0 ? entries : entries.slice(0, count);

  const groupReplies: Reply[] = [];
  for (const [, group] of stream.groups) {
    const pelEntries: Reply[] = [];
    let pelCount = 0;
    for (const [, pe] of group.pel) {
      if (count > 0 && pelCount >= count) break;
      pelEntries.push(
        arrayReply([
          bulkReply(pe.entryId),
          bulkReply(pe.consumer),
          integerReply(pe.deliveryTime),
          integerReply(pe.deliveryCount),
        ])
      );
      pelCount++;
    }

    const consumerReplies: Reply[] = [];
    for (const [, consumer] of group.consumers) {
      const cPelEntries: Reply[] = [];
      let cPelCount = 0;
      for (const [, pe] of consumer.pending) {
        if (count > 0 && cPelCount >= count) break;
        cPelEntries.push(
          arrayReply([
            bulkReply(pe.entryId),
            integerReply(pe.deliveryTime),
            integerReply(pe.deliveryCount),
          ])
        );
        cPelCount++;
      }

      consumerReplies.push(
        arrayReply([
          bulkReply('name'),
          bulkReply(consumer.name),
          bulkReply('seen-time'),
          integerReply(consumer.seenTime),
          bulkReply('active-time'),
          integerReply(consumer.seenTime),
          bulkReply('pel-count'),
          integerReply(consumer.pending.size),
          bulkReply('pel'),
          arrayReply(cPelEntries),
        ])
      );
    }

    groupReplies.push(
      arrayReply([
        bulkReply('name'),
        bulkReply(group.name),
        bulkReply('last-delivered-id'),
        bulkReply(streamIdToString(group.lastDeliveredId)),
        bulkReply('entries-read'),
        integerReply(group.entriesRead),
        bulkReply('pel-count'),
        integerReply(group.pel.size),
        bulkReply('pel'),
        arrayReply(pelEntries),
        bulkReply('consumers'),
        arrayReply(consumerReplies),
      ])
    );
  }

  const result: Reply[] = [
    bulkReply('length'),
    integerReply(stream.length),
    bulkReply('radix-tree-keys'),
    integerReply(1),
    bulkReply('radix-tree-nodes'),
    integerReply(2),
    bulkReply('last-generated-id'),
    bulkReply(stream.lastIdString),
    bulkReply('max-deleted-entry-id'),
    bulkReply(streamIdToString(stream.maxDeletedEntryId)),
    bulkReply('entries-added'),
    integerReply(stream.entriesAdded),
    bulkReply('recorded-first-entry-id'),
    bulkReply(stream.firstEntry()?.id ?? '0-0'),
    bulkReply('entries'),
    arrayReply(limitedEntries.map(entryToReply)),
    bulkReply('groups'),
    arrayReply(groupReplies),
  ];

  return arrayReply(result);
}

function xinfoGroups(db: Database, args: string[]): Reply {
  if (args.length < 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xinfo|groups' command"
    );
  }

  const key = args[0] as string;
  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply('ERR', 'no such key');
  }

  const stream = lookup.stream;
  const result: Reply[] = [];

  for (const [, group] of stream.groups) {
    result.push(
      arrayReply([
        bulkReply('name'),
        bulkReply(group.name),
        bulkReply('consumers'),
        integerReply(group.consumers.size),
        bulkReply('pending'),
        integerReply(group.pel.size),
        bulkReply('last-delivered-id'),
        bulkReply(streamIdToString(group.lastDeliveredId)),
        bulkReply('entries-read'),
        integerReply(group.entriesRead),
        bulkReply('lag'),
        integerReply(Math.max(0, stream.entriesAdded - group.entriesRead)),
      ])
    );
  }

  return arrayReply(result);
}

function xinfoConsumers(db: Database, clockMs: number, args: string[]): Reply {
  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'xinfo|consumers' command"
    );
  }

  const key = args[0] as string;
  const groupName = args[1] as string;

  const lookup = getStream(db, key);
  if (lookup.error) return lookup.error;
  if (!lookup.stream) {
    return errorReply('ERR', 'no such key');
  }

  const group = lookup.stream.getGroup(groupName);
  if (!group) {
    return errorReply(
      'NOGROUP',
      "No such consumer group '" + groupName + "' for key name '" + key + "'"
    );
  }

  const result: Reply[] = [];
  for (const [, consumer] of group.consumers) {
    const idle = clockMs - consumer.seenTime;
    result.push(
      arrayReply([
        bulkReply('name'),
        bulkReply(consumer.name),
        bulkReply('pending'),
        integerReply(consumer.pending.size),
        bulkReply('idle'),
        integerReply(Math.max(0, idle)),
        bulkReply('inactive'),
        integerReply(Math.max(0, idle)),
      ])
    );
  }

  return arrayReply(result);
}

function xinfo(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply('ERR', "wrong number of arguments for 'xinfo' command");
  }

  const subcommand = (args[0] as string).toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'STREAM':
      return xinfoStream(ctx.db, subArgs);
    case 'GROUPS':
      return xinfoGroups(ctx.db, subArgs);
    case 'CONSUMERS':
      return xinfoConsumers(ctx.db, ctx.engine.clock(), subArgs);
    default:
      return errorReply(
        'ERR',
        `unknown subcommand or wrong number of arguments for 'xinfo|${args[0]}' command`
      );
  }
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
    flags: ['readonly', 'blocking', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@read', '@stream', '@slow', '@blocking'],
  },
  {
    name: 'xgroup',
    handler: (ctx, args) => xgroup(ctx, args),
    arity: -2,
    flags: ['write'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@write', '@stream', '@slow'],
    subcommands: [
      {
        name: 'xgroup|create',
        handler: (ctx, args) => xgroupCreate(ctx.db, args),
        arity: -5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|setid',
        handler: (ctx, args) => xgroupSetid(ctx.db, args),
        arity: -5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|destroy',
        handler: (ctx, args) => xgroupDestroy(ctx.db, args),
        arity: 4,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|delconsumer',
        handler: (ctx, args) => xgroupDelconsumer(ctx.db, args),
        arity: 5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
      {
        name: 'xgroup|createconsumer',
        handler: (ctx, args) => xgroupCreateconsumer(ctx.db, args),
        arity: 5,
        flags: ['write'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@write', '@stream', '@slow'],
      },
    ],
  },
  {
    name: 'xreadgroup',
    handler: (ctx, args) => xreadgroup(ctx.db, ctx.engine.clock(), args),
    arity: -7,
    flags: ['write', 'blocking', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@write', '@stream', '@slow', '@blocking'],
  },
  {
    name: 'xack',
    handler: (ctx, args) => xack(ctx.db, args),
    arity: -4,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xpending',
    handler: (ctx, args) => xpending(ctx.db, ctx.engine.clock(), args),
    arity: -3,
    flags: ['readonly'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
  },
  {
    name: 'xdel',
    handler: (ctx, args) => xdel(ctx.db, args),
    arity: -3,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xtrim',
    handler: (ctx, args) => xtrim(ctx.db, args),
    arity: -4,
    flags: ['write'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@slow'],
  },
  {
    name: 'xsetid',
    handler: (ctx, args) => xsetid(ctx.db, args),
    arity: -3,
    flags: ['write'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@slow'],
  },
  {
    name: 'xclaim',
    handler: (ctx, args) => xclaim(ctx.db, ctx.engine.clock(), args),
    arity: -6,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xautoclaim',
    handler: (ctx, args) => xautoclaim(ctx.db, ctx.engine.clock(), args),
    arity: -7,
    flags: ['write', 'fast'],
    firstKey: 1,
    lastKey: 1,
    keyStep: 1,
    categories: ['@write', '@stream', '@fast'],
  },
  {
    name: 'xinfo',
    handler: (ctx, args) => xinfo(ctx, args),
    arity: -2,
    flags: ['readonly'],
    firstKey: 2,
    lastKey: 2,
    keyStep: 1,
    categories: ['@read', '@stream', '@slow'],
    subcommands: [
      {
        name: 'xinfo|stream',
        handler: (ctx, args) => xinfoStream(ctx.db, args),
        arity: -3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@stream', '@slow'],
      },
      {
        name: 'xinfo|groups',
        handler: (ctx, args) => xinfoGroups(ctx.db, args),
        arity: 3,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@stream', '@slow'],
      },
      {
        name: 'xinfo|consumers',
        handler: (ctx, args) =>
          xinfoConsumers(ctx.db, ctx.engine.clock(), args),
        arity: 4,
        flags: ['readonly'],
        firstKey: 2,
        lastKey: 2,
        keyStep: 1,
        categories: ['@read', '@stream', '@slow'],
      },
    ],
  },
];
