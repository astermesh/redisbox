/**
 * TIME, DEBUG, and MONITOR command implementations.
 */

import type { Database } from '../database.ts';
import type { Reply } from '../types.ts';
import {
  statusReply,
  arrayReply,
  bulkReply,
  OK,
  NO_SUCH_KEY_ERR,
  unknownSubcommandError,
  wrongArityError,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import { getLruClock, estimateIdleTime } from '../memory/lru.ts';
import type { RedisStream } from '../stream.ts';

// ---------------------------------------------------------------------------
// TIME
// ---------------------------------------------------------------------------

/**
 * TIME — returns [unix-timestamp-seconds, microseconds].
 * Redis returns two bulk strings in an array.
 */
export function time(clock: () => number): Reply {
  const nowMs = clock();
  const seconds = Math.floor(nowMs / 1000);
  const microseconds = (nowMs % 1000) * 1000;
  return arrayReply([
    bulkReply(String(seconds)),
    bulkReply(String(microseconds)),
  ]);
}

// ---------------------------------------------------------------------------
// DEBUG OBJECT
// ---------------------------------------------------------------------------

function estimateSerializedLength(entry: {
  type: string;
  value: unknown;
}): number {
  switch (entry.type) {
    case 'string': {
      const v = entry.value as string;
      return v.length;
    }
    case 'list': {
      const arr = entry.value as unknown[];
      let size = 0;
      for (const item of arr) {
        size += String(item).length + 11;
      }
      return size || 1;
    }
    case 'set': {
      const s = entry.value as Set<string>;
      let size = 0;
      for (const item of s) {
        size += item.length + 11;
      }
      return size || 1;
    }
    case 'zset': {
      const m = entry.value as Map<string, number>;
      let size = 0;
      for (const [member] of m) {
        size += member.length + 19;
      }
      return size || 1;
    }
    case 'hash': {
      const h = entry.value as Map<string, string>;
      let size = 0;
      for (const [k, v] of h) {
        size += k.length + v.length + 11;
      }
      return size || 1;
    }
    case 'stream': {
      const s = entry.value as RedisStream;
      let size = 0;
      for (const e of s.getEntries()) {
        size += e.id.length + 8;
        for (const [k, v] of e.fields) {
          size += k.length + v.length + 11;
        }
      }
      return size || 1;
    }
    default:
      return 1;
  }
}

/**
 * DEBUG OBJECT <key> — returns debug information about a key.
 * Format: "Value at:<addr> refcount:<n> encoding:<enc> serializedlength:<len> lru:<lru> lru_seconds_idle:<idle> type:<type>"
 */
export function debugObject(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  const key = args[0] ?? '';
  const entry = db.getWithoutTouch(key);
  if (!entry) return NO_SUCH_KEY_ERR;

  const idle = estimateIdleTime(getLruClock(clock()), entry.lruClock);
  const idleSec = Math.floor(idle / 1000);
  const serialized = estimateSerializedLength(entry);

  const info =
    `Value at:0x0000000000 refcount:1 encoding:${entry.encoding}` +
    ` serializedlength:${serialized}` +
    ` lru:${entry.lruClock}` +
    ` lru_seconds_idle:${idleSec}` +
    ` type:${entry.type}`;

  return statusReply(info);
}

// ---------------------------------------------------------------------------
// DEBUG SLEEP
// ---------------------------------------------------------------------------

/**
 * DEBUG SLEEP <seconds> — in real Redis, blocks the server for N seconds.
 * Since RedisBox is an in-memory emulator, we acknowledge but don't actually block.
 * Real Redis uses strtod() and always returns OK regardless of input.
 */
export function debugSleep(): Reply {
  return OK;
}

// ---------------------------------------------------------------------------
// DEBUG SET-ACTIVE-EXPIRE
// ---------------------------------------------------------------------------

/**
 * DEBUG SET-ACTIVE-EXPIRE <0|1> — enable or disable active expiration.
 * Stub: acknowledges but doesn't affect active expiration cycle.
 * Real Redis uses atoi() and always returns OK regardless of input.
 */
export function debugSetActiveExpire(): Reply {
  return OK;
}

// ---------------------------------------------------------------------------
// DEBUG HELP
// ---------------------------------------------------------------------------

export function debugHelp(): Reply {
  return arrayReply([
    bulkReply('DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'),
    bulkReply('OBJECT <key>'),
    bulkReply('    Show low-level info about key and associated value.'),
    bulkReply('SLEEP <seconds>'),
    bulkReply(
      '    Sleep (pause the server) for the specified number of seconds.'
    ),
    bulkReply('SET-ACTIVE-EXPIRE <0|1>'),
    bulkReply('    Enable/disable active expiration.'),
    bulkReply('HELP'),
    bulkReply('    Print this help message.'),
  ]);
}

// ---------------------------------------------------------------------------
// DEBUG dispatcher
// ---------------------------------------------------------------------------

export function debug(
  db: Database,
  clock: () => number,
  args: string[]
): Reply {
  if (args.length === 0) {
    return wrongArityError('debug');
  }

  const subcommand = (args[0] ?? '').toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'OBJECT':
      if (subArgs.length !== 1) {
        return wrongArityError('debug|object');
      }
      return debugObject(db, clock, subArgs);
    case 'SLEEP':
      if (subArgs.length !== 1) {
        return wrongArityError('debug|sleep');
      }
      return debugSleep();
    case 'SET-ACTIVE-EXPIRE':
      if (subArgs.length !== 1) {
        return wrongArityError('debug|set-active-expire');
      }
      return debugSetActiveExpire();
    case 'HELP':
      return debugHelp();
    default:
      return unknownSubcommandError('debug', (args[0] ?? '').toLowerCase());
  }
}

// ---------------------------------------------------------------------------
// MONITOR
// ---------------------------------------------------------------------------

/**
 * MONITOR — enters monitor mode. In real Redis, the client receives copies
 * of all commands processed by the server. Since RedisBox is an in-memory
 * emulator without persistent connections, we return OK to acknowledge.
 * Server-level integration would route commands to monitored clients.
 */
export function monitor(): Reply {
  return OK;
}

// ---------------------------------------------------------------------------
// Command specs
// ---------------------------------------------------------------------------

export const specs: CommandSpec[] = [
  {
    name: 'time',
    handler: (ctx) => time(ctx.engine.clock),
    arity: 1,
    flags: ['readonly', 'fast', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@read'],
  },
  {
    name: 'debug',
    handler: (ctx, args) => debug(ctx.db, ctx.engine.clock, args),
    arity: -2,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
    subcommands: [
      {
        name: 'object',
        handler: (ctx, args) =>
          debug(ctx.db, ctx.engine.clock, ['OBJECT', ...args]),
        arity: 3,
        flags: ['admin', 'noscript', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'sleep',
        handler: (ctx, args) =>
          debug(ctx.db, ctx.engine.clock, ['SLEEP', ...args]),
        arity: 3,
        flags: ['admin', 'noscript', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'set-active-expire',
        handler: (ctx, args) =>
          debug(ctx.db, ctx.engine.clock, ['SET-ACTIVE-EXPIRE', ...args]),
        arity: 3,
        flags: ['admin', 'noscript', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'help',
        handler: () => debugHelp(),
        arity: 2,
        flags: ['admin', 'noscript', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
    ],
  },
  {
    name: 'monitor',
    handler: () => monitor(),
    arity: 1,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
];
