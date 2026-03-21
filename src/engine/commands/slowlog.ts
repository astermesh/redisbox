/**
 * SLOWLOG command implementation.
 *
 * Subcommands: GET, LEN, RESET, HELP
 */

import type { Reply, CommandContext } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  errorReply,
  integerReply,
  unknownSubcommandError,
  OK,
  EMPTY_ARRAY,
} from '../types.ts';
import type { CommandSpec } from '../command-table.ts';

const SLOWLOG_COUNT_ERR = errorReply(
  'ERR',
  'count should be greater than or equal to -1'
);

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

export function slowlogGet(ctx: CommandContext, args: string[]): Reply {
  const slowlog = ctx.engine.slowlog;

  let count: number | undefined;
  if (args.length > 0) {
    const raw = args[0] ?? '';
    const n = parseInt(raw, 10);
    if (isNaN(n) || raw !== String(n)) {
      return SLOWLOG_COUNT_ERR;
    }
    if (n < -1) {
      return SLOWLOG_COUNT_ERR;
    }
    count = n;
  }

  // When count is undefined, SlowlogManager.get() returns default 10
  const entries = slowlog.get(count);
  if (entries.length === 0) return EMPTY_ARRAY;

  const result: Reply[] = entries.map((e) => {
    const cmdArgs: Reply[] = e.args.map((a) => bulkReply(a));
    return arrayReply([
      integerReply(e.id),
      integerReply(e.timestamp),
      integerReply(e.duration),
      arrayReply(cmdArgs),
      bulkReply(e.clientAddr),
      bulkReply(e.clientName),
    ]);
  });

  return arrayReply(result);
}

export function slowlogLen(ctx: CommandContext): Reply {
  return integerReply(ctx.engine.slowlog.len());
}

export function slowlogReset(ctx: CommandContext): Reply {
  ctx.engine.slowlog.reset();
  return OK;
}

export function slowlogHelp(): Reply {
  const lines = [
    'SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:',
    'GET [<count>]',
    '    Return top <count> entries from the slowlog (default: 10, -1 mean all).',
    '    Entries are made of:',
    '        id, timestamp, time in microseconds, arguments vector, client',
    '        IP and port, client name',
    'LEN',
    '    Return the length of the slowlog.',
    'RESET',
    '    Reset the slowlog.',
    'HELP',
    '    Print this help.',
  ];
  return arrayReply(lines.map((l) => bulkReply(l)));
}

// ---------------------------------------------------------------------------
// Main dispatcher
// ---------------------------------------------------------------------------

function slowlog(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('slowlog', '');
  }

  const sub = (args[0] ?? '').toUpperCase();

  switch (sub) {
    case 'GET':
      return slowlogGet(ctx, args.slice(1));
    case 'LEN':
      return slowlogLen(ctx);
    case 'RESET':
      return slowlogReset(ctx);
    case 'HELP':
      return slowlogHelp();
    default:
      return unknownSubcommandError('slowlog', args[0] ?? '');
  }
}

// ---------------------------------------------------------------------------
// Command spec
// ---------------------------------------------------------------------------

export const specs: CommandSpec[] = [
  {
    name: 'SLOWLOG',
    handler: slowlog,
    arity: -2,
    flags: ['admin', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
    subcommands: [
      {
        name: 'GET',
        handler: slowlog,
        arity: -2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'LEN',
        handler: slowlog,
        arity: 2,
        flags: ['admin', 'loading', 'stale', 'fast'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'RESET',
        handler: slowlog,
        arity: 2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
      {
        name: 'HELP',
        handler: slowlog,
        arity: 2,
        flags: ['admin', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@admin', '@slow', '@dangerous'],
      },
    ],
  },
];
