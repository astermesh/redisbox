import type { Reply, CommandContext } from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import { statusReply, integerReply, errorReply, OK } from '../types.ts';

// --- Command implementations ---

export function bgsave(args: string[]): Reply {
  if (args.length > 0) {
    const sub = (args[0] ?? '').toUpperCase();
    if (sub !== 'SCHEDULE') {
      return errorReply(
        'ERR',
        "Unknown option or wrong number of arguments for 'bgsave' command"
      );
    }
  }
  return statusReply('Background saving started');
}

export function bgrewriteaof(): Reply {
  return statusReply('Background append only file rewriting started');
}

export function save(): Reply {
  return OK;
}

export function lastsave(ctx: CommandContext): Reply {
  return integerReply(Math.floor(ctx.engine.clock() / 1000));
}

const SHUTDOWN_OPTIONS = new Set(['NOSAVE', 'SAVE', 'NOW', 'FORCE']);

export function shutdown(args: string[]): Reply {
  let hasNosave = false;
  let hasSave = false;

  for (const arg of args) {
    const upper = arg.toUpperCase();
    if (!SHUTDOWN_OPTIONS.has(upper)) {
      return errorReply(
        'ERR',
        `Unrecognized option or bad number of args for SHUTDOWN: '${arg}'`
      );
    }
    if (upper === 'NOSAVE') hasNosave = true;
    if (upper === 'SAVE') hasSave = true;
  }

  if (hasNosave && hasSave) {
    return errorReply(
      'ERR',
      `Unrecognized option or bad number of args for SHUTDOWN: 'SAVE'`
    );
  }

  // Stub: acknowledge without actual shutdown
  return OK;
}

// --- Command specs ---

export const specs: CommandSpec[] = [
  {
    name: 'bgsave',
    handler: (_ctx, args) => bgsave(args),
    arity: -1,
    flags: ['admin', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
  {
    name: 'bgrewriteaof',
    handler: () => bgrewriteaof(),
    arity: 1,
    flags: ['admin', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
  {
    name: 'save',
    handler: () => save(),
    arity: 1,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
  {
    name: 'lastsave',
    handler: (ctx) => lastsave(ctx),
    arity: 1,
    flags: ['admin', 'noscript', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@fast', '@dangerous'],
  },
  {
    name: 'shutdown',
    handler: (_ctx, args) => shutdown(args),
    arity: -1,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
];
