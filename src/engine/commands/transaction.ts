import type { Reply } from '../types.ts';
import { OK, errorReply } from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import type { CommandContext } from '../types.ts';

/**
 * MULTI
 * Marks the start of a transaction block.
 * Subsequent commands will be queued for atomic execution via EXEC.
 *
 * State management (inMulti, multiQueue, flagMulti) is handled
 * by the CommandDispatcher, not here — because the handler
 * doesn't have access to the dispatcher's ClientState.
 */
export function multi(): Reply {
  return OK;
}

/**
 * EXEC
 * Executes all commands issued after MULTI.
 *
 * Actual execution logic is in CommandDispatcher.execTransaction().
 * This handler is only called outside MULTI as a fallback.
 */
export function exec(): Reply {
  return errorReply('ERR', 'EXEC without MULTI');
}

/**
 * DISCARD
 * Flushes all previously queued commands in a transaction and restores
 * the connection state to normal.
 *
 * Actual discard logic is in CommandDispatcher.discardTransaction().
 * This handler is only called outside MULTI as a fallback.
 */
export function discard(): Reply {
  return errorReply('ERR', 'DISCARD without MULTI');
}

/**
 * WATCH key [key ...]
 * Marks the given keys to be watched for conditional execution of a transaction.
 *
 * Version recording into TransactionState.watchedKeys is handled by the
 * CommandDispatcher — the handler only validates and returns OK.
 */
export function watch(_ctx: CommandContext, _args: string[]): Reply {
  return OK;
}

/**
 * UNWATCH
 * Flushes all the previously watched keys for a transaction.
 *
 * Actual clearing of watchedKeys is handled by the CommandDispatcher.
 */
export function unwatch(): Reply {
  return OK;
}

export const specs: CommandSpec[] = [
  {
    name: 'multi',
    handler: () => multi(),
    arity: 1,
    flags: ['noscript', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@transaction'],
  },
  {
    name: 'exec',
    handler: () => exec(),
    arity: 1,
    flags: ['noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@transaction'],
  },
  {
    name: 'discard',
    handler: () => discard(),
    arity: 1,
    flags: ['noscript', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@transaction'],
  },
  {
    name: 'watch',
    handler: (ctx, args) => watch(ctx, args),
    arity: -2,
    flags: ['noscript', 'loading', 'stale', 'fast'],
    firstKey: 1,
    lastKey: -1,
    keyStep: 1,
    categories: ['@fast', '@transaction'],
  },
  {
    name: 'unwatch',
    handler: () => unwatch(),
    arity: 1,
    flags: ['noscript', 'loading', 'stale', 'fast'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@fast', '@transaction'],
  },
];
