import type { Reply } from '../types.ts';
import { OK, errorReply } from '../types.ts';

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
