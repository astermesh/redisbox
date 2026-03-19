import type { Reply } from '../types.ts';
import { OK } from '../types.ts';

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
