import type { Reply } from '../types.ts';
import { statusReply, bulkReply, OK } from '../types.ts';

const PONG: Reply = statusReply('PONG');
const RESET_REPLY: Reply = statusReply('RESET');

export function ping(args: string[]): Reply {
  if (args.length === 0) return PONG;
  return bulkReply(args[0] ?? '');
}

export function echo(args: string[]): Reply {
  return bulkReply(args[0] ?? '');
}

export function quit(): Reply {
  return OK;
}

export function reset(): Reply {
  return RESET_REPLY;
}
