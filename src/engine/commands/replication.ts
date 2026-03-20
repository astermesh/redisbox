import type { Reply, CommandContext } from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import {
  statusReply,
  integerReply,
  arrayReply,
  errorReply,
  OK,
} from '../types.ts';

// --- Consistent replication ID (40-char hex, matches info replication output) ---

const REPL_ID = '0'.repeat(40);

// --- Command implementations ---

export function replicaof(args: string[]): Reply {
  if (args.length !== 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'replicaof' command"
    );
  }
  const host = (args[0] ?? '').toUpperCase();
  const port = (args[1] ?? '').toUpperCase();

  if (host === 'NO' && port === 'ONE') {
    return OK;
  }

  const portNum = parseInt(args[1] ?? '', 10);
  if (isNaN(portNum)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  // Accept silently — no actual replication in RedisBox
  return OK;
}

export function slaveof(args: string[]): Reply {
  return replicaof(args);
}

export function replconf(_args: string[]): Reply {
  return OK;
}

export function psync(_args: string[]): Reply {
  return statusReply(`FULLRESYNC ${REPL_ID} 0`);
}

export function wait(ctx: CommandContext, args: string[]): Reply {
  void ctx;
  if (args.length !== 2) {
    return errorReply('ERR', "wrong number of arguments for 'wait' command");
  }
  const numreplicas = parseInt(args[0] ?? '', 10);
  const timeout = parseInt(args[1] ?? '', 10);
  if (isNaN(numreplicas)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  if (isNaN(timeout)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  // No replicas in RedisBox — always return 0 (negative timeout clamped to 0 like Redis)
  return integerReply(0);
}

export function waitaof(ctx: CommandContext, args: string[]): Reply {
  void ctx;
  if (args.length !== 3) {
    return errorReply('ERR', "wrong number of arguments for 'waitaof' command");
  }
  const numlocal = parseInt(args[0] ?? '', 10);
  const numreplicas = parseInt(args[1] ?? '', 10);
  const timeout = parseInt(args[2] ?? '', 10);
  if (isNaN(numlocal) || isNaN(numreplicas) || isNaN(timeout)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }
  // No AOF or replicas in RedisBox — return [0, 0]
  return arrayReply([integerReply(0), integerReply(0)]);
}

// --- Command specs ---

export const specs: CommandSpec[] = [
  {
    name: 'replicaof',
    handler: (_ctx, args) => replicaof(args),
    arity: 3,
    flags: ['admin', 'noscript', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
  {
    name: 'slaveof',
    handler: (_ctx, args) => slaveof(args),
    arity: 3,
    flags: ['admin', 'noscript', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
  {
    name: 'replconf',
    handler: (_ctx, args) => replconf(args),
    arity: -1,
    flags: ['admin', 'noscript', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
  },
  {
    name: 'psync',
    handler: (_ctx, args) => psync(args),
    arity: -3,
    flags: ['admin', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow'],
  },
  {
    name: 'wait',
    handler: (ctx, args) => wait(ctx, args),
    arity: 3,
    flags: ['blocking', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@connection'],
  },
  {
    name: 'waitaof',
    handler: (ctx, args) => waitaof(ctx, args),
    arity: 4,
    flags: ['blocking', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@connection'],
  },
];
