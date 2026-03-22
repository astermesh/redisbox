/**
 * EVAL/EVALSHA/EVAL_RO/EVALSHA_RO command handlers.
 *
 * Lua scripts run atomically via the ScriptManager.
 * KEYS and ARGV are populated from command arguments.
 * Read-only variants reject write commands inside scripts.
 */

import type { Reply, CommandContext } from '../types.ts';
import { errorReply } from '../types.ts';
import type { CommandSpec } from '../command-table.ts';
import type { CommandExecutor } from '../scripting/redis-bridge.ts';

/**
 * Parse numkeys and split remaining args into keys and argv.
 */
function parseEvalArgs(
  args: string[]
): { keys: string[]; argv: string[] } | Reply {
  const numkeysStr = args[0] ?? '';
  const numkeys = parseInt(numkeysStr, 10);

  if (isNaN(numkeys) || numkeysStr !== String(numkeys)) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  if (numkeys < 0) {
    return errorReply('ERR', "Number of keys can't be negative");
  }

  if (numkeys > args.length - 1) {
    return errorReply(
      'ERR',
      "Number of keys can't be greater than number of args"
    );
  }

  return {
    keys: args.slice(1, 1 + numkeys),
    argv: args.slice(1 + numkeys),
  };
}

/**
 * Create a synchronous command executor for redis.call/redis.pcall inside Lua.
 * Enforces noscript flag and arity checking.
 */
function makeExecutor(ctx: CommandContext): CommandExecutor {
  return (rawArgs: string[]) => {
    if (!ctx.commandTable || rawArgs.length === 0) {
      return errorReply(
        'ERR',
        `unknown command '${rawArgs[0] ?? ''}', with args beginning with: `
      );
    }

    const cmdName = rawArgs[0] ?? '';
    const args = rawArgs.slice(1);

    const def = ctx.commandTable.get(cmdName);
    if (!def) {
      const argsStr = args.map((a) => `'${a}'`).join(' ');
      return errorReply(
        'ERR',
        `unknown command '${cmdName}', with args beginning with: ${argsStr}`
      );
    }

    // Commands marked noscript can't run from scripts
    if (def.flags.has('noscript')) {
      return errorReply('ERR', 'This Redis command is not allowed from script');
    }

    // Check arity
    const arityCheck = ctx.commandTable.checkArity(def, rawArgs.length);
    if (arityCheck) return arityCheck;

    return def.handler(ctx, args);
  };
}

/**
 * EVAL script numkeys key [key ...] arg [arg ...]
 */
export function evalCmd(ctx: CommandContext, args: string[]): Reply {
  return evalCommon(ctx, args, false);
}

/**
 * EVALSHA sha1 numkeys key [key ...] arg [arg ...]
 */
export function evalshaCmd(ctx: CommandContext, args: string[]): Reply {
  return evalshaCommon(ctx, args, false);
}

/**
 * EVAL_RO script numkeys key [key ...] arg [arg ...]
 */
export function evalRoCmd(ctx: CommandContext, args: string[]): Reply {
  return evalCommon(ctx, args, true);
}

/**
 * EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
 */
export function evalshaRoCmd(ctx: CommandContext, args: string[]): Reply {
  return evalshaCommon(ctx, args, true);
}

function evalCommon(
  ctx: CommandContext,
  args: string[],
  readOnly: boolean
): Reply {
  const script = args[0] ?? '';
  const parsed = parseEvalArgs(args.slice(1));
  if ('kind' in parsed) return parsed;

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  const executor = makeExecutor(ctx);
  return mgr.evalScript(
    script,
    parsed.keys,
    parsed.argv,
    readOnly,
    ctx.commandTable,
    executor
  );
}

function evalshaCommon(
  ctx: CommandContext,
  args: string[],
  readOnly: boolean
): Reply {
  const sha = (args[0] ?? '').toLowerCase();
  const parsed = parseEvalArgs(args.slice(1));
  if ('kind' in parsed) return parsed;

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  const script = mgr.getScript(sha);
  if (script === undefined) {
    return errorReply('NOSCRIPT', 'No matching script. Use EVAL.');
  }

  const executor = makeExecutor(ctx);
  return mgr.evalScript(
    script,
    parsed.keys,
    parsed.argv,
    readOnly,
    ctx.commandTable,
    executor
  );
}

export const specs: CommandSpec[] = [
  {
    name: 'eval',
    handler: (ctx, args) => evalCmd(ctx, args),
    arity: -3,
    flags: ['noscript', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
  },
  {
    name: 'evalsha',
    handler: (ctx, args) => evalshaCmd(ctx, args),
    arity: -3,
    flags: ['noscript', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
  },
  {
    name: 'eval_ro',
    handler: (ctx, args) => evalRoCmd(ctx, args),
    arity: -3,
    flags: ['noscript', 'readonly', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
  },
  {
    name: 'evalsha_ro',
    handler: (ctx, args) => evalshaRoCmd(ctx, args),
    arity: -3,
    flags: ['noscript', 'readonly', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
  },
];
