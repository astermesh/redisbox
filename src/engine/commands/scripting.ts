/**
 * EVAL/EVALSHA/EVAL_RO/EVALSHA_RO and SCRIPT command handlers.
 *
 * Lua scripts run atomically via the ScriptManager.
 * KEYS and ARGV are populated from command arguments.
 * Read-only variants reject write commands inside scripts.
 * SCRIPT subcommands manage the per-server script cache.
 */

import type { Reply, CommandContext } from '../types.ts';
import {
  errorReply,
  statusReply,
  bulkReply,
  integerReply,
  arrayReply,
  unknownSubcommandError,
} from '../types.ts';
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

/**
 * SCRIPT LOAD script
 */
function scriptLoad(ctx: CommandContext, args: string[]): Reply {
  if (args.length !== 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'script|load' command"
    );
  }

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  const script = args[0] ?? '';

  // Redis compiles the script on LOAD and returns an error for syntax errors
  const syntaxError = mgr.validateScript(script);
  if (syntaxError) {
    return errorReply('ERR', syntaxError);
  }

  const digest = mgr.cacheScript(script);
  return bulkReply(digest);
}

/**
 * SCRIPT EXISTS sha1 [sha1 ...]
 */
function scriptExists(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'script|exists' command"
    );
  }

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  const results: Reply[] = args.map((sha) =>
    integerReply(mgr.hasScript(sha) ? 1 : 0)
  );
  return arrayReply(results);
}

/**
 * SCRIPT FLUSH [ASYNC|SYNC]
 */
function scriptFlush(ctx: CommandContext, args: string[]): Reply {
  if (args.length > 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'script|flush' command"
    );
  }

  if (args.length === 1) {
    const mode = (args[0] ?? '').toUpperCase();
    if (mode !== 'ASYNC' && mode !== 'SYNC') {
      return errorReply('ERR', 'SCRIPT FLUSH only supports ASYNC|SYNC option');
    }
  }

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  mgr.flushScripts();
  return statusReply('OK');
}

/**
 * SCRIPT DEBUG YES|SYNC|NO (stub — debugging is not supported)
 */
function scriptDebug(_ctx: CommandContext, args: string[]): Reply {
  if (args.length !== 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'script|debug' command"
    );
  }

  const mode = (args[0] ?? '').toUpperCase();
  if (mode !== 'YES' && mode !== 'SYNC' && mode !== 'NO') {
    return errorReply('ERR', 'Use SCRIPT DEBUG YES/SYNC/NO');
  }

  return statusReply('OK');
}

/**
 * SCRIPT HELP — return help text for SCRIPT subcommands.
 */
function scriptHelp(): Reply {
  return arrayReply([
    bulkReply(
      'SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
    ),
    bulkReply('DEBUG (YES|SYNC|NO)'),
    bulkReply('    Set the debug mode for subsequent scripts executed.'),
    bulkReply('EXISTS <sha1> [<sha1> ...]'),
    bulkReply(
      '    Return information about the existence of the scripts in the script cache.'
    ),
    bulkReply('FLUSH [ASYNC|SYNC]'),
    bulkReply(
      '    Flush the Lua scripts cache. Defaults to ASYNC, but can be SYNC.'
    ),
    bulkReply('HELP'),
    bulkReply('    Prints this help.'),
    bulkReply('LOAD <script>'),
    bulkReply('    Load a script into the scripts cache without executing it.'),
  ]);
}

/**
 * SCRIPT <subcommand> [args ...]
 */
export function scriptCmd(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('script', '');
  }

  const sub = (args[0] ?? '').toUpperCase();
  const rest = args.slice(1);

  switch (sub) {
    case 'LOAD':
      return scriptLoad(ctx, rest);
    case 'EXISTS':
      return scriptExists(ctx, rest);
    case 'FLUSH':
      return scriptFlush(ctx, rest);
    case 'DEBUG':
      return scriptDebug(ctx, rest);
    case 'HELP':
      return scriptHelp();
    default:
      return unknownSubcommandError('script', (args[0] ?? '').toLowerCase());
  }
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
  {
    name: 'script',
    handler: (ctx, args) => scriptCmd(ctx, args),
    arity: -2,
    flags: ['noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
    subcommands: [
      {
        name: 'load',
        handler: (ctx, args) => scriptLoad(ctx, args),
        arity: 3,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'exists',
        handler: (ctx, args) => scriptExists(ctx, args),
        arity: -3,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'flush',
        handler: (ctx, args) => scriptFlush(ctx, args),
        arity: -2,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'debug',
        handler: (ctx, args) => scriptDebug(ctx, args),
        arity: 3,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'help',
        handler: () => scriptHelp(),
        arity: 2,
        flags: ['loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
    ],
  },
];
