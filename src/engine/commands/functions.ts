/**
 * FUNCTION and FCALL/FCALL_RO command handlers.
 *
 * FUNCTION subcommands manage the library registry:
 *   LOAD, DELETE, LIST, FLUSH, DUMP (stub), RESTORE (stub), STATS, HELP
 *
 * FCALL/FCALL_RO execute registered functions with KEYS and ARGV.
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
import type { FunctionFlags } from '../scripting/function-registry.ts';

/**
 * Parse numkeys and split remaining args into keys and argv.
 * Reused by FCALL and FCALL_RO (same format as EVAL).
 */
function parseNumkeysArgs(
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
 * Create a synchronous command executor for redis.call/redis.pcall inside functions.
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

    if (def.flags.has('noscript')) {
      return errorReply('ERR', 'This Redis command is not allowed from script');
    }

    const arityCheck = ctx.commandTable.checkArity(def, rawArgs.length);
    if (arityCheck) return arityCheck;

    return def.handler(ctx, args);
  };
}

// ---- FCALL / FCALL_RO ----

/**
 * FCALL function numkeys key [key ...] arg [arg ...]
 */
export function fcallCmd(ctx: CommandContext, args: string[]): Reply {
  return fcallCommon(ctx, args, false);
}

/**
 * FCALL_RO function numkeys key [key ...] arg [arg ...]
 */
export function fcallRoCmd(ctx: CommandContext, args: string[]): Reply {
  return fcallCommon(ctx, args, true);
}

function fcallCommon(
  ctx: CommandContext,
  args: string[],
  readOnly: boolean
): Reply {
  const funcName = args[0] ?? '';
  const parsed = parseNumkeysArgs(args.slice(1));
  if ('kind' in parsed) return parsed;

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  const executor = makeExecutor(ctx);
  return mgr.callFunction(
    funcName,
    parsed.keys,
    parsed.argv,
    readOnly,
    ctx.commandTable,
    executor
  );
}

// ---- FUNCTION subcommands ----

/**
 * FUNCTION LOAD [REPLACE] function-code
 */
function functionLoad(ctx: CommandContext, args: string[]): Reply {
  let replace = false;
  let code: string;

  if (args.length === 1) {
    code = args[0] ?? '';
  } else if (args.length === 2) {
    if ((args[0] ?? '').toUpperCase() !== 'REPLACE') {
      return errorReply(
        'ERR',
        "wrong number of arguments for 'function|load' command"
      );
    }
    replace = true;
    code = args[1] ?? '';
  } else {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'function|load' command"
    );
  }

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  const executor = makeExecutor(ctx);
  return mgr.loadLibrary(code, replace, executor);
}

/**
 * FUNCTION DELETE library-name
 */
function functionDelete(ctx: CommandContext, args: string[]): Reply {
  if (args.length !== 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'function|delete' command"
    );
  }

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  return mgr.deleteLibrary(args[0] ?? '');
}

/**
 * FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
 */
function functionList(ctx: CommandContext, args: string[]): Reply {
  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  let pattern: string | undefined;
  let withCode = false;

  let i = 0;
  while (i < args.length) {
    const arg = (args[i] ?? '').toUpperCase();
    if (arg === 'LIBRARYNAME') {
      i++;
      if (i >= args.length) {
        return errorReply('ERR', 'Missing library name pattern');
      }
      pattern = args[i] ?? '';
    } else if (arg === 'WITHCODE') {
      withCode = true;
    } else {
      return errorReply('ERR', `Invalid argument: ${args[i]}`);
    }
    i++;
  }

  const libraries = mgr.registry.listLibraries(pattern);
  const result: Reply[] = [];

  for (const lib of libraries) {
    const libEntry: Reply[] = [
      bulkReply('library_name'),
      bulkReply(lib.name),
      bulkReply('engine'),
      bulkReply(lib.engine),
      bulkReply('functions'),
    ];

    // Build functions array
    const funcEntries: Reply[] = [];
    for (const [, func] of lib.functions) {
      const funcEntry: Reply[] = [
        bulkReply('name'),
        bulkReply(func.name),
        bulkReply('description'),
        bulkReply(func.description || null),
        bulkReply('flags'),
        arrayReply(flagsToReply(func.flags)),
      ];
      funcEntries.push(arrayReply(funcEntry));
    }
    libEntry.push(arrayReply(funcEntries));

    if (withCode) {
      libEntry.push(bulkReply('library_code'));
      libEntry.push(bulkReply(lib.code));
    }

    result.push(arrayReply(libEntry));
  }

  return arrayReply(result);
}

/**
 * FUNCTION FLUSH [ASYNC|SYNC]
 */
function functionFlush(ctx: CommandContext, args: string[]): Reply {
  if (args.length > 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'function|flush' command"
    );
  }

  if (args.length === 1) {
    const mode = (args[0] ?? '').toUpperCase();
    if (mode !== 'ASYNC' && mode !== 'SYNC') {
      return errorReply(
        'ERR',
        'FUNCTION FLUSH only supports ASYNC|SYNC option'
      );
    }
  }

  const mgr = ctx.scriptManager;
  if (!mgr || !mgr.ready) {
    return errorReply('ERR', 'Lua engine not initialized');
  }

  mgr.flushFunctions();
  return statusReply('OK');
}

/**
 * FUNCTION DUMP — stub returning empty bulk string.
 */
function functionDump(): Reply {
  return bulkReply('');
}

/**
 * FUNCTION RESTORE serialized-data [FLUSH|APPEND|REPLACE] — stub.
 */
function functionRestore(_ctx: CommandContext, _args: string[]): Reply {
  return statusReply('OK');
}

/**
 * FUNCTION STATS — return running script info and per-engine stats.
 */
function functionStats(ctx: CommandContext): Reply {
  const mgr = ctx.scriptManager;
  const libCount = mgr?.ready ? mgr.registry.libraryCount : 0;
  const funcCount = mgr?.ready ? mgr.registry.functionCount : 0;

  return arrayReply([
    bulkReply('running_script'),
    bulkReply(null),
    bulkReply('engines'),
    arrayReply([
      bulkReply('LUA'),
      arrayReply([
        bulkReply('libraries_count'),
        integerReply(libCount),
        bulkReply('functions_count'),
        integerReply(funcCount),
      ]),
    ]),
  ]);
}

/**
 * FUNCTION HELP
 */
function functionHelp(): Reply {
  return arrayReply([
    bulkReply(
      'FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
    ),
    bulkReply('DELETE <library-name>'),
    bulkReply('    Delete a function library.'),
    bulkReply('DUMP'),
    bulkReply('    Dump all function libraries.'),
    bulkReply('FLUSH [ASYNC|SYNC]'),
    bulkReply('    Delete all function libraries.'),
    bulkReply('HELP'),
    bulkReply('    Prints this help.'),
    bulkReply('LIST [LIBRARYNAME pattern] [WITHCODE]'),
    bulkReply('    List all function libraries.'),
    bulkReply('LOAD [REPLACE] <function-code>'),
    bulkReply('    Load a function library.'),
    bulkReply('RESTORE <serialized-data> [FLUSH|APPEND|REPLACE]'),
    bulkReply('    Restore function libraries from dump.'),
    bulkReply('STATS'),
    bulkReply('    Show function statistics.'),
  ]);
}

/**
 * FUNCTION <subcommand> [args ...]
 */
export function functionCmd(ctx: CommandContext, args: string[]): Reply {
  const sub = (args[0] ?? '').toUpperCase();
  const rest = args.slice(1);

  switch (sub) {
    case 'LOAD':
      return functionLoad(ctx, rest);
    case 'DELETE':
      return functionDelete(ctx, rest);
    case 'LIST':
      return functionList(ctx, rest);
    case 'FLUSH':
      return functionFlush(ctx, rest);
    case 'DUMP':
      return functionDump();
    case 'RESTORE':
      return functionRestore(ctx, rest);
    case 'STATS':
      return functionStats(ctx);
    case 'HELP':
      return functionHelp();
    default:
      return unknownSubcommandError('function', (args[0] ?? '').toLowerCase());
  }
}

/** Convert FunctionFlags to an array of bulk string replies. */
function flagsToReply(flags: FunctionFlags): Reply[] {
  const result: Reply[] = [];
  if (flags.noWrites) result.push(bulkReply('no-writes'));
  if (flags.allowOom) result.push(bulkReply('allow-oom'));
  if (flags.allowStale) result.push(bulkReply('allow-stale'));
  if (flags.noCluster) result.push(bulkReply('no-cluster'));
  return result;
}

export const specs: CommandSpec[] = [
  {
    name: 'fcall',
    handler: (ctx, args) => fcallCmd(ctx, args),
    arity: -3,
    flags: ['noscript', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
  },
  {
    name: 'fcall_ro',
    handler: (ctx, args) => fcallRoCmd(ctx, args),
    arity: -3,
    flags: ['noscript', 'readonly', 'movablekeys'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
  },
  {
    name: 'function',
    handler: (ctx, args) => functionCmd(ctx, args),
    arity: -2,
    flags: ['noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@scripting'],
    subcommands: [
      {
        name: 'load',
        handler: (ctx, args) => functionLoad(ctx, args),
        arity: -3,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'delete',
        handler: (ctx, args) => functionDelete(ctx, args),
        arity: 3,
        flags: ['noscript', 'write'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'list',
        handler: (ctx, args) => functionList(ctx, args),
        arity: -2,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'flush',
        handler: (ctx, args) => functionFlush(ctx, args),
        arity: -2,
        flags: ['noscript', 'write'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'dump',
        handler: () => functionDump(),
        arity: 2,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'restore',
        handler: (ctx, args) => functionRestore(ctx, args),
        arity: -3,
        flags: ['noscript', 'write'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'stats',
        handler: (ctx) => functionStats(ctx),
        arity: 2,
        flags: ['noscript'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@scripting'],
      },
      {
        name: 'help',
        handler: () => functionHelp(),
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
