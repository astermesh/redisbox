import type {
  CommandTable,
  CommandDefinition,
  CommandSpec,
} from '../command-table.ts';
import type { Reply, CommandContext } from '../types.ts';
import {
  arrayReply,
  bulkReply,
  statusReply,
  integerReply,
  errorReply,
  wrongArityError,
  unknownSubcommandError,
  EMPTY_ARRAY,
} from '../types.ts';

/**
 * Build the 10-element info array for a single command, matching Redis 7 format:
 *   [name, arity, [flags...], firstKey, lastKey, step, [aclCategories...], [tips], [keySpecs], [subcommands]]
 */
function commandInfoEntry(def: CommandDefinition): Reply {
  const flags: Reply[] = [];
  for (const f of def.flags) {
    flags.push(statusReply(f));
  }

  const categories: Reply[] = [];
  for (const c of def.categories) {
    categories.push(bulkReply(c));
  }

  const subcommands: Reply[] = [];
  if (def.subcommands) {
    for (const sub of def.subcommands.values()) {
      subcommands.push(commandInfoEntry(sub));
    }
  }

  return arrayReply([
    bulkReply(def.name.toLowerCase()),
    integerReply(def.arity),
    arrayReply(flags),
    integerReply(def.firstKey),
    integerReply(def.lastKey),
    integerReply(def.keyStep),
    arrayReply(categories),
    arrayReply([]), // tips
    arrayReply([]), // key specifications
    arrayReply(subcommands),
  ]);
}

/**
 * COMMAND — returns info for all registered commands.
 */
export function command(table: CommandTable): Reply {
  const entries: Reply[] = [];
  for (const def of table.all()) {
    entries.push(commandInfoEntry(def));
  }
  return arrayReply(entries);
}

/**
 * COMMAND COUNT — returns total number of registered commands.
 */
export function commandCount(table: CommandTable, args: string[]): Reply {
  if (args.length !== 0) return wrongArityError('command|count');
  return integerReply(table.size);
}

/**
 * COMMAND LIST [FILTERBY MODULE name | ACLCAT category | PATTERN pattern]
 * Redis requires the FILTERBY keyword before the filter type.
 */
export function commandList(table: CommandTable, args: string[]): Reply {
  if (args.length === 0) {
    // Return all command names
    const names: Reply[] = [];
    for (const def of table.all()) {
      names.push(bulkReply(def.name.toLowerCase()));
    }
    return arrayReply(names);
  }

  // Redis requires exactly: FILTERBY <type> <value>
  const keyword = (args[0] ?? '').toUpperCase();
  if (keyword !== 'FILTERBY' || args.length !== 3) {
    return errorReply('ERR', 'syntax error');
  }

  const filterType = (args[1] ?? '').toUpperCase();
  const filterValue = args[2] ?? '';

  switch (filterType) {
    case 'MODULE': {
      // We don't support modules — always return empty
      return EMPTY_ARRAY;
    }
    case 'ACLCAT': {
      const category = `@${filterValue.toLowerCase()}`;
      const names: Reply[] = [];
      for (const def of table.all()) {
        if (def.categories.has(category)) {
          names.push(bulkReply(def.name.toLowerCase()));
        }
      }
      return arrayReply(names);
    }
    case 'PATTERN': {
      const names: Reply[] = [];
      const regex = globToRegex(filterValue.toLowerCase());
      for (const def of table.all()) {
        if (regex.test(def.name.toLowerCase())) {
          names.push(bulkReply(def.name.toLowerCase()));
        }
      }
      return arrayReply(names);
    }
    default:
      return errorReply('ERR', 'syntax error');
  }
}

/**
 * Convert a Redis glob pattern to a RegExp.
 * Supports: * (any), ? (single char), [abc] (character class), \ (escape).
 */
function globToRegex(pattern: string): RegExp {
  let regex = '^';
  let i = 0;
  while (i < pattern.length) {
    const ch = pattern[i] as string;
    switch (ch) {
      case '*':
        regex += '.*';
        break;
      case '?':
        regex += '.';
        break;
      case '[': {
        let j = i + 1;
        while (j < pattern.length && pattern[j] !== ']') {
          j++;
        }
        if (j < pattern.length) {
          regex += pattern.slice(i, j + 1);
          i = j;
        } else {
          regex += '\\[';
        }
        break;
      }
      case '\\':
        i++;
        if (i < pattern.length) {
          regex += '\\' + pattern[i];
        }
        break;
      default:
        // Escape regex special chars
        regex += ch.replace(/[$()+.^{|}]/g, '\\$&');
    }
    i++;
  }
  regex += '$';
  return new RegExp(regex);
}

/**
 * COMMAND INFO [command-name ...]
 * Returns info for specified commands, or null for unknown commands.
 */
export function commandInfo(table: CommandTable, args: string[]): Reply {
  if (args.length === 0) {
    return wrongArityError('command|info');
  }

  const entries: Reply[] = [];
  for (const name of args) {
    const def = table.get(name);
    if (!def) {
      entries.push(bulkReply(null));
    } else {
      entries.push(commandInfoEntry(def));
    }
  }
  return arrayReply(entries);
}

/**
 * COMMAND DOCS [command-name ...]
 * Returns documentation for commands. We return minimal docs.
 */
export function commandDocs(table: CommandTable, args: string[]): Reply {
  const defs: CommandDefinition[] = [];

  if (args.length === 0) {
    for (const def of table.all()) {
      defs.push(def);
    }
  } else {
    for (const name of args) {
      const def = table.get(name);
      if (def) {
        defs.push(def);
      }
    }
  }

  // Redis returns a flat array of [name, [doc-fields...], name, [doc-fields...], ...]
  const result: Reply[] = [];
  for (const def of defs) {
    result.push(bulkReply(def.name.toLowerCase()));
    const docFields: Reply[] = [
      bulkReply('summary'),
      bulkReply(''),
      bulkReply('since'),
      bulkReply('1.0.0'),
      bulkReply('group'),
      bulkReply(''),
      bulkReply('complexity'),
      bulkReply(''),
    ];
    result.push(arrayReply(docFields));
  }
  return arrayReply(result);
}

/**
 * COMMAND GETKEYS command [arg ...]
 * Returns the keys that the given command would extract.
 */
export function commandGetkeys(table: CommandTable, args: string[]): Reply {
  if (args.length === 0) {
    return wrongArityError('command|getkeys');
  }

  const cmdName = args[0] ?? '';
  const def = table.get(cmdName);
  if (!def) {
    return errorReply('ERR', 'Invalid command specified');
  }

  if (def.firstKey === 0) {
    return errorReply('ERR', 'The command has no key arguments');
  }

  // Check arity of the specified command (args includes the command name)
  const argc = args.length;
  if (def.arity > 0 && argc !== def.arity) {
    return errorReply(
      'ERR',
      'Invalid number of arguments specified for command'
    );
  }
  if (def.arity < 0 && argc < Math.abs(def.arity)) {
    return errorReply(
      'ERR',
      'Invalid number of arguments specified for command'
    );
  }

  const keys: Reply[] = [];
  const lastKey = def.lastKey < 0 ? args.length - 1 : def.lastKey;

  for (let i = def.firstKey; i <= lastKey; i += def.keyStep) {
    const key = args[i];
    if (key !== undefined) {
      keys.push(bulkReply(key));
    }
  }

  return arrayReply(keys);
}

/**
 * COMMAND HELP — returns help text for the COMMAND family.
 */
export function commandHelp(): Reply {
  return arrayReply([
    bulkReply(
      'COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
    ),
    bulkReply('COUNT'),
    bulkReply('    Return number of commands.'),
    bulkReply('DOCS [<command-name> [<command-name> ...]]'),
    bulkReply(
      '    Return documentation details about multiple Redis commands.'
    ),
    bulkReply('GETKEYS <full-command>'),
    bulkReply('    Return the keys from a full Redis command.'),
    bulkReply('HELP'),
    bulkReply('    Print this help.'),
    bulkReply('INFO [<command-name> [<command-name> ...]]'),
    bulkReply('    Return details about multiple Redis commands.'),
    bulkReply(
      'LIST [FILTERBY (MODULE <module-name>|ACLCAT <category>|PATTERN <pattern>)]'
    ),
    bulkReply('    Return a list of command names.'),
  ]);
}

/**
 * Main COMMAND dispatcher — routes to subcommands.
 */
export function commandDispatch(table: CommandTable, args: string[]): Reply {
  // COMMAND with no subcommand returns all commands
  if (args.length === 0) {
    return command(table);
  }

  const subcommand = (args[0] ?? '').toUpperCase();
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'COUNT':
      return commandCount(table, subArgs);
    case 'LIST':
      return commandList(table, subArgs);
    case 'INFO':
      return commandInfo(table, subArgs);
    case 'DOCS':
      return commandDocs(table, subArgs);
    case 'GETKEYS':
      return commandGetkeys(table, subArgs);
    case 'HELP':
      return commandHelp();
    default:
      return unknownSubcommandError('command', (args[0] ?? '').toLowerCase());
  }
}

function getTable(ctx: CommandContext): CommandTable {
  if (!ctx.commandTable) {
    throw new Error('commandTable not set on context');
  }
  return ctx.commandTable;
}

export const specs: CommandSpec[] = [
  {
    name: 'command',
    handler: (ctx, args) => commandDispatch(getTable(ctx), args),
    arity: -1,
    flags: ['readonly', 'loading', 'stale'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@slow', '@connection'],
    subcommands: [
      {
        name: 'count',
        handler: (ctx, args) => commandCount(getTable(ctx), args),
        arity: 2,
        flags: ['readonly', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'list',
        handler: (ctx, args) => commandList(getTable(ctx), args),
        arity: -2,
        flags: ['readonly', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'info',
        handler: (ctx, args) => commandInfo(getTable(ctx), args),
        arity: -3,
        flags: ['readonly', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'docs',
        handler: (ctx, args) => commandDocs(getTable(ctx), args),
        arity: -2,
        flags: ['readonly', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'getkeys',
        handler: (ctx, args) => commandGetkeys(getTable(ctx), args),
        arity: -4,
        flags: ['readonly', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
      {
        name: 'help',
        handler: () => commandHelp(),
        arity: 2,
        flags: ['readonly', 'loading', 'stale'],
        firstKey: 0,
        lastKey: 0,
        keyStep: 0,
        categories: ['@slow', '@connection'],
      },
    ],
  },
];
