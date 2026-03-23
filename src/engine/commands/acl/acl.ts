import type { Reply, CommandContext } from '../../types.ts';
import {
  statusReply,
  bulkReply,
  integerReply,
  arrayReply,
  errorReply,
  unknownSubcommandError,
  OK,
  EMPTY_ARRAY,
} from '../../types.ts';
import type { CommandSpec } from '../../command-table.ts';
import {
  ACL_CATEGORIES,
  getAcl,
  userFlags,
  userPasswordHashes,
  userCommandsString,
  userKeysString,
  userChannelsString,
  formatUserForList,
  makeSubSpec,
} from './utils.ts';
import { applyRule } from './rules.ts';

// ---------------------------------------------------------------------------
// ACL SETUSER
// ---------------------------------------------------------------------------

function aclSetuser(ctx: CommandContext, args: string[]): Reply {
  const acl = getAcl(ctx);
  if (!acl) return errorReply('ERR', 'ACL store not available');

  if (args.length === 0) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'acl|setuser' command"
    );
  }

  const username = args[0] ?? '';
  const rules = args.slice(1);

  const user = acl.createOrGetUser(username);

  for (const rule of rules) {
    const err = applyRule(user, rule);
    if (err) {
      return errorReply('ERR', err.startsWith('ERR ') ? err.slice(4) : err);
    }
  }

  return OK;
}

// ---------------------------------------------------------------------------
// ACL DELUSER
// ---------------------------------------------------------------------------

function aclDeluser(ctx: CommandContext, args: string[]): Reply {
  const acl = getAcl(ctx);
  if (!acl) return errorReply('ERR', 'ACL store not available');

  if (args.length === 0) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'acl|deluser' command"
    );
  }

  // Cannot delete default user
  for (const name of args) {
    if (name === 'default') {
      return errorReply('ERR', "The 'default' user cannot be removed");
    }
  }

  const count = acl.deleteUsers(args);
  return integerReply(count);
}

// ---------------------------------------------------------------------------
// ACL GETUSER
// ---------------------------------------------------------------------------

function aclGetuser(ctx: CommandContext, args: string[]): Reply {
  const acl = getAcl(ctx);
  if (!acl) return errorReply('ERR', 'ACL store not available');

  if (args.length !== 1) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'acl|getuser' command"
    );
  }

  const user = acl.getUser(args[0] ?? '');
  if (!user) return bulkReply(null);

  const keysPattern = userKeysString(user);
  const channelsPattern = userChannelsString(user);

  return arrayReply([
    bulkReply('flags'),
    userFlags(user),
    bulkReply('passwords'),
    userPasswordHashes(user),
    bulkReply('commands'),
    bulkReply(userCommandsString(user)),
    bulkReply('keys'),
    bulkReply(keysPattern),
    bulkReply('channels'),
    bulkReply(channelsPattern),
    bulkReply('selectors'),
    EMPTY_ARRAY,
  ]);
}

// ---------------------------------------------------------------------------
// ACL LIST
// ---------------------------------------------------------------------------

function aclList(ctx: CommandContext): Reply {
  const acl = getAcl(ctx);
  if (!acl) return errorReply('ERR', 'ACL store not available');

  const entries: Reply[] = [];
  for (const user of acl.allUsers()) {
    entries.push(bulkReply(formatUserForList(user)));
  }
  return arrayReply(entries);
}

// ---------------------------------------------------------------------------
// ACL WHOAMI
// ---------------------------------------------------------------------------

function aclWhoami(ctx: CommandContext): Reply {
  const username = ctx.client?.username ?? 'default';
  return bulkReply(username);
}

// ---------------------------------------------------------------------------
// ACL CAT [category]
// ---------------------------------------------------------------------------

function aclCat(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return arrayReply(ACL_CATEGORIES.map((c) => bulkReply(c)));
  }

  if (args.length > 1) {
    return errorReply('ERR', "wrong number of arguments for 'acl|cat' command");
  }

  const catArg = args[0] ?? '';
  const category = `@${catArg.toLowerCase()}`;

  // Validate the category exists
  if (!ACL_CATEGORIES.includes(catArg.toLowerCase())) {
    return errorReply('ERR', `Unknown ACL cat category '${catArg}'`);
  }

  // Return commands that belong to this category
  const table = ctx.commandTable;
  if (!table) return EMPTY_ARRAY;

  const names: Reply[] = [];
  for (const def of table.all()) {
    if (def.categories.has(category)) {
      names.push(bulkReply(def.name.toLowerCase()));
    }
    // Also check subcommands
    if (def.subcommands) {
      for (const sub of def.subcommands.values()) {
        if (sub.categories.has(category)) {
          names.push(
            bulkReply(`${def.name.toLowerCase()}|${sub.name.toLowerCase()}`)
          );
        }
      }
    }
  }

  return arrayReply(names);
}

// ---------------------------------------------------------------------------
// ACL LOG [count|RESET]
// ---------------------------------------------------------------------------

function aclLog(ctx: CommandContext, args: string[]): Reply {
  const acl = getAcl(ctx);
  if (!acl) return errorReply('ERR', 'ACL store not available');

  if (args.length === 0) {
    return formatLogEntries(acl.getLog(), ctx.engine.clock());
  }

  if (args.length > 1) {
    return errorReply('ERR', "wrong number of arguments for 'acl|log' command");
  }

  const logArg = args[0] ?? '';
  if (logArg.toUpperCase() === 'RESET') {
    acl.resetLog();
    return OK;
  }

  const count = parseInt(logArg, 10);
  if (isNaN(count) || count < 0) {
    return errorReply('ERR', 'value is not an integer or out of range');
  }

  return formatLogEntries(acl.getLog(count), ctx.engine.clock());
}

function formatLogEntries(
  entries: import('../../acl-store.ts').AclLogEntry[],
  now: number
): Reply {
  const result: Reply[] = [];
  for (const entry of entries) {
    const age = ((now - entry.timestampCreated) / 1000).toFixed(3);
    result.push(
      arrayReply([
        bulkReply('count'),
        integerReply(entry.count),
        bulkReply('reason'),
        bulkReply(entry.reason),
        bulkReply('context'),
        bulkReply(entry.context),
        bulkReply('object'),
        bulkReply(entry.object),
        bulkReply('username'),
        bulkReply(entry.username),
        bulkReply('age-seconds'),
        bulkReply(age),
        bulkReply('client-info'),
        bulkReply(entry.clientInfo),
        bulkReply('entry-id'),
        integerReply(entry.entryId),
        bulkReply('timestamp-created'),
        integerReply(entry.timestampCreated),
        bulkReply('timestamp-last-updated'),
        integerReply(entry.timestampLastUpdated),
      ])
    );
  }
  return arrayReply(result);
}

// ---------------------------------------------------------------------------
// ACL GENPASS [bits]
// ---------------------------------------------------------------------------

function aclGenpass(ctx: CommandContext, args: string[]): Reply {
  let bits = 256;

  if (args.length > 0) {
    const n = parseInt(args[0] ?? '', 10);
    if (isNaN(n) || n < 1 || n > 6144) {
      return errorReply(
        'ERR',
        'ACL GENPASS argument must be the number of bits for the output password, a positive number up to 6144'
      );
    }
    bits = n;
  }

  // Generate random hex string. Rounds up to full bytes.
  const bytes = Math.ceil(bits / 8);
  const rng = ctx.engine.rng;
  let hex = '';
  for (let i = 0; i < bytes; i++) {
    const byte = Math.floor(rng() * 256);
    hex += byte.toString(16).padStart(2, '0');
  }

  // Truncate to exact number of hex chars needed for the requested bits
  // Redis always returns ceil(bits/4) hex chars
  const hexChars = Math.ceil(bits / 4);
  return bulkReply(hex.slice(0, hexChars));
}

// ---------------------------------------------------------------------------
// ACL LOAD / ACL SAVE (stubs)
// ---------------------------------------------------------------------------

function aclLoad(): Reply {
  return errorReply(
    'ERR',
    'This Redis instance is not configured to use an ACL file. You may want to use the command CONFIG REWRITE instead.'
  );
}

function aclSave(): Reply {
  return errorReply(
    'ERR',
    'This Redis instance is not configured to use an ACL file. You may want to use the command CONFIG REWRITE instead.'
  );
}

// ---------------------------------------------------------------------------
// ACL DRYRUN user command [arg ...]
// ---------------------------------------------------------------------------

function aclDryrun(ctx: CommandContext, args: string[]): Reply {
  const acl = getAcl(ctx);
  if (!acl) return errorReply('ERR', 'ACL store not available');

  if (args.length < 2) {
    return errorReply(
      'ERR',
      "wrong number of arguments for 'acl|dryrun' command"
    );
  }

  const username = args[0] ?? '';
  const cmdName = args[1] ?? '';

  const user = acl.getUser(username);
  if (!user) {
    return errorReply('ERR', `User '${username}' not found`);
  }

  // Check if command exists
  const table = ctx.commandTable;
  if (!table) return statusReply('OK');

  const def = table.get(cmdName);
  if (!def) {
    return errorReply('ERR', `Command '${cmdName}' not found`);
  }

  // Check command permission
  if (!user.allCommands) {
    return bulkReply(
      `This user has no permissions to run the '${cmdName.toLowerCase()}' command`
    );
  }

  // Check key permission
  if (!user.allKeys && def.firstKey > 0) {
    const keyIndex = def.firstKey;
    if (keyIndex < args.length - 1) {
      const key = args[keyIndex + 1];
      return bulkReply(
        `This user has no permissions to access the '${key}' key`
      );
    }
  }

  // Check channel permission for pubsub commands
  if (!user.allChannels && def.flags.has('pubsub') && args.length > 2) {
    const channel = args[2];
    return bulkReply(
      `This user has no permissions to access the '${channel}' channel`
    );
  }

  return statusReply('OK');
}

// ---------------------------------------------------------------------------
// ACL HELP
// ---------------------------------------------------------------------------

const ACL_HELP_LINES = [
  'ACL <subcommand> [<arg> [value] [opt] ...]. subcommands are:',
  'CAT [<category>]',
  '    List all commands that belong to <category>, or all command categories',
  '    when no category is specified.',
  'DELUSER <username> [<username> ...]',
  '    Delete a list of users.',
  'DRYRUN <username> <command> [<arg> ...]',
  '    Returns whether the user can execute the given command without executing the command.',
  'GENPASS [<bits>]',
  '    Generate a secure password. The optional `bits` argument can be used to',
  '    specify the size (bits) of the random string.',
  'GETUSER <username>',
  '    Get the user details.',
  'LIST',
  '    Show users details in config file format.',
  'LOAD',
  '    Reload users from the ACL file.',
  'LOG [<count> | RESET]',
  '    List latest events denied because of ACLs.',
  '    RESET: Clears the ACL log entries.',
  'SAVE',
  '    Save the current ACL rules to the ACL file.',
  'SETUSER <username> <property> [<property> ...]',
  '    Create or modify a user with the specified properties.',
  'WHOAMI',
  '    Return the current connection username.',
  'HELP',
  '    Print this help.',
];

function aclHelp(): Reply {
  return arrayReply(ACL_HELP_LINES.map((l) => bulkReply(l)));
}

// ---------------------------------------------------------------------------
// Main ACL dispatcher
// ---------------------------------------------------------------------------

export function aclDispatch(ctx: CommandContext, args: string[]): Reply {
  if (args.length === 0) {
    return unknownSubcommandError('acl', '');
  }

  const sub = args[0] ?? '';
  const subUpper = sub.toUpperCase();
  const rest = args.slice(1);

  switch (subUpper) {
    case 'SETUSER':
      return aclSetuser(ctx, rest);
    case 'DELUSER':
      return aclDeluser(ctx, rest);
    case 'GETUSER':
      return aclGetuser(ctx, rest);
    case 'LIST':
      return aclList(ctx);
    case 'WHOAMI':
      return aclWhoami(ctx);
    case 'CAT':
      return aclCat(ctx, rest);
    case 'LOG':
      return aclLog(ctx, rest);
    case 'GENPASS':
      return aclGenpass(ctx, rest);
    case 'LOAD':
      return aclLoad();
    case 'SAVE':
      return aclSave();
    case 'DRYRUN':
      return aclDryrun(ctx, rest);
    case 'HELP':
      return aclHelp();
    default:
      return unknownSubcommandError('acl', sub.toLowerCase());
  }
}

// ---------------------------------------------------------------------------
// Command spec
// ---------------------------------------------------------------------------

export const specs: CommandSpec[] = [
  {
    name: 'acl',
    handler: (ctx, args) => aclDispatch(ctx, args),
    arity: -2,
    flags: ['admin', 'loading', 'stale', 'noscript'],
    firstKey: 0,
    lastKey: 0,
    keyStep: 0,
    categories: ['@admin', '@slow', '@dangerous'],
    subcommands: [
      makeSubSpec('setuser', (ctx, args) => aclSetuser(ctx, args.slice(1)), -3),
      makeSubSpec('deluser', (ctx, args) => aclDeluser(ctx, args.slice(1)), -3),
      makeSubSpec('getuser', (ctx, args) => aclGetuser(ctx, args.slice(1)), 3),
      makeSubSpec('list', (ctx) => aclList(ctx), 2),
      makeSubSpec('whoami', (ctx) => aclWhoami(ctx), 2),
      makeSubSpec('cat', (ctx, args) => aclCat(ctx, args.slice(1)), -2),
      makeSubSpec('log', (ctx, args) => aclLog(ctx, args.slice(1)), -2),
      makeSubSpec('genpass', (ctx, args) => aclGenpass(ctx, args.slice(1)), -2),
      makeSubSpec('load', () => aclLoad(), 2),
      makeSubSpec('save', () => aclSave(), 2),
      makeSubSpec('dryrun', (ctx, args) => aclDryrun(ctx, args.slice(1)), -4),
      makeSubSpec('help', () => aclHelp(), 2),
    ],
  },
];
