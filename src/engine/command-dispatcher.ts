import type { CommandDefinition } from './command-table.ts';
import { CommandTable } from './command-table.ts';
import type { Reply, CommandContext } from './types.ts';
import {
  statusReply,
  errorReply,
  arrayReply,
  NIL_ARRAY,
  unknownCommandError,
  wrongArityError,
} from './types.ts';
import type { AclUser } from './acl-store.ts';

export interface QueuedCommand {
  def: CommandDefinition;
  args: string[];
}

export interface TransactionState {
  inMulti: boolean;
  multiDirty: boolean;
  multiQueue: QueuedCommand[];
  subscribed: boolean;
  watchedKeys: Map<string, number>;
}

export function createTransactionState(): TransactionState {
  return {
    inMulti: false,
    multiDirty: false,
    multiQueue: [],
    subscribed: false,
    watchedKeys: new Map(),
  };
}

const QUEUED: Reply = statusReply('QUEUED');
const NOAUTH_ERR: Reply = errorReply('NOAUTH', 'Authentication required.');

function nopermCommandError(cmdName: string): Reply {
  return errorReply(
    'NOPERM',
    `this user has no permissions to run the '${cmdName.toLowerCase()}' command`
  );
}

const NOPERM_KEY_ERR: Reply = errorReply(
  'NOPERM',
  'this user has no permissions to access one of the keys used as arguments'
);

const NOPERM_CHANNEL_ERR: Reply = errorReply(
  'NOPERM',
  'this user has no permissions to access one of the channels used as arguments'
);

/**
 * Commands allowed in subscribe mode (Redis 7.0+).
 */
const SUBSCRIBE_ALLOWED = new Set([
  'SUBSCRIBE',
  'UNSUBSCRIBE',
  'PSUBSCRIBE',
  'PUNSUBSCRIBE',
  'SSUBSCRIBE',
  'SUNSUBSCRIBE',
  'PING',
  'QUIT',
  'RESET',
]);

/**
 * Commands that are NOT queued inside MULTI — they execute immediately
 * or return a special error.
 */
const MULTI_PASSTHROUGH = new Set(['EXEC', 'DISCARD', 'MULTI', 'WATCH']);

function checkArity(def: CommandDefinition, argc: number): boolean {
  if (def.arity > 0) {
    return argc === def.arity;
  }
  return argc >= Math.abs(def.arity);
}

function requiresAuth(ctx: CommandContext): boolean {
  if (!ctx.client || ctx.client.authenticated) return false;

  // When an ACL store is available, check if the default user requires a
  // password.  Sync with requirepass first so the two stay consistent.
  if (ctx.acl) {
    if (ctx.config) {
      const result = ctx.config.get('requirepass');
      ctx.acl.syncRequirePass(result[1] ?? '');
    }
    const defaultUser = ctx.acl.getDefaultUser();
    return !defaultUser.nopass;
  }

  // Legacy path (no ACL store)
  if (!ctx.config) return false;
  const result = ctx.config.get('requirepass');
  const pass = result[1] ?? '';
  return pass.length > 0;
}

/**
 * Check ACL permissions for the current user against the command and its args.
 * Returns null if permitted, or a NOPERM Reply if denied.
 */
function checkAclPermission(
  ctx: CommandContext,
  def: CommandDefinition,
  args: string[]
): Reply | null {
  if (!ctx.acl || !ctx.client) return null;

  const user: AclUser | undefined = ctx.acl.getUser(ctx.client.username);

  // Unknown user — deny command
  if (!user) {
    ctx.acl.addLogEntry(
      'command',
      'toplevel',
      def.name.toLowerCase(),
      ctx.client.username,
      `id=${ctx.client.id}`,
      ctx.engine.clock()
    );
    return nopermCommandError(def.name);
  }

  // Command permission check
  if (!user.allCommands) {
    ctx.acl.addLogEntry(
      'command',
      'toplevel',
      def.name.toLowerCase(),
      ctx.client.username,
      `id=${ctx.client.id}`,
      ctx.engine.clock()
    );
    return nopermCommandError(def.name);
  }

  // Key permission check — only for commands that access keys
  if (!user.allKeys && def.firstKey > 0) {
    // Find the first key for logging
    const firstKeyArg = args[def.firstKey - 1] ?? '';
    ctx.acl.addLogEntry(
      'key',
      'toplevel',
      firstKeyArg,
      ctx.client.username,
      `id=${ctx.client.id}`,
      ctx.engine.clock()
    );
    return NOPERM_KEY_ERR;
  }

  // Channel permission check — only for pubsub commands
  if (!user.allChannels && def.flags.has('pubsub') && args.length > 0) {
    const firstChannel = args[0] ?? '';
    ctx.acl.addLogEntry(
      'channel',
      'toplevel',
      firstChannel,
      ctx.client.username,
      `id=${ctx.client.id}`,
      ctx.engine.clock()
    );
    return NOPERM_CHANNEL_ERR;
  }

  return null;
}

export class CommandDispatcher {
  constructor(private readonly table: CommandTable) {}

  private clearTransactionState(
    state: TransactionState,
    ctx: CommandContext
  ): void {
    state.inMulti = false;
    state.multiDirty = false;
    state.multiQueue = [];
    state.watchedKeys.clear();
    if (ctx.client) {
      ctx.client.flagMulti = false;
    }
  }

  private execTransaction(state: TransactionState, ctx: CommandContext): Reply {
    const queue = state.multiQueue;
    const dirty = state.multiDirty;
    // Copy watched keys before clearing state (clearTransactionState mutates the map)
    const watched = new Map(state.watchedKeys);

    // Always clear state first
    this.clearTransactionState(state, ctx);

    // EXECABORT on syntax errors (takes priority over WATCH failure)
    if (dirty) {
      return errorReply(
        'EXECABORT',
        'Transaction discarded because of previous errors.'
      );
    }

    // Check watched key versions — if any changed, return null array
    for (const [key, version] of watched) {
      if (ctx.db.getVersion(key) !== version) {
        return NIL_ARRAY;
      }
    }

    // Execute all queued commands atomically
    const results: Reply[] = [];
    for (const cmd of queue) {
      // Eviction check for denyoom commands inside MULTI/EXEC
      if (cmd.def.flags.has('denyoom') && ctx.eviction) {
        if (!ctx.eviction.tryEvict()) {
          results.push(ctx.eviction.oomReply());
          continue;
        }
      }
      results.push(cmd.def.handler(ctx, cmd.args));
    }
    return arrayReply(results);
  }

  private discardTransaction(
    state: TransactionState,
    ctx: CommandContext
  ): Reply {
    this.clearTransactionState(state, ctx);
    return statusReply('OK');
  }

  dispatch(
    state: TransactionState,
    ctx: CommandContext,
    rawArgs: string[]
  ): Reply {
    ctx.commandTable = this.table;

    if (rawArgs.length === 0) {
      return unknownCommandError('', []);
    }

    const cmdName = rawArgs[0] ?? '';
    const upperName = cmdName.toUpperCase();
    const args = rawArgs.slice(1);

    // Subscribe mode: only allow specific commands
    if (state.subscribed && !SUBSCRIBE_ALLOWED.has(upperName)) {
      return errorReply(
        'ERR',
        `Can't execute '${cmdName.toLowerCase()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context`
      );
    }

    // RESET: clear all client state and execute
    if (upperName === 'RESET') {
      const def = this.table.get(cmdName);
      if (!def) {
        return unknownCommandError(cmdName, args);
      }
      const arityError = this.table.checkArity(def, rawArgs.length);
      if (arityError) return arityError;

      state.inMulti = false;
      state.multiDirty = false;
      state.multiQueue = [];
      state.subscribed = false;
      state.watchedKeys.clear();

      if (ctx.client) {
        // Clean up actual pubsub subscriptions before clearing the flag
        ctx.pubsub?.removeClient(ctx.client.id);

        ctx.client.dbIndex = 0;
        ctx.client.flagMulti = false;
        ctx.client.flagSubscribed = false;
        ctx.client.authenticated = false;
        ctx.db = ctx.engine.db(0);
      }

      return def.handler(ctx, args);
    }

    // MULTI mode: handle special commands before lookup
    // These commands may not be registered yet but must not be queued.
    if (state.inMulti && MULTI_PASSTHROUGH.has(upperName)) {
      if (upperName === 'MULTI') {
        return errorReply('ERR', 'MULTI calls can not be nested');
      }
      if (upperName === 'WATCH') {
        return errorReply('ERR', 'WATCH inside MULTI is not allowed');
      }
      // EXEC and DISCARD — verify registered and check arity, then handle
      const def = this.table.get(cmdName);
      if (!def) {
        return unknownCommandError(cmdName, args);
      }
      const arityError = this.table.checkArity(def, rawArgs.length);
      if (arityError) return arityError;

      if (upperName === 'EXEC') {
        return this.execTransaction(state, ctx);
      }
      if (upperName === 'DISCARD') {
        return this.discardTransaction(state, ctx);
      }
      return def.handler(ctx, args);
    }

    // Look up command
    const def = this.table.get(cmdName);
    if (!def) {
      if (state.inMulti) {
        state.multiDirty = true;
      }
      return unknownCommandError(cmdName, args);
    }

    // Resolve subcommand for arity checking
    let arityDef: CommandDefinition = def;
    let isSubcommand = false;
    if (def.subcommands && def.subcommands.size > 0 && args.length > 0) {
      const subName = args[0] ?? '';
      const subDef = def.subcommands.get(subName.toLowerCase());
      if (subDef) {
        arityDef = subDef;
        isSubcommand = true;
      }
    }

    // Check arity
    if (!checkArity(arityDef, rawArgs.length)) {
      if (state.inMulti) {
        state.multiDirty = true;
      }
      if (isSubcommand) {
        return wrongArityError(
          `${def.name.toLowerCase()}|${arityDef.name.toLowerCase()}`
        );
      }
      return wrongArityError(def.name.toLowerCase());
    }

    // Auth check: reject commands without noauth flag when not authenticated
    if (!def.flags.has('noauth') && requiresAuth(ctx)) {
      return NOAUTH_ERR;
    }

    // ACL permission check: skip for noauth commands (AUTH, HELLO, QUIT, RESET)
    if (!def.flags.has('noauth')) {
      const aclDenied = checkAclPermission(ctx, def, args);
      if (aclDenied) return aclDenied;
    }

    // MULTI: enter transaction mode
    if (upperName === 'MULTI') {
      state.inMulti = true;
      state.multiDirty = false;
      state.multiQueue = [];
      if (ctx.client) {
        ctx.client.flagMulti = true;
      }
      return def.handler(ctx, args);
    }

    // MULTI mode: queue non-passthrough commands
    if (state.inMulti) {
      state.multiQueue.push({ def, args });
      return QUEUED;
    }

    // EXEC/DISCARD outside MULTI — error
    if (upperName === 'EXEC') {
      return errorReply('ERR', 'EXEC without MULTI');
    }
    if (upperName === 'DISCARD') {
      return errorReply('ERR', 'DISCARD without MULTI');
    }

    // WATCH: record current version of each key
    if (upperName === 'WATCH') {
      for (const key of args) {
        state.watchedKeys.set(key, ctx.db.getVersion(key));
      }
      return def.handler(ctx, args);
    }

    // UNWATCH: clear all watched keys
    if (upperName === 'UNWATCH') {
      state.watchedKeys.clear();
      return def.handler(ctx, args);
    }

    // Eviction check: reject denyoom commands when OOM and eviction fails
    if (def.flags.has('denyoom') && ctx.eviction) {
      if (!ctx.eviction.tryEvict()) {
        return ctx.eviction.oomReply();
      }
    }

    // Execute handler with timing for slowlog
    const startUs = performance.now() * 1000;
    const result = def.handler(ctx, args);
    const durationUs = Math.round(performance.now() * 1000 - startUs);

    // Record to slowlog if duration exceeds threshold
    if (ctx.config) {
      const thresholdUs = parseInt(
        ctx.config.get('slowlog-log-slower-than')[1] ?? '10000',
        10
      );
      const maxLen = parseInt(
        ctx.config.get('slowlog-max-len')[1] ?? '128',
        10
      );
      const timestampSec = Math.floor(ctx.engine.clock() / 1000);
      ctx.engine.slowlog.record(
        durationUs,
        thresholdUs,
        maxLen,
        timestampSec,
        rawArgs,
        '',
        ctx.client?.name ?? ''
      );
    }

    // Sync subscriber mode from client state (set by SUBSCRIBE/UNSUBSCRIBE handlers)
    if (ctx.client) {
      state.subscribed = ctx.client.flagSubscribed;
    }

    return result;
  }
}
