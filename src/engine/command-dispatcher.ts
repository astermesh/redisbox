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

export interface QueuedCommand {
  def: CommandDefinition;
  args: string[];
}

export interface ClientState {
  inMulti: boolean;
  multiDirty: boolean;
  multiQueue: QueuedCommand[];
  subscribed: boolean;
  watchedKeys: Map<string, number>;
}

export function createClientState(): ClientState {
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

export class CommandDispatcher {
  constructor(private readonly table: CommandTable) {}

  private clearTransactionState(state: ClientState, ctx: CommandContext): void {
    state.inMulti = false;
    state.multiDirty = false;
    state.multiQueue = [];
    state.watchedKeys.clear();
    if (ctx.client) {
      ctx.client.flagMulti = false;
    }
  }

  private execTransaction(state: ClientState, ctx: CommandContext): Reply {
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
      results.push(cmd.def.handler(ctx, cmd.args));
    }
    return arrayReply(results);
  }

  private discardTransaction(state: ClientState, ctx: CommandContext): Reply {
    this.clearTransactionState(state, ctx);
    return statusReply('OK');
  }

  dispatch(state: ClientState, ctx: CommandContext, rawArgs: string[]): Reply {
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

    // Execute handler
    const result = def.handler(ctx, args);

    // Sync subscriber mode from client state (set by SUBSCRIBE/UNSUBSCRIBE handlers)
    if (ctx.client) {
      state.subscribed = ctx.client.flagSubscribed;
    }

    return result;
  }
}
