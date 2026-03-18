import type { CommandDefinition } from './command-table.ts';
import { CommandTable } from './command-table.ts';
import type { Reply, CommandContext } from './types.ts';
import { statusReply, errorReply } from './types.ts';

export interface QueuedCommand {
  def: CommandDefinition;
  args: string[];
}

export interface ClientState {
  inMulti: boolean;
  multiDirty: boolean;
  multiQueue: QueuedCommand[];
  subscribed: boolean;
}

export function createClientState(): ClientState {
  return {
    inMulti: false,
    multiDirty: false,
    multiQueue: [],
    subscribed: false,
  };
}

const QUEUED: Reply = statusReply('QUEUED');

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

function formatUnknownCommandError(name: string, args: string[]): string {
  const argsStr = args.map((a) => `'${a}'`).join(' ');
  return `unknown command '${name}', with args beginning with: ${argsStr}`;
}

function checkArity(def: CommandDefinition, argc: number): boolean {
  if (def.arity > 0) {
    return argc === def.arity;
  }
  return argc >= Math.abs(def.arity);
}

export class CommandDispatcher {
  constructor(private readonly table: CommandTable) {}

  dispatch(state: ClientState, ctx: CommandContext, rawArgs: string[]): Reply {
    if (rawArgs.length === 0) {
      return errorReply(
        'ERR',
        "unknown command '', with args beginning with: "
      );
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

    // MULTI mode: handle special commands before lookup
    // These commands may not be registered yet but must not be queued.
    if (state.inMulti && MULTI_PASSTHROUGH.has(upperName)) {
      if (upperName === 'MULTI') {
        return errorReply('ERR', 'MULTI calls can not be nested');
      }
      if (upperName === 'WATCH') {
        return errorReply('ERR', 'WATCH inside MULTI is not allowed');
      }
      // EXEC, DISCARD — look up and execute normally (fall through)
      const def = this.table.get(cmdName);
      if (!def) {
        return errorReply('ERR', formatUnknownCommandError(cmdName, args));
      }
      const arityError = this.table.checkArity(def, rawArgs.length);
      if (arityError) return arityError;
      return def.handler(ctx, args);
    }

    // Look up command
    const def = this.table.get(cmdName);
    if (!def) {
      if (state.inMulti) {
        state.multiDirty = true;
      }
      return errorReply('ERR', formatUnknownCommandError(cmdName, args));
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
        return errorReply(
          'ERR',
          `wrong number of arguments for '${def.name.toLowerCase()}|${arityDef.name.toLowerCase()}' command`
        );
      }
      return errorReply(
        'ERR',
        `wrong number of arguments for '${def.name.toLowerCase()}' command`
      );
    }

    // MULTI mode: queue non-passthrough commands
    if (state.inMulti) {
      state.multiQueue.push({ def, args });
      return QUEUED;
    }

    // Execute handler
    return def.handler(ctx, args);
  }
}
