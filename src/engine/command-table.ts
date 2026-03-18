import type { Reply } from './types.ts';
import { wrongArityError, unknownCommandError } from './types.ts';
import type { CommandContext } from './types.ts';

export type CommandFlag =
  | 'write'
  | 'readonly'
  | 'denyoom'
  | 'fast'
  | 'loading'
  | 'stale'
  | 'admin'
  | 'pubsub'
  | 'noscript'
  | 'blocking'
  | 'movablekeys'
  | 'sort_for_script'
  | 'noauth';

export type CommandHandler = (ctx: CommandContext, args: string[]) => Reply;

export interface CommandDefinition {
  name: string;
  handler: CommandHandler;
  arity: number;
  flags: Set<CommandFlag>;
  firstKey: number;
  lastKey: number;
  keyStep: number;
  categories: Set<string>;
  subcommands?: Map<string, CommandDefinition>;
}

export class CommandTable {
  private readonly commands = new Map<string, CommandDefinition>();

  register(def: CommandDefinition): void {
    this.commands.set(def.name.toLowerCase(), def);
  }

  get(name: string): CommandDefinition | undefined {
    return this.commands.get(name.toLowerCase());
  }

  has(name: string): boolean {
    return this.commands.has(name.toLowerCase());
  }

  all(): IterableIterator<CommandDefinition> {
    return this.commands.values();
  }

  get size(): number {
    return this.commands.size;
  }

  /**
   * Validate arity for a command invocation.
   * Arity includes the command name itself.
   * Positive arity = exact count required.
   * Negative arity = minimum count required (abs value).
   *
   * @param def - command definition
   * @param argc - total argument count including the command name
   * @returns error reply if arity check fails, null if ok
   */
  checkArity(def: CommandDefinition, argc: number): Reply | null {
    if (def.arity > 0) {
      if (argc !== def.arity) {
        return wrongArityError(def.name.toLowerCase());
      }
    } else {
      if (argc < Math.abs(def.arity)) {
        return wrongArityError(def.name.toLowerCase());
      }
    }
    return null;
  }

  /**
   * Look up a command and return error reply for unknown commands.
   */
  lookup(name: string): CommandDefinition | Reply {
    const def = this.get(name);
    if (!def) {
      return unknownCommandError(name, []);
    }
    return def;
  }
}
