export type RedisType =
  | 'string'
  | 'list'
  | 'set'
  | 'zset'
  | 'hash'
  | 'stream'
  | 'none';

export type RedisEncoding =
  | 'raw'
  | 'int'
  | 'embstr'
  | 'ziplist'
  | 'listpack'
  | 'linkedlist'
  | 'quicklist'
  | 'hashtable'
  | 'intset'
  | 'skiplist'
  | 'stream';

export interface RedisEntry {
  type: RedisType;
  encoding: RedisEncoding;
  value: unknown;
  lruClock: number;
  lruFreq: number;
}

export interface EngineDeps {
  clock: () => number;
  rng: () => number;
}

export type Reply =
  | { kind: 'status'; value: string }
  | { kind: 'integer'; value: number | bigint }
  | { kind: 'bulk'; value: string | null }
  | { kind: 'array'; value: Reply[] }
  | { kind: 'nil-array' }
  | { kind: 'error'; prefix: string; message: string }
  | { kind: 'multi'; value: Reply[] };

export function statusReply(value: string): Reply {
  return { kind: 'status', value };
}

export function integerReply(value: number | bigint): Reply {
  return { kind: 'integer', value };
}

export function bulkReply(value: string | null): Reply {
  return { kind: 'bulk', value };
}

export function arrayReply(value: Reply[]): Reply {
  return { kind: 'array', value };
}

export function errorReply(prefix: string, message: string): Reply {
  return { kind: 'error', prefix, message };
}

export function multiReply(value: Reply[]): Reply {
  return { kind: 'multi', value };
}

export const OK = statusReply('OK');
export const ZERO = integerReply(0);
export const ONE = integerReply(1);
export const NIL = bulkReply(null);
export const EMPTY_ARRAY = arrayReply([]);
export const NIL_ARRAY: Reply = { kind: 'nil-array' };

// --- Standardized error constants (byte-identical to real Redis) ---

export const WRONGTYPE_ERR = errorReply(
  'WRONGTYPE',
  'Operation against a key holding the wrong kind of value'
);

export const SYNTAX_ERR = errorReply('ERR', 'syntax error');

export const NOT_INTEGER_ERR = errorReply(
  'ERR',
  'value is not an integer or out of range'
);

export const NOT_FLOAT_ERR = errorReply('ERR', 'value is not a valid float');

export const OVERFLOW_ERR = errorReply(
  'ERR',
  'increment or decrement would overflow'
);

export const INF_NAN_ERR = errorReply(
  'ERR',
  'increment would produce NaN or Infinity'
);

export const STRING_EXCEEDS_512MB_ERR = errorReply(
  'ERR',
  'string exceeds maximum allowed size (512MB)'
);

export const OFFSET_OUT_OF_RANGE_ERR = errorReply(
  'ERR',
  'offset is out of range'
);

export const NO_SUCH_KEY_ERR = errorReply('ERR', 'no such key');

/**
 * Generate a "wrong number of arguments" error for a command.
 * Matches Redis format: ERR wrong number of arguments for '<cmd>' command
 */
export function wrongArityError(commandName: string): Reply {
  return errorReply(
    'ERR',
    `wrong number of arguments for '${commandName}' command`
  );
}

/**
 * Generate an "invalid expire time" error for a command.
 * Matches Redis format: ERR invalid expire time in '<cmd>' command
 */
export function invalidExpireTimeError(commandName: string): Reply {
  return errorReply('ERR', `invalid expire time in '${commandName}' command`);
}

/**
 * Generate an "unknown command" error.
 * Matches Redis format: ERR unknown command '<cmd>', with args beginning with: 'arg1' 'arg2' ...
 */
export function unknownCommandError(name: string, args: string[]): Reply {
  const argsStr = args.map((a) => `'${a}'`).join(' ');
  return errorReply(
    'ERR',
    `unknown command '${name}', with args beginning with: ${argsStr}`
  );
}

/**
 * Generate an "unknown subcommand" error.
 * Matches Redis format: ERR unknown subcommand or wrong number of arguments for '<parent>|<sub>' command
 */
export function unknownSubcommandError(
  parentName: string,
  subName: string
): Reply {
  return errorReply(
    'ERR',
    `unknown subcommand or wrong number of arguments for '${parentName}|${subName}' command`
  );
}

export interface CommandContext {
  db: import('./database.ts').Database;
  engine: import('./engine.ts').RedisEngine;
  client?: import('../server/client-state.ts').ClientState;
  config?: import('../config-store.ts').ConfigStore;
  clientStore?: import('../server/client-state.ts').ClientStateStore;
  commandTable?: import('./command-table.ts').CommandTable;
  pubsub?: import('./pubsub-manager.ts').PubSubManager;
  blocking?: import('./blocking-manager.ts').BlockingManager;
  acl?: import('./acl-store.ts').AclStore;
  eviction?: import('./eviction-manager.ts').EvictionManager;
  ibi?: import('./hooks/ibi.ts').IbiHookManager;
  scriptManager?: import('./scripting/script-manager.ts').ScriptManager;
}
