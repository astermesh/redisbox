/**
 * CONFIG command implementations.
 *
 * Each function takes a ConfigStore and the command arguments,
 * returning a Redis-compatible response.
 */

import type { ConfigStore } from './config-store.ts';

// ---------------------------------------------------------------------------
// Response types (match Redis RESP2 semantics)
// ---------------------------------------------------------------------------

export type ConfigResponse =
  | { kind: 'ok' }
  | { kind: 'array'; data: string[] }
  | { kind: 'error'; message: string };

const OK: ConfigResponse = { kind: 'ok' };

function error(message: string): ConfigResponse {
  return { kind: 'error', message };
}

function array(data: string[]): ConfigResponse {
  return { kind: 'array', data };
}

// ---------------------------------------------------------------------------
// CONFIG subcommand dispatch
// ---------------------------------------------------------------------------

export function executeConfig(
  store: ConfigStore,
  args: string[]
): ConfigResponse {
  if (args.length === 0) {
    return error("ERR wrong number of arguments for 'config' command");
  }

  const sub = args[0] ?? '';
  const subcommand = sub.toUpperCase();
  const rest = args.slice(1);

  switch (subcommand) {
    case 'GET':
      return configGet(store, rest);
    case 'SET':
      return configSet(store, rest);
    case 'RESETSTAT':
      return configResetStat(store, rest);
    case 'REWRITE':
      return configRewrite(rest);
    default:
      return error(
        `ERR unknown subcommand or wrong number of arguments for 'config|${sub.toLowerCase()}' command`
      );
  }
}

// ---------------------------------------------------------------------------
// CONFIG GET pattern [pattern ...]
// ---------------------------------------------------------------------------

function configGet(store: ConfigStore, args: string[]): ConfigResponse {
  if (args.length === 0) {
    return error("ERR wrong number of arguments for 'config|get' command");
  }

  if (args.length === 1) {
    return array(store.get(args[0] ?? ''));
  }

  return array(store.getMulti(args));
}

// ---------------------------------------------------------------------------
// CONFIG SET key value [key value ...]
// ---------------------------------------------------------------------------

function configSet(store: ConfigStore, args: string[]): ConfigResponse {
  if (args.length === 0 || args.length % 2 !== 0) {
    return error("ERR wrong number of arguments for 'config|set' command");
  }

  if (args.length === 2) {
    const err = store.set(args[0] ?? '', args[1] ?? '');
    return err ? error(err) : OK;
  }

  // Multiple key-value pairs — atomic set
  const pairs: [string, string][] = [];
  for (let i = 0; i < args.length; i += 2) {
    pairs.push([args[i] ?? '', args[i + 1] ?? '']);
  }

  const err = store.setMulti(pairs);
  return err ? error(err) : OK;
}

// ---------------------------------------------------------------------------
// CONFIG RESETSTAT
// ---------------------------------------------------------------------------

function configResetStat(store: ConfigStore, args: string[]): ConfigResponse {
  if (args.length !== 0) {
    return error(
      "ERR wrong number of arguments for 'config|resetstat' command"
    );
  }

  store.resetStat();
  return OK;
}

// ---------------------------------------------------------------------------
// CONFIG REWRITE (no-op in emulator — returns OK)
// ---------------------------------------------------------------------------

function configRewrite(args: string[]): ConfigResponse {
  if (args.length !== 0) {
    return error("ERR wrong number of arguments for 'config|rewrite' command");
  }

  return error('ERR The server is running without a config file');
}
