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
  | { kind: 'integer'; value: number }
  | { kind: 'bulk'; value: string | null }
  | { kind: 'array'; value: Reply[] }
  | { kind: 'error'; prefix: string; message: string };

export function statusReply(value: string): Reply {
  return { kind: 'status', value };
}

export function integerReply(value: number): Reply {
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

export const OK = statusReply('OK');
export const ZERO = integerReply(0);
export const ONE = integerReply(1);
export const NIL = bulkReply(null);
export const EMPTY_ARRAY = arrayReply([]);

export function wrongTypeError(): Reply {
  return errorReply(
    'WRONGTYPE',
    'Operation against a key holding the wrong kind of value'
  );
}

export interface CommandContext {
  db: import('./database.ts').Database;
  engine: import('./engine.ts').RedisEngine;
}
