/**
 * Redis bridge for Lua scripting — redis.call() and redis.pcall().
 *
 * Registers the `redis` table in the Lua VM with `call`, `pcall`,
 * `error_reply`, `status_reply`, `log`, and log-level constants.
 *
 * Type mapping follows exact Redis behavior:
 *   Reply → Lua: status→{ok}, error→{err}, integer→number,
 *                 bulk→string|false, array→table, nil-array→false
 *   Lua → Reply: number→integer(truncated), string→bulk, true→integer 1,
 *                 false/nil→nil bulk, {ok}→status, {err}→error, table→array
 *
 * JS→Lua bridge uses tagged encoding to avoid wasmoon's 0-indexed
 * array proxy behavior. The JS bridge returns tagged objects ({t,v,...})
 * and Lua-side decode() converts them to proper 1-indexed Lua tables.
 */

import type { LuaEngine } from './lua-engine.ts';
import type { Reply } from '../types.ts';
import {
  statusReply,
  integerReply,
  bulkReply,
  arrayReply,
  errorReply,
} from '../types.ts';
import { sha1 } from '../sha1.ts';

/**
 * Function that executes a Redis command and returns the reply.
 * Arguments include the command name as the first element.
 */
export type CommandExecutor = (args: string[]) => Reply;

// Tag constants for JS→Lua bridge encoding.
// Must match the constants in the Lua decode() function.
const TAG_STATUS = 1;
const TAG_ERROR = 2;
const TAG_INTEGER = 3;
const TAG_NIL = 4;
const TAG_BULK = 5;
const TAG_ARRAY = 6;

/**
 * Convert a Redis Reply to a tagged JS object for Lua bridge transfer.
 *
 * Returns `{t: tag, v: value}` objects that the Lua-side decode()
 * function converts to proper Lua values. Array elements are stored
 * as string-keyed properties ("0", "1", ...) plus `n` for length.
 */
function replyToTagged(reply: Reply): Record<string, unknown> {
  switch (reply.kind) {
    case 'status':
      return { t: TAG_STATUS, v: reply.value };
    case 'error':
      return { t: TAG_ERROR, v: `${reply.prefix} ${reply.message}` };
    case 'integer':
      return { t: TAG_INTEGER, v: Number(reply.value) };
    case 'bulk':
      if (reply.value === null) return { t: TAG_NIL };
      return { t: TAG_BULK, v: reply.value };
    case 'array': {
      const obj: Record<string, unknown> = {
        t: TAG_ARRAY,
        n: reply.value.length,
      };
      for (let i = 0; i < reply.value.length; i++) {
        const elem = reply.value[i];
        if (elem !== undefined) obj[String(i)] = replyToTagged(elem);
      }
      return obj;
    }
    case 'nil-array':
      return { t: TAG_NIL };
    case 'multi': {
      const obj: Record<string, unknown> = {
        t: TAG_ARRAY,
        n: reply.value.length,
      };
      for (let i = 0; i < reply.value.length; i++) {
        const elem = reply.value[i];
        if (elem !== undefined) obj[String(i)] = replyToTagged(elem);
      }
      return obj;
    }
  }
}

/**
 * Convert a Redis Reply to a Lua-compatible JS value.
 *
 * Used outside the Lua bridge (e.g., for testing type conversions).
 * For the actual Lua bridge, replyToTagged + Lua decode() is used.
 */
export function replyToLua(reply: Reply): unknown {
  switch (reply.kind) {
    case 'status':
      return { ok: reply.value };
    case 'error':
      return { err: `${reply.prefix} ${reply.message}` };
    case 'integer':
      return Number(reply.value);
    case 'bulk':
      return reply.value === null ? false : reply.value;
    case 'array':
      return reply.value.map((v) => replyToLua(v));
    case 'nil-array':
      return false;
    case 'multi':
      return reply.value.map((v) => replyToLua(v));
  }
}

/**
 * Convert a Lua value (JS-side representation) to a Redis Reply.
 *
 * Follows Lua → Redis type conversion rules:
 * - number       → integer reply (truncated toward zero)
 * - string       → bulk string reply
 * - true         → integer reply 1
 * - false / nil  → nil bulk reply
 * - {ok: string} → status reply
 * - {err: string}→ error reply (prefix parsed from message)
 * - array table  → array reply (recursively converted)
 */
export function luaToReply(value: unknown): Reply {
  if (value === null || value === undefined) {
    return bulkReply(null);
  }

  if (typeof value === 'boolean') {
    return value ? integerReply(1) : bulkReply(null);
  }

  if (typeof value === 'number') {
    // Truncate toward zero, matching Redis behavior (Lua 5.1 has only doubles)
    return integerReply(value < 0 ? Math.ceil(value) : Math.floor(value));
  }

  if (typeof value === 'string') {
    return bulkReply(value);
  }

  if (typeof value === 'object') {
    // Check for {ok: string} status table
    const obj = value as Record<string, unknown>;
    if ('ok' in obj && typeof obj['ok'] === 'string') {
      return statusReply(obj['ok']);
    }

    // Check for {err: string} error table
    if ('err' in obj && typeof obj['err'] === 'string') {
      return parseErrorReply(obj['err']);
    }

    // Array-like table
    if (Array.isArray(value)) {
      return arrayReply(value.map((v) => luaToReply(v)));
    }
  }

  // Fallback: nil
  return bulkReply(null);
}

/**
 * Parse "PREFIX message" into an error reply.
 * If no space, prefix is the whole string and message is empty.
 */
function parseErrorReply(err: string): Reply {
  const spaceIdx = err.indexOf(' ');
  if (spaceIdx === -1) {
    return errorReply(err, '');
  }
  return errorReply(err.slice(0, spaceIdx), err.slice(spaceIdx + 1));
}

/**
 * Register the `redis` table in the Lua VM with call/pcall and helpers.
 *
 * Sets up:
 * - redis.call(cmd, ...)     — execute command, raise on error
 * - redis.pcall(cmd, ...)    — execute command, return {err} on error
 * - redis.error_reply(msg)   — create {err = msg}
 * - redis.status_reply(msg)  — create {ok = msg}
 * - redis.log(level, msg)    — no-op stub
 * - redis.LOG_DEBUG/VERBOSE/NOTICE/WARNING — log level constants
 */
export async function registerRedisBridge(
  engine: LuaEngine,
  executor: CommandExecutor
): Promise<void> {
  // Raw bridge: validates args, calls executor, returns tagged reply.
  // For call: error replies are tagged with a special error tag that
  // the Lua-side wrapper raises as an error.
  // For pcall: error replies are returned as {err=...} tables.
  function callRaw(...rawArgs: unknown[]): Record<string, unknown> {
    if (rawArgs.length === 0) {
      throw new Error('wrong number of arguments for redis.call');
    }
    const args = rawArgs.map((a) => String(a));
    const reply = executor(args);
    if (reply.kind === 'error') {
      // Throw so Lua error() propagates to the script
      throw new Error(`${reply.prefix} ${reply.message}`);
    }
    return replyToTagged(reply);
  }

  function pcallRaw(...rawArgs: unknown[]): Record<string, unknown> {
    if (rawArgs.length === 0) {
      throw new Error('wrong number of arguments for redis.pcall');
    }
    const args = rawArgs.map((a) => String(a));
    const reply = executor(args);
    return replyToTagged(reply);
  }

  engine.setGlobal('__rb_call', callRaw);
  engine.setGlobal('__rb_pcall', pcallRaw);
  engine.setGlobal('__rb_sha1hex', (s: unknown) => sha1(String(s ?? '')));

  // The Lua decode function converts tagged objects to proper Lua values.
  // Array elements are stored as string-keyed properties on the JS object,
  // accessed via tostring(i) to match JS property lookup through wasmoon proxy.
  // Capture JS bridge functions in locals before clearing globals.
  await engine.execute(`
    local T_STATUS, T_ERROR, T_INTEGER, T_NIL, T_BULK, T_ARRAY = 1, 2, 3, 4, 5, 6

    local raw_call = __rb_call
    local raw_pcall = __rb_pcall
    local raw_sha1hex = __rb_sha1hex
    __rb_call = nil
    __rb_pcall = nil
    __rb_sha1hex = nil

    local function decode(raw)
      local tag = raw.t
      if tag == T_STATUS then return {ok = raw.v} end
      if tag == T_ERROR then return {err = raw.v} end
      if tag == T_INTEGER then return raw.v end
      if tag == T_NIL then return false end
      if tag == T_BULK then return raw.v end
      if tag == T_ARRAY then
        local result = {}
        local len = raw.n
        for i = 0, len - 1 do
          result[i + 1] = decode(raw[tostring(i)])
        end
        return result
      end
      return false
    end

    redis = {}

    redis.call = function(...)
      local ok, raw = pcall(raw_call, ...)
      if not ok then
        -- Match Redis: prefix error with @user_script:LINE:
        local info = debug.getinfo(2, "Sl")
        local line = info and info.currentline or 0
        error("@user_script:" .. line .. ": " .. tostring(raw), 0)
      end
      return decode(raw)
    end

    redis.pcall = function(...)
      local ok, raw = pcall(raw_pcall, ...)
      if not ok then
        error(raw, 0)
      end
      return decode(raw)
    end

    redis.error_reply = function(msg)
      return {err = tostring(msg)}
    end

    redis.status_reply = function(msg)
      return {ok = tostring(msg)}
    end

    redis.sha1hex = function(s)
      return raw_sha1hex(tostring(s))
    end

    redis.log = function() end

    redis.LOG_DEBUG = 0
    redis.LOG_VERBOSE = 1
    redis.LOG_NOTICE = 2
    redis.LOG_WARNING = 3
  `);
}
