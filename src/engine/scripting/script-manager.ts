/**
 * ScriptManager — manages Lua script caching and execution for EVAL/EVALSHA.
 *
 * Handles:
 * - Script cache (SHA-1 → script body)
 * - Lua VM lifecycle (async init, sync execution)
 * - KEYS/ARGV table setup per eval call
 * - Read-only mode enforcement for EVAL_RO/EVALSHA_RO
 *
 * Result conversion uses a Lua-side encode function that converts the script
 * return value to a tagged JS object (same format as the redis bridge), then
 * the JS side decodes tagged objects into Reply values. This avoids LuaTable
 * proxy limitations with numeric keys and array detection.
 */

import type { LuaEngine } from './lua-engine.ts';
import { LuaScriptError } from './lua-engine.ts';
import type { CommandExecutor } from './redis-bridge.ts';
import { registerRedisBridge } from './redis-bridge.ts';
import { WasmoonEngine } from './wasmoon-engine.ts';
import { applySandbox, resetPrngState } from './sandbox.ts';
import { sha1 } from '../sha1.ts';
import type { Reply } from '../types.ts';
import {
  errorReply,
  statusReply,
  integerReply,
  bulkReply,
  arrayReply,
} from '../types.ts';
import type { CommandTable } from '../command-table.ts';

// Tag constants matching the redis bridge encoding.
const TAG_STATUS = 1;
const TAG_ERROR = 2;
const TAG_INTEGER = 3;
const TAG_NIL = 4;
const TAG_BULK = 5;
const TAG_ARRAY = 6;

export class ScriptManager {
  private engine: LuaEngine | null = null;
  private readonly scriptCache = new Map<string, string>();
  private initPromise: Promise<void> | null = null;
  private currentExecutor: CommandExecutor = () =>
    errorReply('ERR', 'Lua engine not initialized');

  /**
   * Whether the Lua engine has been initialized and is ready for sync execution.
   */
  get ready(): boolean {
    return this.engine !== null && !this.engine.closed;
  }

  /**
   * Initialize the Lua VM and register the redis bridge.
   * The bridge delegates to a mutable executor ref, so it can be swapped
   * per-eval for read-only mode enforcement.
   */
  async init(baseExecutor: CommandExecutor): Promise<void> {
    if (this.engine && !this.engine.closed) return;
    if (this.initPromise) {
      await this.initPromise;
      return;
    }
    this.initPromise = this.doInit(baseExecutor);
    await this.initPromise;
    this.initPromise = null;
  }

  private async doInit(baseExecutor: CommandExecutor): Promise<void> {
    if (this.engine && !this.engine.closed) {
      this.engine.close();
    }
    this.currentExecutor = baseExecutor;
    this.engine = await WasmoonEngine.create();
    // Bridge delegates to this.currentExecutor which can be swapped per-eval
    await registerRedisBridge(this.engine, (args: string[]) =>
      this.currentExecutor(args)
    );

    // Register a Lua-side encode function for converting script return values
    // to tagged JS objects. Uses the same tag format as the redis bridge.
    // The encode function is stored as a global for use in the wrapper script.
    // Must be registered before sandbox locks _G.
    await this.engine.execute(`
      local T_STATUS, T_ERROR, T_INTEGER, T_NIL, T_BULK, T_ARRAY = 1, 2, 3, 4, 5, 6

      function __rb_encode(v)
        local t = type(v)
        if t == "number" then
          if v >= 0 then
            return {t = T_INTEGER, v = math.floor(v)}
          else
            return {t = T_INTEGER, v = -math.floor(-v)}
          end
        end
        if t == "string" then return {t = T_BULK, v = v} end
        if t == "boolean" then
          if v then return {t = T_INTEGER, v = 1} end
          return {t = T_NIL}
        end
        if t == "nil" then return {t = T_NIL} end
        if t == "table" then
          if v.ok ~= nil then return {t = T_STATUS, v = tostring(v.ok)} end
          if v.err ~= nil then return {t = T_ERROR, v = tostring(v.err)} end
          local result = {t = T_ARRAY, n = #v}
          for i = 1, #v do
            result[tostring(i - 1)] = __rb_encode(v[i])
          end
          return result
        end
        return {t = T_NIL}
      end
    `);

    // Apply sandbox: library shims, PRNG override, global restrictions.
    // Must be after bridge and __rb_encode are registered (sandbox locks _G).
    await applySandbox(this.engine);
  }

  /**
   * Cache a script and return its SHA-1 digest.
   */
  cacheScript(script: string): string {
    const digest = sha1(script);
    this.scriptCache.set(digest, script);
    return digest;
  }

  /**
   * Validate script syntax by attempting to compile it with loadstring.
   * Returns null on success, or an error message string on failure.
   * Used by SCRIPT LOAD to match Redis behavior (compile check on load).
   */
  validateScript(script: string): string | null {
    if (!this.engine || this.engine.closed) {
      return 'Lua engine not initialized';
    }
    try {
      const result = this.engine.executeSync(
        `local f, err = loadstring(${luaStringLiteral(script)}); if err then return err else return nil end`
      );
      const val = result.values.length > 0 ? result.values[0] : null;
      if (val !== null && val !== undefined) {
        return String(val);
      }
      return null;
    } catch {
      return 'Failed to validate script';
    }
  }

  /**
   * Check if a script SHA exists in the cache.
   */
  hasScript(digest: string): boolean {
    return this.scriptCache.has(digest.toLowerCase());
  }

  /**
   * Get a cached script by SHA-1 digest.
   */
  getScript(digest: string): string | undefined {
    return this.scriptCache.get(digest.toLowerCase());
  }

  /**
   * Flush all cached scripts.
   */
  flushScripts(): void {
    this.scriptCache.clear();
  }

  /**
   * Execute a Lua script synchronously with KEYS and ARGV tables.
   *
   * The script is wrapped to pass the return value through __rb_encode(),
   * which converts it to a tagged JS object. The JS side then decodes
   * the tagged object into a Redis Reply.
   */
  evalScript(
    script: string,
    keys: string[],
    argv: string[],
    readOnly: boolean,
    commandTable: CommandTable | undefined,
    executor: CommandExecutor
  ): Reply {
    if (!this.engine || this.engine.closed) {
      return errorReply('ERR', 'Lua engine not initialized');
    }

    // Swap executor for this eval call (supports read-only wrapping)
    const prevExecutor = this.currentExecutor;
    this.currentExecutor = readOnly
      ? makeReadOnlyExecutor(executor, commandTable)
      : executor;

    try {
      // Redis resets PRNG to srand48(0) before every EVAL for deterministic replication
      resetPrngState();

      // Set KEYS and ARGV as Lua tables
      this.setKeysAndArgv(keys, argv);

      // Cache the script
      this.cacheScript(script);

      // Wrap the script to encode its return value as a tagged object
      const wrappedScript = `return __rb_encode((function()\n${script}\nend)())`;
      const result = this.engine.executeSync(wrappedScript);
      const tagged = result.values.length > 0 ? result.values[0] : null;

      return taggedToReply(tagged);
    } catch (err: unknown) {
      if (err instanceof LuaScriptError) {
        return errorReply('ERR', err.message);
      }
      const message = err instanceof Error ? err.message : String(err);
      return errorReply('ERR', message);
    } finally {
      // Restore previous executor
      this.currentExecutor = prevExecutor;
    }
  }

  private setKeysAndArgv(keys: string[], argv: string[]): void {
    if (!this.engine) return;

    // Build Lua table constructor string for KEYS
    const keysLua =
      keys.length === 0
        ? 'KEYS = {}'
        : `KEYS = {${keys.map((k) => luaStringLiteral(k)).join(',')}}`;

    // Build Lua table constructor string for ARGV
    const argvLua =
      argv.length === 0
        ? 'ARGV = {}'
        : `ARGV = {${argv.map((a) => luaStringLiteral(a)).join(',')}}`;

    this.engine.executeSync(`${keysLua}; ${argvLua}`);
  }

  /**
   * Close the Lua engine and release resources.
   */
  close(): void {
    if (this.engine && !this.engine.closed) {
      this.engine.close();
    }
    this.engine = null;
  }
}

/**
 * Decode a tagged object (returned from Lua __rb_encode) into a Reply.
 * The tagged object uses the same format as the redis bridge:
 * {t: tag, v: value, n: length, "0": elem, "1": elem, ...}
 *
 * LuaTable proxy provides string-key access via property lookup.
 */
function taggedToReply(tagged: unknown): Reply {
  if (tagged === null || tagged === undefined) {
    return bulkReply(null);
  }

  if (typeof tagged !== 'object') {
    return bulkReply(null);
  }

  const obj = tagged as Record<string, unknown>;
  const tag = obj['t'] as number;

  switch (tag) {
    case TAG_STATUS:
      return statusReply(String(obj['v'] ?? ''));
    case TAG_ERROR: {
      const errMsg = String(obj['v'] ?? '');
      const spaceIdx = errMsg.indexOf(' ');
      if (spaceIdx === -1) return errorReply(errMsg, '');
      return errorReply(errMsg.slice(0, spaceIdx), errMsg.slice(spaceIdx + 1));
    }
    case TAG_INTEGER:
      return integerReply(Number(obj['v'] ?? 0));
    case TAG_NIL:
      return bulkReply(null);
    case TAG_BULK:
      return bulkReply(
        obj['v'] === null || obj['v'] === undefined ? null : String(obj['v'])
      );
    case TAG_ARRAY: {
      const len = Number(obj['n'] ?? 0);
      const elements: Reply[] = [];
      for (let i = 0; i < len; i++) {
        elements.push(taggedToReply(obj[String(i)]));
      }
      return arrayReply(elements);
    }
    default:
      return bulkReply(null);
  }
}

/**
 * Wrap an executor to reject write commands (for EVAL_RO/EVALSHA_RO).
 */
function makeReadOnlyExecutor(
  executor: CommandExecutor,
  commandTable: CommandTable | undefined
): CommandExecutor {
  return (args: string[]) => {
    const cmdName = (args[0] ?? '').toLowerCase();
    if (commandTable) {
      const def = commandTable.get(cmdName);
      if (def && def.flags.has('write')) {
        return errorReply(
          'ERR',
          'Write commands are not allowed from read-only scripts'
        );
      }
    }
    return executor(args);
  };
}

/**
 * Escape a string for use in a Lua string literal.
 * Converts to UTF-8 bytes first, then escapes each byte.
 * This correctly handles all Unicode characters (including CJK, emoji, etc.)
 * since Lua's \ddd escape supports byte values 0-255.
 */
function luaStringLiteral(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let result = '"';
  for (const byte of bytes) {
    if (byte === 0x22) {
      result += '\\"';
    } else if (byte === 0x5c) {
      result += '\\\\';
    } else if (byte === 0x0a) {
      result += '\\n';
    } else if (byte === 0x0d) {
      result += '\\r';
    } else if (byte === 0x00) {
      result += '\\0';
    } else if (byte < 32 || byte > 126) {
      result += '\\' + byte.toString();
    } else {
      result += String.fromCharCode(byte);
    }
  }
  result += '"';
  return result;
}
