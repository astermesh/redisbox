import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { ScriptManager } from './script-manager.ts';
import { sha1 } from '../sha1.ts';
import { statusReply, errorReply, bulkReply, integerReply } from '../types.ts';
import type { Reply } from '../types.ts';
import type { CommandExecutor } from './redis-bridge.ts';
import { createCommandTable } from '../command-registry.ts';

let manager: ScriptManager;
const store = new Map<string, string>();

function makeExecutor(): CommandExecutor {
  return (args: string[]): Reply => {
    const cmd = (args[0] ?? '').toUpperCase();
    if (cmd === 'SET') {
      store.set(args[1] ?? '', args[2] ?? '');
      return statusReply('OK');
    }
    if (cmd === 'GET') {
      const val = store.get(args[1] ?? '');
      return bulkReply(val ?? null);
    }
    return errorReply('ERR', `unknown command '${args[0]}'`);
  };
}

describe('ScriptManager', () => {
  beforeEach(async () => {
    store.clear();
    manager = new ScriptManager();
    await manager.init(makeExecutor());
  });

  afterEach(() => {
    manager.close();
  });

  describe('lifecycle', () => {
    it('reports ready after init', () => {
      expect(manager.ready).toBe(true);
    });

    it('reports not ready after close', () => {
      manager.close();
      expect(manager.ready).toBe(false);
    });

    it('allows re-init after close', async () => {
      manager.close();
      await manager.init(makeExecutor());
      expect(manager.ready).toBe(true);
    });

    it('init is idempotent', async () => {
      await manager.init(makeExecutor());
      expect(manager.ready).toBe(true);
    });
  });

  describe('script cache', () => {
    it('caches script and returns SHA-1', () => {
      const digest = manager.cacheScript('return 1');
      expect(digest).toBe(sha1('return 1'));
    });

    it('hasScript returns true for cached', () => {
      const digest = manager.cacheScript('return 1');
      expect(manager.hasScript(digest)).toBe(true);
    });

    it('hasScript returns false for unknown', () => {
      expect(
        manager.hasScript('0000000000000000000000000000000000000000')
      ).toBe(false);
    });

    it('getScript returns cached script body', () => {
      const digest = manager.cacheScript('return 1');
      expect(manager.getScript(digest)).toBe('return 1');
    });

    it('flushScripts clears all cached scripts', () => {
      const d1 = manager.cacheScript('return 1');
      const d2 = manager.cacheScript('return 2');
      manager.flushScripts();
      expect(manager.hasScript(d1)).toBe(false);
      expect(manager.hasScript(d2)).toBe(false);
    });

    it('hasScript is case-insensitive', () => {
      const digest = manager.cacheScript('return 1');
      expect(manager.hasScript(digest.toUpperCase())).toBe(true);
    });
  });

  describe('validateScript', () => {
    it('returns null for valid script', () => {
      expect(manager.validateScript('return 1')).toBeNull();
    });

    it('returns null for empty script', () => {
      expect(manager.validateScript('')).toBeNull();
    });

    it('returns error string for syntax error', () => {
      const err = manager.validateScript('invalid lua !!!');
      expect(err).not.toBeNull();
      expect(typeof err).toBe('string');
    });

    it('returns error for incomplete script', () => {
      const err = manager.validateScript('function foo()');
      expect(err).not.toBeNull();
    });

    it('does not execute the script (no side effects)', () => {
      // If the script were executed, it would set a global
      manager.validateScript('MY_VALIDATE_TEST = true');
      // Verify no global was set
      const result = manager.evalScript(
        'return MY_VALIDATE_TEST',
        [],
        [],
        false,
        undefined,
        makeExecutor()
      );
      // Should be nil (not set), which maps to bulkReply(null)
      expect(result).toEqual(bulkReply(null));
    });
  });

  describe('evalScript', () => {
    it('executes a simple script', () => {
      const result = manager.evalScript(
        'return 42',
        [],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(integerReply(42));
    });

    it('sets KEYS table', () => {
      const result = manager.evalScript(
        'return KEYS[1]',
        ['mykey'],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('mykey'));
    });

    it('sets ARGV table', () => {
      const result = manager.evalScript(
        'return ARGV[1]',
        [],
        ['myarg'],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('myarg'));
    });

    it('calls redis.call via executor', () => {
      const result = manager.evalScript(
        'redis.call("SET", "k", "v"); return redis.call("GET", "k")',
        [],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('v'));
    });

    it('returns error for syntax error', () => {
      const result = manager.evalScript(
        'invalid!!!',
        [],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });

    it('returns error for runtime error', () => {
      const result = manager.evalScript(
        'error("boom")',
        [],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });

    it('caches script after eval', () => {
      const script = 'return 99';
      manager.evalScript(script, [], [], false, undefined, makeExecutor());
      expect(manager.hasScript(sha1(script))).toBe(true);
    });

    it('handles special characters in KEYS', () => {
      const result = manager.evalScript(
        'return KEYS[1]',
        ['key with "quotes" and [brackets]'],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('key with "quotes" and [brackets]'));
    });

    it('handles script ending with line comment', () => {
      const result = manager.evalScript(
        'return 42 -- this is a comment',
        [],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(integerReply(42));
    });

    it('handles multi-byte UTF-8 characters in KEYS', () => {
      const result = manager.evalScript(
        'return KEYS[1]',
        ['\u4e16\u754c'],
        [],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('\u4e16\u754c'));
    });

    it('handles emoji in ARGV', () => {
      const result = manager.evalScript(
        'return ARGV[1]',
        [],
        ['\ud83d\ude00'],
        false,
        undefined,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('\ud83d\ude00'));
    });
  });

  describe('read-only mode', () => {
    it('allows read commands in read-only mode', () => {
      store.set('k', 'v');
      const table = createCommandTable();
      const result = manager.evalScript(
        'return redis.call("GET", "k")',
        [],
        [],
        true,
        table,
        makeExecutor()
      );
      expect(result).toEqual(bulkReply('v'));
    });

    it('rejects write commands in read-only mode', () => {
      const table = createCommandTable();
      const result = manager.evalScript(
        'return redis.call("SET", "k", "v")',
        [],
        [],
        true,
        table,
        makeExecutor()
      );
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });
  });
});
