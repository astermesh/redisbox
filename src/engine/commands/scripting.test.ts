import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  CommandDispatcher,
  createTransactionState,
} from '../command-dispatcher.ts';
import type { TransactionState } from '../command-dispatcher.ts';
import { createCommandTable } from '../command-registry.ts';
import { CommandTable } from '../command-table.ts';
import { RedisEngine } from '../engine.ts';
import type { CommandContext, Reply } from '../types.ts';
import {
  statusReply,
  errorReply,
  bulkReply,
  integerReply,
  arrayReply,
} from '../types.ts';
import { ScriptManager } from '../scripting/script-manager.ts';
import { sha1 } from '../sha1.ts';

let scriptManager: ScriptManager;
let dispatcher: CommandDispatcher;
let state: TransactionState;
let ctx: CommandContext;
let table: CommandTable;

async function setup(): Promise<void> {
  table = createCommandTable();
  dispatcher = new CommandDispatcher(table);
  state = createTransactionState();
  const engine = new RedisEngine({ clock: () => 1000, rng: () => 0.5 });
  scriptManager = new ScriptManager();

  ctx = {
    db: engine.db(0),
    engine,
    commandTable: table,
    scriptManager,
  };

  // Create a base executor for the script manager that dispatches via the command table
  const baseExecutor = (args: string[]): Reply => {
    if (!ctx.commandTable || args.length === 0) {
      return errorReply('ERR', 'unknown command');
    }
    const cmdName = args[0] ?? '';
    const cmdArgs = args.slice(1);
    const def = ctx.commandTable.get(cmdName);
    if (!def) {
      return errorReply('ERR', `unknown command '${cmdName}'`);
    }
    return def.handler(ctx, cmdArgs);
  };

  await scriptManager.init(baseExecutor);
}

function dispatch(args: string[]): Reply {
  return dispatcher.dispatch(state, ctx, args);
}

describe('EVAL', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  describe('basic execution', () => {
    it('returns integer from script', () => {
      const result = dispatch(['EVAL', 'return 1', '0']);
      expect(result).toEqual(integerReply(1));
    });

    it('returns string from script', () => {
      const result = dispatch(['EVAL', 'return "hello"', '0']);
      expect(result).toEqual(bulkReply('hello'));
    });

    it('returns nil from script with no return', () => {
      const result = dispatch(['EVAL', 'local x = 1', '0']);
      expect(result).toEqual(bulkReply(null));
    });

    it('returns boolean true as integer 1', () => {
      const result = dispatch(['EVAL', 'return true', '0']);
      expect(result).toEqual(integerReply(1));
    });

    it('returns boolean false as nil', () => {
      const result = dispatch(['EVAL', 'return false', '0']);
      expect(result).toEqual(bulkReply(null));
    });

    it('returns table as array', () => {
      const result = dispatch(['EVAL', 'return {1, 2, 3}', '0']);
      expect(result).toEqual(
        arrayReply([integerReply(1), integerReply(2), integerReply(3)])
      );
    });

    it('returns status reply from table with ok field', () => {
      const result = dispatch(['EVAL', 'return {ok = "PONG"}', '0']);
      expect(result).toEqual(statusReply('PONG'));
    });

    it('returns error reply from table with err field', () => {
      const result = dispatch([
        'EVAL',
        'return {err = "ERR custom error"}',
        '0',
      ]);
      expect(result).toEqual(errorReply('ERR', 'custom error'));
    });

    it('truncates float to integer', () => {
      const result = dispatch(['EVAL', 'return 3.7', '0']);
      expect(result).toEqual(integerReply(3));
    });

    it('truncates negative float toward zero', () => {
      const result = dispatch(['EVAL', 'return -3.7', '0']);
      expect(result).toEqual(integerReply(-3));
    });

    it('handles script ending with line comment', () => {
      const result = dispatch(['EVAL', 'return 1 -- trailing comment', '0']);
      expect(result).toEqual(integerReply(1));
    });
  });

  describe('KEYS and ARGV', () => {
    it('provides KEYS table from arguments', () => {
      const result = dispatch(['EVAL', 'return KEYS[1]', '1', 'mykey']);
      expect(result).toEqual(bulkReply('mykey'));
    });

    it('provides ARGV table from arguments', () => {
      const result = dispatch([
        'EVAL',
        'return ARGV[1]',
        '1',
        'mykey',
        'myarg',
      ]);
      expect(result).toEqual(bulkReply('myarg'));
    });

    it('provides multiple KEYS', () => {
      const result = dispatch([
        'EVAL',
        'return {KEYS[1], KEYS[2]}',
        '2',
        'k1',
        'k2',
        'a1',
      ]);
      expect(result).toEqual(arrayReply([bulkReply('k1'), bulkReply('k2')]));
    });

    it('provides multiple ARGV', () => {
      const result = dispatch([
        'EVAL',
        'return {ARGV[1], ARGV[2]}',
        '1',
        'k1',
        'a1',
        'a2',
      ]);
      expect(result).toEqual(arrayReply([bulkReply('a1'), bulkReply('a2')]));
    });

    it('works with empty KEYS and ARGV', () => {
      const result = dispatch(['EVAL', 'return #KEYS', '0']);
      expect(result).toEqual(integerReply(0));
    });

    it('KEYS and ARGV have correct lengths', () => {
      const result = dispatch([
        'EVAL',
        'return {#KEYS, #ARGV}',
        '2',
        'k1',
        'k2',
        'a1',
        'a2',
        'a3',
      ]);
      expect(result).toEqual(arrayReply([integerReply(2), integerReply(3)]));
    });
  });

  describe('redis.call integration', () => {
    it('executes SET and GET via redis.call', () => {
      const result = dispatch([
        'EVAL',
        'redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])',
        '1',
        'mykey',
        'myval',
      ]);
      expect(result).toEqual(bulkReply('myval'));
    });

    it('propagates errors from redis.call', () => {
      const result = dispatch(['EVAL', 'return redis.call("GET")', '0']);
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });

    it('redis.pcall returns error as table', () => {
      const result = dispatch([
        'EVAL',
        'local ok, err = pcall(function() return redis.call("NOTACMD") end); return "caught"',
        '0',
      ]);
      expect(result).toEqual(bulkReply('caught'));
    });
  });

  describe('script caching', () => {
    it('caches script after EVAL', () => {
      const script = 'return 42';
      dispatch(['EVAL', script, '0']);
      expect(scriptManager.hasScript(sha1(script))).toBe(true);
    });

    it('EVALSHA works after EVAL caches the script', () => {
      const script = 'return 42';
      dispatch(['EVAL', script, '0']);
      const digest = sha1(script);
      const result = dispatch(['EVALSHA', digest, '0']);
      expect(result).toEqual(integerReply(42));
    });
  });

  describe('argument validation', () => {
    it('rejects non-integer numkeys', () => {
      const result = dispatch(['EVAL', 'return 1', 'abc']);
      expect(result).toEqual(
        errorReply('ERR', 'value is not an integer or out of range')
      );
    });

    it('rejects negative numkeys', () => {
      const result = dispatch(['EVAL', 'return 1', '-1']);
      expect(result).toEqual(
        errorReply('ERR', "Number of keys can't be negative")
      );
    });

    it('rejects numkeys greater than remaining args', () => {
      const result = dispatch(['EVAL', 'return 1', '3', 'k1']);
      expect(result).toEqual(
        errorReply('ERR', "Number of keys can't be greater than number of args")
      );
    });

    it('rejects float numkeys', () => {
      const result = dispatch(['EVAL', 'return 1', '1.5']);
      expect(result).toEqual(
        errorReply('ERR', 'value is not an integer or out of range')
      );
    });
  });

  describe('error handling', () => {
    it('returns error for Lua syntax errors', () => {
      const result = dispatch(['EVAL', 'invalid lua code !!!', '0']);
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });

    it('returns error for Lua runtime errors', () => {
      const result = dispatch(['EVAL', 'error("custom error")', '0']);
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });

    it('returns error for calling nonexistent Redis command', () => {
      const result = dispatch([
        'EVAL',
        'return redis.call("NONEXISTENT")',
        '0',
      ]);
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });
  });

  describe('noscript enforcement', () => {
    it('rejects EVAL from inside scripts (noscript flag)', () => {
      const result = dispatch([
        'EVAL',
        'return redis.call("EVAL", "return 1", "0")',
        '0',
      ]);
      expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
    });
  });
});

describe('EVALSHA', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns NOSCRIPT error for unknown SHA', () => {
    const result = dispatch([
      'EVALSHA',
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      '0',
    ]);
    expect(result).toEqual(
      errorReply('NOSCRIPT', 'No matching script. Use EVAL.')
    );
  });

  it('executes previously cached script', () => {
    const script = 'return "from cache"';
    const digest = scriptManager.cacheScript(script);
    const result = dispatch(['EVALSHA', digest, '0']);
    expect(result).toEqual(bulkReply('from cache'));
  });

  it('passes KEYS and ARGV to cached script', () => {
    const script = 'return KEYS[1] .. ":" .. ARGV[1]';
    const digest = scriptManager.cacheScript(script);
    const result = dispatch(['EVALSHA', digest, '1', 'k1', 'a1']);
    expect(result).toEqual(bulkReply('k1:a1'));
  });

  it('is case-insensitive for SHA', () => {
    const script = 'return 99';
    const digest = scriptManager.cacheScript(script);
    const result = dispatch(['EVALSHA', digest.toUpperCase(), '0']);
    expect(result).toEqual(integerReply(99));
  });

  it('rejects non-integer numkeys', () => {
    const script = 'return 1';
    const digest = scriptManager.cacheScript(script);
    const result = dispatch(['EVALSHA', digest, 'abc']);
    expect(result).toEqual(
      errorReply('ERR', 'value is not an integer or out of range')
    );
  });
});

describe('EVAL_RO', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('allows read-only commands', () => {
    // First set a key directly
    dispatch(['SET', 'mykey', 'myval']);
    const result = dispatch([
      'EVAL_RO',
      'return redis.call("GET", "mykey")',
      '0',
    ]);
    expect(result).toEqual(bulkReply('myval'));
  });

  it('rejects write commands', () => {
    const result = dispatch([
      'EVAL_RO',
      'return redis.call("SET", "key", "val")',
      '0',
    ]);
    expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
  });

  it('returns value for pure computation', () => {
    const result = dispatch(['EVAL_RO', 'return 1 + 2', '0']);
    expect(result).toEqual(integerReply(3));
  });

  it('provides KEYS and ARGV', () => {
    const result = dispatch(['EVAL_RO', 'return KEYS[1]', '1', 'k1']);
    expect(result).toEqual(bulkReply('k1'));
  });
});

describe('EVALSHA_RO', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns NOSCRIPT for unknown SHA', () => {
    const result = dispatch([
      'EVALSHA_RO',
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      '0',
    ]);
    expect(result).toEqual(
      errorReply('NOSCRIPT', 'No matching script. Use EVAL.')
    );
  });

  it('executes cached script in read-only mode', () => {
    dispatch(['SET', 'mykey', 'myval']);
    const script = 'return redis.call("GET", "mykey")';
    const digest = scriptManager.cacheScript(script);
    const result = dispatch(['EVALSHA_RO', digest, '0']);
    expect(result).toEqual(bulkReply('myval'));
  });

  it('rejects write commands in cached script', () => {
    const script = 'return redis.call("SET", "key", "val")';
    const digest = scriptManager.cacheScript(script);
    const result = dispatch(['EVALSHA_RO', digest, '0']);
    expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
  });
});
