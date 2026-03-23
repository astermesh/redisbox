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

describe('SCRIPT LOAD', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('caches script and returns SHA1', () => {
    const script = 'return 1';
    const result = dispatch(['SCRIPT', 'LOAD', script]);
    expect(result).toEqual(bulkReply(sha1(script)));
  });

  it('returns same SHA1 for same script', () => {
    const script = 'return "hello"';
    const r1 = dispatch(['SCRIPT', 'LOAD', script]);
    const r2 = dispatch(['SCRIPT', 'LOAD', script]);
    expect(r1).toEqual(r2);
  });

  it('allows EVALSHA after SCRIPT LOAD', () => {
    const script = 'return 42';
    const loadResult = dispatch(['SCRIPT', 'LOAD', script]);
    expect(loadResult).toEqual(bulkReply(sha1(script)));
    const result = dispatch(['EVALSHA', sha1(script), '0']);
    expect(result).toEqual(integerReply(42));
  });

  it('rejects wrong number of arguments (no script)', () => {
    const result = dispatch(['SCRIPT', 'LOAD']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script|load' command")
    );
  });

  it('rejects extra arguments', () => {
    const result = dispatch(['SCRIPT', 'LOAD', 'return 1', 'extra']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script|load' command")
    );
  });
});

describe('SCRIPT EXISTS', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns 0 for unknown SHA', () => {
    const result = dispatch([
      'SCRIPT',
      'EXISTS',
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    ]);
    expect(result).toEqual(arrayReply([integerReply(0)]));
  });

  it('returns 1 for cached script', () => {
    const script = 'return 1';
    const digest = sha1(script);
    dispatch(['SCRIPT', 'LOAD', script]);
    const result = dispatch(['SCRIPT', 'EXISTS', digest]);
    expect(result).toEqual(arrayReply([integerReply(1)]));
  });

  it('checks multiple SHAs at once', () => {
    const script1 = 'return 1';
    const script2 = 'return 2';
    dispatch(['SCRIPT', 'LOAD', script1]);
    const result = dispatch([
      'SCRIPT',
      'EXISTS',
      sha1(script1),
      sha1(script2),
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    ]);
    expect(result).toEqual(
      arrayReply([integerReply(1), integerReply(0), integerReply(0)])
    );
  });

  it('is case-insensitive for SHA', () => {
    const script = 'return 1';
    dispatch(['SCRIPT', 'LOAD', script]);
    const result = dispatch(['SCRIPT', 'EXISTS', sha1(script).toUpperCase()]);
    expect(result).toEqual(arrayReply([integerReply(1)]));
  });

  it('rejects with no arguments', () => {
    const result = dispatch(['SCRIPT', 'EXISTS']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script|exists' command")
    );
  });
});

describe('SCRIPT FLUSH', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('clears all cached scripts', () => {
    dispatch(['SCRIPT', 'LOAD', 'return 1']);
    dispatch(['SCRIPT', 'LOAD', 'return 2']);
    const result = dispatch(['SCRIPT', 'FLUSH']);
    expect(result).toEqual(statusReply('OK'));

    // Verify scripts are gone
    const exists = dispatch([
      'SCRIPT',
      'EXISTS',
      sha1('return 1'),
      sha1('return 2'),
    ]);
    expect(exists).toEqual(arrayReply([integerReply(0), integerReply(0)]));
  });

  it('accepts ASYNC option', () => {
    dispatch(['SCRIPT', 'LOAD', 'return 1']);
    const result = dispatch(['SCRIPT', 'FLUSH', 'ASYNC']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('accepts SYNC option', () => {
    dispatch(['SCRIPT', 'LOAD', 'return 1']);
    const result = dispatch(['SCRIPT', 'FLUSH', 'SYNC']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('rejects invalid option', () => {
    const result = dispatch(['SCRIPT', 'FLUSH', 'INVALID']);
    expect(result).toEqual(
      errorReply('ERR', 'SCRIPT FLUSH only supports ASYNC|SYNC option')
    );
  });

  it('rejects extra arguments', () => {
    const result = dispatch(['SCRIPT', 'FLUSH', 'ASYNC', 'extra']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script|flush' command")
    );
  });

  it('works with no cached scripts', () => {
    const result = dispatch(['SCRIPT', 'FLUSH']);
    expect(result).toEqual(statusReply('OK'));
  });
});

describe('SCRIPT DEBUG', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('accepts YES', () => {
    const result = dispatch(['SCRIPT', 'DEBUG', 'YES']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('accepts SYNC', () => {
    const result = dispatch(['SCRIPT', 'DEBUG', 'SYNC']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('accepts NO', () => {
    const result = dispatch(['SCRIPT', 'DEBUG', 'NO']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('rejects invalid mode', () => {
    const result = dispatch(['SCRIPT', 'DEBUG', 'INVALID']);
    expect(result).toEqual(errorReply('ERR', 'Use SCRIPT DEBUG YES/SYNC/NO'));
  });

  it('rejects missing argument', () => {
    const result = dispatch(['SCRIPT', 'DEBUG']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script|debug' command")
    );
  });

  it('rejects extra arguments', () => {
    const result = dispatch(['SCRIPT', 'DEBUG', 'YES', 'extra']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script|debug' command")
    );
  });
});

describe('SCRIPT unknown subcommand', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns error for unknown subcommand', () => {
    const result = dispatch(['SCRIPT', 'INVALID']);
    expect(result).toEqual(
      errorReply(
        'ERR',
        "unknown subcommand or wrong number of arguments for 'script|invalid' command"
      )
    );
  });

  it('returns error for no subcommand', () => {
    const result = dispatch(['SCRIPT']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'script' command")
    );
  });
});
