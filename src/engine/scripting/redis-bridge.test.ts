import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WasmoonEngine } from './wasmoon-engine.ts';
import {
  replyToLua,
  luaToReply,
  registerRedisBridge,
  type CommandExecutor,
} from './redis-bridge.ts';
import type { Reply } from '../types.ts';
import {
  statusReply,
  integerReply,
  bulkReply,
  arrayReply,
  errorReply,
  NIL_ARRAY,
} from '../types.ts';

// --- replyToLua ---

describe('replyToLua', () => {
  it('converts status reply to table with ok field', () => {
    expect(replyToLua(statusReply('OK'))).toEqual({ ok: 'OK' });
  });

  it('converts status reply PONG', () => {
    expect(replyToLua(statusReply('PONG'))).toEqual({ ok: 'PONG' });
  });

  it('converts error reply to table with err field', () => {
    expect(replyToLua(errorReply('ERR', 'something went wrong'))).toEqual({
      err: 'ERR something went wrong',
    });
  });

  it('converts WRONGTYPE error reply', () => {
    expect(
      replyToLua(
        errorReply(
          'WRONGTYPE',
          'Operation against a key holding the wrong kind of value'
        )
      )
    ).toEqual({
      err: 'WRONGTYPE Operation against a key holding the wrong kind of value',
    });
  });

  it('converts integer reply to number', () => {
    expect(replyToLua(integerReply(42))).toBe(42);
  });

  it('converts zero integer reply', () => {
    expect(replyToLua(integerReply(0))).toBe(0);
  });

  it('converts negative integer reply', () => {
    expect(replyToLua(integerReply(-1))).toBe(-1);
  });

  it('converts bigint integer reply to number', () => {
    expect(replyToLua(integerReply(BigInt(100)))).toBe(100);
  });

  it('converts bulk string reply to string', () => {
    expect(replyToLua(bulkReply('hello'))).toBe('hello');
  });

  it('converts empty bulk string reply', () => {
    expect(replyToLua(bulkReply(''))).toBe('');
  });

  it('converts nil bulk reply to false', () => {
    expect(replyToLua(bulkReply(null))).toBe(false);
  });

  it('converts array reply to array', () => {
    const reply = arrayReply([bulkReply('a'), bulkReply('b'), bulkReply('c')]);
    expect(replyToLua(reply)).toEqual(['a', 'b', 'c']);
  });

  it('converts empty array reply', () => {
    expect(replyToLua(arrayReply([]))).toEqual([]);
  });

  it('converts nested array reply', () => {
    const reply = arrayReply([
      arrayReply([integerReply(1), integerReply(2)]),
      bulkReply('hello'),
    ]);
    expect(replyToLua(reply)).toEqual([[1, 2], 'hello']);
  });

  it('converts mixed array reply', () => {
    const reply = arrayReply([
      integerReply(1),
      bulkReply('two'),
      bulkReply(null),
      statusReply('OK'),
    ]);
    expect(replyToLua(reply)).toEqual([1, 'two', false, { ok: 'OK' }]);
  });

  it('converts nil-array reply to false', () => {
    expect(replyToLua(NIL_ARRAY)).toBe(false);
  });

  it('converts multi reply to array', () => {
    const reply: Reply = {
      kind: 'multi',
      value: [integerReply(1), bulkReply('x')],
    };
    expect(replyToLua(reply)).toEqual([1, 'x']);
  });

  it('truncates array at first nil (Redis Lua convention)', () => {
    // In Redis, Lua tables converted from arrays truncate at the first nil
    // redis.call returns arrays where nil elements stop the array
    const reply = arrayReply([bulkReply('a'), bulkReply(null), bulkReply('c')]);
    const result = replyToLua(reply);
    // The array contains false for nil, matching Redis behavior
    // Lua tables with false don't truncate - only nil does
    expect(result).toEqual(['a', false, 'c']);
  });
});

// --- luaToReply ---

describe('luaToReply', () => {
  it('converts number to integer reply (truncated)', () => {
    expect(luaToReply(42)).toEqual(integerReply(42));
  });

  it('converts float to integer reply (truncated toward zero)', () => {
    expect(luaToReply(3.7)).toEqual(integerReply(3));
  });

  it('converts negative float to integer reply (truncated toward zero)', () => {
    expect(luaToReply(-3.7)).toEqual(integerReply(-3));
  });

  it('converts zero', () => {
    expect(luaToReply(0)).toEqual(integerReply(0));
  });

  it('converts string to bulk string reply', () => {
    expect(luaToReply('hello')).toEqual(bulkReply('hello'));
  });

  it('converts empty string to bulk string reply', () => {
    expect(luaToReply('')).toEqual(bulkReply(''));
  });

  it('converts boolean true to integer 1', () => {
    expect(luaToReply(true)).toEqual(integerReply(1));
  });

  it('converts boolean false to nil bulk reply', () => {
    expect(luaToReply(false)).toEqual(bulkReply(null));
  });

  it('converts null to nil bulk reply', () => {
    expect(luaToReply(null)).toEqual(bulkReply(null));
  });

  it('converts undefined to nil bulk reply', () => {
    expect(luaToReply(undefined)).toEqual(bulkReply(null));
  });

  it('converts table with ok field to status reply', () => {
    expect(luaToReply({ ok: 'OK' })).toEqual(statusReply('OK'));
  });

  it('converts table with err field to error reply', () => {
    expect(luaToReply({ err: 'ERR something wrong' })).toEqual(
      errorReply('ERR', 'something wrong')
    );
  });

  it('converts table with err field - WRONGTYPE prefix', () => {
    expect(luaToReply({ err: 'WRONGTYPE bad type' })).toEqual(
      errorReply('WRONGTYPE', 'bad type')
    );
  });

  it('converts table with err field - no space (prefix only)', () => {
    expect(luaToReply({ err: 'ERR' })).toEqual(errorReply('ERR', ''));
  });

  it('converts array-like table to array reply', () => {
    expect(luaToReply([1, 2, 3])).toEqual(
      arrayReply([integerReply(1), integerReply(2), integerReply(3)])
    );
  });

  it('converts nested array table', () => {
    expect(luaToReply([[1, 2], 'x'])).toEqual(
      arrayReply([
        arrayReply([integerReply(1), integerReply(2)]),
        bulkReply('x'),
      ])
    );
  });

  it('converts empty array table', () => {
    expect(luaToReply([])).toEqual(arrayReply([]));
  });
});

// --- registerRedisBridge + integration ---

describe('registerRedisBridge', () => {
  let engine: WasmoonEngine;

  beforeEach(async () => {
    engine = await WasmoonEngine.create();
  });

  afterEach(() => {
    if (!engine.closed) {
      engine.close();
    }
  });

  function mockExecutor(replies: Map<string, Reply>): CommandExecutor {
    return (args: string[]) => {
      const cmd = args[0]?.toUpperCase() ?? '';
      const reply = replies.get(cmd);
      if (reply) return reply;
      return errorReply(
        'ERR',
        `unknown command '${args[0]}', with args beginning with: ${args
          .slice(1)
          .map((a) => `'${a}'`)
          .join(' ')}`
      );
    };
  }

  // --- redis.call ---

  describe('redis.call', () => {
    it('executes a command and returns status reply as table', async () => {
      const executor = mockExecutor(new Map([['SET', statusReply('OK')]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(
        'local r = redis.call("SET", "k", "v"); return r.ok'
      );
      expect(result.values).toEqual(['OK']);
    });

    it('executes a command and returns integer reply', async () => {
      const executor = mockExecutor(new Map([['INCR', integerReply(5)]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.call("INCR", "k")');
      expect(result.values).toEqual([5]);
    });

    it('executes a command and returns bulk string reply', async () => {
      const executor = mockExecutor(new Map([['GET', bulkReply('hello')]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.call("GET", "k")');
      expect(result.values).toEqual(['hello']);
    });

    it('returns false for nil bulk reply', async () => {
      const executor = mockExecutor(new Map([['GET', bulkReply(null)]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.call("GET", "k")');
      expect(result.values).toEqual([false]);
    });

    it('returns array for array reply', async () => {
      const executor = mockExecutor(
        new Map([
          [
            'KEYS',
            arrayReply([bulkReply('a'), bulkReply('b'), bulkReply('c')]),
          ],
        ])
      );
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.call("KEYS", "*")
        return r[1] .. r[2] .. r[3]
      `);
      expect(result.values).toEqual(['abc']);
    });

    it('raises error on error reply', async () => {
      const executor = mockExecutor(new Map());
      await registerRedisBridge(engine, executor);

      await expect(
        engine.execute('return redis.call("BADCMD")')
      ).rejects.toThrow(/unknown command/);
    });

    it('error includes @user_script prefix', async () => {
      const executor = mockExecutor(new Map());
      await registerRedisBridge(engine, executor);

      await expect(
        engine.execute('return redis.call("BADCMD")')
      ).rejects.toThrow(/@user_script:-?\d+:/);
    });

    it('propagates WRONGTYPE error', async () => {
      const executor = mockExecutor(
        new Map([
          [
            'INCR',
            errorReply(
              'WRONGTYPE',
              'Operation against a key holding the wrong kind of value'
            ),
          ],
        ])
      );
      await registerRedisBridge(engine, executor);

      await expect(
        engine.execute('return redis.call("INCR", "k")')
      ).rejects.toThrow(/WRONGTYPE/);
    });

    it('passes all arguments to executor', async () => {
      const receivedArgs: string[][] = [];
      const executor: CommandExecutor = (args: string[]) => {
        receivedArgs.push(args);
        return statusReply('OK');
      };
      await registerRedisBridge(engine, executor);

      await engine.execute('redis.call("SET", "mykey", "myval")');
      expect(receivedArgs).toEqual([['SET', 'mykey', 'myval']]);
    });

    it('converts numeric arguments to strings', async () => {
      const receivedArgs: string[][] = [];
      const executor: CommandExecutor = (args: string[]) => {
        receivedArgs.push(args);
        return statusReply('OK');
      };
      await registerRedisBridge(engine, executor);

      await engine.execute('redis.call("SET", "k", 42)');
      expect(receivedArgs[0]).toEqual(['SET', 'k', '42']);
    });

    it('errors when called with no arguments', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      await expect(engine.execute('return redis.call()')).rejects.toThrow(
        /wrong number of arguments/i
      );
    });
  });

  // --- redis.pcall ---

  describe('redis.pcall', () => {
    it('returns result on success', async () => {
      const executor = mockExecutor(new Map([['GET', bulkReply('hello')]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.pcall("GET", "k")');
      expect(result.values).toEqual(['hello']);
    });

    it('catches error and returns table with err field', async () => {
      const executor = mockExecutor(new Map());
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.pcall("BADCMD")
        return r.err
      `);
      expect(result.values[0]).toMatch(/unknown command/);
    });

    it('catches WRONGTYPE error', async () => {
      const executor = mockExecutor(
        new Map([
          [
            'INCR',
            errorReply(
              'WRONGTYPE',
              'Operation against a key holding the wrong kind of value'
            ),
          ],
        ])
      );
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.pcall("INCR", "k")
        return r.err
      `);
      expect(result.values[0]).toMatch(/WRONGTYPE/);
    });

    it('returns status reply on success', async () => {
      const executor = mockExecutor(new Map([['SET', statusReply('OK')]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.pcall("SET", "k", "v")
        return r.ok
      `);
      expect(result.values).toEqual(['OK']);
    });

    it('errors when called with no arguments', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      // pcall still raises on argument validation errors (not command errors)
      await expect(engine.execute('return redis.pcall()')).rejects.toThrow(
        /wrong number of arguments/i
      );
    });
  });

  // --- redis.error_reply / redis.status_reply ---

  describe('redis.error_reply', () => {
    it('creates error reply table', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.error_reply("MY_ERR something bad")
        return r.err
      `);
      expect(result.values).toEqual(['MY_ERR something bad']);
    });
  });

  describe('redis.status_reply', () => {
    it('creates status reply table', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.status_reply("PONG")
        return r.ok
      `);
      expect(result.values).toEqual(['PONG']);
    });
  });

  // --- redis.log (stub) ---

  describe('redis.log', () => {
    it('exists and does not error', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      // redis.log should exist and not throw
      const result = await engine.execute(`
        redis.log(redis.LOG_WARNING, "test message")
        return true
      `);
      expect(result.values).toEqual([true]);
    });

    it('has log level constants', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        return redis.LOG_DEBUG + redis.LOG_VERBOSE + redis.LOG_NOTICE + redis.LOG_WARNING
      `);
      // LOG_DEBUG=0, LOG_VERBOSE=1, LOG_NOTICE=2, LOG_WARNING=3
      expect(result.values).toEqual([6]);
    });
  });

  // --- redis.sha1hex ---

  describe('redis.sha1hex', () => {
    it('returns SHA1 hex digest of a string', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.sha1hex("hello")');
      expect(result.values[0]).toBe('aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d');
    });

    it('returns SHA1 of empty string', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.sha1hex("")');
      expect(result.values[0]).toBe('da39a3ee5e6b4b0d3255bfef95601890afd80709');
    });

    it('handles numeric argument via tostring', async () => {
      const executor: CommandExecutor = () => statusReply('OK');
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.sha1hex(42)');
      // tostring(42) = "42"
      expect(result.values[0]).toHaveLength(40);
    });
  });

  // --- Round-trip type conversion ---

  describe('round-trip type conversion', () => {
    it('Lua number → executor string arg → integer reply → Lua number', async () => {
      const executor: CommandExecutor = (args: string[]) => {
        // Echo the second arg as integer
        return integerReply(parseInt(args[1] ?? '0', 10));
      };
      await registerRedisBridge(engine, executor);

      const result = await engine.execute('return redis.call("ECHO", 42)');
      expect(result.values).toEqual([42]);
    });

    it('Lua string → executor → bulk reply → Lua string', async () => {
      const executor: CommandExecutor = (args: string[]) => {
        return bulkReply(args[1] ?? null);
      };
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(
        'return redis.call("ECHO", "hello world")'
      );
      expect(result.values).toEqual(['hello world']);
    });
  });

  // --- redis.call error caught by Lua pcall ---

  describe('redis.call error caught by Lua pcall', () => {
    it('pcall(redis.call) catches error with @user_script prefix', async () => {
      const executor = mockExecutor(new Map());
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local ok, err = pcall(redis.call, "BADCMD")
        return tostring(err)
      `);
      expect(result.values[0]).toMatch(/@user_script:-?\d+:.*unknown command/);
    });

    it('redis.pcall returns error without @user_script prefix', async () => {
      const executor = mockExecutor(new Map());
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.pcall("BADCMD")
        return r.err
      `);
      // pcall error table should NOT have @user_script prefix
      expect(result.values[0]).not.toMatch(/@user_script/);
      expect(result.values[0]).toMatch(/unknown command/);
    });
  });

  // --- nested array handling ---

  describe('nested array handling', () => {
    it('handles nested arrays from executor', async () => {
      const executor = mockExecutor(
        new Map([
          [
            'TEST',
            arrayReply([
              arrayReply([bulkReply('a'), bulkReply('b')]),
              arrayReply([integerReply(1), integerReply(2)]),
            ]),
          ],
        ])
      );
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.call("TEST")
        return r[1][1] .. r[1][2] .. r[2][1] .. r[2][2]
      `);
      expect(result.values).toEqual(['ab12']);
    });

    it('handles nil elements in arrays (converted to false)', async () => {
      const executor = mockExecutor(
        new Map([
          [
            'TEST',
            arrayReply([bulkReply('a'), bulkReply(null), bulkReply('c')]),
          ],
        ])
      );
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.call("TEST")
        return tostring(r[1]) .. "," .. tostring(r[2]) .. "," .. tostring(r[3])
      `);
      expect(result.values).toEqual(['a,false,c']);
    });

    it('handles empty array reply', async () => {
      const executor = mockExecutor(new Map([['KEYS', arrayReply([])]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        local r = redis.call("KEYS", "*")
        return #r
      `);
      expect(result.values).toEqual([0]);
    });
  });

  // --- error reply within array ---

  describe('mixed reply types in arrays', () => {
    it('handles error elements in array replies', async () => {
      const executor = mockExecutor(
        new Map([
          [
            'EXEC',
            arrayReply([
              statusReply('OK'),
              errorReply('ERR', 'some error'),
              integerReply(42),
            ]),
          ],
        ])
      );
      await registerRedisBridge(engine, executor);

      // Note: redis.call doesn't raise on error elements within arrays,
      // only on top-level error replies
      const result = await engine.execute(`
        local r = redis.call("EXEC")
        return r[1].ok .. "," .. r[2].err .. "," .. r[3]
      `);
      expect(result.values).toEqual(['OK,ERR some error,42']);
    });
  });

  // --- nil-array reply ---

  describe('nil-array reply', () => {
    it('returns false for nil-array reply via redis.call', async () => {
      const executor = mockExecutor(new Map([['TEST', NIL_ARRAY]]));
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        return redis.call("TEST")
      `);
      expect(result.values).toEqual([false]);
    });
  });

  // --- multiple sequential calls ---

  describe('multiple sequential calls', () => {
    it('handles multiple redis.call invocations', async () => {
      let callCount = 0;
      const executor: CommandExecutor = (args: string[]) => {
        callCount++;
        if (args[0]?.toUpperCase() === 'SET') return statusReply('OK');
        if (args[0]?.toUpperCase() === 'GET')
          return bulkReply('val' + callCount);
        return errorReply('ERR', 'unknown');
      };
      await registerRedisBridge(engine, executor);

      const result = await engine.execute(`
        redis.call("SET", "k", "v")
        return redis.call("GET", "k")
      `);
      expect(result.values).toEqual(['val2']);
    });
  });
});
