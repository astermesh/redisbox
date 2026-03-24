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

const LIB_SIMPLE = `#!lua name=mylib
redis.register_function('myfunc', function(keys, args)
  return 'hello'
end)
`;

const LIB_WITH_FLAGS = `#!lua name=readlib
redis.register_function{
  function_name = 'myfunc_ro',
  callback = function(keys, args)
    return 'readonly'
  end,
  flags = {'no-writes'}
}
`;

const LIB_MULTI = `#!lua name=multilib
redis.register_function('func1', function(keys, args)
  return 'one'
end)

redis.register_function('func2', function(keys, args)
  return 'two'
end)
`;

const LIB_KEYS_ARGS = `#!lua name=kalib
redis.register_function('getset', function(keys, args)
  redis.call('SET', keys[1], args[1])
  return redis.call('GET', keys[1])
end)
`;

const LIB_READ_ONLY = `#!lua name=rolib
redis.register_function{
  function_name = 'getter',
  callback = function(keys, args)
    return redis.call('GET', keys[1])
  end,
  flags = {'no-writes'}
}
redis.register_function('setter', function(keys, args)
  return redis.call('SET', keys[1], args[1])
end)
`;

describe('FUNCTION LOAD', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('loads a simple library', () => {
    const result = dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    expect(result).toEqual(bulkReply('mylib'));
  });

  it('loads library with extended register_function syntax', () => {
    const result = dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);
    expect(result).toEqual(bulkReply('readlib'));
  });

  it('loads library with multiple functions', () => {
    const result = dispatch(['FUNCTION', 'LOAD', LIB_MULTI]);
    expect(result).toEqual(bulkReply('multilib'));
  });

  it('rejects loading library with same name twice', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    expect(result).toEqual(errorReply('ERR', "Library 'mylib' already exists"));
  });

  it('replaces library with REPLACE option', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FUNCTION', 'LOAD', 'REPLACE', LIB_SIMPLE]);
    expect(result).toEqual(bulkReply('mylib'));
  });

  it('rejects code without shebang', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      'redis.register_function("f", function() end)',
    ]);
    expect(result).toEqual(errorReply('ERR', 'Missing library metadata'));
  });

  it('rejects shebang with missing name', () => {
    const result = dispatch(['FUNCTION', 'LOAD', '#!lua\nreturn 1']);
    expect(result).toEqual(errorReply('ERR', 'Library name was not given'));
  });

  it('rejects shebang with non-lua engine', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      '#!python name=mylib\nreturn 1',
    ]);
    expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
  });

  it('rejects library with no functions registered', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      '#!lua name=emptylib\nlocal x = 1',
    ]);
    expect(result).toEqual(errorReply('ERR', 'No functions registered'));
  });

  it('rejects function name that already exists in another library', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const code = `#!lua name=otherlib
redis.register_function('myfunc', function(keys, args)
  return 'other'
end)
`;
    const result = dispatch(['FUNCTION', 'LOAD', code]);
    expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
  });

  it('rejects wrong number of arguments', () => {
    const result = dispatch(['FUNCTION', 'LOAD']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'function|load' command")
    );
  });

  it('rejects too many arguments', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      'REPLACE',
      LIB_SIMPLE,
      'extra',
    ]);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'function|load' command")
    );
  });

  it('rejects Lua syntax errors in library code', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      '#!lua name=badlib\ninvalid lua !!!',
    ]);
    expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
  });

  it('rejects invalid library name characters', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      '#!lua name=my-lib\nredis.register_function("f", function() end)',
    ]);
    expect(result).toEqual(
      errorReply(
        'ERR',
        'Library names can only contain letters, numbers, or underscores(_) and must be at least one character long'
      )
    );
  });

  it('accepts library name with underscores', () => {
    const lib = `#!lua name=my_lib
redis.register_function('my_func', function(keys, args)
  return 'ok'
end)
`;
    const result = dispatch(['FUNCTION', 'LOAD', lib]);
    expect(result).toEqual(bulkReply('my_lib'));
  });

  it('returns exact error for missing shebang', () => {
    const result = dispatch([
      'FUNCTION',
      'LOAD',
      'redis.register_function("f", function() end)',
    ]);
    expect(result).toEqual(errorReply('ERR', 'Missing library metadata'));
  });

  it('returns exact error for missing name', () => {
    const result = dispatch(['FUNCTION', 'LOAD', '#!lua\nreturn 1']);
    expect(result).toEqual(errorReply('ERR', 'Library name was not given'));
  });
});

describe('FCALL', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('calls a registered function', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FCALL', 'myfunc', '0']);
    expect(result).toEqual(bulkReply('hello'));
  });

  it('passes KEYS and ARGV to function', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_KEYS_ARGS]);
    const result = dispatch(['FCALL', 'getset', '1', 'mykey', 'myval']);
    expect(result).toEqual(bulkReply('myval'));
  });

  it('returns error for unknown function', () => {
    const result = dispatch(['FCALL', 'nonexistent', '0']);
    expect(result).toEqual(errorReply('ERR', 'Function not found'));
  });

  it('validates numkeys', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FCALL', 'myfunc', 'abc']);
    expect(result).toEqual(
      errorReply('ERR', 'value is not an integer or out of range')
    );
  });

  it('rejects negative numkeys', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FCALL', 'myfunc', '-1']);
    expect(result).toEqual(
      errorReply('ERR', "Number of keys can't be negative")
    );
  });

  it('calls different functions from same library', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_MULTI]);
    expect(dispatch(['FCALL', 'func1', '0'])).toEqual(bulkReply('one'));
    expect(dispatch(['FCALL', 'func2', '0'])).toEqual(bulkReply('two'));
  });

  it('can use redis.call inside function', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_KEYS_ARGS]);
    dispatch(['FCALL', 'getset', '1', 'k1', 'v1']);
    const result = dispatch(['GET', 'k1']);
    expect(result).toEqual(bulkReply('v1'));
  });

  it('provides correct KEYS count', () => {
    const lib = `#!lua name=countlib
redis.register_function('countkeys', function(keys, args)
  return #keys
end)
`;
    dispatch(['FUNCTION', 'LOAD', lib]);
    const result = dispatch(['FCALL', 'countkeys', '3', 'a', 'b', 'c']);
    expect(result).toEqual(integerReply(3));
  });

  it('provides correct ARGV values', () => {
    const lib = `#!lua name=arglib
redis.register_function('getarg', function(keys, args)
  return args[1]
end)
`;
    dispatch(['FUNCTION', 'LOAD', lib]);
    const result = dispatch(['FCALL', 'getarg', '1', 'mykey', 'myarg']);
    expect(result).toEqual(bulkReply('myarg'));
  });

  it('returns integer from function', () => {
    const lib = `#!lua name=intlib
redis.register_function('getnum', function(keys, args)
  return 42
end)
`;
    dispatch(['FUNCTION', 'LOAD', lib]);
    const result = dispatch(['FCALL', 'getnum', '0']);
    expect(result).toEqual(integerReply(42));
  });

  it('returns table from function', () => {
    const lib = `#!lua name=tablelib
redis.register_function('gettable', function(keys, args)
  return {1, 2, 3}
end)
`;
    dispatch(['FUNCTION', 'LOAD', lib]);
    const result = dispatch(['FCALL', 'gettable', '0']);
    expect(result).toEqual(
      arrayReply([integerReply(1), integerReply(2), integerReply(3)])
    );
  });

  it('works after function replacement', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const updated = `#!lua name=mylib
redis.register_function('myfunc', function(keys, args)
  return 'updated'
end)
`;
    dispatch(['FUNCTION', 'LOAD', 'REPLACE', updated]);
    const result = dispatch(['FCALL', 'myfunc', '0']);
    expect(result).toEqual(bulkReply('updated'));
  });
});

describe('FCALL_RO', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('calls a read-only function', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);
    const result = dispatch(['FCALL_RO', 'myfunc_ro', '0']);
    expect(result).toEqual(bulkReply('readonly'));
  });

  it('rejects function without no-writes flag', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FCALL_RO', 'myfunc', '0']);
    expect(result).toEqual(
      errorReply(
        'ERR',
        'Can not execute a script with write flag using *_ro command.'
      )
    );
  });

  it('allows read commands in no-writes function', () => {
    dispatch(['SET', 'mykey', 'myval']);
    dispatch(['FUNCTION', 'LOAD', LIB_READ_ONLY]);
    const result = dispatch(['FCALL_RO', 'getter', '1', 'mykey']);
    expect(result).toEqual(bulkReply('myval'));
  });

  it('rejects write commands even in no-writes function', () => {
    const lib = `#!lua name=sneakylib
redis.register_function{
  function_name = 'sneakwrite',
  callback = function(keys, args)
    return redis.call('SET', keys[1], args[1])
  end,
  flags = {'no-writes'}
}
`;
    dispatch(['FUNCTION', 'LOAD', lib]);
    const result = dispatch(['FCALL_RO', 'sneakwrite', '1', 'k1', 'v1']);
    expect(result).toEqual(expect.objectContaining({ kind: 'error' }));
  });

  it('returns error for unknown function', () => {
    const result = dispatch(['FCALL_RO', 'nonexistent', '0']);
    expect(result).toEqual(errorReply('ERR', 'Function not found'));
  });
});

describe('FUNCTION DELETE', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('deletes a loaded library', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FUNCTION', 'DELETE', 'mylib']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('functions become unavailable after delete', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'DELETE', 'mylib']);
    const result = dispatch(['FCALL', 'myfunc', '0']);
    expect(result).toEqual(errorReply('ERR', 'Function not found'));
  });

  it('returns error for non-existent library', () => {
    const result = dispatch(['FUNCTION', 'DELETE', 'nonexistent']);
    expect(result).toEqual(errorReply('ERR', 'Library not found'));
  });

  it('allows reloading library after delete', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'DELETE', 'mylib']);
    const result = dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    expect(result).toEqual(bulkReply('mylib'));
  });

  it('deletes all functions in library', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_MULTI]);
    dispatch(['FUNCTION', 'DELETE', 'multilib']);
    expect(dispatch(['FCALL', 'func1', '0'])).toEqual(
      errorReply('ERR', 'Function not found')
    );
    expect(dispatch(['FCALL', 'func2', '0'])).toEqual(
      errorReply('ERR', 'Function not found')
    );
  });

  it('rejects wrong number of arguments', () => {
    const result = dispatch(['FUNCTION', 'DELETE']);
    expect(result).toEqual(
      errorReply(
        'ERR',
        "wrong number of arguments for 'function|delete' command"
      )
    );
  });
});

describe('FUNCTION LIST', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns empty array when no libraries loaded', () => {
    const result = dispatch(['FUNCTION', 'LIST']);
    expect(result).toEqual(arrayReply([]));
  });

  it('lists loaded library', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FUNCTION', 'LIST']);
    expect(result.kind).toBe('array');
    if (result.kind !== 'array') return;
    expect(result.value.length).toBe(1);
    const lib = result.value[0];
    if (!lib || lib.kind !== 'array') return;
    // Check library_name
    expect(lib.value[0]).toEqual(bulkReply('library_name'));
    expect(lib.value[1]).toEqual(bulkReply('mylib'));
    // Check engine
    expect(lib.value[2]).toEqual(bulkReply('engine'));
    expect(lib.value[3]).toEqual(bulkReply('LUA'));
    // Check functions section exists
    expect(lib.value[4]).toEqual(bulkReply('functions'));
    // Check function description is nil when not provided
    const functions = lib.value[5];
    if (functions?.kind === 'array' && functions.value[0]?.kind === 'array') {
      const func = functions.value[0];
      const descIdx = func.value.findIndex(
        (v) => v.kind === 'bulk' && v.value === 'description'
      );
      expect(descIdx).toBeGreaterThanOrEqual(0);
      expect(func.value[descIdx + 1]).toEqual(bulkReply(null));
    }
  });

  it('lists functions with flags', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);
    const result = dispatch(['FUNCTION', 'LIST']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array' && result.value[0]?.kind === 'array') {
      const lib = result.value[0];
      // functions is at index 5
      const functions = lib.value[5];
      expect(functions).toBeDefined();
      if (functions?.kind === 'array' && functions.value[0]?.kind === 'array') {
        const func = functions.value[0];
        // Find flags
        const flagsIdx = func.value.findIndex(
          (v) => v.kind === 'bulk' && v.value === 'flags'
        );
        expect(flagsIdx).toBeGreaterThanOrEqual(0);
        const flags = func.value[flagsIdx + 1];
        expect(flags?.kind).toBe('array');
        if (flags?.kind === 'array') {
          expect(flags.value).toContainEqual(bulkReply('no-writes'));
        }
      }
    }
  });

  it('filters by LIBRARYNAME pattern', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);

    const result = dispatch(['FUNCTION', 'LIST', 'LIBRARYNAME', 'my*']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(1);
    }
  });

  it('returns all when pattern matches all', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);

    const result = dispatch(['FUNCTION', 'LIST', 'LIBRARYNAME', '*']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(2);
    }
  });

  it('includes library code with WITHCODE', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    const result = dispatch(['FUNCTION', 'LIST', 'WITHCODE']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array' && result.value[0]?.kind === 'array') {
      const lib = result.value[0];
      const codeIdx = lib.value.findIndex(
        (v) => v.kind === 'bulk' && v.value === 'library_code'
      );
      expect(codeIdx).toBeGreaterThanOrEqual(0);
      const code = lib.value[codeIdx + 1];
      expect(code?.kind).toBe('bulk');
      if (code?.kind === 'bulk') {
        expect(code.value).toBe(LIB_SIMPLE);
      }
    }
  });

  it('supports LIBRARYNAME and WITHCODE together', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);

    const result = dispatch([
      'FUNCTION',
      'LIST',
      'LIBRARYNAME',
      'my*',
      'WITHCODE',
    ]);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBe(1);
      if (result.value[0]?.kind === 'array') {
        const lib = result.value[0];
        const codeIdx = lib.value.findIndex(
          (v) => v.kind === 'bulk' && v.value === 'library_code'
        );
        expect(codeIdx).toBeGreaterThanOrEqual(0);
      }
    }
  });
});

describe('FUNCTION FLUSH', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('flushes all functions', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'LOAD', LIB_WITH_FLAGS]);
    const result = dispatch(['FUNCTION', 'FLUSH']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('functions become unavailable after flush', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'FLUSH']);
    const result = dispatch(['FCALL', 'myfunc', '0']);
    expect(result).toEqual(errorReply('ERR', 'Function not found'));
  });

  it('list returns empty after flush', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'FLUSH']);
    const result = dispatch(['FUNCTION', 'LIST']);
    expect(result).toEqual(arrayReply([]));
  });

  it('accepts ASYNC option', () => {
    const result = dispatch(['FUNCTION', 'FLUSH', 'ASYNC']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('accepts SYNC option', () => {
    const result = dispatch(['FUNCTION', 'FLUSH', 'SYNC']);
    expect(result).toEqual(statusReply('OK'));
  });

  it('rejects invalid option', () => {
    const result = dispatch(['FUNCTION', 'FLUSH', 'INVALID']);
    expect(result).toEqual(
      errorReply('ERR', 'FUNCTION FLUSH only supports ASYNC|SYNC option')
    );
  });

  it('allows reloading after flush', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'FLUSH']);
    const result = dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    expect(result).toEqual(bulkReply('mylib'));
  });
});

describe('FUNCTION DUMP', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns a bulk reply (stub)', () => {
    const result = dispatch(['FUNCTION', 'DUMP']);
    expect(result).toEqual(bulkReply(''));
  });
});

describe('FUNCTION RESTORE', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns OK (stub)', () => {
    const result = dispatch(['FUNCTION', 'RESTORE', 'dummydata']);
    expect(result).toEqual(statusReply('OK'));
  });
});

describe('FUNCTION STATS', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns stats with no libraries', () => {
    const result = dispatch(['FUNCTION', 'STATS']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      // running_script field — nil when idle (matches Redis)
      expect(result.value[0]).toEqual(bulkReply('running_script'));
      expect(result.value[1]).toEqual(bulkReply(null));
      // engines field
      expect(result.value[2]).toEqual(bulkReply('engines'));
    }
  });

  it('returns nil for running_script when idle', () => {
    const result = dispatch(['FUNCTION', 'STATS']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value[0]).toEqual(bulkReply('running_script'));
      expect(result.value[1]).toEqual(bulkReply(null));
    }
  });

  it('returns correct counts after loading libraries', () => {
    dispatch(['FUNCTION', 'LOAD', LIB_SIMPLE]);
    dispatch(['FUNCTION', 'LOAD', LIB_MULTI]);
    const result = dispatch(['FUNCTION', 'STATS']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      // engines section
      const engines = result.value[3];
      expect(engines?.kind).toBe('array');
      if (engines?.kind === 'array') {
        // LUA engine entry
        expect(engines.value[0]).toEqual(bulkReply('LUA'));
        const luaStats = engines.value[1];
        expect(luaStats?.kind).toBe('array');
        if (luaStats?.kind === 'array') {
          expect(luaStats.value[0]).toEqual(bulkReply('libraries_count'));
          expect(luaStats.value[1]).toEqual(integerReply(2));
          expect(luaStats.value[2]).toEqual(bulkReply('functions_count'));
          expect(luaStats.value[3]).toEqual(integerReply(3)); // 1 + 2
        }
      }
    }
  });
});

describe('FUNCTION HELP', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns array of help strings', () => {
    const result = dispatch(['FUNCTION', 'HELP']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array') {
      expect(result.value.length).toBeGreaterThan(0);
      expect(result.value[0]).toEqual(
        bulkReply(
          'FUNCTION <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'
        )
      );
    }
  });
});

describe('FUNCTION unknown subcommand', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('returns error for unknown subcommand', () => {
    const result = dispatch(['FUNCTION', 'INVALID']);
    expect(result).toEqual(
      errorReply(
        'ERR',
        "unknown subcommand or wrong number of arguments for 'function|invalid' command"
      )
    );
  });

  it('returns error for no subcommand', () => {
    const result = dispatch(['FUNCTION']);
    expect(result).toEqual(
      errorReply('ERR', "wrong number of arguments for 'function' command")
    );
  });
});

describe('function flags', () => {
  beforeEach(async () => {
    await setup();
  });

  afterEach(() => {
    scriptManager.close();
  });

  it('supports allow-oom flag', () => {
    const lib = `#!lua name=oomlib
redis.register_function{
  function_name = 'oomfunc',
  callback = function(keys, args) return 'ok' end,
  flags = {'allow-oom'}
}
`;
    const result = dispatch(['FUNCTION', 'LOAD', lib]);
    expect(result).toEqual(bulkReply('oomlib'));
  });

  it('supports allow-stale flag', () => {
    const lib = `#!lua name=stalelib
redis.register_function{
  function_name = 'stalefunc',
  callback = function(keys, args) return 'ok' end,
  flags = {'allow-stale'}
}
`;
    const result = dispatch(['FUNCTION', 'LOAD', lib]);
    expect(result).toEqual(bulkReply('stalelib'));
  });

  it('supports no-cluster flag', () => {
    const lib = `#!lua name=noclusterlib
redis.register_function{
  function_name = 'noclusterfunc',
  callback = function(keys, args) return 'ok' end,
  flags = {'no-cluster'}
}
`;
    const result = dispatch(['FUNCTION', 'LOAD', lib]);
    expect(result).toEqual(bulkReply('noclusterlib'));
  });

  it('supports multiple flags', () => {
    const lib = `#!lua name=multiflaglib
redis.register_function{
  function_name = 'multiflagfunc',
  callback = function(keys, args) return 'ok' end,
  flags = {'no-writes', 'allow-oom', 'allow-stale'}
}
`;
    dispatch(['FUNCTION', 'LOAD', lib]);
    // Verify through FUNCTION LIST
    const result = dispatch(['FUNCTION', 'LIST']);
    expect(result.kind).toBe('array');
    if (result.kind === 'array' && result.value[0]?.kind === 'array') {
      const lib = result.value[0];
      const functions = lib.value[5];
      if (functions?.kind === 'array' && functions.value[0]?.kind === 'array') {
        const func = functions.value[0];
        const flagsIdx = func.value.findIndex(
          (v) => v.kind === 'bulk' && v.value === 'flags'
        );
        const flags = func.value[flagsIdx + 1];
        if (flags?.kind === 'array') {
          expect(flags.value.length).toBe(3);
        }
      }
    }
  });
});
